#ifndef STORAGE_LSM_TREE_H
#define STORAGE_LSM_TREE_H

#include <vkdb/concepts.h>
#include <vkdb/sstable.h>
#include <vkdb/mem_table.h>
#include <vkdb/write_ahead_log.h>
#include <vkdb/lru_cache.h>
#include <vkdb/wal_lsm.h>
#include <ranges>
#include <deque>
#include <set>

namespace vkdb {
/**
 * @brief Type alias for a predicate on a TimeSeriesKey.
 * 
 */
using TimeSeriesKeyFilter = std::function<bool(const TimeSeriesKey&)>;

/**
 * @brief A predicate on a TimeSeriesKey that always returns true.
 * 
 */
static const TimeSeriesKeyFilter TRUE_TIME_SERIES_KEY_FILTER =
  [](const TimeSeriesKey&) { return true; };

/**
 * @brief LSM tree on TimeSeriesKey.
 * 
 * @tparam TValue Value type.
 */
template <ArithmeticNoCVRefQuals TValue>
class LSMTree {
public:
  using key_type = TimeSeriesKey;
  using mapped_type = std::optional<TValue>;
  using value_type = std::pair<const key_type, mapped_type>;
  using size_type = uint64_t;
  using table_type = typename MemTable<TValue>::table_type;

  static constexpr size_type LAYER_COUNT{8};

  /**
   * @brief Max number of entries in the cache.
   * 
   */
  static constexpr size_type CACHE_CAPACITY{10'000};

  /**
   * @brief Construct a new LSMTree object.
   * @details Loads SSTables from disk.
   * 
   * @param path Path of the LSM tree.
   */
  explicit LSMTree(FilePath path) noexcept
    : wal_{path}
    , path_{std::move(path)}
    , sstable_id_{0}
    , ck_layers_{LAYER_COUNT}
    , cache_{CACHE_CAPACITY} {
      std::filesystem::create_directories(path_);
      load_sstables();
    }

  /**
   * @brief Move-construct a LSMTree object.
   * 
   */
  LSMTree(LSMTree&&) noexcept = default;
  
  /**
   * @brief Move-assign a LSMTree object.
   * 
   */
  LSMTree& operator=(LSMTree&&) noexcept = default;  

  /**
   * @brief Deleted copy constructor.
   * 
   */
  LSMTree(const LSMTree&) = delete;
  
  /**
   * @brief Deleted copy assignment operator.
   * 
   */
  LSMTree& operator=(const LSMTree&) = delete;

  /**
   * @brief Destroy the LSMTree object.
   * 
   */
  ~LSMTree() noexcept = default;

  /**
   * @brief Put a key-value pair into the LSM tree.
   * 
   * @param key Key.
   * @param value Value.
   * @param log Whether to log the operation in the WAL.
   * 
   * @throw std::runtime_error If the memtable is flushed when the C1
   * layer is full.
   */
  void put(const key_type& key, const TValue& value, bool log = true) {
    mem_table_.put(key, value);
    dirty_table_[key] = true;
    if (mem_table_.size() == MemTable<TValue>::C0_LAYER_SSTABLE_MAX_ENTRIES) {
      flush();
    }
    if (log) {
      wal_.append({WALRecordType::PUT, {key, value}});
    }
  }

  /**
   * @brief Remove a key pair from the LSM tree.
   * 
   * @param key Key.
   * @param log Whether to log the operation in the WAL.
   * 
   * @throw std::runtime_error If the memtable is flushed when the C1
   * layer is full.
   */
  void remove(const key_type& key, bool log = true) {
    mem_table_.put(key, std::nullopt);
    dirty_table_[key] = true;
    if (mem_table_.size() == MemTable<TValue>::C0_LAYER_SSTABLE_MAX_ENTRIES) {
      flush();
    }
    if (log) {
      wal_.append({WALRecordType::REMOVE, {key, std::nullopt}});
    }
  }

  /**
   * @brief Get a value from the LSM tree.
   * 
   * @param key Key.
   * @return mapped_type The value if it exists, std::nullopt otherwise.
   * 
   * @throw std::exception If getting the value fails.
   */
  [[nodiscard]] mapped_type get(const key_type& key) const {
    if (!dirty_table_[key] && cache_.contains(key)) {
      return cache_.get(key);
    }

    if (mem_table_.contains(key)) {
      const auto value{mem_table_.get(key)};
      cache_.put(key, value);
      dirty_table_[key] = false;
      return value;
    }

    for (const auto& ck_layer : ck_layers_) {
      for (const auto& sstable : ck_layer | std::views::reverse) {
        if (sstable.contains(key)) {
          const auto value{sstable.get(key)};
          cache_.put(key, value);
          dirty_table_[key] = false;
          return value;
        }
      }
    }

    return std::nullopt;
  }

  /**
   * @brief Get a filtered set of entries in a timestamp range.
   * 
   * @param start Start timestamp.
   * @param end End timestamp.
   * @param filter Filter.
   * @return std::vector<value_type> Entries.
   * 
   * @throw std::exception If getting the entries fails.
   */
  [[nodiscard]] std::vector<value_type> getRange(
    const key_type& start,
    const key_type& end,
    TimeSeriesKeyFilter&& filter
  ) const {
    table_type entry_table;
    for (const auto& ck_layer : ck_layers_ | std::views::reverse) {
      for (const auto& sstable : ck_layer) {
        for (const auto& [key, value] : sstable.getRange(start, end)) {
          if (!filter(key)) {
            continue;
          }
          if (!value.has_value()) {
            entry_table.erase(key);
            continue;
          }
          entry_table[key] = value;
        }
      }
    }
    for (const auto& [key, value] : mem_table_.getRange(start, end)) {
      if (!filter(key)) {
        continue;
      }
      if (!value.has_value()) {
        entry_table.erase(key);
        continue;
      }
      entry_table[key] = value;
    }
    return {entry_table.begin(), entry_table.end()};
  }

  /**
   * @brief Replay the write-ahead log.
   * 
   */
  void replayWAL() {
    wal_.replay(*this);
  }

  /**
   * @brief Clear the LSM tree.
   * @details Remove all SSTable files and the WAL file.
   * 
   */
  void clear() noexcept {
    for (const auto& ck_layer : ck_layers_) {
      for (const auto& sstable : ck_layer) {
        std::filesystem::remove(sstable.path());
        std::filesystem::remove(sstable.metadataPath());
      }
    }
    std::filesystem::remove(wal_.path());
    mem_table_.clear();
    ck_layers_ = CkLayers{LAYER_COUNT};
    wal_.clear();
    sstable_id_ = std::array<size_type, LAYER_COUNT>{0};
    cache_.clear();
    dirty_table_.clear();
  }

  /**
   * @brief Convert the LSM tree to a string.
   * 
   * @return std::string The string representation of the LSM tree.
   */
  [[nodiscard]] std::string str() const noexcept {
    std::stringstream ss;
    ss << mem_table_.str();
    for (const auto& ck_layer : ck_layers_) {
      for (const auto& sstable : ck_layer) {
        ss << sstable.str();
      }
    }
    return ss.str();
  }

  /**
   * @brief Get the number of SSTables in the LSM tree.
   * 
   * @return size_type The number of SSTables.
   */
  [[nodiscard]] size_type sstableCount() const noexcept {
    size_type count{0};
    for (const auto& ck_layer : ck_layers_) {
      count += ck_layer.size();
    }
    return count;
  }

  /**
   * @brief Get the number of SSTables in a Ck layer.
   * 
   * @param k Ck layer index.
   * @return size_type The number of SSTables.
   * 
   * @throw std::out_of_range If the layer index is out of range.
   */
  [[nodiscard]] size_type sstableCount(size_type k) const {
    if (k < 0 || k >= LAYER_COUNT) {
      throw std::out_of_range{
        "LSMTree::sstableCount(): Layer index out of range."
      };
    }
    return ck_layers_[k].size();
  }

  /**
   * @brief Check if the LSM tree is empty.
   * 
   * @return true if the LSM tree is empty.
   * @return false if the LSM tree is not empty.
   */
  [[nodiscard]] bool empty() const noexcept {
    auto empty{mem_table_.empty()};
    for (const auto& ck_layer : ck_layers_) {
      empty = empty && ck_layer.empty();
      if (!empty) {
        break;
      }
    }
    return empty;
  }

private:
  /**
   * @brief Type alias for a deque of SSTables.
   * 
   */
  using SSTables = std::deque<SSTable<TValue>>;

  /**
   * @brief Type alias for SSTables.
   * 
   */
  using CkLayer = SSTables;

  /**
   * @brief Type alias for CkLayer vector.
   * 
   */
  using CkLayers = std::vector<CkLayer>;

  /**
   * @brief Type alias for time range.
   * 
   */
  using TimeWindow = TimeRange;

  /**
   * @brief Type alias for LRU cache.
   * 
   */
  using Cache = LRUCache<key_type, TValue>;

  /**
   * @brief Type alias for mapping from key to bool.
   * 
   */
  using DirtyTable = std::unordered_map<key_type, bool>;

  /**
   * @brief Type alias for mapping from time window to entries.
   * 
   */
  using TimeWindowToEntriesMap = std::map<TimeWindow, table_type, std::greater<>>;

  /**
   * @brief Time window sizes of each Ck layer.
   * @details The time window sizes are in seconds.
   * - C0: 0 (any overlap)
   * - C1: 30 minutes
   * - C2: 1 hour
   * - C3: 1 day
   * - C4: 1 week
   * - C5: 1 month
   * - C6: 3 months
   * - C7: 1 year
   * 
   */
  static constexpr std::array<size_type, LAYER_COUNT> CK_LAYER_WINDOW_SIZE{
    0,
    1800,
    3600,
    86400,
    604800,
    2592000,
    7776000,
    31536000
  };

  /**
   * @brief Number of SSTables in each Ck layer.
   * @details The number of SSTables in each Ck layer is monotonically
   * non-decreasing, either multiplying by a factor of 10 or staying the same.
   * 
   */
  static constexpr std::array<size_type, LAYER_COUNT> CK_LAYER_TABLE_COUNT{
    10,
    100,
    100,
    1000,
    1000,
    1000,
    10000,
    10000
  };

  /**
   * @brief Get the next SSTable file path for a given layer.
   * 
   * @param k Layer index.
   * @return FilePath SSTable file path.
   */
  [[nodiscard]] FilePath get_next_file_path(size_type k) noexcept {
    return path_ / ("sstable_l" + std::to_string(k) + "_"
      + std::to_string(sstable_id_[k]++) + ".sst");
  }

  /**
   * @brief Load the SSTables from disk.
   * 
   * @throw std::runtime_error If loading of any SSTable fails.
   */
  void load_sstables() {
    std::array<std::set<FilePath>, LAYER_COUNT> sstable_files;
    for (const auto& file : std::filesystem::directory_iterator(path_)) {
      if (!file.is_regular_file() || file.path().extension() != ".sst") {
        continue;
      }
      const auto l_pos{file.path().filename().string().find("_l")};
      const auto layer_idx{
        std::stoull(file.path().filename().string().substr(l_pos + 2))
      };
      sstable_files[layer_idx].insert(file.path());
      const auto id{
        std::stoull(file.path().filename().string().substr(l_pos + 4))
      };
      sstable_id_[layer_idx] = std::max(sstable_id_[layer_idx], id + 1);
    }
    for (size_type k{0}; k < LAYER_COUNT; ++k) {
      for (const auto& sstable_file : sstable_files[k]) {
        ck_layers_[k].emplace_back(std::move(sstable_file));
      }
    }
    compact();
  }

  /**
   * @brief Validate a layer index.
   * 
   * @param k Layer index.
   * @throw std::out_of_range If the layer index is out of range.
   */
  void validate_layer_index(size_type k) const {
    if (k < 0 || k >= LAYER_COUNT) {
      throw std::out_of_range{
        "LSMTree::validate_layer_index(): Layer index out of range."
      };
    }
  }

  /**
   * @brief Reset a layer.
   * @details Clear the layer and reset the SSTable ID.
   * 
   * @param k Layer index.
   * @throw std::out_of_range If the layer index is out of range.
   */
  void reset_layer(size_type k) noexcept {
    validate_layer_index(k);
    ck_layers_[k].clear();
    sstable_id_[k] = 0;
  }

  /**
   * @brief Flush the memtable to an SSTable.
   * 
   * @throw std::runtime_error if the C1 layer is full.
   */
  void flush() {
    FilePath sstable_file_path{get_next_file_path(0)};
    ck_layers_[0].emplace_back(
      sstable_file_path,
      std::move(mem_table_),
      mem_table_.size()
    );
    compact_layer_if_needed(0);
    mem_table_.clear();
    wal_.clear();
  }

  /**
   * @brief Compact the LSM tree.
   * @details Compact the C0 layer if needed, which will compact the C1 layer
   * if needed after the compaction, and so on.
   * 
   * @throw std::exception If the compaction fails.
   */
  void compact() {
    compact_layer_if_needed(0);
  }

  /**
   * @brief Compact a layer if needed.
   * 
   * @param k Layer index.
   * 
   * @throw std::out_of_range If the layer index is out of range.
   */
  void compact_layer_if_needed(size_type k) {
    validate_layer_index(k);
    if (k == LAYER_COUNT - 1) {
      return;
    }
    if (ck_layers_[k].size() <= CK_LAYER_TABLE_COUNT[k]) {
      return;
    }
    compact_layer(k);
    compact_layer_if_needed(k + 1);
  }

  /**
   * @brief Compact a layer.
   * 
   * @param k Layer index.
   * 
   * @throw std::out_of_range If the layer index is out of range.
   */
  void compact_layer(size_type k) {
    if (k == 0) {
      merge_all_in_layer(k);
      return;
    }
    while (ck_layers_[k].size() > CK_LAYER_TABLE_COUNT[k]) {
      merge_oldest_in_layer(k);
    }
  }

  /**
   * @brief Update the entries with the SSTable.
   * @details Adds all SSTable entries to the map based on their time windows,
   * and adds the SSTable files to the vector of files to remove.
   * 
   * @param sstable SSTable.
   * @param window_size Window size.
   * @param time_window_to_entries Time window to entries map.
   * @param files_to_remove Files to remove vector.
   */
  void update_entries_with_sstable(
    const SSTable<TValue>& sstable,
    size_type window_size,
    TimeWindowToEntriesMap& time_window_to_entries,
    std::vector<FilePath>& files_to_remove
  ) const noexcept {
    for (const auto& [key, value] : sstable.entries()) {
      const auto start{(key.timestamp() / window_size) * window_size};
      const auto time_window{TimeWindow{start, start + window_size}};
      time_window_to_entries[time_window][key] = value;
    }
    files_to_remove.push_back(sstable.path());
    files_to_remove.push_back(sstable.metadataPath());
  }

  /**
   * @brief Merge all SSTables in a layer with the next layer's SSTables.
   * 
   * @param k Layer index.
   */
  void merge_all_in_layer(size_type k) {
    auto& curr_layer{ck_layers_[k]};
    auto& next_layer{ck_layers_[k + 1]};
    const auto window_size{CK_LAYER_WINDOW_SIZE[k + 1]};

    TimeWindowToEntriesMap time_window_to_entries;
    std::vector<FilePath> files_to_remove;

    for (const auto& sstable : next_layer) {
      update_entries_with_sstable(
        sstable, window_size,
        time_window_to_entries,
        files_to_remove
      );
    }
    reset_layer(k + 1);

    for (const auto& sstable : curr_layer) {
      update_entries_with_sstable(
        sstable, window_size,
        time_window_to_entries,
        files_to_remove
      );
    }
    reset_layer(k);

    for (const auto& file : files_to_remove) {
      if (!std::filesystem::remove(file)) {
        throw std::runtime_error{
          "LSMTree::merge_all_in_layer(): Failed to remove file: '"
          + std::string{file} + "' after compaction."
        };
      }
    }

    for (auto& [time_window, entries] : time_window_to_entries) {
      auto sstable{merge_entries(std::move(entries), k)};
      next_layer.push_back(std::move(sstable));
    }
  }

  /**
   * @brief Merge the oldest SSTables in a layer with the next layer's.
   * @details Take the oldest SSTables in the layer, and merge it with the
   * next layer's SSTables by time window.
   * 
   * @param k Layer index.
   */
  void merge_oldest_in_layer(size_type k) {
    auto& curr_layer{ck_layers_[k]};
    auto& next_layer{ck_layers_[k + 1]};
    const auto window_size{CK_LAYER_WINDOW_SIZE[k + 1]};

    TimeWindowToEntriesMap time_window_to_entries;
    std::vector<FilePath> files_to_remove;

    for (const auto& sstable : next_layer) {
      update_entries_with_sstable(
        sstable, window_size,
        time_window_to_entries,
        files_to_remove
      );
    }
    reset_layer(k + 1);

    while (curr_layer.size() > CK_LAYER_TABLE_COUNT[k]) {
      auto& oldest_sstable{curr_layer.front()};
      update_entries_with_sstable(
        oldest_sstable, window_size,
        time_window_to_entries,
        files_to_remove
      );
      curr_layer.pop_front();
    }

    for (const auto& file : files_to_remove) {
      if (!std::filesystem::remove(file)) {
        throw std::runtime_error{
          "LSMTree::merge_oldest_in_layer(): Failed to remove file: '"
          + std::string{file} + "' after compaction."
        };
      }
    }

    for (auto& [time_window, entries] : time_window_to_entries) {
      auto sstable{merge_entries(std::move(entries), k)};
      next_layer.push_back(std::move(sstable));
    }
  }

  /**
   * @brief Merge entries into an SSTable.
   * 
   * @param entries Entries to merge.
   * @param k Layer index.
   * @return SSTable Merged SSTable.
   */
  SSTable<TValue> merge_entries(table_type&& entries, size_type k) {
    auto memtable{MemTable<TValue>{std::move(entries)}};
    auto sstable{SSTable<TValue>{
      get_next_file_path(k + 1),
      std::move(memtable),
      memtable.size()
    }};
    return sstable;
  }

  /**
   * @brief Memtable.
   * 
   */
  MemTable<TValue> mem_table_;

  /**
   * @brief SSTable layers.
   * 
   */
  CkLayers ck_layers_;

  /**
   * @brief Write-ahead log.
   * 
   */
  WriteAheadLog<TValue> wal_;

  /**
   * @brief Path.
   * 
   */
  FilePath path_;

  /**
   * @brief Next SSTable ID for each layer.
   * 
   */
  std::array<size_type, LAYER_COUNT> sstable_id_;

  /**
   * @brief Cache.
   * 
   */
  mutable Cache cache_;

  /**
   * @brief Dirty table.
   * 
   */
  mutable DirtyTable dirty_table_;
};
}  // namespace vkdb

#endif // STORAGE_LSM_TREE_H