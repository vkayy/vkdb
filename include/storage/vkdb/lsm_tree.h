#ifndef STORAGE_LSM_TREE_H
#define STORAGE_LSM_TREE_H

#include <vkdb/concepts.h>
#include <vkdb/sstable.h>
#include <vkdb/mem_table.h>
#include <vkdb/write_ahead_log.h>
#include <vkdb/lru_cache.h>
#include <vkdb/wal_lsm.h>
#include <ranges>
#include <future>
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
    , ck_layers_{CK_LAYER_TABLE_COUNTS.size()}
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
    c0_layer_.put(key, value);
    dirty_table_[key] = true;
    if (c0_layer_.size() == MemTable<TValue>::MAX_ENTRIES) {
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
    c0_layer_.put(key, std::nullopt);
    dirty_table_[key] = true;
    if (c0_layer_.size() == MemTable<TValue>::MAX_ENTRIES) {
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
    if (c0_layer_.contains(key)) {
      const auto value{c0_layer_.get(key)};
      cache_.put(key, value);
      dirty_table_[key] = false;
      return value;
    }
    const auto& c1_layer{ck_layers_[0]};
    for (const auto& sstable : c1_layer | std::views::reverse) {
      if (sstable.contains(key)) {
        const auto value{sstable.get(key)};
        cache_.put(key, value);
        dirty_table_[key] = false;
        return value;
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
    for (const auto& sstable : ck_layers_[0]) {
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
    for (const auto& [key, value] : c0_layer_.getRange(start, end)) {
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
   * @brief Get a filtered set of entries in a timestamp range in parallel.
   * 
   * @param start Start timestamp.
   * @param end End timestamp.
   * @param filter Filter.
   * @return std::vector<value_type> Entries.
   * 
   * @throw std::exception If getting the entries fails.
   */
  [[nodiscard]] std::vector<value_type> getRangeParallel(
      const key_type& start,
      const key_type& end,
      TimeSeriesKeyFilter filter
  ) const {
    const auto& c1_layer{ck_layers_[0]};
    std::vector<std::future<std::vector<value_type>>> range_futures;
    range_futures.reserve(c1_layer.size() + 1);
    
    range_futures.push_back(std::async(std::launch::async,
      [this, &start, &end, &filter]() {
        std::vector<value_type> c0_layer_entries;
        for (const auto& [key, value] : c0_layer_.getRange(start, end)) {
          if (filter(key)) {
            c0_layer_entries.emplace_back(key, value);
          }
        }
        return c0_layer_entries;
      }
    ));

    for (const auto& sstable : c1_layer | std::views::reverse) {
      range_futures.push_back(std::async(std::launch::async, 
        [&sstable, &start, &end, &filter]() {
          std::vector<value_type> sstable_entries;
          for (const auto& [key, value] : sstable.getRange(start, end)) {
            if (filter(key)) {
              sstable_entries.emplace_back(key, value);
            }
          }
          return sstable_entries;
        }
      ));
    }

    table_type entry_table;
    for (auto& range_future : range_futures) {
      const auto entries{range_future.get()};
      for (const auto& [key, value] : entries) {
        if (!entry_table.contains(key)) {
          entry_table[key] = value;
        }
      }
    }

    std::vector<value_type> entries;
    entries.reserve(entry_table.size());
    for (const auto& [key, value] : entry_table) {
      if (value.has_value()) {
        entries.emplace_back(key, value);
      }
    }

    return entries;
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
    for (const auto& sstable : ck_layers_[0]) {
      std::filesystem::remove(sstable.path());
      std::filesystem::remove(sstable.metadataPath());
    }
    std::filesystem::remove(wal_.path());
  }

  /**
   * @brief Convert the LSM tree to a string.
   * 
   * @return std::string The string representation of the LSM tree.
   */
  [[nodiscard]] std::string str() const noexcept {
    std::stringstream ss;
    ss << c0_layer_.str();
    for (const auto& sstable : ck_layers_[0]) {
      ss << sstable.str();
    }
    return ss.str();
  }

  /**
   * @brief Get the number of SSTables in the LSM tree.
   * 
   * @return size_type The number of SSTables.
   */
  [[nodiscard]] size_type sstableCount() const noexcept {
    return ck_layers_[0].size();
  }

  /**
   * @brief Check if the LSM tree is empty.
   * 
   * @return true if the LSM tree is empty.
   * @return false if the LSM tree is not empty.
   */
  [[nodiscard]] bool empty() const noexcept {
    return c0_layer_.empty() && ck_layers_[0].empty();
  }

private:
  /**
   * @brief Time window.
   * @details A time window is a range of timestamps.
   * 
   */
  struct TimeWindow {
    /**
     * @brief Range.
     * 
     */
    DataRange<Timestamp> range;

    /**
     * @brief Check if two time windows overlap.
     * 
     * @param other Other time window.
     * @return true if the time windows overlap.
     * @return false if the time windows do not overlap.
     */
    bool overlaps(const TimeWindow& other) const noexcept {
      return range.overlapsWith(other.range);
    }
  };

  /**
   * @brief Type alias for memtable.
   * 
   */
  using C0Layer = MemTable<TValue>;

  /**
   * @brief Type alias for a vector of SSTables.
   * 
   */
  using CkLayer = std::vector<SSTable<TValue>>;

  /**
   * @brief Type alias for CkLayer vector.
   * 
   */
  using CkLayers = std::vector<CkLayer>;

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
   * @brief Time window sizes of each Ck layer.
   * @details The time window sizes are in seconds.
   * - C0: 0 (any overlap)
   * - C1: 0 (any overlap)
   * - C2: 1 day
   * - C3: 1 week
   * - C4: 1 month
   * - C5: 3 months
   * - C6: 6 months
   * - C7: 1 year
   * 
   */
  static constexpr std::array<size_type, 8> CK_LAYER_WINDOWS{
    0,
    0,
    86400,
    604800,
    2592000,
    7776000,
    15552000,
    31536000
  };

  /**
   * @brief Number of SSTables in each Ck layer.
   * @details The number of SSTables in each Ck layer is 1000.
   * 
   */
  static constexpr std::array<size_type, 8> CK_LAYER_TABLE_COUNTS{
    1'000,
    1'000,
    1'000,
    1'000,
    1'000,
    1'000,
    1'000,
    1'000
  };

  /**
   * @brief Load the SSTables from disk.
   * 
   * @throw std::runtime_error If loading of any SSTable fails.
   */
  void load_sstables() {
    auto& c1_layer{ck_layers_[0]};
    c1_layer.reserve(CK_LAYER_TABLE_COUNTS[0]);
    std::set<FilePath> sstable_files;
    for (const auto& file : std::filesystem::directory_iterator(path_)) {
      if (!file.is_regular_file() || file.path().extension() != ".sst") {
        continue;
      }
      sstable_files.insert(file.path());
    }
    for (const auto& sstable_file : sstable_files) {
      c1_layer.emplace_back(sstable_file);
    }
  }

  /**
   * @brief Flush the memtable to an SSTable.
   * 
   * @throw std::runtime_error if the C1 layer is full.
   */
  void flush() {
    FilePath sstable_file_path{
      path_ / ("sstable_" + std::to_string(sstable_id_++) + ".sst")
    };
    auto& c1_layer{ck_layers_[0]};
    if (c1_layer.size() == CK_LAYER_TABLE_COUNTS[0]) {
      throw std::runtime_error{
        "LSMTree::flush(): C1 layer is full. Unable to flush memtable."
      };
    }
    c1_layer.emplace_back(sstable_file_path, std::move(c0_layer_));
    c0_layer_.clear();
    wal_.clear();
  }

  /**
   * @brief Memtable.
   * 
   */
  C0Layer c0_layer_;

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
   * @brief Next SSTable ID.
   * 
   */
  size_type sstable_id_;

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