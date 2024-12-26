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
using TimeSeriesKeyFilter = std::function<bool(const TimeSeriesKey&)>;

static const TimeSeriesKeyFilter TRUE_TIME_SERIES_KEY_FILTER =
  [](const TimeSeriesKey&) { return true; };

template <ArithmeticNoCVRefQuals TValue>
class LSMTree {
public:
  using key_type = TimeSeriesKey;
  using mapped_type = std::optional<TValue>;
  using value_type = std::pair<const key_type, mapped_type>;
  using size_type = uint64_t;
  using table_type = typename MemTable<TValue>::table_type;

  static constexpr size_type C1_LAYER_SIZE{1'000};
  static constexpr size_type CACHE_CAPACITY{10'000};

  explicit LSMTree(FilePath path) noexcept
    : wal_{path}
    , path_{std::move(path)}
    , sstable_id_{0}
    , cache_{CACHE_CAPACITY} {
      std::filesystem::create_directories(path_);
      load_sstables();
    }

  LSMTree(LSMTree&&) noexcept = default;
  LSMTree& operator=(LSMTree&&) noexcept = default;  

  LSMTree(const LSMTree&) = delete;
  LSMTree& operator=(const LSMTree&) = delete;

  ~LSMTree() = default;

  void put(const key_type& key, const TValue& value, bool log = true) {
    mem_table_.put(key, value);
    dirty_table_[key] = true;
    if (mem_table_.size() == MemTable<TValue>::MAX_ENTRIES) {
      flush();
    }
    if (log) {
      wal_.append({WALRecordType::PUT, {key, value}});
    }
  }

  void remove(const key_type& key, bool log = true) {
    mem_table_.put(key, std::nullopt);
    dirty_table_[key] = true;
    if (mem_table_.size() == MemTable<TValue>::MAX_ENTRIES) {
      flush();
    }
    if (log) {
      wal_.append({WALRecordType::REMOVE, {key, std::nullopt}});
    }
  }

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
    for (const auto& sstable : sstables_ | std::views::reverse) {
      if (sstable.contains(key)) {
        const auto value{sstable.get(key)};
        cache_.put(key, value);
        dirty_table_[key] = false;
        return value;
      }
    }
    return std::nullopt;
  }

  [[nodiscard]] std::vector<value_type> getRange(
    const key_type& start,
    const key_type& end,
    TimeSeriesKeyFilter&& filter
  ) const {
    table_type entry_table;
    for (const auto& sstable : sstables_) {
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

  [[nodiscard]] std::vector<value_type> getRangeParallel(
      const key_type& start,
      const key_type& end,
      TimeSeriesKeyFilter filter
  ) const {
    std::vector<std::future<std::vector<value_type>>> range_futures;
    range_futures.reserve(sstables_.size() + 1);
    
    range_futures.push_back(std::async(std::launch::async,
      [this, &start, &end, &filter]() {
        std::vector<value_type> mem_table_entries;
        for (const auto& [key, value] : mem_table_.getRange(start, end)) {
          if (filter(key)) {
            mem_table_entries.emplace_back(key, value);
          }
        }
        return mem_table_entries;
      }
    ));

    for (const auto& sstable : sstables_ | std::views::reverse) {
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

  void replayWAL() {
    wal_.replay(*this);
  }

  void clear() noexcept {
    for (const auto& sstable : sstables_) {
      std::filesystem::remove(sstable.path());
      std::filesystem::remove(sstable.metadataPath());
    }
    std::filesystem::remove(wal_.path());
  }

  [[nodiscard]] std::string str() const noexcept {
    std::stringstream ss;
    ss << mem_table_.str();
    for (const auto& sstable : sstables_) {
      ss << sstable.str();
    }
    return ss.str();
  }

  [[nodiscard]] size_type sstableCount() const noexcept {
    return sstables_.size();
  }

  [[nodiscard]] bool empty() const noexcept {
    return mem_table_.empty() && sstables_.empty();
  }

private:
  using C0Layer = MemTable<TValue>;
  using C1Layer = std::vector<SSTable<TValue>>;
  using Cache = LRUCache<key_type, TValue>;
  using DirtyTable = std::unordered_map<key_type, bool>;

  void flush() {
    FilePath sstable_file_path{
      path_ / ("sstable_" + std::to_string(sstable_id_++) + ".sst")
    };
    if (sstables_.size() == C1_LAYER_SIZE) {
      throw std::runtime_error{
        "LSMTree::flush(): C1 layer is full. Unable to flush memtable."
      };
    }
    sstables_.emplace_back(sstable_file_path, std::move(mem_table_));
    mem_table_.clear();
    wal_.clear();
  }

  void load_sstables() {
    sstables_.reserve(C1_LAYER_SIZE);
    std::set<FilePath> sstable_files;
    for (const auto& file : std::filesystem::directory_iterator(path_)) {
      if (!file.is_regular_file() || file.path().extension() != ".sst") {
        continue;
      }
      sstable_files.insert(file.path());
    }
    for (const auto& sstable_file : sstable_files) {
      sstables_.emplace_back(sstable_file);
    }
  }

  C0Layer mem_table_;
  C1Layer sstables_;
  WriteAheadLog<TValue> wal_;
  FilePath path_;
  size_type sstable_id_;
  mutable Cache cache_;
  mutable DirtyTable dirty_table_;
};
}  // namespace vkdb

#endif // STORAGE_LSM_TREE_H