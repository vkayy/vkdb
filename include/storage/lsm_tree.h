#ifndef STORAGE_LSM_TREE_H
#define STORAGE_LSM_TREE_H

#include "utils/concepts.h"
#include "storage/sstable.h"
#include "storage/mem_table.h"
#include "storage/write_ahead_log.h"
#include "storage/wal_lsm.h"
#include <ranges>
#include <future>

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

  explicit LSMTree(FilePath path) noexcept
    : wal_{path}
    , path_{std::move(path)}
    , sstable_id_{0} {}

  LSMTree(LSMTree&&) noexcept = default;
  LSMTree& operator=(LSMTree&&) noexcept = default;  

  LSMTree(const LSMTree&) = delete;
  LSMTree& operator=(const LSMTree&) = delete;

  ~LSMTree() {
    for (const auto& sstable : sstables_) {
      std::remove(sstable.filePath().c_str());
    }
  };

  void put(const key_type& key, const TValue& value) {
    mem_table_.put(key, value);
    if (mem_table_.size() == MemTable<TValue>::MAX_ENTRIES) {
      flush();
    }
    wal_.append({WALRecordType::Put, {key, value}});
  }

  void remove(const key_type& key) {
    mem_table_.put(key, std::nullopt);
    if (mem_table_.size() == MemTable<TValue>::MAX_ENTRIES) {
      flush();
    }
    wal_.append({WALRecordType::Remove, {key, std::nullopt}});
  }

  [[nodiscard]] mapped_type get(const key_type& key) const {
    if (mem_table_.contains(key)) {
      return mem_table_.get(key);
    }
    for (const auto& sstable : sstables_ | std::views::reverse) {
      if (sstable.contains(key)) {
        return sstable.get(key);
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

  [[nodiscard]] std::string toString() const noexcept {
    std::stringstream ss;
    ss << mem_table_.toString();
    for (const auto& sstable : sstables_) {
      ss << sstable.toString();
    }
    return ss.str();
  }

  [[nodiscard]] size_type sstableCount() const noexcept {
    return sstables_.size();
  }

private:
  using C0Layer = MemTable<TValue>;
  using C1Layer = std::vector<SSTable<TValue>>;

  void flush() {
    FilePath sstable_file_path{
      path_ + "/sstable_" + std::to_string(sstable_id_++) + ".sst"
    };
    sstables_.emplace_back(sstable_file_path, std::move(mem_table_));
    mem_table_.clear();
    wal_.clear();
  }

  C0Layer mem_table_;
  C1Layer sstables_;
  WriteAheadLog<TValue> wal_;
  FilePath path_;
  size_type sstable_id_;
};
}  // namespace vkdb

#endif // STORAGE_LSM_TREE_H