#ifndef STORAGE_SSTABLE_H
#define STORAGE_SSTABLE_H

#include "storage/time_series_key.h"
#include "storage/bloom_filter.h"
#include "storage/data_range.h"
#include "storage/mem_table.h"
#include "utils/concepts.h"
#include "utils/string.h"
#include <string>
#include <fstream>

using FilePath = std::string;

template <ArithmeticNoCVRefQuals TValue>
class SSTable {
public:
  using key_type = TimeSeriesKey;
  using mapped_type = std::optional<const TValue>;
  using value_type = std::pair<const key_type, mapped_type>;
  using size_type = uint64_t;

  static constexpr double BLOOM_FILTER_FALSE_POSITIVE_RATE{0.01};

  SSTable() = delete;

  explicit SSTable(FilePath file_path)
    : file_path_{file_path}
    , bloom_filter_{
        MemTable<TValue>::MAX_ENTRIES,
        BLOOM_FILTER_FALSE_POSITIVE_RATE
      } {}
  
  explicit SSTable(FilePath file_path, MemTable<TValue> mem_table)
    : file_path_{file_path}
    , bloom_filter_{
        MemTable<TValue>::MAX_ENTRIES,
        BLOOM_FILTER_FALSE_POSITIVE_RATE
      }
    {
      writeFromMemTable(mem_table);
    }

  SSTable(SSTable&&) noexcept = default;
  SSTable& operator=(SSTable&&) noexcept = default;

  SSTable(const SSTable&) = delete;
  SSTable& operator=(const SSTable&) = delete;

  ~SSTable() = default;

  void writeMemTableToFile(const MemTable<TValue>& mem_table) {
    std::ofstream file{file_path_};
    if (!file.is_open()) {
      throw std::runtime_error{
        "SSTable::writeFromMemTable(): Unable to open file."
      };
    }

    file << mem_table.size();
    for (const auto& [key, value] : mem_table.table()) {
      auto pos{file.tellp()};
      if (pos == -1) {
        throw std::runtime_error{
          "SSTable::writeFromMemTable(): Unable to get file position."
        };
      }
      update_metadata(key, pos);
      file << entryToString<TValue>(value_type{key, value});
    }

    file.close();
  }

  [[nodiscard]] mapped_type get(const key_type& key) const {
    if (!may_contain(key) || !in_range(key) || !in_index(key)) {
      return std::nullopt;
    }

    std::ifstream file{file_path_};
    if (!file.is_open()) {
      throw std::runtime_error{
        "SSTable::get(): Unable to open file."
      };
    }

    file.seekg(index_.at(key));
    if (!file) {
      throw std::runtime_error{
        "SSTable::get(): Unable to seek to position."
      };
    }

    std::string entry_str;
    std::getline(file, entry_str, '[');
    std::getline(file, entry_str, '[');
    auto [entry_key, entry_value] = entryFromString<TValue>(entry_str);

    return entry_value;
  }

private:
  using Index = std::map<const key_type, std::streampos>;
  using TimeRange = DataRange<Timestamp>;
  using KeyRange = DataRange<key_type>;

  void update_metadata(const key_type& key, std::streampos pos) {
    time_range_.updateRange(key.timestamp());
    key_range_.updateRange(key);
    bloom_filter_.insert(key);
    index_.emplace(key, pos);
  }

  [[nodiscard]] bool may_contain(const key_type& key) const noexcept {
    return bloom_filter_.mayContain(key);
  }

  [[nodiscard]] bool in_range(const key_type& key) const noexcept {
    return time_range_.inRange(key.timestamp()) && key_range_.inRange(key);
  }

  [[nodiscard]] bool in_index(const key_type& key) const noexcept {
    return index_.count(key) > 0;
  }

  BloomFilter bloom_filter_;
  TimeRange time_range_;
  KeyRange key_range_;
  Index index_;
  FilePath file_path_;
};

#endif // STORAGE_SSTABLE_H