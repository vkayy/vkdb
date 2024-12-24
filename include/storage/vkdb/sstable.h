#ifndef STORAGE_SSTABLE_H
#define STORAGE_SSTABLE_H

#include <vkdb/time_series_key.h>
#include <vkdb/bloom_filter.h>
#include <vkdb/data_range.h>
#include <vkdb/mem_table.h>
#include <vkdb/concepts.h>
#include <vkdb/string.h>
#include <string>
#include <fstream>

namespace vkdb {
using FilePath = std::filesystem::path;

template <ArithmeticNoCVRefQuals TValue>
class SSTable {
public:
  using key_type = TimeSeriesKey;
  using mapped_type = std::optional<TValue>;
  using value_type = std::pair<const key_type, mapped_type>;
  using size_type = uint64_t;

  static constexpr double BLOOM_FILTER_FALSE_POSITIVE_RATE{0.01};

  SSTable() = delete;

  explicit SSTable(FilePath file_path)
    : file_path_{file_path}
    , bloom_filter_{
        MemTable<TValue>::MAX_ENTRIES,
        BLOOM_FILTER_FALSE_POSITIVE_RATE
      }
  {
    if (!std::filesystem::exists(file_path_)) {
      return;
    }
    load_metadata();
  }
  
  explicit SSTable(FilePath file_path, MemTable<TValue>&& mem_table)
    : file_path_{file_path}
    , bloom_filter_{
        MemTable<TValue>::MAX_ENTRIES,
        BLOOM_FILTER_FALSE_POSITIVE_RATE
      }
    {
      writeDataToDisk(std::move(mem_table));
    }

  SSTable(SSTable&&) noexcept = default;
  SSTable& operator=(SSTable&&) noexcept = default;

  SSTable(const SSTable&) = delete;
  SSTable& operator=(const SSTable&) = delete;

  ~SSTable() = default;

  void writeDataToDisk(MemTable<TValue>&& mem_table) {
    save_memtable(std::move(mem_table));
    save_metadata();
  }

  [[nodiscard]] bool contains(const key_type& key) const noexcept {
    return may_contain(key) && in_range(key) && in_index(key);
  }

  [[nodiscard]] mapped_type get(const key_type& key) const {
    if (!contains(key)) {
      return std::nullopt;
    }

    std::ifstream file{file_path_};
    if (!file.is_open()) {
      throw std::runtime_error{
        "SSTable::get(): Unable to open file '"
        + std::string(file_path_) + "'."
      };
    }

    file.seekg(index_.at(key));
    if (!file) {
      throw std::runtime_error{
        "SSTable::get(): Unable to seek to position "
        + std::to_string(index_.at(key)) + " in file '"
        + std::string(file_path_) + "'."
      };
    }

    std::string entry_str;
    std::getline(file, entry_str, '[');
    std::getline(file, entry_str, '[');
    auto [entry_key, entry_value] = entryFromString<TValue>(entry_str);

    return entry_value;
  }

  [[nodiscard]] std::vector<value_type> getRange(
    const key_type& start,
    const key_type& end
  ) const {
    if (!overlaps_with(start, end)) {
      return {};
    }
    std::vector<value_type> entries;
    for (auto it{index_.lower_bound(start)};
         it != index_.end() && it->first < end; ++it) {
      const auto [key, pos] = *it;
      if (key < start) {
        continue;
      }
      if (key > end) {
        break;
      }
      entries.emplace_back(key, get(key));
    }
    return entries;
  }

  [[nodiscard]] FilePath path() const noexcept {
    return file_path_;
  }

  [[nodiscard]] FilePath metadataPath() const noexcept {
    auto file_path{file_path_};
    file_path.replace_extension(".metadata");
    return file_path;
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

  void save_memtable(MemTable<TValue>&& mem_table) {
    std::ofstream file{file_path_};
    if (!file.is_open()) {
      throw std::runtime_error{
        "SSTable::save_memtable(): Unable to open file '"
        + std::string(file_path_) + "'."
      };
    }

    file << mem_table.size();
    for (const auto& [key, value] : mem_table.table()) {
      auto pos{file.tellp()};
      if (pos == -1) {
        throw std::runtime_error{
          "SSTable::save_memtable(): Unable to get current position "
          " of filestream for '" + std::string(file_path_) + "'."
        };
      }
      update_metadata(key, pos);
      file << entryToString<TValue>(value_type{key, value});
    }

    file.close();
  }

  void save_metadata() {
    std::ofstream file{metadataPath()};
    if (!file.is_open()) {
      throw std::runtime_error{
        "SSTable::save_metadata(): Unable to open file '"
        + std::string(metadataPath()) + "'."
      };
    }

    file << time_range_.str() << "\n";
    file << key_range_.str() << "\n";
    file << bloom_filter_.str() << "\n";
    file << index_.size() << "\n";
    for (const auto& [key, pos] : index_) {
      file << key.str() << ":" << pos << "\n";
    }

    file.close();
  }
  
  void load_metadata() {
    std::ifstream file{metadataPath()};
    if (!file.is_open()) {
      throw std::runtime_error{
        "SSTable::load_metadata(): Unable to open file '"
        + std::string(metadataPath()) + "'."
      };
    }

    std::string line;
    std::getline(file, line);
    time_range_ = DataRange<Timestamp>{std::move(line)};
    std::getline(file, line);
    key_range_ = DataRange<key_type>{std::move(line)};
    std::getline(file, line);
    bloom_filter_ = BloomFilter{std::move(line)};
    std::getline(file, line);
    auto no_of_entries{std::stoull(line)};
    for (auto i{0}; i < no_of_entries; ++i) {
      std::getline(file, line);
      auto colon_pos{line.find(':')};
      if (colon_pos == std::string::npos) {
        throw std::runtime_error{
          "SSTable::load_metadata(): Invalid index entry '" + line + "'."
        };
      }
      key_type key{std::move(line.substr(0, colon_pos))};
      auto pos{std::stoull(line.substr(colon_pos + 1))};
      index_.emplace(key, pos);
    }

    file.close();
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

  [[nodiscard]] bool overlaps_with(const key_type& start,
                              const key_type& end) const noexcept {
    return time_range_.overlaps_with(start.timestamp(), end.timestamp())
      || key_range_.overlaps_with(start, end);
  }

  BloomFilter bloom_filter_;
  TimeRange time_range_;
  KeyRange key_range_;
  Index index_;
  FilePath file_path_;
};
}  // namespace vkdb

#endif // STORAGE_SSTABLE_H