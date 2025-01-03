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
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

namespace vkdb {
/**
 * @brief Type alias for std::filesystem::path.
 * 
 */
using FilePath = std::filesystem::path;

/**
 * @brief Sorted string table for storing key-value pairs.
 * 
 * @tparam TValue Value type.
 */
template <ArithmeticNoCVRefQuals TValue>
class SSTable {
public:
  using key_type = TimeSeriesKey;
  using mapped_type = std::optional<TValue>;
  using value_type = std::pair<const key_type, mapped_type>;
  using size_type = uint64_t;

  /**
   * @brief False positive rate for the Bloom filters.
   * 
   */
  static constexpr double BLOOM_FILTER_FALSE_POSITIVE_RATE{0.01};

  /**
   * @brief Deleted default constructor.
   * 
   */
  SSTable() = delete;

  /**
   * @brief Construct a new SSTable object given a file path.
   * 
   * @param file_path Path.
   * 
   * @throws std::runtime_error If metadata loading fails.
   */
  explicit SSTable(FilePath file_path)
    : file_path_{file_path}
    , bloom_filter_{
        MemTable<TValue>::C0_LAYER_SSTABLE_MAX_ENTRIES,
        BLOOM_FILTER_FALSE_POSITIVE_RATE
      }
  {
    if (!std::filesystem::exists(file_path_)) {
      return;
    }
    load_metadata();
  }
  
  /**
   * @brief Construct a new SSTable object given a file path and a memtable.
   * 
   * @param file_path Path.
   * @param mem_table Memtable.
   * 
   * @throws std::runtime_error If writing data to disk fails.
   */
  explicit SSTable(
    FilePath file_path,
    MemTable<TValue>&& mem_table,
    size_type expected_entries = MemTable<TValue>::C0_LAYER_SSTABLE_MAX_ENTRIES
  )
    : file_path_{file_path}
    , bloom_filter_{
        expected_entries,
        BLOOM_FILTER_FALSE_POSITIVE_RATE
      }
    {
      writeDataToDisk(std::move(mem_table));
    }

  /**
   * @brief Move-construct a SSTable object.
   * 
   */
  SSTable(SSTable&&) noexcept = default;
  
  /**
   * @brief Move-assign a SSTable object.
   * 
   */
  SSTable& operator=(SSTable&&) noexcept = default;

  /**
   * @brief Deleted copy constructor.
   * 
   */
  SSTable(const SSTable&) = delete;
  
  /**
   * @brief Deleted copy assignment operator.
   * 
   */
  SSTable& operator=(const SSTable&) = delete;

  /**
   * @brief Destroy the SSTable object.
   * 
   */
  ~SSTable() noexcept = default;

  [[nodiscard]] bool operator==(const SSTable& other) const noexcept {
    return file_path_ == other.file_path_;
  }

  /**
   * @brief Write data to disk.
   * @details Saves the memtable to disk and saves the metadata.
   * 
   * @param mem_table Memtable.
   * 
   * @throws std::runtime_error If saving the memtable or metadata fails.
   */
  void writeDataToDisk(MemTable<TValue>&& mem_table) {
    save_memtable(std::move(mem_table));
    save_metadata();
  }

  /**
   * @brief Check if the SSTable may contain the given key.
   * 
   * @param key Key.
   * @return true if the SSTable may contain the key.
   * @return false if the SSTable does not contain the key.
   */
  [[nodiscard]] bool contains(const key_type& key) const noexcept {
    return may_contain(key) && in_range(key) && in_index(key);
  }

  /**
   * @brief Get the value associated with a key.
   * 
   * @param key Key.
   * @return mapped_type The value if it exists, std::nullopt otherwise.
   * 
   * @throws std::runtime_error If the position is invalid or the key does not match
   * the entry read.
   */
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
    auto [entry_key, entry_value] = entryFromString<TValue>(std::move(entry_str));

    file.close();

    return entry_value;
  }

  /**
   * @brief Get a filtered set of entries in a timestamp range.
   * 
   * @param start Start timestamp.
   * @param end End timestamp.
   * @return std::vector<value_type> Entries.
   */
  [[nodiscard]] std::vector<value_type> getRange(
    const key_type& start,
    const key_type& end
  ) const noexcept {
    if (!overlaps_with(start, end)) {
      return {};
    }

    auto start_it{index_.lower_bound(start)};
    auto end_it{index_.upper_bound(end)};
    if (start_it == end_it) {
      return {};
    }

    std::vector<value_type> entries;
    entries.reserve(std::distance(start_it, end_it));

    std::ifstream file{file_path_, std::ios::binary};
    for (auto it{start_it}; it != end_it; ++it) {
      const auto& [key, pos] = *it;
      file.seekg(pos);
      std::string entry_str;
      std::getline(file, entry_str, '[');
      std::getline(file, entry_str, '[');
      auto [entry_key, entry_value] 
        = entryFromString<TValue>(std::move(entry_str));
      entries.emplace_back(entry_key, entry_value);
    }
    
    return entries;
  }

  /**
   * @brief Get the entries of the SSTable.
   * 
   * @return std::vector<value_type> Entries.
   */
  [[nodiscard]] std::vector<value_type> entries() const noexcept {
    return getRange(MIN_TIME_SERIES_KEY, MAX_TIME_SERIES_KEY);
  }

  /**
   * @brief Get the path of the SSTable.
   * 
   * @return FilePath Path.
   */
  [[nodiscard]] FilePath path() const noexcept {
    return file_path_;
  }

  /**
   * @brief Get the path of the metadata file.
   * 
   * @return FilePath Path.
   */
  [[nodiscard]] FilePath metadataPath() const noexcept {
    auto file_path{file_path_};
    file_path.replace_extension(".metadata");
    return file_path;
  }

  /**
   * @brief Get the key range of the SSTable.
   * 
   * @return KeyRange Key range.
   */
  [[nodiscard]] KeyRange keyRange() const noexcept {
    return key_range_;
  }

  /**
   * @brief Get the time range of the SSTable.
   * 
   * @return TimeRange Time range.
   */
  [[nodiscard]] TimeRange timeRange() const noexcept {
    return time_range_;
  }

private:
  /**
   * @brief Type alias for ordered mapping of keys to stream positions.
   * 
   */
  using Index = std::map<const key_type, std::streampos>;

  /**
   * @brief Update the metadata with a key and stream position.
   * 
   * @param key Key.
   * @param pos Stream position.
   * 
   * @throws std::exception If inserting to index fails.
   */
  void update_metadata(const key_type& key, std::streampos pos) {
    time_range_.updateRange(key.timestamp());
    key_range_.updateRange(key);
    bloom_filter_.insert(key);
    index_.emplace(key, pos);
  }

  /**
   * @brief Save the memtable to disk.
   * 
   * @param mem_table Memtable.
   * 
   * @throws std::runtime_error If unable to open file or get current
   * stream position.
   */
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

  /**
   * @brief Save the metadata to disk.
   * 
   * @throws std::runtime_error If unable to open file.
   */
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
      file << key.str() << "^" << pos << "\n";
    }

    file.close();
  }
  
  /**
   * @brief Load the metadata from disk.
   * 
   * @throws std::runtime_error If unable to open file or format
   * is invalid.
   */
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
      auto caret_pos{line.find('^')};
      if (caret_pos == std::string::npos) {
        throw std::runtime_error{
          "SSTable::load_metadata(): Invalid index entry '" + line + "'."
        };
      }
      key_type key{std::move(line.substr(0, caret_pos))};
      auto pos{std::stoull(line.substr(caret_pos + 1))};
      index_.emplace(key, pos);
    }

    file.close();
  }

  /**
   * @brief Check if the Bloom filter may contain a key.
   * 
   * @param key Key.
   * @return true if the Bloom filter may contain the key.
   * @return false if the Bloom filter does not contain the key.
   */
  [[nodiscard]] bool may_contain(const key_type& key) const noexcept {
    return bloom_filter_.mayContain(key);
  }

  /**
   * @brief Check if a key is within the valid range.
   * 
   * @param key Key.
   * @return true If the key is within the valid range.
   * @return false If the key is not within the valid range.
   */
  [[nodiscard]] bool in_range(const key_type& key) const noexcept {
    return time_range_.inRange(key.timestamp()) && key_range_.inRange(key);
  }

  /**
   * @brief Check if a key is in the index.
   * 
   * @param key Key.
   * @return true If the key is in the index.
   * @return false If the key is not in the index.
   */
  [[nodiscard]] bool in_index(const key_type& key) const noexcept {
    return index_.count(key) > 0;
  }

  /**
   * @brief Check if the SSTable overlaps with the given range.
   * 
   * @param start The start key of the range.
   * @param end The end key of the range.
   * @return true if the range overlaps with the valid ranges.
   * @return false If the range does not overlap with the valid ranges.
   */
  [[nodiscard]] bool overlaps_with(
    const key_type& start,
    const key_type& end
  ) const noexcept {
    return time_range_.overlapsWith(start.timestamp(), end.timestamp())
      || key_range_.overlapsWith(start, end);
  }

  /**
   * @brief Bloom filter.
   * 
   */
  BloomFilter bloom_filter_;

  /**
   * @brief Time range.
   * 
   */
  TimeRange time_range_;

  /**
   * @brief Key range.
   * 
   */
  KeyRange key_range_;

  /**
   * @brief Index.
   * 
   */
  Index index_;

  /**
   * @brief Path.
   * 
   */
  FilePath file_path_;
};
}  // namespace vkdb

#endif // STORAGE_SSTABLE_H