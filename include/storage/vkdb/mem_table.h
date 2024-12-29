#ifndef STORAGE_MEM_TABLE_H
#define STORAGE_MEM_TABLE_H

#include <vkdb/concepts.h>
#include <vkdb/string.h>
#include <vkdb/time_series_key.h>
#include <vkdb/data_range.h>

namespace vkdb {

/**
 * @brief In-memory table for storing key-value pairs.
 * 
 * @tparam TValue The type of the value to be stored in the table.
 */
template <ArithmeticNoCVRefQuals TValue>
class MemTable {
public:
  using key_type = TimeSeriesKey;
  using mapped_type = std::optional<TValue>;
  using value_type = std::pair<const key_type, mapped_type>;
  using size_type = uint64_t;
  using table_type = std::map<const key_type, mapped_type>;

  static constexpr size_type MAX_ENTRIES{1'000};

  /**
   * @brief Construct a new MemTable object.
   */
  MemTable() noexcept = default;

  /**
   * @brief Move-construct a MemTable object.
   */
  MemTable(MemTable&&) noexcept = default;

  /**
   * @brief Move-assign a MemTable object.
   * 
   * @return MemTable& Reference to the moved MemTable.
   */
  MemTable& operator=(MemTable&&) noexcept = default;

  /**
   * @brief Deleted copy constructor.
   */
  MemTable(const MemTable&) = delete;

  /**
   * @brief Deleted copy assignment operator.
   */
  MemTable& operator=(const MemTable&) = delete;

  /**
   * @brief Destroy the MemTable object.
   */
  ~MemTable() noexcept = default;

  /**
   * @brief Inserts or updates a key-value pair in the table.
   * 
   * @param key The key to insert or update.
   * @param value The value to associate with the key.
   * 
   * @throw std::exception If inserting or updating the key-value pair fails.
   */
  void put(const key_type& key, const mapped_type& value) {
    table_.insert_or_assign(key, value);
    update_ranges(key);
  }

  /**
   * @brief Checks if the table contains a key.
   * 
   * @param key The key to check.
   * @return true If the key is in the table.
   * @return false Otherwise.
   */
  [[nodiscard]] bool contains(const key_type& key) const noexcept {
    return in_range(key) && table_.contains(key);
  }

  /**
   * @brief Retrieves the value associated with a key.
   * 
   * @param key The key to retrieve the value for.
   * @return mapped_type The value associated with the key, or std::nullopt if the key is not found.
   */
  [[nodiscard]] mapped_type get(const key_type& key) const noexcept {
    if (!contains(key)) {
      return std::nullopt;
    }
    return table_.at(key);
  }

  /**
   * @brief Retrieves a range of key-value pairs.
   * 
   * @param start The start key of the range.
   * @param end The end key of the range.
   * @return std::vector<value_type> A vector of key-value pairs in the specified range.
   * 
   * @throw std::exception If getting the range fails.
   */
  [[nodiscard]] std::vector<value_type> getRange(
    const key_type& start,
    const key_type& end
  ) const {
    if (!overlaps_with(start, end)) {
      return {};
    }
    std::vector<value_type> entries;
    for (auto it{table_.lower_bound(start)};
         it != table_.end() && it->first < end; ++it) {
      entries.push_back(*it);
    }
    return entries;
  }

  /**
   * @brief Clears the table.
   * 
   */
  void clear() noexcept {
    table_.clear();
    time_range_.clear();
    key_range_.clear();
  }

  /**
   * @brief Returns the table.
   * 
   * @return table_type The table.
   */
  [[nodiscard]] table_type table() const noexcept {
    return table_;
  }

  /**
   * @brief Returns the size of the table.
   * 
   * @return size_type The size of the table.
   */
  [[nodiscard]] size_type size() const noexcept {
    return table_.size();
  }

  /**
   * @brief Checks if the table is empty.
   * 
   * @return true If the table is empty.
   * @return false Otherwise.
   */
  [[nodiscard]] bool empty() const noexcept {
    return table_.empty();
  }

  /**
   * @brief Converts the table to a string representation.
   * 
   * @return std::string The string representation of the table.
   */
  [[nodiscard]] std::string str() const noexcept {
    std::stringstream ss;
    ss << size();
    for (const auto& entry : table()) {
      ss << entryToString<TValue>(entry);
    }
    return ss.str();
  }

  /**
   * @brief Converts a string representation to a table.
   * 
   * @param str The string representation of the table.
   * @param table The table to populate.
   * 
   * @throw std::exception If converting any string to entry fails.
   */
  static void fromString(const std::string& str, MemTable& table) {
    std::stringstream ss(str);
    size_type size;
    ss >> size;

    std::string entry_str;
    std::getline(ss, entry_str, '[');
    
    while (std::getline(ss, entry_str, '[')) {
      auto [entry_key, entry_value]
        = entryFromString<TValue>(std::move(entry_str));
      table.put(entry_key, entry_value);
    }
  }

private:

  using TimeRange = DataRange<Timestamp>;
  using KeyRange = DataRange<key_type>;

  /**
   * @brief Checks if a key is within the valid range.
   * 
   * @param key The key to check.
   * @return true If the key is within the valid range.
   * @return false Otherwise.
   */
  [[nodiscard]] bool in_range(const key_type& key) const noexcept {
    return time_range_.inRange(key.timestamp()) && key_range_.inRange(key);
  }

  /**
   * @brief Updates the ranges based on a key.
   * 
   * @param key The key to update the ranges with.
   */
  void update_ranges(const key_type& key) noexcept {
    time_range_.updateRange(key.timestamp());
    key_range_.updateRange(key);
  }

  /**
   * @brief Checks if a range of keys overlaps with the ranges.
   * 
   * @param start The start key of the range.
   * @param end The end key of the range.
   * @return true If the range overlaps with the valid ranges.
   * @return false Otherwise.
   */
  [[nodiscard]] bool overlaps_with(const key_type& start,
                              const key_type& end) const noexcept {
    return time_range_.overlapsWith(start.timestamp(), end.timestamp())
      || key_range_.overlapsWith(start, end);
  }

  /**
   * @brief The time range.
   * 
   */
  TimeRange time_range_;

  /**
   * @brief The key range.
   * 
   */
  KeyRange key_range_;

  /**
   * @brief The table.
   * 
   */
  table_type table_;
};

/**
 * @brief Overload of the stream insertion operator for MemTable.
 * 
 * @tparam TValue The type of the value to be stored in the table.
 * @param os The output stream.
 * @param table The table to insert into the stream.
 * @return std::ostream& The output stream.
 * 
 * @throw std::exception If the table cannot be converted to a string.
 */
template <ArithmeticNoCVRefQuals TValue>
std::ostream& operator<<(std::ostream& os, const MemTable<TValue>& table) {
  os << table.str();
  return os;
}

/**
 * @brief Overload of the stream extraction operator for MemTable.
 * 
 * @tparam TValue The type of the value to be stored in the table.
 * @param is The input stream.
 * @param table The table to extract from the stream.
 * @return std::istream& The input stream.
 * 
 * @throw std::exception If the string cannot be converted to a table.
 */
template <ArithmeticNoCVRefQuals TValue>
std::istream& operator>>(std::istream& is, MemTable<TValue>& table) {
  std::string str;
  is >> str;
  MemTable<TValue>::fromString(str, table);
  return is;
}
}  // namespace vkdb

#endif // STORAGE_MEM_TABLE_H
