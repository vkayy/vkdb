#ifndef STORAGE_MEM_TABLE_H
#define STORAGE_MEM_TABLE_H

#include "utils/concepts.h"
#include "utils/string.h"
#include "storage/time_series_key.h"
#include "storage/data_range.h"

template <ArithmeticNoCVRefQuals TValue>
class MemTable {
public:
  using key_type = TimeSeriesKey;
  using mapped_type = std::optional<TValue>;
  using value_type = std::pair<const key_type, mapped_type>;
  using size_type = uint64_t;
  using table_type = std::map<const key_type, mapped_type>;

  static constexpr size_type MAX_ENTRIES{1000};

  MemTable() noexcept = default;
  
  MemTable(MemTable&&) noexcept = default;
  MemTable& operator=(MemTable&&) noexcept = default;
  
  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  ~MemTable() = default;

  void put(const key_type& key, const mapped_type& value) {
    table_.insert_or_assign(key, value);
    update_ranges(key);
  }

  [[nodiscard]] bool contains(const key_type& key) const noexcept {
    return in_range(key) && table_.contains(key);
  }

  [[nodiscard]] mapped_type get(const key_type& key) const {
    if (!contains(key)) {
      return std::nullopt;
    }
    return table_.at(key);
  }

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

  void clear() noexcept {
    table_.clear();
    time_range_.clear();
    key_range_.clear();
  }

  [[nodiscard]] table_type table() const noexcept {
    return table_;
  }

  [[nodiscard]] size_type size() const noexcept {
    return table_.size();
  }

  [[nodiscard]] std::string toString() const noexcept {
    std::stringstream ss;
    ss << size();
    for (const auto& entry : table()) {
      ss << entryToString<TValue>(entry);
    }
    return ss.str();
  }

  static void fromString(const std::string& str, MemTable& table) {
    std::stringstream ss(str);
    size_type size;
    ss >> size;

    std::string entry_str;
    std::getline(ss, entry_str, '[');
    
    while (std::getline(ss, entry_str, '[')) {
      auto [entry_key, entry_value] = entryFromString<TValue>(entry_str);
      table.put(entry_key, entry_value);
    }
  }

private:
  using TimeRange = DataRange<Timestamp>;
  using KeyRange = DataRange<key_type>;

  [[nodiscard]] bool in_range(const key_type& key) const noexcept {
    return time_range_.inRange(key.timestamp()) && key_range_.inRange(key);
  }

  void update_ranges(const key_type& key) noexcept {
    time_range_.updateRange(key.timestamp());
    key_range_.updateRange(key);
  }

  [[nodiscard]] bool overlaps_with(const key_type& start,
                              const key_type& end) const noexcept {
    return time_range_.overlaps_with(start.timestamp(), end.timestamp())
      || key_range_.overlaps_with(start, end);
  }

  TimeRange time_range_;
  KeyRange key_range_;
  table_type table_;
};

template <ArithmeticNoCVRefQuals TValue>
std::ostream& operator<<(std::ostream& os, const MemTable<TValue>& table) {
  os << table.toString();
  return os;
}

template <ArithmeticNoCVRefQuals TValue>
std::istream& operator>>(std::istream& is, MemTable<TValue>& table) {
  std::string str;
  is >> str;
  MemTable<TValue>::fromString(str, table);
  return is;
}


#endif // STORAGE_MEM_TABLE_H