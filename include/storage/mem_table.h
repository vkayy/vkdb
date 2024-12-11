#ifndef STORAGE_MEM_TABLE_H
#define STORAGE_MEM_TABLE_H

#include "utils/concepts.h"
#include "storage/time_series_key.h"
#include "storage/data_range.h"

template <ArithmeticNoCVRefQuals TValue>
class MemTable {
public:
  using key_type = TimeSeriesKey;
  using mapped_type = std::optional<const TValue>;
  using value_type = std::pair<const key_type, mapped_type>;
  using size_type = uint64_t;
  using table_type = std::multimap<const key_type, mapped_type>;

  MemTable() noexcept = default;
  
  MemTable(MemTable&&) noexcept = default;
  MemTable& operator=(MemTable&&) noexcept = default;
  
  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  ~MemTable() = default;

  void put(const key_type& key, const mapped_type& value) {
    table_.emplace(key, value);
    update_ranges(key);
  }

  [[nodiscard]] mapped_type get(const key_type& key) const {
    if (!in_range(key)) {
      return std::nullopt;
    }
    auto it{table_.find(key)};
    if (it == table_.end()) {
      return std::nullopt;
    }
    return it->second;
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
    for (const auto& [key, value] : table()) {
      ss << "[" << key.toString() << "|";
      if (value.has_value()) {
        ss << value.value();
      } else {
        ss << "null";
      }
      ss << "]";
    }
    return ss.str();
  }

  static void fromString(const std::string& str, MemTable& table) {
    std::stringstream ss(str);
    size_type size;
    ss >> size;

    std::string entry;
    std::getline(ss, entry, '[');
    
    while (std::getline(ss, entry, '[')) {
      auto sep{entry.find('|')};
      auto end{entry.find(']')};
      auto key_str{entry.substr(0, sep)};
      auto value_str{entry.substr(sep + 1, end - sep - 1)};

      auto key{key_type::fromString(key_str)};
      std::optional<TValue> value;
      if (value_str != "null") {
        std::stringstream value_ss{value_str};
        TValue parsed_value;
        value_ss >> parsed_value;
        value = parsed_value;
      }
      table.put(key, value);
    }
  }

private:
  using TimeRange = DataRange<Timestamp>;
  using KeyRange = DataRange<key_type>;
  using Table = std::multimap<const key_type, mapped_type>;

  [[nodiscard]] bool in_range(const key_type& key) const noexcept {
    return time_range_.inRange(key.timestamp()) && key_range_.inRange(key);
  }

  void update_ranges(const key_type& key) noexcept {
    time_range_.updateRange(key.timestamp());
    key_range_.updateRange(key);
  }

  TimeRange time_range_;
  KeyRange key_range_;
  Table table_;
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