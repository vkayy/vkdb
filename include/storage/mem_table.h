#ifndef STORAGE_MEM_TABLE_H
#define STORAGE_MEM_TABLE_H

#include "utils/concepts.h"
#include "storage/time_series_key.h"
#include "storage/data_range.h"

template <Arithmetic TValue>
class MemTable {
public:
  using key_type = TimeSeriesKey;
  using mapped_type = std::optional<const TValue>;
  using value_type = std::pair<const key_type, mapped_type>;
  using size_type = uint64_t;

  MemTable() noexcept = default;
  
  MemTable(const MemTable&&) noexcept = default;
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

  [[nodiscard]] const std::vector<value_type> table() const noexcept {
    std::vector<value_type> result;
    for (const auto& [key, value] : table_) {
      result.emplace_back(key, value);
    }
    return result;
  }

  [[nodiscard]] size_type size() const noexcept {
    return table_.size();
  }

  [[nodiscard]] std::string toString() const noexcept {
    std::stringstream ss;
    ss << size() << "\n";
    for (const auto& [key, value] : table()) {
      ss << key.toString() << " | ";
      if (value.has_value()) {
        ss << value.value();
      } else {
        ss << "null";
      }
      ss << "\n";
    }
    return ss.str();
  }

  static void fromString(const std::string& str, MemTable& table) {
    std::istringstream is(str);
    
    size_t size;
    is >> size;
    
    std::string line;
    std::getline(is, line);
    
    for (size_t i = 0; i < size; ++i) {
      std::getline(is, line);
      auto separator_pos = line.find(" | ");
      
      auto key_str = line.substr(0, separator_pos);
      auto value_str = line.substr(separator_pos + 3);
      
      key_type key = key_type::fromString(key_str);
      std::optional<TValue> value;
      
      if (value_str == "null") {
        value = std::nullopt;
      } else {
        TValue temp;
        std::istringstream{value_str} >> temp;
        value = temp;
      }
      
      table.put(key, value);
    }
  }

private:
  using TimeRange = DataRange<Timestamp>;
  using KeyRange = DataRange<key_type>;
  using Table = std::multimap<const key_type, mapped_type>;

  [[nodiscard]] bool in_range(const key_type& key) const noexcept {
    return time_range_.in_range(key.timestamp()) && key_range_.in_range(key);
  }

  void update_ranges(const key_type& key) noexcept {
    time_range_.update_range(key.timestamp());
    key_range_.update_range(key);
  }

  TimeRange time_range_;
  KeyRange key_range_;
  Table table_;
};

#endif // STORAGE_MEM_TABLE_H