#ifndef STORAGE_MEM_TABLE_H
#define STORAGE_MEM_TABLE_H

#include "utils/concepts.h"
#include "storage/time_series_key.h"
#include "storage/data_range.h"

template <Arithmetic TValue>
class MemTable {
public:
  using key_type = TimeSeriesKey;
  using mapped_type = TValue;
  using size_type = uint64_t;
  using const_mapped = const mapped_type;
  using opt_const_mapped = std::optional<const_mapped>;

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

  [[nodiscard]] opt_const_mapped get(const key_type& key) const {
    if (!in_range(key)) {
      return std::nullopt;
    }
    auto it{table_.find(key)};
    if (it == table_.end()) {
      return std::nullopt;
    }
    return it->second;
  }

private:
  using TimeRange = DataRange<Timestamp>;
  using KeyRange = DataRange<key_type>;
  using Table = std::multimap<key_type, opt_const_mapped>;

  bool in_range(const key_type& key) const noexcept {
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