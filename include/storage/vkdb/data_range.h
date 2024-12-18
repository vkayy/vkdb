#ifndef STORAGE_DATA_RANGE_H
#define STORAGE_DATA_RANGE_H

#include <vkdb/concepts.h>

namespace vkdb {
template <RegularNoCVRefQuals TData>
  requires std::totally_ordered<TData>
class DataRange {
public:
  using data_type = TData;

  DataRange() noexcept = default;

  DataRange(DataRange&&) noexcept = default;
  DataRange& operator=(DataRange&&) noexcept = default;

  DataRange(const DataRange&) = delete;
  DataRange& operator=(const DataRange&) = delete;

  ~DataRange() = default;

  void updateRange(const data_type& data) noexcept {
    if (!is_set_) {
      range_.first = data;
      range_.second = data;
      is_set_ = true;
      return;
    }

    range_.first = std::min(range_.first, data);
    range_.second = std::max(range_.second, data);
  }

  [[nodiscard]] bool inRange(const data_type& data) const noexcept {
    return is_set_ && data >= range_.first && data <= range_.second;
  }

  [[nodiscard]] bool overlaps_with(const data_type& start,
                              const data_type& end) const noexcept {
    return is_set_ && range_.first <= end && range_.second >= start;
  }

  [[nodiscard]] const data_type& lower() const {
    if (!is_set_) {
      throw std::logic_error{"DataRange::lower(): Range is not set."};
    }
    return range_.first;
  }

  [[nodiscard]] const data_type& upper() const {
    if (!is_set_) {
      throw std::logic_error{"DataRange::upper(): Range is not set."};
    }
    return range_.second;
  }

  void clear() noexcept {
    is_set_ = false;
  }

private:
  bool is_set_;
  std::pair<data_type, data_type> range_;
};
}  // namespace vkdb

#endif // STORAGE_DATA_RANGE_H