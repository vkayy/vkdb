#ifndef STORAGE_DATA_RANGE_H
#define STORAGE_DATA_RANGE_H

#include <vkdb/concepts.h>
#include <vkdb/time_series_key.h>

namespace vkdb {
template <RegularNoCVRefQuals TData>
  requires std::totally_ordered<TData>
class DataRange {
public:
  using data_type = TData;

  DataRange() noexcept = default;

  DataRange(std::string&& str) {
    if (str == "null") {
      return;
    }

    auto colon_pos{str.find(':')};
    if (colon_pos == std::string::npos) {
      throw std::invalid_argument{
        "DataRange::DataRange(): Invalid range string '" + str + "'."
      };
    }

    if constexpr (std::is_same_v<data_type, TimeSeriesKey>) {
      range_.first = TimeSeriesKey{str.substr(0, colon_pos)};
      range_.second = TimeSeriesKey{str.substr(colon_pos + 1)};
    } else {
      range_.first = std::stod(str.substr(0, colon_pos));
      range_.second = std::stod(str.substr(colon_pos + 1));
    }

    is_set_ = true;
  }

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

  [[nodiscard]] data_type lower() const {
    if (!is_set_) {
      throw std::logic_error{"DataRange::lower(): Range is not set."};
    }
    return range_.first;
  }

  [[nodiscard]] data_type upper() const {
    if (!is_set_) {
      throw std::logic_error{"DataRange::upper(): Range is not set."};
    }
    return range_.second;
  }

  void clear() noexcept {
    is_set_ = false;
  }

  [[nodiscard]] std::string str() const noexcept {
    if (!is_set_) {
      return "null";
    }

    if constexpr (std::is_same_v<data_type, TimeSeriesKey>) {
      return range_.first.str() + ":" + range_.second.str();
    } else {
      return std::to_string(range_.first) + ":" + std::to_string(range_.second);
    }
  }

private:
  bool is_set_;
  std::pair<data_type, data_type> range_;
};
}  // namespace vkdb

#endif // STORAGE_DATA_RANGE_H