#ifndef STORAGE_DATA_RANGE_H
#define STORAGE_DATA_RANGE_H

#include <vkdb/concepts.h>
#include <vkdb/time_series_key.h>

namespace vkdb {
/**
 * @brief A range of data.
 * 
 * @tparam TData The data type.
 */
template <RegularNoCVRefQuals TData>
  requires std::totally_ordered<TData>
class DataRange {
public:
  using data_type = TData;

  /**
   * @brief Construct a new DataRange object.
   * 
   */
  DataRange() noexcept = default;

  /**
   * @brief Construct a new DataRange object from the given start and end.
   * 
   * @param start The start of the range.
   * @param end The end of the range.
   */
  DataRange(const data_type& start, const data_type& end) noexcept
    : is_set_{true}, range_{start, end} {}

  /**
   * @brief Construct a new DataRange object from the given string.
   * 
   * @param str The string representation of the range.
   * 
   * @throws std::invalid_argument If the range string is invalid.
   */
  DataRange(std::string&& str) {
    if (str == "null") {
      return;
    }

    auto ampersand_pos{str.find('&')};
    if (ampersand_pos == std::string::npos) {
      throw std::invalid_argument{
        "DataRange::DataRange(): Invalid range string '" + str + "'."
      };
    }

    if constexpr (std::is_same_v<data_type, TimeSeriesKey>) {
      range_.first = TimeSeriesKey{str.substr(0, ampersand_pos)};
      range_.second = TimeSeriesKey{str.substr(ampersand_pos + 1)};
    } else {
      range_.first = std::stod(str.substr(0, ampersand_pos));
      range_.second = std::stod(str.substr(ampersand_pos + 1));
    }

    is_set_ = true;
  }

  /**
   * @brief Move-construct a DataRange object.
   * 
   */
  DataRange(DataRange&&) noexcept = default;

  /**
   * @brief Move-assign a DataRange object.
   * 
   */
  DataRange& operator=(DataRange&&) noexcept = default;

  /**
   * @brief Copy-construct a DataRange object.
   * 
   */
  DataRange(const DataRange&) noexcept = default;

  /**
   * @brief Copy-assign a DataRange object.
   * 
   */
  DataRange& operator=(const DataRange&) noexcept = default;

  /**
   * @brief Destroy the DataRange object.
   * 
   */
  ~DataRange() noexcept = default;

  [[nodiscard]] auto operator<=>(const DataRange& other) const noexcept {
    if (!is_set_) {
      return std::strong_ordering::less;
    }
    if (!other.is_set_) {
      return std::strong_ordering::greater;
    }
    return range_ <=> other.range_;
  }

  /**
   * @brief Update the range with the given data.
   * @details If the range is not set, the range is set to the data, otherwise
   * the range is updated to include the data.
   * 
   * @param data The data.
   */
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

  /**
   * @brief Check if the data is in the range.
   * 
   * @param data The data.
   * @return true if the data is in the range.
   * @return false if the data is not in the range.
   */
  [[nodiscard]] bool inRange(const data_type& data) const noexcept {
    return is_set_ && data >= range_.first && data <= range_.second;
  }

  /**
   * @brief Check if the range overlaps with the given range.
   * 
   * @param start The start of the range.
   * @param end The end of the range.
   * @return true if the ranges overlap.
   * @return false if the ranges do not overlap.
   */
  [[nodiscard]] bool overlapsWith(
    const data_type& start,
    const data_type& end
  ) const noexcept {
    return is_set_ && range_.first <= end && range_.second >= start;
  }

  /**
   * @brief Get the lower bound of the range.
   * 
   * @return data_type The lower bound of the range.
   * 
   * @throws std::logic_error If the range is not set.
   */
  [[nodiscard]] data_type lower() const {
    if (!is_set_) {
      throw std::logic_error{"DataRange::lower(): Range is not set."};
    }
    return range_.first;
  }

  /**
   * @brief Get the upper bound of the range.
   * 
   * @return data_type The upper bound of the range.
   * 
   * @throws std::logic_error If the range is not set.
   */
  [[nodiscard]] data_type upper() const {
    if (!is_set_) {
      throw std::logic_error{"DataRange::upper(): Range is not set."};
    }
    return range_.second;
  }

  /**
   * @brief Clear the range.
   * 
   */
  void clear() noexcept {
    is_set_ = false;
  }

  /**
   * @brief Convert the range to a string.
   * 
   * @return std::string The string representation of the range.
   */
  [[nodiscard]] std::string str() const noexcept {
    if (!is_set_) {
      return "null";
    }

    if constexpr (std::is_same_v<data_type, TimeSeriesKey>) {
      return range_.first.str() + "&" + range_.second.str();
    } else {
      return std::to_string(range_.first)
        + "&" + std::to_string(range_.second);
    }
  }

private:
  /**
   * @brief Flag to indicate if the range is set.
   * 
   */
  bool is_set_;

  /**
   * @brief The lower and upper bounds of the range.
   * 
   */
  std::pair<data_type, data_type> range_;
};

/**
 * @brief Type alias for timestamp data range.
 * 
 */
using TimeRange = DataRange<Timestamp>;

/**
 * @brief Type alias for key data range.
 * 
 */
using KeyRange = DataRange<TimeSeriesKey>;
}  // namespace vkdb

#endif // STORAGE_DATA_RANGE_H