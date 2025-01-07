#ifndef STORAGE_TIME_SERIES_KEY_H
#define STORAGE_TIME_SERIES_KEY_H

#include <string>
#include <map>
#include <optional>
#include <sstream>
#include <iomanip>
#include <vkdb/concepts.h>

namespace vkdb {
/**
 * @brief Type alias for uint64_t.
 * 
 */
using Timestamp = uint64_t;

/**
 * @brief Type alias for string.
 * 
 */
using Metric = std::string;

/**
 * @brief Type alias for string.
 * 
 */
using TagKey = std::string;

/**
 * @brief Type alias for string.
 * 
 */
using TagValue = std::string;

/**
 * @brief Type alias for a pair of tag key and tag value.
 * 
 */
using Tag = std::pair<TagKey, TagValue>;

/**
 * @brief Type alias for an ordered map of tag keys to tag values.
 * 
 */
using TagTable = std::map<TagKey, TagValue>;

/**
 * @brief Represents a datapoint in vkdb.
 * 
 * @tparam TValue Value type.
 */
template <ArithmeticNoCVRefQuals TValue>
struct DataPoint {
  /**
   * @brief Timestamp.
   * 
   */
  Timestamp timestamp;

  /**
   * @brief Metric.
   * 
   */
  Metric metric;

  /**
   * @brief Tags.
   * 
   */
  TagTable tags;

  /**
   * @brief Value.
   * 
   */
  TValue value;
};

/**
 * @brief Represents a key in vkdb.
 * 
 */
class TimeSeriesKey {
public:
  /**
   * @brief Timestamp width.
   * 
   */
  static constexpr auto TIMESTAMP_WIDTH{20};

  /**
   * @brief Metric width.
   * 
   */
  static constexpr auto MAX_METRIC_LENGTH{15};

  /**
   * @brief Construct a new TimeSeriesKey object.
   * 
   */
  TimeSeriesKey() noexcept = default;

  /**
   * @brief Construct a new TimeSeriesKey object from the given string.
   * 
   * @param str The string representation of the key.
   * 
   * @throws std::exception If the string is invalid.
   */
  explicit TimeSeriesKey(std::string&& str);

  /**
   * @brief Construct a new Time Series Key objec from the given timestamp,
   * metric, and tags.
   * 
   * @param timestamp Timestamp.
   * @param metric Metric.
   * @param tags Tags.
   */
  explicit TimeSeriesKey(
    Timestamp timestamp,
    Metric metric,
    TagTable tags
  ) noexcept;

  /**
   * @brief Move-construct a new TimeSeriesKey object.
   * 
   */
  TimeSeriesKey(TimeSeriesKey&&) noexcept = default;
  
  /**
   * @brief Move-assign a new TimeSeriesKey object.
   * 
   */
  TimeSeriesKey& operator=(TimeSeriesKey&&) noexcept = default;

  /**
   * @brief Copy-construct a new TimeSeriesKey object.
   * 
   */
  TimeSeriesKey(const TimeSeriesKey&) noexcept = default;
  
  /**
   * @brief Copy-assign a new TimeSeriesKey object.
   * 
   */
  TimeSeriesKey& operator=(const TimeSeriesKey&) noexcept = default;

  /**
   * @brief Destroy the TimeSeriesKey object.
   * 
   */
  ~TimeSeriesKey() noexcept = default;

  /**
   * @brief Equality operator.
   * 
   * @param other The other key.
   * 
   * @return true If the keys are equal.
   * @return false If the keys are not equal.
   */
  [[nodiscard]] bool operator==(const TimeSeriesKey& other) const noexcept;

  /**
   * @brief Inequality operator.
   * 
   * @param other The other key.
   * 
   * @return true If the keys are not equal.
   * @return false If the keys are equal.
   */
  [[nodiscard]] bool operator!=(const TimeSeriesKey& other) const noexcept;

  /**
   * @brief Less-than operator.
   * @details First, the keys are checked to see if they are the minimum or
   * maximum keys. If they aren't, the comparison is done based on the key's
   * properties. The comparison is done in the following order:
   * The comparison is done in the following order:
   * 1. Timestamp.
   * 2. Metric.
   * 3. Tags.
   * 
   * @param other The other key.
   * 
   * @return true If this key is less than the other key.
   * @return false If this key is not less than the other key.
   */
  [[nodiscard]] bool operator<(const TimeSeriesKey& other) const noexcept;

  /**
   * @brief Greater-than operator.
   * 
   * @param other The other key.
   * 
   * @return true If this key is greater than the other key.
   * @return false If this key is not greater than the other key.
   */
  [[nodiscard]] bool operator>(const TimeSeriesKey& other) const noexcept;

  /**
   * @brief Less-than-or-equal-to operator.
   * 
   * @param other The other key.
   * 
   * @return true If this key is less than or equal to the other key.
   * @return false If this key is not less than or equal to the other key.
   */
  [[nodiscard]] bool operator<=(const TimeSeriesKey& other) const noexcept;

  /**
   * @brief Greater-than-or-equal-to operator.
   * 
   * @param other The other key.
   * 
   * @return true If this key is greater than or equal to the other key.
   * @return false If this key is not greater than or equal to the other key.
   */
  [[nodiscard]] bool operator>=(const TimeSeriesKey& other) const noexcept;

  /**
   * @brief Get the timestamp.
   * 
   * @return Timestamp The timestamp.
   */
  [[nodiscard]] Timestamp timestamp() const noexcept;

  /**
   * @brief Get the metric.
   * 
   * @return Metric The metric.
   */
  [[nodiscard]] Metric metric() const noexcept;

  /**
   * @brief Get the tags.
   * 
   * @return const TagTable& The tags.
   */
  [[nodiscard]] const TagTable& tags() const noexcept;

  /**
   * @brief Get the string representation of the key.
   * 
   * @return std::string The string representation of the key.
   */
  [[nodiscard]] std::string str() const noexcept;

private:
  /**
   * @brief Timestamp.
   * 
   */
  Timestamp timestamp_;

  /**
   * @brief Metric.
   * 
   */
  Metric metric_;
  
  /**
   * @brief Tags.
   * 
   */
  TagTable tags_;
};

/**
 * @brief Type alias for a pair of TimeSeriesKey and optional value.
 * 
 * @tparam TValue Value type.
 */
template <ArithmeticNoCVRefQuals TValue>
using TimeSeriesEntry = std::pair<const TimeSeriesKey, std::optional<TValue>>;
}  // namespace vkdb

/**
 * @brief Overload of the stream insertion operator for TimeSeriesKey.
 * 
 * @param os Output stream.
 * @param key Key.
 * 
 * @return std::ostream& The output stream.
 */
std::ostream& operator<<(std::ostream& os, const vkdb::TimeSeriesKey& key);

/**
 * @brief Overload of the stream extraction operator for TimeSeriesKey.
 * 
 * @param is Input stream.
 * @param key TimeSeriesKey.
 * 
 * @return std::istream& The input stream.
 */
std::istream& operator>>(std::istream& is, vkdb::TimeSeriesKey& key);

/**
 * @brief Minimum TimeSeriesKey.
 * 
 */
static const vkdb::TimeSeriesKey MIN_TIME_SERIES_KEY{
  0,
  "MIN_TIME_SERIES_KEY",
  {{"MIN_TIME_SERIES_KEY", "MIN_TIME_SERIES_KEY"}}
};

/**
 * @brief Maximum TimeSeriesKey.
 * 
 */
static const vkdb::TimeSeriesKey MAX_TIME_SERIES_KEY{
  std::numeric_limits<vkdb::Timestamp>::max(),
  "MAX_TIME_SERIES_KEY",
  {{"MAX_TIME_SERIES_KEY", "MAX_TIME_SERIES_KEY"}}
};

/**
 * @brief Minimum metric.
 * 
 */
const vkdb::Metric MIN_METRIC{std::string()};

/**
 * @brief Maximum metric.
 * 
 */
const vkdb::Metric MAX_METRIC{
  std::string(vkdb::TimeSeriesKey::MAX_METRIC_LENGTH + 1, '\xFF')
};

namespace std {
/**
 * @brief Specialisation of std::hash for TimeSeriesKey.
 * 
 * @tparam TimeSeriesKey TimeSeriesKey.
 */
template <>
struct hash<vkdb::TimeSeriesKey> {
  size_t operator()(const vkdb::TimeSeriesKey& key) const noexcept {
    return hash<string>{}(key.str());
  }
};
}  // namespace std


#endif // STORAGE_TIME_SERIES_KEY_H