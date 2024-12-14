#ifndef STORAGE_TIME_SERIES_KEY_H
#define STORAGE_TIME_SERIES_KEY_H

#include <string>
#include <map>
#include <sstream>
#include <iomanip>
#include "utils/concepts.h"

namespace vkdb {
using Timestamp = uint64_t;
using Metric = std::string;
using TagKey = std::string;
using TagValue = std::string;
using Tag = std::pair<TagKey, TagValue>;
using TagTable = std::map<TagKey, TagValue>;

template <ArithmeticNoCVRefQuals TValue>
struct DataPoint {
  Timestamp timestamp;
  Metric metric;
  TagTable tags;
  TValue value;
};

class TimeSeriesKey {
public:
  static constexpr auto TIMESTAMP_WIDTH{20};
  static constexpr auto MAX_METRIC_LENGTH{64};

  TimeSeriesKey() = default;

  explicit TimeSeriesKey(Timestamp timestamp, Metric metric, TagTable tags);

  TimeSeriesKey(TimeSeriesKey&&) noexcept = default;
  TimeSeriesKey& operator=(TimeSeriesKey&&) noexcept = default;

  TimeSeriesKey(const TimeSeriesKey&) noexcept = default;
  TimeSeriesKey& operator=(const TimeSeriesKey&) noexcept = default;

  ~TimeSeriesKey() = default;

  [[nodiscard]] bool operator==(const TimeSeriesKey& other) const noexcept;
  [[nodiscard]] bool operator!=(const TimeSeriesKey& other) const noexcept;
  [[nodiscard]] bool operator<(const TimeSeriesKey& other) const noexcept;
  [[nodiscard]] bool operator>(const TimeSeriesKey& other) const noexcept;
  [[nodiscard]] bool operator<=(const TimeSeriesKey& other) const noexcept;
  [[nodiscard]] bool operator>=(const TimeSeriesKey& other) const noexcept;

  [[nodiscard]] Timestamp timestamp() const noexcept;
  [[nodiscard]] Metric metric() const noexcept;
  [[nodiscard]] const TagTable& tags() const noexcept;
  [[nodiscard]] std::string toString() const noexcept;

  [[nodiscard]] static TimeSeriesKey fromString(const std::string& str);

private:
  Timestamp timestamp_;
  Metric metric_;
  TagTable tags_;
};

template <ArithmeticNoCVRefQuals TValue>
using TimeSeriesEntry = std::pair<const TimeSeriesKey, std::optional<TValue>>;
}  // namespace vkdb

std::ostream& operator<<(std::ostream& os, const vkdb::TimeSeriesKey& key);

std::istream& operator>>(std::istream& is, vkdb::TimeSeriesKey& key);

static const vkdb::TimeSeriesKey MIN_TIME_SERIES_KEY{
  0,
  "MIN_TIME_SERIES_KEY",
  {{"MIN_TIME_SERIES_KEY", "MIN_TIME_SERIES_KEY"}}
};

static const vkdb::TimeSeriesKey MAX_TIME_SERIES_KEY{
  std::numeric_limits<vkdb::Timestamp>::max(),
  "MAX_TIME_SERIES_KEY",
  {{"MAX_TIME_SERIES_KEY", "MAX_TIME_SERIES_KEY"}}
};

const vkdb::Metric MIN_METRIC{std::string()};
const vkdb::Metric MAX_METRIC{
  std::string(vkdb::TimeSeriesKey::MAX_METRIC_LENGTH + 1, '\xFF')
};

namespace std {
template <>
struct hash<vkdb::TimeSeriesKey> {
  size_t operator()(const vkdb::TimeSeriesKey& key) const noexcept {
    return hash<string>{}(key.toString());
  }
};
}  // namespace std


#endif // STORAGE_TIME_SERIES_KEY_H