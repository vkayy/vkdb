#ifndef STORAGE_TIME_SERIES_KEY_H
#define STORAGE_TIME_SERIES_KEY_H

#include <string>
#include <map>
#include <sstream>
#include <iomanip>
#include "utils/concepts.h"

using Timestamp = uint64_t;
using Metric = std::string;
using TagKey = std::string;
using TagValue = std::string;
using Tag = std::pair<TagKey, TagValue>;
using TagTable = std::map<TagKey, TagValue>;

class TimeSeriesKey {
public:
  static constexpr auto TIMESTAMP_WIDTH{20};

  TimeSeriesKey() = default;

  explicit TimeSeriesKey(Timestamp timestamp,
                         Metric metric, TagTable tags) noexcept;

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

std::ostream& operator<<(std::ostream& os, const TimeSeriesKey& key);

std::istream& operator>>(std::istream& is, TimeSeriesKey& key);

namespace std {
template <>
struct hash<TimeSeriesKey> {
  size_t operator()(const TimeSeriesKey& key) const noexcept {
    return hash<string>{}(key.toString());
  }
};
}  // namespace std

template <ArithmeticNoCVRefQuals TValue>
using TimeSeriesEntry = std::pair<const TimeSeriesKey, std::optional<TValue>>;

static const TimeSeriesKey MIN_TIME_SERIES_KEY{
  0,
  "MIN_TIME_SERIES_KEY",
  {{"MIN_TIME_SERIES_KEY", "MIN_TIME_SERIES_KEY"}}
};

static const TimeSeriesKey MAX_TIME_SERIES_KEY{
  std::numeric_limits<Timestamp>::max(),
  "MAX_TIME_SERIES_KEY",
  {{"MAX_TIME_SERIES_KEY", "MAX_TIME_SERIES_KEY"}}
};

#endif // STORAGE_TIME_SERIES_KEY_H