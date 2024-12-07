#ifndef STORAGE_TIME_SERIES_KEY_H
#define STORAGE_TIME_SERIES_KEY_H

#include <string>
#include <map>
#include <sstream>
#include <iomanip>

using Timestamp = uint64_t;
using Metric = std::string;
using TagKey = std::string;
using TagValue = std::string;
using Tags = std::map<TagKey, TagValue>;

class TimeSeriesKey {
public:
  static constexpr auto TIMESTAMP_WIDTH{20};

  TimeSeriesKey() = default;

  explicit TimeSeriesKey(Timestamp timestamp,
                         Metric metric, Tags tags) noexcept;

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
  [[nodiscard]] const Tags& tags() const noexcept;
  [[nodiscard]] std::string toString() const noexcept;

  [[nodiscard]] static TimeSeriesKey fromString(const std::string& str);

private:
  Timestamp timestamp_;
  Metric metric_;
  Tags tags_;
};

std::ostream& operator<<(std::ostream& os, const TimeSeriesKey& key);

std::istream& operator>>(std::istream& is, TimeSeriesKey& key);

#endif // STORAGE_TIME_SERIES_KEY_H