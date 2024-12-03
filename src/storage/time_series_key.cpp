#include "storage/time_series_key.h"

TimeSeriesKey::TimeSeriesKey(Timestamp timestamp,
                             Metric metric, Tags tags) noexcept
  : timestamp_{timestamp}
  , metric_{std::move(metric)}
  , tags_{std::move(tags)} {}

bool TimeSeriesKey::operator==(const TimeSeriesKey& other) const noexcept {
  return timestamp_ == other.timestamp_ &&
         metric_ == other.metric_ &&
         tags_ == other.tags_;
}

bool TimeSeriesKey::operator!=(const TimeSeriesKey& other) const noexcept {
  return !(*this == other);
}

bool TimeSeriesKey::operator<(const TimeSeriesKey& other) const noexcept {
  if (metric_ != other.metric_) {
    return metric_ < other.metric_;
  }
  if (tags_ != other.tags_) {
    return tags_ < other.tags_;
  }
  return timestamp_ < other.timestamp_;
}

bool TimeSeriesKey::operator>(const TimeSeriesKey& other) const noexcept {
  return other < *this;
}

bool TimeSeriesKey::operator<=(const TimeSeriesKey& other) const noexcept {
  return !(*this > other);
}

bool TimeSeriesKey::operator>=(const TimeSeriesKey& other) const noexcept {
  return !(*this < other);
}

Timestamp TimeSeriesKey::timestamp() const noexcept {
  return timestamp_;
}

Metric TimeSeriesKey::metric() const noexcept {
  return metric_;
}

const Tags& TimeSeriesKey::tags() const noexcept {
  return tags_;
}

std::string TimeSeriesKey::toString() const noexcept {
  std::stringstream ss;
  ss << "{" << std::setw(TIMESTAMP_WIDTH) << std::setfill('0');
  ss << timestamp_ << "}{" << metric_ << "}{";
  for (const auto& [key, value] : tags_) {
    ss << key << ":" << value << ",";
  }
  ss.seekp(-!tags_.empty(), std::ios_base::end);
  ss << "}";
  return ss.str();
}

TimeSeriesKey TimeSeriesKey::fromString(const std::string& str) {
  auto timestamp_end{str.find('}')};
  auto metric_start{str.find('{', timestamp_end) + 1};
  auto metric_end{str.find('}', metric_start)};
  auto tags_start{str.find('{', metric_end) + 1};
  auto tags_end{str.find('}', tags_start)};

  auto timestamp{std::stoull(str.substr(1, timestamp_end - 1))};
  auto metric{str.substr(metric_start, metric_end - metric_start)};
  auto tags_str{str.substr(tags_start, tags_end - tags_start)};

  Tags tags;
  TagKey key;
  TagValue value;
  std::istringstream ss{tags_str};
  while (std::getline(ss, key, ':')) {
    std::getline(ss, value, ',');
    tags[key] = value;
  }

  return TimeSeriesKey{timestamp, metric, tags};
}