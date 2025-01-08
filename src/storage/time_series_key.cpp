#include <vkdb/time_series_key.h>

namespace vkdb {
TimeSeriesKey::TimeSeriesKey(std::string&& str) {
  auto timestamp_end{str.find('}')};
  auto metric_start{str.find('{', timestamp_end) + 1};
  auto metric_end{str.find('}', metric_start)};
  auto tags_start{str.find('{', metric_end) + 1};
  auto tags_end{str.find('}', tags_start)};

  timestamp_ = std::stoull(str.substr(1, timestamp_end - 1));
  metric_ = {str.substr(metric_start, metric_end - metric_start)};
  auto tags_str{str.substr(tags_start, tags_end - tags_start)};

  TagKey key;
  TagValue value;
  std::istringstream ss{tags_str};
  while (std::getline(ss, key, ':')) {
    std::getline(ss, value, ',');
    tags_[key] = value;
  }

  hash_ = std::hash<std::string>{}(std::move(str));
}

TimeSeriesKey::TimeSeriesKey(
  Timestamp timestamp,
  Metric metric,
  TagTable tags
) noexcept
  : timestamp_{timestamp}
  , metric_{std::move(metric)}
  , tags_{std::move(tags)}
  , hash_{std::hash<TimeSeriesKey>{}(*this)} {}

bool TimeSeriesKey::operator==(const TimeSeriesKey& other) const noexcept {
  return hash_ == other.hash_;
}

bool TimeSeriesKey::operator!=(const TimeSeriesKey& other) const noexcept {
  return !(*this == other);
}

bool TimeSeriesKey::operator<(const TimeSeriesKey& other) const noexcept {
  if (other.hash_ == MIN_TIME_SERIES_KEY_HASH) {
    return false;
  }
  if (hash_ == MIN_TIME_SERIES_KEY_HASH) {
    return true;
  }
  if (hash_ == MAX_TIME_SERIES_KEY_HASH) {
    return false;
  }
  if (other.hash_ == MAX_TIME_SERIES_KEY_HASH) {
    return true;
  }
  if (timestamp_ != other.timestamp_) {
    return timestamp_ < other.timestamp_;
  }
  if (metric_ != other.metric_) {
    return metric_ < other.metric_;
  }
  return tags_ < other.tags_;
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

const TagTable& TimeSeriesKey::tags() const noexcept {
  return tags_;
}

std::string TimeSeriesKey::str() const noexcept {
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
}  // namespace vkdb

std::ostream& operator<<(std::ostream& os, const vkdb::TimeSeriesKey& key) {
  os << key.str();
  return os;
}

std::istream& operator>>(std::istream& is, vkdb::TimeSeriesKey& key) {
  std::string str;
  is >> str;
  key = vkdb::TimeSeriesKey{std::move(str)};
  return is;
}
