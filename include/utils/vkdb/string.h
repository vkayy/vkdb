#ifndef UTILS_STRING_H
#define UTILS_STRING_H

#include <vkdb/time_series_key.h>
#include <vkdb/concepts.h>
#include <optional>
#include <sstream>
#include <iostream>

namespace vkdb {
/**
 * @brief Convert a string to a TimeSeriesEntry.
 * 
 * @tparam TValue Value type.
 * @param entry String representation.
 * @return TimeSeriesEntry<TValue> Entry.
 * 
 * @throw std::exception If the string representation is invalid.
 */
template <ArithmeticNoCVRefQuals TValue>
TimeSeriesEntry<TValue> entryFromString(std::string&& entry) {
  auto sep{entry.find('|')};
  auto end{entry.find(']')};
  auto key_str{entry.substr(0, sep)};
  auto value_str{entry.substr(sep + 1, end - sep - 1)};

  auto entry_key{TimeSeriesKey{std::move(key_str)}};
  std::optional<TValue> entry_value;
  if (value_str != "null") {
    if constexpr (std::is_integral_v<TValue>) {
      entry_value = static_cast<TValue>(std::stoll(value_str));
    } else if constexpr (std::is_floating_point_v<TValue>) {
      entry_value = static_cast<TValue>(std::stod(value_str)); 
    }
  }

  return {entry_key, entry_value};
}

/**
 * @brief Convert a TimeSeriesEntry to a string.
 * 
 * @tparam TValue Value type. 
 * @param entry Entry.
 * @return std::string String representation.
 * 
 * @throw std::exception If the entry is invalid.
 */
template <ArithmeticNoCVRefQuals TValue>
std::string entryToString(const TimeSeriesEntry<TValue>& entry) {
  std::stringstream ss;
  ss << "[" << entry.first.str() << "|";
  if (entry.second.has_value()) {
    ss << entry.second.value();
  } else {
    ss << "null";
  }
  ss << "]";
  return ss.str();
}

/**
 * @brief Convert a string to a vector of datapoints.
 * 
 * @tparam TValue Value type.
 * @param datapoints String representation.
 * @return std::vector<DataPoint<TValue>> Datapoints.
 * 
 * @throw std::exception If the string representation is invalid.
 */
template <ArithmeticNoCVRefQuals TValue>
std::vector<DataPoint<TValue>> datapointsFromString(
  const std::string& datapoints
) {
  std::vector<DataPoint<TValue>> result;
  std::string data{datapoints.substr(1, datapoints.size() - 2)};
  std::istringstream ss{data};
  std::string entry_str;
  while (std::getline(ss, entry_str, ';')) {
    auto entry_data{entryFromString<TValue>(entry_str.substr(1))};
    result.push_back({
      entry_data.first.timestamp(),
      entry_data.first.metric(),
      entry_data.first.tags(),
      entry_data.second.value()
    });
  }
  return result;
}

/**
 * @brief Convert a vector of datapoints to a string.
 * 
 * @tparam TValue Value type.
 * @param datapoints Datapoints.
 * @return std::string String representation.
 * 
 * @throw std::exception If any entry is invalid.
 */
template <ArithmeticNoCVRefQuals TValue>
std::string datapointsToString(
  const std::vector<DataPoint<TValue>>& datapoints
) {
  std::ostringstream output;
  output << "[";
  for (const auto& datapoint : datapoints) {
    TimeSeriesKey key{datapoint.timestamp, datapoint.metric, datapoint.tags};
    TimeSeriesEntry<TValue> entry{key, datapoint.value};
    output << entryToString(entry) << ";";
  }
  output.seekp(-!datapoints.empty(), std::ios_base::end);
  output << "]";
  return output.str();
}
}  // namespace vkdb

#endif // UTILS_STRING_H