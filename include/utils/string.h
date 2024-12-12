#ifndef UTILS_STRING_H
#define UTILS_STRING_H

#include "storage/time_series_key.h"
#include "utils/concepts.h"
#include <optional>
#include <sstream>

template <ArithmeticNoCVRefQuals TValue>
TimeSeriesEntry<TValue> entryFromString(const std::string& entry) {
  auto sep{entry.find('|')};
  auto end{entry.find(']')};
  auto key_str{entry.substr(0, sep)};
  auto value_str{entry.substr(sep + 1, end - sep - 1)};

  auto entry_key{TimeSeriesKey::fromString(key_str)};
  std::optional<TValue> entry_value;
  if (value_str != "null") {
    std::stringstream value_ss{value_str};
    TValue value;
    value_ss >> value;
    entry_value = value;
  }

  return {entry_key, entry_value};
}

template <ArithmeticNoCVRefQuals TValue>
std::string entryToString(const TimeSeriesEntry<TValue>& entry) {
  std::stringstream ss;
  ss << "[" << entry.first.toString() << "|";
  if (entry.second.has_value()) {
    ss << entry.second.value();
  } else {
    ss << "null";
  }
  ss << "]";
  return ss.str();
}

#endif // UTILS_STRING_H