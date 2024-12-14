#ifndef UTILS_RANDOM_H
#define UTILS_RANDOM_H

#include <random>
#include "utils/concepts.h"

namespace vkdb {
template <ArithmeticNoCVRefQuals T>
T random(T min = std::numeric_limits<T>::min(),
         T max = std::numeric_limits<T>::max()) {
  std::random_device rd;
  std::mt19937 gen{rd()};
  std::uniform_int_distribution<T> dist{min, max};
  return dist(gen);
}
}  // namespace vkdb

#endif // UTILS_RANDOM_H