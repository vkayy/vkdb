#ifndef UTILS_RANDOM_H
#define UTILS_RANDOM_H

#include <vkdb/concepts.h>

#include <random>

namespace vkdb {
/**
 * @brief Generate a random number.
 * @details Minimum and maximum values are inclusive, defaulting to
 * the minimum and maximum values of the number type.
 * 
 * @tparam T Number type.
 * @param min Minimum value.
 * @param max Maximum value.
 * @return T Random number.
 */
template <ArithmeticNoCVRefQuals T>
T random(
  T min = std::numeric_limits<T>::min(),
  T max = std::numeric_limits<T>::max()
) noexcept {
  std::random_device rd;
  std::mt19937 gen{rd()};
  if constexpr (std::integral<T>) {
    std::uniform_int_distribution<T> dist{min, max};
    return dist(gen);
  } else {
    std::uniform_real_distribution<T> dist{min, max};
    return dist(gen);
  }
}
}  // namespace vkdb

#endif // UTILS_RANDOM_H