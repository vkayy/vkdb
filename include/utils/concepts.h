#ifndef UTILS_CONCEPTS_H
#define UTILS_CONCEPTS_H

#include <concepts>

template <typename T, typename U>
concept SameNoCVRefQuals =
  std::is_same_v<std::remove_cvref_t<T>, std::remove_cvref_t<U>>;

#endif // UTILS_CONCEPTS_H