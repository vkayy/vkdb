#ifndef UTILS_CONCEPTS_H
#define UTILS_CONCEPTS_H

#include <concepts>

template <typename T, typename U>
concept SameNoCVRefQuals =
  std::is_same_v<std::remove_cvref_t<T>, std::remove_cvref_t<U>>;

template <typename T>
concept HasNoCVRefQuals = std::is_same_v<T, std::remove_cvref_t<T>>;

template <typename T>
concept RegularNoCVRefQuals = std::regular<T> && HasNoCVRefQuals<T>;

template <typename T>
concept Arithmetic = std::is_arithmetic_v<T>;

template <typename T>
concept ArithmeticNoCVRefQuals = Arithmetic<T> && HasNoCVRefQuals<T>;

template <typename U, typename... Ts>
concept AllSameNoCVRefEquals = (SameNoCVRefQuals<Ts, U> && ...);

template <typename T, typename U>
concept ConvertibleNoCVRefEquals = std::convertible_to<T, U> && HasNoCVRefQuals<T>;

template <typename U, typename... Ts>
concept AllConvertibleNoCVRefEquals = (ConvertibleNoCVRefEquals<Ts, U> && ...);

#endif // UTILS_CONCEPTS_H