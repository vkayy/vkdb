#ifndef UTILS_CONCEPTS_H
#define UTILS_CONCEPTS_H

#include <concepts>

/**
 * @brief Concept for types that are the same after removing cv-
 * and ref-qualifiers.
 * 
 * @tparam T First type.
 * @tparam U Second type.
 */
template <typename T, typename U>
concept SameNoCVRefQuals =
  std::is_same_v<std::remove_cvref_t<T>, std::remove_cvref_t<U>>;

/**
 * @brief Concept for types that have no cv- or ref-qualifiers.
 * 
 * @tparam T Type.
 */
template <typename T>
concept HasNoCVRefQuals = std::is_same_v<T, std::remove_cvref_t<T>>;

/**
 * @brief Concept for regular types that have no cv- or ref-qualifiers.
 * 
 * @tparam T Type.
 */
template <typename T>
concept RegularNoCVRefQuals = std::regular<T> && HasNoCVRefQuals<T>;

/**
 * @brief Concept for arithmetic types.
 * 
 * @tparam T Type.
 */
template <typename T>
concept Arithmetic = std::is_arithmetic_v<T>;

/**
 * @brief Concept for arithmetic types that have no cv- or ref-qualifiers.
 * 
 * @tparam T Type.
 */
template <typename T>
concept ArithmeticNoCVRefQuals = Arithmetic<T> && HasNoCVRefQuals<T>;

/**
 * @brief Concept for types that are all the same as another after
 * removing cv- and ref-qualifiers.
 * 
 * @tparam Ts Types to compare.
 * @tparam U Type to compare against.
 */
template <typename... Ts, typename U>
concept AllSameNoCVRefQuals = (SameNoCVRefQuals<Ts, U> && ...);

/**
 * @brief Concept for a type that is convertible to another and
 * has no cv- or ref-qualifiers.
 * 
 * @tparam T Type to convert.
 * @tparam U Type to convert to.
 */
template <typename T, typename U>
concept ConvertibleToNoCVRefQuals
  = std::convertible_to<T, U> && HasNoCVRefQuals<T>;

/**
 * @brief Concept for types that are all convertible to another and
 * have no cv- or ref-qualifiers.
 * 
 * @tparam Ts Types to convert.
 * @tparam U Type to convert to.
 */
template <typename... Ts, typename U>
concept AllConvertibleToNoCVRefQuals
  = (ConvertibleToNoCVRefQuals<Ts, U> && ...);

/**
 * @brief Concept for a type that is constructible to another and
 * has no cv- or ref-qualifiers.
 * 
 * @tparam T Type to construct from.
 * @tparam U Type to construct to.
 */
template <typename T, typename U>
concept ConstructibleToNoCVRefQuals
  = std::constructible_from<U, T> && HasNoCVRefQuals<T>;

/**
 * @brief Concept for types that are all constructible to another and
 * have no cv- or ref-qualifiers.
 * 
 * @tparam Ts Types to construct from.
 * @tparam U Type to construct to.
 */
template <typename... Ts, typename U>
concept AllConstructibleToNoCVRefQuals
  = (ConstructibleToNoCVRefQuals<Ts, U> && ...);


#endif // UTILS_CONCEPTS_H