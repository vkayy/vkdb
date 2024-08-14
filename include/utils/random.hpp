#ifndef RANDOM_HPP
#define RANDOM_HPP

#include <ctime>
#include <random>
#include <string>

/**
 * @brief Creates a random number generator with a seed based on random device.
 *
 * @tparam T The type of the random number generated, defaulting to `std::mt19937`.
 * @return `T` A random number generator, seeded with a random sequence.
 */
template <typename T = std::mt19937>
T randomGenerator() {
    auto constexpr seed_bytes = sizeof(typename T::result_type) * T::state_size;
    auto constexpr seed_len = seed_bytes / sizeof(std::seed_seq::result_type);
    auto seed = std::array<std::seed_seq::result_type, seed_len>();
    auto rd = std::random_device();
    std::generate_n(begin(seed), seed_len, std::ref(rd));
    auto seed_seq = std::seed_seq(begin(seed), end(seed));
    return T{seed_seq};
}

/**
 * @brief Generates a random 32-bit integer within a specified range.
 *
 * The generated 32-bit integer will be between `start` and `end` (inclusive).
 *
 * @param start The lower bound of the random integer range.
 * @param end The upper bound of the random integer range.
 * @return `int32_t` A randomly generated 32-bit integer within the specified range.
 */
int32_t generateRandomInt32(int32_t start, int32_t end);

/**
 * @brief Generates a random timestamp within a specified range.
 *
 * Creates a random timestamp, which is essentially a random 32-bit integer
 * between 1 and 100000, and then casts it to `std::time_t`.
 *
 * @return `std::time_t` A randomly generated timestamp.
 */
std::time_t generateRandomTime();

/**
 * @brief Generates a random string of a specified length.
 *
 * Creates a string of a given length, consisting of random characters
 * from a predefined set of alphanumeric characters.
 *
 * @param length The length of the random string to generate.
 * @return `std::string` A randomly generated string of the specified length.
 */
std::string generateRandomString(size_t length);

#endif // RANDOM_HPP
