#include "utils/random.hpp"
#include <ctime>
#include <random>

int32_t generateRandomInt32(int32_t start, int32_t end) {
    thread_local auto gen = randomGenerator<>();
    std::uniform_int_distribution<> dist(start, end);
    return dist(gen);
}

std::time_t generateRandomTimestamp() {
    return std::time_t(generateRandomInt32(1, 100000));
}

std::string generateRandomString(size_t length) {
    constexpr static auto chars =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    thread_local auto rng = randomGenerator<>();
    auto dist = std::uniform_int_distribution{{}, std::strlen(chars) - 1};
    auto result = std::string(length, '\0');
    std::generate_n(result.begin(), length, [&]() { return chars[dist(rng)]; });
    return result;
}
