#ifndef RANDOM_HPP
#define RANDOM_HPP

#include <ctime>
#include <string>

int32_t generateRandomInt32(int32_t start, int32_t end);

std::time_t generateRandomTime();

std::string generateRandomString(size_t length);

#endif // RANDOM_HPP
