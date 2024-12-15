#ifndef UTILS_MURMUR_HASH_3_HPP
#define UTILS_MURMUR_HASH_3_HPP

#include <cstdint>

void MurmurHash3_x86_32(const void *key, const int len, const uint32_t seed, void *out);

void MurmurHash3_x86_128(const void *key, const int len, const uint32_t seed, void *out);

#endif // UTILS_MURMUR_HASH_3_HPP
