#ifndef MURMUR_HASH_3_HPP
#define MURMUR_HASH_3_HPP

#include <cstdint>

/**
 * @brief MurmurHash3 hash function for x86 architecture.
 *
 * Hashes the key into a 32-bit hash.
 *
 * @param key The key to hash.
 * @param len The length of the key.
 * @param seed The seed for the hash function.
 * @param out The output hash.
 */
void MurmurHash3_x86_32(const void *key, const int len, const uint32_t seed, void *out);

/**
 * @brief MurmurHash3 hash function for x86 architecture.
 *
 * Hashes the key into a 128-bit hash.
 *
 * @param key
 * @param len
 * @param seed
 * @param out
 */
void MurmurHash3_x86_128(const void *key, const int len, const uint32_t seed, void *out);

#endif // MURMUR_HASH_3_HPP
