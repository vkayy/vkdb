#ifndef BLOOM_FILTER_HPP
#define BLOOM_FILTER_HPP

#include "utils/murmur_hash_3.hpp"
#include <array>
#include <cmath>
#include <fstream>
#include <functional>
#include <iostream>
#include <vector>

/**
 * @brief A Bloom filter data structure.
 *
 * A Bloom filter is a probabilistic data structure that is used to test whether an element
 * is a member of a set. False positives are possible, but false negatives are not.
 *
 * @tparam `T` The type of the items to store in the Bloom filter.
 */
template <typename T>
class BloomFilter {
private:
    std::vector<uint32_t> seeds; // Seeds for the hash functions.
    std::vector<bool> bits;      // The bit array.

    /**
     * @brief Initialise the seeds for the hash functions.
     *
     * @param num_hashes The number of hash functions.
     */
    void initialiseSeeds(size_t num_hashes) {
        seeds.resize(num_hashes);
        for (size_t i = 0; i < num_hashes; ++i) {
            MurmurHash3_x86_32(&i, sizeof(i), i, &seeds[i]);
        }
    }

    /**
     * @brief Hash the item with the i-th hash function.
     *
     * Whilst the only function that can be used is MurmurHash3_x86_128, the hash function
     * is given a seed to generate a different hash value.
     *
     * @param item The item to hash.
     * @param i The hash function index.
     * @return `size_t` The hash value.
     */
    size_t hash(const T &item, size_t i) const {
        size_t result;
        MurmurHash3_x86_32(&item, sizeof(item), seeds[i], &result);
        return result % bits.size();
    }

public:
    /**
     * @brief Construct a new BloomFilter object.
     *
     */
    BloomFilter() = default;

    /**
     * @brief Construct a new BloomFilter object.
     *
     * This approximates the optimal number of bits and hash functions, based on
     * the expected number of items and the false positive rate.
     *
     * @param expected_items The expected number of items.
     * @param false_positive_rate The false positive rate.
     */
    BloomFilter(size_t expected_items, double false_positive_rate) {
        if (false_positive_rate <= 0.0 || false_positive_rate >= 1.0) {
            throw std::invalid_argument("false positive must be between 0 and 1");
        }

        double log_fp_rate = std::log(false_positive_rate);
        double log2_squared = std::log(2) * std::log(2);

        size_t num_bits = -(log_fp_rate / log2_squared) * static_cast<double>(expected_items);
        bits.resize(num_bits, false);

        size_t num_hashes = (double)num_bits / (double)expected_items * std::log(2);
        initialiseSeeds(num_hashes);
    }

    /**
     * @brief Insert an item into the Bloom filter.
     *
     * @param item The item to insert.
     */
    void insert(const T &item) {
        for (size_t i = 0; i < seeds.size(); ++i) {
            bits[hash(item, i)] = true;
        }
    }

    /**
     * @brief Check if the Bloom filter might contain an item.
     *
     * @param item The item to check.
     * @return `true` if the Bloom filter might contain the item.
     * @return `false` if the Bloom filter does not contain the item.
     */
    bool mightContain(const T &item) const {
        for (size_t i = 0; i < seeds.size(); ++i) {
            if (!bits[hash(item, i)]) {
                return false;
            }
        }
        return true;
    }

    /**
     * @brief Serialize the Bloom filter to a file.
     *
     * @param ofs The `ofstream` to serialize into.
     */
    void serialize(std::ofstream &ofs) const {
        size_t size = bits.size();
        ofs.write(reinterpret_cast<const char *>(&size), sizeof(size));

        size_t num_hashes = seeds.size();
        ofs.write(reinterpret_cast<const char *>(&num_hashes), sizeof(num_hashes));

        for (const bool bit : bits) {
            ofs.write(reinterpret_cast<const char *>(&bit), sizeof(bit));
        }
    }

    /**
     * @brief Deserialize the Bloom filter from a file.
     *
     * @param ifs The `ifstream` to deserialize from.
     */
    void deserialize(std::ifstream &ifs) {
        size_t size;
        ifs.read(reinterpret_cast<char *>(&size), sizeof(size));

        size_t num_hashes;
        ifs.read(reinterpret_cast<char *>(&num_hashes), sizeof(num_hashes));

        bits.resize(size);
        initialiseSeeds(num_hashes);

        for (size_t i = 0; i < size; ++i) {
            bool bit;
            ifs.read(reinterpret_cast<char *>(&bit), sizeof(bit));
            bits[i] = bit;
        }
    }
};

#endif // BLOOM_FILTER_HPP
