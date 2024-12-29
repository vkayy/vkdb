#ifndef STORAGE_BLOOM_FILTER_H
#define STORAGE_BLOOM_FILTER_H

#include <vkdb/murmur_hash_3.h>
#include <vkdb/time_series_key.h>
#include <vkdb/random.h>
#include <vector>
#include <stdexcept>
#include <cmath>

namespace vkdb {
/**
 * @brief Bloom filter for time series keys.
 * 
 */
class BloomFilter {
public:
  using key_type = TimeSeriesKey;
  using size_type = uint64_t;

  /**
   * @brief Minimum (exclusive) false positive rate.
   * 
   */
  static constexpr double MIN_FALSE_POSITIVE_RATE{0.0};

  /**
   * @brief Maximum (exclusive) false positive rate.
   * 
   */
  static constexpr double MAX_FALSE_POSITIVE_RATE{1.0};

  /**
   * @brief Deleted default constructor.
   * 
   */
  BloomFilter() = delete;

  /**
   * @brief Construct a new BloomFilter object from the given string.
   * 
   * @param str The string representation of the Bloom filter.
   */
  BloomFilter(std::string&& str) noexcept;

  /**
   * @brief Construct a new BloomFilter object from the given expected number of
   * elements and false positive rate.
   * 
   * @param expected_no_of_elems The expected number of elements.
   * @param false_positive_rate The false positive rate.
   * 
   * @throws std::invalid_argument If the expected number of elements is zero
   * or the false positive rate is not in range (MIN_FALSE_POSITIVE_RATE,
   * MAX_FALSE_POSITIVE_RATE).
   */
  explicit BloomFilter(
    uint64_t expected_no_of_elems,
    double false_positive_rate
  );

  /**
   * @brief Destroy the BloomFilter object.
   * 
   */
  ~BloomFilter() noexcept = default;

  /**
   * @brief Move-construct a BloomFilter object.
   * 
   */
  BloomFilter(BloomFilter&&) noexcept = default;

  /**
   * @brief Move-assign a BloomFilter object.
   * 
   */
  BloomFilter& operator=(BloomFilter&&) noexcept = default;

  /**
   * @brief Deleted copy constructor.
   * 
   */
  BloomFilter(const BloomFilter&) = delete;

  /**
   * @brief Deleted copy assignment operator.
   * 
   */
  BloomFilter& operator=(const BloomFilter&) = delete;

  /**
   * @brief Insert a key into the Bloom filter.
   * 
   * @param key The key to insert.
   */
  void insert(const key_type& key) noexcept;

  /**
   * @brief Check if the Bloom filter may contain a key.
   * 
   * @param key The key to check.
   * @return true if the Bloom filter may contain the key.
   * @return false if the Bloom filter does not contain the key.
   */
  [[nodiscard]] bool mayContain(const key_type& key) const noexcept;

  /**
   * @brief Get the string representation of the Bloom filter.
   * 
   * @return std::string The string representation of the Bloom filter.
   */
  [[nodiscard]] std::string str() const noexcept;

private:
  /**
   * @brief Type alias for uint32_t.
   * 
   */
  using HashValue = uint32_t;

  /**
   * @brief Initialise the seeds for the hash functions.
   * 
   * @param no_of_hashes Number of hash functions.
   */
  void initialise_seeds(size_type no_of_hashes) noexcept;

  /**
   * @brief Hash a key with the given seed index.
   * 
   * @param key Key.
   * @param i Seed index.
   * @return HashValue Hash value.
   */
  [[nodiscard]] HashValue hash(
    const key_type& key,
    size_type i
  ) const noexcept;

  /**
   * @brief Seeds.
   * 
   */
  std::vector<size_type> seeds_;

  /**
   * @brief Bit vector.
   * 
   */
  std::vector<bool> bits_;
};
}  // namespace vkdb

#endif // STORAGE_BLOOM_FILTER_H