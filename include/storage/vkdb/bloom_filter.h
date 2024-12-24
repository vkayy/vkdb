#ifndef STORAGE_BLOOM_FILTER_H
#define STORAGE_BLOOM_FILTER_H

#include <vkdb/murmur_hash_3.h>
#include <vkdb/time_series_key.h>
#include <vkdb/random.h>
#include <vector>
#include <stdexcept>
#include <cmath>

namespace vkdb {
class BloomFilter {
public:
  using key_type = TimeSeriesKey;
  using size_type = uint64_t;

  static constexpr double MIN_FALSE_POSITIVE_RATE{0.0};
  static constexpr double MAX_FALSE_POSITIVE_RATE{1.0};

  BloomFilter() = delete;

  BloomFilter(std::string&& str) noexcept;
  explicit BloomFilter(uint64_t expected_no_of_elems, double false_positive_rate);

  ~BloomFilter() = default;

  BloomFilter(BloomFilter&&) noexcept = default;
  BloomFilter& operator=(BloomFilter&&) noexcept = default;

  BloomFilter(const BloomFilter&) = delete;
  BloomFilter& operator=(const BloomFilter&) = delete;

  void insert(const key_type& key) noexcept;

  [[nodiscard]] bool mayContain(const key_type& key) const noexcept;

  [[nodiscard]] std::string str() const noexcept;

private:
  using HashValue = uint32_t;

  void initialise_seeds(size_type no_of_hashes);

  [[nodiscard]] HashValue hash(const key_type& key, size_type i) const noexcept;

  std::vector<size_type> seeds_;
  std::vector<bool> bits_;
};
}  // namespace vkdb

#endif // STORAGE_BLOOM_FILTER_H