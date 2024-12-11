#ifndef STORAGE_BLOOM_FILTER_H
#define STORAGE_BLOOM_FILTER_H

#include "utils/murmur_hash_3.h"
#include "utils/random.h"

template <RegularNoCVRefQuals TKey>
class BloomFilter {
public:
  using key_type = TKey;
  using size_type = uint64_t;

  static constexpr double MIN_FALSE_POSITIVE_RATE{0.0};
  static constexpr double MAX_FALSE_POSITIVE_RATE{1.0};

  BloomFilter() = delete;

  explicit BloomFilter(uint64_t expected_no_of_elems, double false_positive_rate) {
    if (expected_no_of_elems == 0) {
      throw std::invalid_argument{
        "BloomFilter(): Expected elements must be greater than 0."
      };
    }

    if (false_positive_rate <= MIN_FALSE_POSITIVE_RATE ||
        false_positive_rate >= MAX_FALSE_POSITIVE_RATE) {
      throw std::invalid_argument{
        "BloomFilter(): False positive rate must be in the range (0, 1)."
      };
    }

    auto log_false_positive_rate{std::log(false_positive_rate)};
    auto log_2_squared{std::log(2) * std::log(2)};

    auto no_of_bits{static_cast<size_type>(
      -(expected_no_of_elems * log_false_positive_rate) / log_2_squared
    )};
    bits_.resize(no_of_bits);

    auto no_of_hashes{static_cast<size_type>(
      (no_of_bits / expected_no_of_elems) * std::log(2)
    )};
    initialise_seeds(no_of_hashes);
  }

  ~BloomFilter() = default;

  BloomFilter(BloomFilter&&) noexcept = default;
  BloomFilter& operator=(BloomFilter&&) noexcept = default;

  BloomFilter(const BloomFilter&) = delete;
  BloomFilter& operator=(const BloomFilter&) = delete;

  void insert(const key_type& key) noexcept {
    for (size_type i{0}; i < seeds_.size(); ++i) {
      bits_[hash(key, i)] = true;
    }
  }

  [[nodiscard]] bool mayContain(const key_type& key) const noexcept {
    for (size_type i{0}; i < seeds_.size(); ++i) {
      if (!bits_[hash(key, i)]) {
        return false;
      }
    }
    return true;
  }

private:
  using HashValue = uint32_t;

  void initialise_seeds(size_type no_of_hashes) {
    seeds_.resize(no_of_hashes);
    for (size_type i{0}; i < no_of_hashes; ++i) {
      seeds_[i] = random<size_type>();
    }
  }

  [[nodiscard]] HashValue hash(const key_type& key, size_type i) const noexcept {
    HashValue hash_value{0};
    MurmurHash3_x86_32(&key, sizeof(key), i, &hash_value);
    return hash_value % bits_.size();
  }

  std::vector<size_type> seeds_;
  std::vector<bool> bits_;
};

#endif // STORAGE_BLOOM_FILTER_H