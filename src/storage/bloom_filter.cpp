#include <vkdb/bloom_filter.h>
#include <iostream>

namespace vkdb {
BloomFilter::BloomFilter(std::string&& str) noexcept {
  std::stringstream ss{str};
  size_type no_of_bits;
  ss >> no_of_bits;
  bits_.resize(no_of_bits);

  size_type no_of_hashes;
  ss >> no_of_hashes;
  seeds_.resize(no_of_hashes);
  for (size_type i{0}; i < no_of_hashes; ++i) {
    ss >> seeds_[i];
  }

  std::string bits;
  ss >> bits;
  for (size_type i{0}; i < bits.size(); ++i) {
    bits_[i] = bits[i] - '0';
  }
}

BloomFilter::BloomFilter(uint64_t expected_no_of_elems, double false_positive_rate) {
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

void BloomFilter::insert(const key_type& key) noexcept {
  for (size_type i{0}; i < seeds_.size(); ++i) {
    bits_[hash(key, i)] = true;
  }
}

bool BloomFilter::mayContain(const key_type& key) const noexcept {
  for (size_type i{0}; i < seeds_.size(); ++i) {
    if (!bits_[hash(key, i)]) {
      return false;
    }
  }
  return true;
}

std::string BloomFilter::str() const noexcept {
  std::stringstream ss;
  ss << bits_.size() << " ";
  ss << seeds_.size() << " ";
  auto first{true};
  for (const auto& seed : seeds_) {
    if (!first) {
      ss << " ";
    }
    ss << seed;
  }
  for (const auto& bit : bits_) {
    ss << bit;
  }
  return ss.str();
}

void BloomFilter::initialise_seeds(size_type no_of_hashes) {
  seeds_.resize(no_of_hashes);
  for (size_type i{0}; i < no_of_hashes; ++i) {
    seeds_[i] = random<size_type>();
  }
}

BloomFilter::HashValue BloomFilter::hash(const key_type& key, size_type i) const noexcept {
  HashValue hash_value{0};
  auto std_hash_value{std::hash<std::string>{}(key.str())};
  MurmurHash3_x86_32(&std_hash_value, sizeof(std_hash_value), i, &hash_value);
  return hash_value % bits_.size();
}
}  // namespace vkdb
