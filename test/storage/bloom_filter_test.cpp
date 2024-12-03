#include "gtest/gtest.h"
#include "storage/bloom_filter.h"

class BloomFilterTest : public ::testing::Test {
protected:
  using Key = int32_t;
  using Filter = BloomFilter<int32_t>;
  static constexpr uint64_t EXPECTED_NO_OF_ELEMS{100};
  static constexpr double FALSE_POSITIVE_RATE{0.01};

  void SetUp() override {
    filter_
      = std::make_unique<Filter>(EXPECTED_NO_OF_ELEMS, FALSE_POSITIVE_RATE);
  }

  std::unique_ptr<Filter> filter_;
};

TEST_F(BloomFilterTest, CanCheckPresenceOfKey) {
  filter_->insert(1);
  filter_->insert(2);
  filter_->insert(3);

  auto may_contain1{filter_->mayContain(1)};
  auto may_contain2{filter_->mayContain(2)};
  auto may_contain3{filter_->mayContain(3)};
  auto may_contain4{filter_->mayContain(4)};

  EXPECT_TRUE(may_contain1);
  EXPECT_TRUE(may_contain2);
  EXPECT_TRUE(may_contain3);
  EXPECT_FALSE(may_contain4);
}

TEST_F(BloomFilterTest, ProducesAccurateFalsePositiveRate) {
  auto no_of_false_positives{0};
  for (Key i{0}; i < EXPECTED_NO_OF_ELEMS; ++i) {
    filter_->insert(i);
  }

  for (Key i{EXPECTED_NO_OF_ELEMS}; i < 2 * EXPECTED_NO_OF_ELEMS; ++i) {
    no_of_false_positives += filter_->mayContain(i);
  }

  auto measured_false_positive_rate{
    static_cast<double>(no_of_false_positives) / EXPECTED_NO_OF_ELEMS
  };

  EXPECT_LE(measured_false_positive_rate, FALSE_POSITIVE_RATE);
}

TEST_F(BloomFilterTest, ProducesZeroFalseNegatives) {
  for (Key i{0}; i < EXPECTED_NO_OF_ELEMS; ++i) {
    filter_->insert(i);
  }

  auto no_of_false_negatives{0};
  for (Key i{0}; i < EXPECTED_NO_OF_ELEMS; ++i) {
    no_of_false_negatives += !filter_->mayContain(i);
  }

  EXPECT_EQ(no_of_false_negatives, 0);
}

TEST_F(BloomFilterTest, ProducesZeroFalsePositivesWhenEmpty) {
  auto no_of_false_positives{0};
  for (Key i{0}; i < EXPECTED_NO_OF_ELEMS; ++i) {
    no_of_false_positives += filter_->mayContain(i);
  }

  EXPECT_EQ(no_of_false_positives, 0);
}

TEST_F(BloomFilterTest, ThrowsWhenExpectedNoOfElemsIsZero) {
  EXPECT_THROW((Filter{0, FALSE_POSITIVE_RATE}), std::invalid_argument);
}

TEST_F(BloomFilterTest, ThrowsWhenFalsePositiveRateIsLessThanOrEqualToZero) {
  EXPECT_THROW((Filter{EXPECTED_NO_OF_ELEMS, -0.1}), std::invalid_argument);
  EXPECT_THROW((Filter{EXPECTED_NO_OF_ELEMS, 0.0}), std::invalid_argument);
}

TEST_F(BloomFilterTest, ThrowsWhenFalsePositiveRateIsGreaterThanOrEqualToOne) {
  EXPECT_THROW((Filter{EXPECTED_NO_OF_ELEMS, 1.1}), std::invalid_argument);
  EXPECT_THROW((Filter{EXPECTED_NO_OF_ELEMS, 1.0}), std::invalid_argument);
}