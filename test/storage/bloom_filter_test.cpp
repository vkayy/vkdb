#include "gtest/gtest.h"
#include "storage/bloom_filter.h"

using namespace vkdb;

class BloomFilterTest : public ::testing::Test {
protected:
  using Key = int32_t;
  using Filter = BloomFilter;
  static constexpr uint64_t EXPECTED_NO_OF_ELEMS{100};
  static constexpr double FALSE_POSITIVE_RATE{0.01};

  void SetUp() override {
    filter_
      = std::make_unique<Filter>(EXPECTED_NO_OF_ELEMS, FALSE_POSITIVE_RATE);
  }

  std::unique_ptr<Filter> filter_;
};

TEST_F(BloomFilterTest, CanCheckPresenceOfKey) {
  TimeSeriesKey key1{1, "metric1", {{"tag1", "value1"}}};
  TimeSeriesKey key2{2, "metric2", {{"tag2", "value2"}}};
  TimeSeriesKey key3{3, "metric3", {{"tag3", "value3"}}};
  TimeSeriesKey key4{4, "metric4", {{"tag4", "value4"}}};

  filter_->insert(key1);
  filter_->insert(key2);
  filter_->insert(key3);

  auto may_contain1{filter_->mayContain(key1)};
  auto may_contain2{filter_->mayContain(key2)};
  auto may_contain3{filter_->mayContain(key3)};
  auto may_contain4{filter_->mayContain(key4)};

  EXPECT_TRUE(may_contain1);
  EXPECT_TRUE(may_contain2);
  EXPECT_TRUE(may_contain3);
  EXPECT_FALSE(may_contain4);
}

TEST_F(BloomFilterTest, ProducesAccurateFalsePositiveRate) {
  auto no_of_false_positives{0};
  for (Key i{0}; i < EXPECTED_NO_OF_ELEMS; ++i) {
    auto t_i{static_cast<Timestamp>(i)};
    TimeSeriesKey key{t_i, "metric", {{"tag", "value"}}};
    filter_->insert(key);
  }

  for (Key i{EXPECTED_NO_OF_ELEMS}; i < 2 * EXPECTED_NO_OF_ELEMS; ++i) {
    auto t_i{static_cast<Timestamp>(i)};
    TimeSeriesKey key{t_i, "metric", {{"tag", "value"}}};
    no_of_false_positives += filter_->mayContain(key);
  }

  auto measured_false_positive_rate{
    static_cast<double>(no_of_false_positives) / EXPECTED_NO_OF_ELEMS
  };

  EXPECT_LE(measured_false_positive_rate, FALSE_POSITIVE_RATE);
}

TEST_F(BloomFilterTest, ProducesZeroFalseNegatives) {
  for (Key i{0}; i < EXPECTED_NO_OF_ELEMS; ++i) {
    auto t_i{static_cast<Timestamp>(i)};
    TimeSeriesKey key{t_i, "metric", {{"tag", "value"}}};
    filter_->insert(key);
  }

  auto no_of_false_negatives{0};
  for (Key i{0}; i < EXPECTED_NO_OF_ELEMS; ++i) {
    auto t_i{static_cast<Timestamp>(i)};
    TimeSeriesKey key{t_i, "metric", {{"tag", "value"}}};
    no_of_false_negatives += !filter_->mayContain(key);
  }

  EXPECT_EQ(no_of_false_negatives, 0);
}

TEST_F(BloomFilterTest, ProducesZeroFalsePositivesWhenEmpty) {
  auto no_of_false_positives{0};
  for (Key i{0}; i < EXPECTED_NO_OF_ELEMS; ++i) {
    auto t_i{static_cast<Timestamp>(i)};
    TimeSeriesKey key{t_i, "metric", {{"tag", "value"}}};
    no_of_false_positives += filter_->mayContain(key);
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