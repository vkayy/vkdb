#include "gtest/gtest.h"
#include "storage/data_range.h"

class DataRangeTest : public ::testing::Test {
protected:
  using Data = int;
  using Range = DataRange<Data>;

  void SetUp() override {
    range_ = std::make_unique<Range>();
  }

  std::unique_ptr<Range> range_;
};

TEST_F(DataRangeTest, CanUpdateRange) {
  range_->updateRange(1);
  range_->updateRange(2);
  range_->updateRange(3);

  auto lower{range_->lower()};
  auto upper{range_->upper()};

  EXPECT_EQ(lower, 1);
  EXPECT_EQ(upper, 3);
}

TEST_F(DataRangeTest, CanCheckIfDataIsInRange) {
  range_->updateRange(1);
  range_->updateRange(2);
  range_->updateRange(3);

  auto in_range1{range_->inRange(1)};
  auto in_range2{range_->inRange(2)};
  auto in_range3{range_->inRange(3)};
  auto in_range4{range_->inRange(4)};

  EXPECT_TRUE(in_range1);
  EXPECT_TRUE(in_range2);
  EXPECT_TRUE(in_range3);
  EXPECT_FALSE(in_range4);
}

TEST_F(DataRangeTest, CanCheckIfDataIsInEmptyRange) {
  auto in_range{range_->inRange(1)};

  EXPECT_FALSE(in_range);
}

TEST_F(DataRangeTest, ThrowsExceptionWhenCheckingEmptyRange) {
  EXPECT_THROW(auto lower{range_->lower()}, std::logic_error);
  EXPECT_THROW(auto upper{range_->upper()}, std::logic_error);
}