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
  range_->update_range(1);
  range_->update_range(2);
  range_->update_range(3);

  auto lower{range_->lower()};
  auto upper{range_->upper()};

  EXPECT_EQ(lower, 1);
  EXPECT_EQ(upper, 3);
}

TEST_F(DataRangeTest, CanCheckIfDataIsInRange) {
  range_->update_range(1);
  range_->update_range(2);
  range_->update_range(3);

  auto in_range1{range_->in_range(1)};
  auto in_range2{range_->in_range(2)};
  auto in_range3{range_->in_range(3)};
  auto in_range4{range_->in_range(4)};

  EXPECT_TRUE(in_range1);
  EXPECT_TRUE(in_range2);
  EXPECT_TRUE(in_range3);
  EXPECT_FALSE(in_range4);
}

TEST_F(DataRangeTest, CanCheckIfDataIsInEmptyRange) {
  auto in_range{range_->in_range(1)};

  EXPECT_FALSE(in_range);
}

TEST_F(DataRangeTest, ThrowsExceptionWhenCheckingEmptyRange) {
  EXPECT_THROW(auto lower{range_->lower()}, std::logic_error);
  EXPECT_THROW(auto upper{range_->upper()}, std::logic_error);
}