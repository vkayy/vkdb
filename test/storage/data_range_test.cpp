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

TEST_F(DataRangeTest, CanCheckIfRangeOverlapsWithRange) {
  range_->updateRange(1);
  range_->updateRange(2);
  range_->updateRange(3);

  auto overlaps1{range_->overlaps_with(0, 1)};
  auto overlaps2{range_->overlaps_with(1, 2)};
  auto overlaps3{range_->overlaps_with(2, 3)};
  auto overlaps4{range_->overlaps_with(3, 4)};
  auto overlaps5{range_->overlaps_with(4, 5)};

  EXPECT_TRUE(overlaps1);
  EXPECT_TRUE(overlaps2);
  EXPECT_TRUE(overlaps3);
  EXPECT_TRUE(overlaps4);
  EXPECT_FALSE(overlaps5);
}

TEST_F(DataRangeTest, CanCheckIfEmptyRangeOverlapsWithRange) {
  auto overlaps{range_->overlaps_with(0, 1)};

  EXPECT_FALSE(overlaps);
}

TEST_F(DataRangeTest, ThrowsExceptionWhenCheckingEmptyRange) {
  EXPECT_THROW(auto lower{range_->lower()}, std::logic_error);
  EXPECT_THROW(auto upper{range_->upper()}, std::logic_error);
}