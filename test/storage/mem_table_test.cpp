#include "gtest/gtest.h"
#include "storage/mem_table.h"

class MemTableTest : public ::testing::Test {
protected:
  void SetUp() override {
    table_ = std::make_unique<MemTable<int>>();
  }

  std::unique_ptr<MemTable<int>> table_;
};

TEST_F(MemTableTest, CanPutAndGetValuesOfKeys) {
  TimeSeriesKey key1{1, "metric1", {}};
  TimeSeriesKey key2{2, "metric2", {}};
  TimeSeriesKey key3{3, "metric3", {}};

  table_->put(key1, 1);
  table_->put(key2, 2);
  table_->put(key3, 3);

  auto value1{table_->get(key1)};
  auto value2{table_->get(key2)};
  auto value3{table_->get(key3)};

  EXPECT_EQ(value1, 1);
  EXPECT_EQ(value2, 2);
  EXPECT_EQ(value3, 3);
}

TEST_F(MemTableTest, CanPutAndGetValuesOfKeysWithTags) {
  TimeSeriesKey key1{1, "metric1", {{"tag1", "value1"}}};
  TimeSeriesKey key2{2, "metric2", {{"tag2", "value2"}}};
  TimeSeriesKey key3{3, "metric3", {{"tag3", "value3"}}};

  table_->put(key1, 1);
  table_->put(key2, 2);
  table_->put(key3, 3);

  auto value1{table_->get(key1)};
  auto value2{table_->get(key2)};
  auto value3{table_->get(key3)};

  EXPECT_EQ(value1, 1);
  EXPECT_EQ(value2, 2);
  EXPECT_EQ(value3, 3);
}

TEST_F(MemTableTest, CanPutAndGetValuesOfKeysWithMultipleTags) {
  TimeSeriesKey key1{1, "metric1", {{"tag1", "value1"}, {"tag2", "value2"}}};
  TimeSeriesKey key2{2, "metric2", {{"tag3", "value3"}, {"tag4", "value4"}}};
  TimeSeriesKey key3{3, "metric3", {{"tag5", "value5"}, {"tag6", "value6"}}};

  table_->put(key1, 1);
  table_->put(key2, 2);
  table_->put(key3, 3);

  auto value1{table_->get(key1)};
  auto value2{table_->get(key2)};
  auto value3{table_->get(key3)};

  EXPECT_EQ(value1, 1);
  EXPECT_EQ(value2, 2);
  EXPECT_EQ(value3, 3);
}