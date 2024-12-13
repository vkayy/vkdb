#include "gtest/gtest.h"
#include "storage/mem_table.h"

#include <sstream>

class MemTableTest : public ::testing::Test {
protected:
  using Table = MemTable<int>;

  void SetUp() override {
    table_ = std::make_unique<Table>();
  }

  std::unique_ptr<Table> table_;
};

TEST_F(MemTableTest, CanCheckContainsKeys) {
  TimeSeriesKey key1{1, "metric1", {}};
  TimeSeriesKey key2{2, "metric2", {}};
  TimeSeriesKey key3{3, "metric3", {}};
  TimeSeriesKey key4{4, "metric4", {}};

  table_->put(key1, 1);
  table_->put(key2, 2);
  table_->put(key3, 3);

  auto value1{table_->contains(key1)};
  auto value2{table_->contains(key2)};
  auto value3{table_->contains(key3)};
  auto value4{table_->contains(key4)};

  EXPECT_EQ(value1, true);
  EXPECT_EQ(value2, true);
  EXPECT_EQ(value3, true);
  EXPECT_EQ(value4, false);
}

TEST_F(MemTableTest, CanPutAndGetValuesOfKeys) {
  TimeSeriesKey key1{1, "metric1", {}};
  TimeSeriesKey key2{2, "metric2", {}};
  TimeSeriesKey key3{3, "metric3", {}};
  TimeSeriesKey key4{4, "metric4", {}};

  table_->put(key1, 1);
  table_->put(key2, 2);
  table_->put(key3, 3);

  auto value1{table_->get(key1)};
  auto value2{table_->get(key2)};
  auto value3{table_->get(key3)};
  auto value4{table_->get(key4)};

  EXPECT_EQ(value1, 1);
  EXPECT_EQ(value2, 2);
  EXPECT_EQ(value3, 3);
  EXPECT_EQ(value4, std::nullopt);
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

TEST_F(MemTableTest, CanGetRangeOfEntriesWithKeys) {
  TimeSeriesKey key1{1, "metric1", {{"tag1", "value1"}, {"tag2", "value2"}}};
  TimeSeriesKey key2{2, "metric2", {{"tag3", "value3"}, {"tag4", "value4"}}};
  TimeSeriesKey key3{3, "metric3", {{"tag5", "value5"}, {"tag6", "value6"}}};
  TimeSeriesKey key4{4, "metric4", {{"tag7", "value7"}, {"tag8", "value8"}}};
  TimeSeriesKey key5{5, "metric5", {{"tag9", "value9"}, {"tag10", "value10"}}};

  table_->put(key1, 1);
  table_->put(key2, 2);
  table_->put(key2, std::nullopt);
  table_->put(key3, 3);
  table_->put(key4, 4);
  table_->put(key4, 3);

  auto entries{table_->getRange(key2, key5)};

  EXPECT_EQ(entries.size(), 3);
  EXPECT_EQ(entries[0].second, std::nullopt);
  EXPECT_EQ(entries[1].second, 3);
  EXPECT_EQ(entries[2].second, 3);
}

TEST_F(MemTableTest, CanUpdateValuesOfKeysWithMultipleTags) {
  TimeSeriesKey key1{1, "metric1", {{"tag1", "value1"}, {"tag2", "value2"}}};

  table_->put(key1, 1);
  table_->put(key1, 2);

  auto value1{table_->get(key1)};

  EXPECT_EQ(value1, 2);
}

TEST_F(MemTableTest, CanConvertToStringRepresentationWithTags) {
  TimeSeriesKey key1{1, "metric1", {{"tag1", "value1"}}};
  TimeSeriesKey key2{2, "metric2", {{"tag2", "value2"}}};
  TimeSeriesKey key3{3, "metric3", {{"tag3", "value3"}}};

  table_->put(key1, 1);
  table_->put(key2, 2);
  table_->put(key3, 3);

  auto str{table_->toString()};

  auto expected_str{
    "3"
    "[{00000000000000000001}{metric1}{tag1:value1}|1]"
    "[{00000000000000000002}{metric2}{tag2:value2}|2]"
    "[{00000000000000000003}{metric3}{tag3:value3}|3]"
  };

  EXPECT_EQ(str, expected_str);
}

TEST_F(MemTableTest, CanConvertFromStringRepresentationWithTags) {
  auto str{
    "3"
    "[{00000000000000000001}{metric1}{tag1:value1}|1]"
    "[{00000000000000000002}{metric2}{tag2:value2}|2]"
    "[{00000000000000000003}{metric3}{tag3:value3}|3]"
  };

  Table table;
  Table::fromString(str, table);

  TimeSeriesKey key1{1, "metric1", {{"tag1", "value1"}}};
  TimeSeriesKey key2{2, "metric2", {{"tag2", "value2"}}};
  TimeSeriesKey key3{3, "metric3", {{"tag3", "value3"}}};

  auto value1{table.get(key1)};
  auto value2{table.get(key2)};
  auto value3{table.get(key3)};

  EXPECT_EQ(value1, 1);
  EXPECT_EQ(value2, 2);
  EXPECT_EQ(value3, 3);
}

TEST_F(MemTableTest, CanInsertIntoStreamWithTags) {
  TimeSeriesKey key1{1, "metric1", {{"tag1", "value1"}}};
  TimeSeriesKey key2{2, "metric2", {{"tag2", "value2"}}};
  TimeSeriesKey key3{3, "metric3", {{"tag3", "value3"}}};

  table_->put(key1, 1);
  table_->put(key2, 2);
  table_->put(key3, 3);

  std::ostringstream ss;
  ss << *table_;

  auto expected_str{
    "3"
    "[{00000000000000000001}{metric1}{tag1:value1}|1]"
    "[{00000000000000000002}{metric2}{tag2:value2}|2]"
    "[{00000000000000000003}{metric3}{tag3:value3}|3]"
  };

  EXPECT_EQ(ss.str(), expected_str);
}

TEST_F(MemTableTest, CanExtractFromStreamWithTags) {
  auto str{
    "3"
    "[{00000000000000000001}{metric1}{tag1:value1}|1]"
    "[{00000000000000000002}{metric2}{tag2:value2}|2]"
    "[{00000000000000000003}{metric3}{tag3:value3}|3]"
  };

  std::istringstream ss{str};
  Table table;
  ss >> table;

  TimeSeriesKey key1{1, "metric1", {{"tag1", "value1"}}};
  TimeSeriesKey key2{2, "metric2", {{"tag2", "value2"}}};
  TimeSeriesKey key3{3, "metric3", {{"tag3", "value3"}}};

  auto value1{table.get(key1)};
  auto value2{table.get(key2)};
  auto value3{table.get(key3)};

  EXPECT_EQ(value1, 1);
  EXPECT_EQ(value2, 2);
  EXPECT_EQ(value3, 3);
}