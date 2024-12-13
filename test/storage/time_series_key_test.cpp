#include "gtest/gtest.h"
#include "storage/time_series_key.h"

class TimeSeriesKeyTest : public ::testing::Test {
protected:
  void SetUp() override {
    tags_ = {
      {"tag1", "value1"},
      {"tag2", "value2"},
      {"tag3", "value3"}
    };
  }

  TagTable tags_;
};

TEST_F(TimeSeriesKeyTest, CanEqualityCompareKeys) {
  TimeSeriesKey key1{1, "metric1", tags_};
  TimeSeriesKey key2{1, "metric1", tags_};
  TimeSeriesKey key3{2, "metric1", tags_};
  TimeSeriesKey key4{1, "metric2", tags_};
  TimeSeriesKey key5{1, "metric1", {{"tag1", "value1"}}};

  EXPECT_EQ(key1, key2);
  EXPECT_NE(key1, key3);
  EXPECT_NE(key1, key4);
  EXPECT_NE(key1, key5);
}

TEST_F(TimeSeriesKeyTest, CanTotallyOrderKeys) {
  TimeSeriesKey key1{1, "metric1", tags_};
  TimeSeriesKey key2{2, "metric1", tags_};
  TimeSeriesKey key3{1, "metric2", tags_};
  TimeSeriesKey key4{1, "metric1", {{"tag1", "value1"}}};

  EXPECT_LT(key1, key2);
  EXPECT_LT(key1, key3);
  EXPECT_GT(key1, key4);
  EXPECT_LT(key2, key3);
  EXPECT_GT(key2, key4);
  EXPECT_GT(key3, key4);
}

TEST_F(TimeSeriesKeyTest, CanObtainTimestamp) {
  TimeSeriesKey key{1, "metric1", tags_};

  auto timestamp{key.timestamp()};

  EXPECT_EQ(timestamp, 1);
}

TEST_F(TimeSeriesKeyTest, CanObtainMetric) {
  TimeSeriesKey key{1, "metric1", tags_};

  auto metric{key.metric()};

  EXPECT_EQ(metric, "metric1");
}

TEST_F(TimeSeriesKeyTest, CanObtainTags) {
  TimeSeriesKey key{1, "metric1", tags_};

  auto tags{key.tags()};

  EXPECT_EQ(tags, tags_);
}

TEST_F(TimeSeriesKeyTest, CanConvertToStringRepresentationWithManyTags) {
  TimeSeriesKey key{1, "metric1", tags_};

  auto str{key.toString()};

  auto expected_str{
    "{00000000000000000001}{metric1}{tag1:value1,tag2:value2,tag3:value3}"
  };

  EXPECT_EQ(str, expected_str);
}

TEST_F(TimeSeriesKeyTest, CanConvertFromStringRepresentationWithManyTags) {
  auto str{
    "{00000000000000000001}{metric1}{tag1:value1,tag2:value2,tag3:value3}"
  };

  auto key{TimeSeriesKey::fromString(str)};

  TimeSeriesKey expected_key{1, "metric1", tags_};

  EXPECT_EQ(key, expected_key);
}

TEST_F(TimeSeriesKeyTest, CanConvertToStringRepresentationWithEmptyTags) {
  TimeSeriesKey key{1, "metric1", {}};

  auto str{key.toString()};

  auto expected_str{
    "{00000000000000000001}{metric1}{}"
  };

  EXPECT_EQ(str, expected_str);
}

TEST_F(TimeSeriesKeyTest, CanConvertFromStringRepresentationWithEmptyTags) {
  auto str{
    "{00000000000000000001}{metric1}{}"
  };

  auto key{TimeSeriesKey::fromString(str)};

  TimeSeriesKey expected_key{1, "metric1", {}};

  EXPECT_EQ(key, expected_key);
}

TEST_F(TimeSeriesKeyTest, CanConvertToStringRepresentationWithSingleTag) {
  TimeSeriesKey key{1, "metric1", {{"tag1", "value1"}}};

  auto str{key.toString()};

  auto expected_str{
    "{00000000000000000001}{metric1}{tag1:value1}"
  };

  EXPECT_EQ(str, expected_str);
}

TEST_F(TimeSeriesKeyTest, CanConvertFromStringRepresentationWithSingleTag) {
  auto str{
    "{00000000000000000001}{metric1}{tag1:value1}"
  };

  auto key{TimeSeriesKey::fromString(str)};

  TimeSeriesKey expected_key{1, "metric1", {{"tag1", "value1"}}};

  EXPECT_EQ(key, expected_key);
}

TEST_F(TimeSeriesKeyTest, CanInsertIntoStreamWithTags) {
  TimeSeriesKey key{1, "metric1", tags_};

  std::ostringstream ss;
  ss << key;

  auto expected_str{
    "{00000000000000000001}{metric1}{tag1:value1,tag2:value2,tag3:value3}"
  };

  EXPECT_EQ(ss.str(), expected_str);
}

TEST_F(TimeSeriesKeyTest, CanExtractFromStreamWithTags) {
  auto str{
    "{00000000000000000001}{metric1}{tag1:value1,tag2:value2,tag3:value3}"
  };

  std::istringstream ss{str};
  TimeSeriesKey key;
  ss >> key;

  TimeSeriesKey expected_key{1, "metric1", tags_};

  EXPECT_EQ(key, expected_key);
}

TEST_F(TimeSeriesKeyTest, CanBeKeyInUnorderedAssociativeContainers) {
  std::unordered_map<TimeSeriesKey, int> map;

  TimeSeriesKey key1{1, "metric1", tags_};
  TimeSeriesKey key2{2, "metric2", tags_};

  map[key1] = 1;
  map[key2] = 2;

  EXPECT_EQ(map[key1], 1);
  EXPECT_EQ(map[key2], 2);
}