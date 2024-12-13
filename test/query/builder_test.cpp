#include "gtest/gtest.h"
#include "query/builder.h"

class QueryBuilderTest : public ::testing::Test {
protected:
  void SetUp() override {
    lsm_tree_ = std::make_unique<LSMTree<int>>("/Users/vkay/Dev/vkdb/output");

    for (Timestamp i{0}; i < 10'000; ++i) {
      TimeSeriesKey key{i, "metric", {}};
      lsm_tree_->put(key, i);
    }
  }

  QueryBuilder<int> query() {
    return QueryBuilder<int>(*lsm_tree_);
  }

  std::unique_ptr<LSMTree<int>> lsm_tree_;
};

TEST_F(QueryBuilderTest, CanPointQuery) {
  TimeSeriesKey key{5'000, "metric", {}};

  auto result{query()
    .point(key)
    .execute()
  };

  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].second, 5'000);
}

TEST_F(QueryBuilderTest, CanRangeQuery) {
  TimeSeriesKey start{5'000, "metric", {}};
  TimeSeriesKey end{10'000, "metric", {}};

  auto result{query()
    .range(start, end)
    .execute()
  };

  EXPECT_EQ(result.size(), 5'000);
  for (Timestamp i{5'000}; i < 10'000; ++i) {
    EXPECT_EQ(result[i - 5'000].second, i);
  }
}

TEST_F(QueryBuilderTest, CanFilterByTag) {
  TimeSeriesKey key1{5'000, "metric", {{"tag1", "value1"}}};
  TimeSeriesKey key2{5'001, "metric", {{"tag1", "value1"}}};
  TimeSeriesKey key3{5'002, "metric", {{"tag1", "value1"}}};

  lsm_tree_->put(key1, 1);
  lsm_tree_->put(key2, 2);
  lsm_tree_->put(key3, 3);

  auto result{query()
    .filterByTag("tag1", "value1")
    .execute()
  };

  EXPECT_EQ(result.size(), 3);
  EXPECT_EQ(result[0].second, 1);
  EXPECT_EQ(result[1].second, 2);
  EXPECT_EQ(result[2].second, 3);
}

TEST_F(QueryBuilderTest, CanFilterByAnyTags) {
  Tag tag1{"tag1", "value1"};
  Tag tag2{"tag2", "value2"};
  Tag tag3{"tag3", "value3"};

  TimeSeriesKey key1{5'000, "metric", {tag1}};
  TimeSeriesKey key2{5'001, "metric", {tag2}};
  TimeSeriesKey key3{5'002, "metric", {tag3}};

  lsm_tree_->put(key1, 1);
  lsm_tree_->put(key2, 2);
  lsm_tree_->put(key3, 3);

  auto result{query()
    .filterByAnyTags(tag1, tag2, tag3)
    .execute()
  };

  EXPECT_EQ(result.size(), 3);
  EXPECT_EQ(result[0].second, 1);
  EXPECT_EQ(result[1].second, 2);
  EXPECT_EQ(result[2].second, 3);
}

TEST_F(QueryBuilderTest, CanFilterByAllTags) {
  Tag tag1{"tag1", "value1"};
  Tag tag2{"tag2", "value2"};

  TimeSeriesKey key1{5'000, "metric", {tag1}};
  TimeSeriesKey key2{5'001, "metric", {tag1, tag2}};
  TimeSeriesKey key3{5'002, "metric", {tag2}};

  lsm_tree_->put(key1, 1);
  lsm_tree_->put(key2, 2);
  lsm_tree_->put(key3, 3);


  auto result{query()
    .filterByAllTags(tag1, tag2)
    .execute()
  };

  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].second, 2);
}

TEST_F(QueryBuilderTest, CanFilterByMetric) {
  TimeSeriesKey key1{5'000, "metric1", {}};
  TimeSeriesKey key2{5'001, "metric1", {}};
  TimeSeriesKey key3{5'002, "metric1", {}};

  lsm_tree_->put(key1, 1);
  lsm_tree_->put(key2, 2);
  lsm_tree_->put(key3, 3);

  auto result{query()
    .filterByMetric("metric1")
    .execute()
  };

  EXPECT_EQ(result.size(), 3);
  EXPECT_EQ(result[0].second, 1);
  EXPECT_EQ(result[1].second, 2);
  EXPECT_EQ(result[2].second, 3);
}

TEST_F(QueryBuilderTest, CanFilterByAnyMetrics) {
  Metric metric1{"metric1"};
  Metric metric2{"metric2"};
  Metric metric3{"metric3"};

  TimeSeriesKey key1{5'000, metric1, {}};
  TimeSeriesKey key2{5'001, metric2, {}};
  TimeSeriesKey key3{5'002, metric3, {}};

  lsm_tree_->put(key1, 1);
  lsm_tree_->put(key2, 2);
  lsm_tree_->put(key3, 3);

  auto result{query()
    .filterByAnyMetrics(metric1, metric2)
    .execute()
  };

  EXPECT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].second, 1);
  EXPECT_EQ(result[1].second, 2);
}

TEST_F(QueryBuilderTest, CanFilterByTimestamp) {
  auto result{query()
    .filterByTimestamp(5'000)
    .execute()
  };

  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].second, 5'000);
}

TEST_F(QueryBuilderTest, CanFilterByAnyTimestamps) {
  Timestamp timestamp1{5'000};
  Timestamp timestamp2{5'001};

  auto result{query()
    .filterByAnyTimestamps(timestamp1, timestamp2)
    .execute()
  };

  EXPECT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].second, 5'000);
  EXPECT_EQ(result[1].second, 5'001);
}

TEST_F(QueryBuilderTest, CanFilterByTagAndMetric) {
  TimeSeriesKey key1{5'000, "metric1", {{"tag1", "value1"}}};
  TimeSeriesKey key2{5'001, "metric2", {{"tag1", "value1"}}};
  TimeSeriesKey key3{5'002, "metric2", {{"tag1", "value1"}}};

  lsm_tree_->put(key1, 1);
  lsm_tree_->put(key2, 2);
  lsm_tree_->put(key3, 3);

  auto result{query()
    .filterByTag("tag1", "value1")
    .filterByMetric("metric2")
    .execute()
  };

  EXPECT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].second, 2);
  EXPECT_EQ(result[1].second, 3);
}

TEST_F(QueryBuilderTest, CanFilterByTagAndMetricAndTimestamp) {
  TimeSeriesKey key1{5'000, "metric1", {{"tag1", "val1"}}};
  TimeSeriesKey key2{5'001, "metric2", {{"tag1", "val1"}}};
  TimeSeriesKey key3{5'002, "metric2", {{"tag1", "val1"}}};
  TimeSeriesKey key4{5'002, "metric2", {{"tag1", "val1"}, {"tag2", "val2"}}};

  lsm_tree_->put(key1, 1);
  lsm_tree_->put(key2, 2);
  lsm_tree_->put(key3, 3);
  lsm_tree_->put(key4, 4);  

  auto result{query()
    .filterByTag("tag1", "val1")
    .filterByMetric("metric2")
    .filterByTimestamp(5'002)
    .execute()
  };

  EXPECT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].second, 3);
  EXPECT_EQ(result[1].second, 4);
}

TEST_F(QueryBuilderTest, CanRangeQueryAndFilterByTagAndMetricAndTimestamp) {
  TimeSeriesKey key1{5'000, "metric1", {{"tag1", "val1"}}};
  TimeSeriesKey key2{5'001, "metric2", {{"tag1", "val1"}}};
  TimeSeriesKey key3{5'002, "metric2", {{"tag1", "val1"}}};
  TimeSeriesKey key4{5'002, "metric2", {{"tag1", "val1"}, {"tag2", "val2"}}};

  lsm_tree_->put(key1, 1);
  lsm_tree_->put(key2, 2);
  lsm_tree_->put(key3, 3);
  lsm_tree_->put(key4, 4);

  TimeSeriesKey range_start{5'000, "metric1", {}};
  TimeSeriesKey range_end{5'002, "metric3", {}};  

  auto result{query()
    .range(range_start, range_end)
    .filterByTag("tag1", "val1")
    .filterByMetric("metric2")
    .filterByTimestamp(5'002)
    .execute()
  };

  EXPECT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].second, 3);
  EXPECT_EQ(result[1].second, 4);
}

TEST_F(QueryBuilderTest, CanPutAndUpdate) {
  TimeSeriesKey key{5'000, "metric", {}};

  query()
    .put(key, 1)
    .execute();

  auto result{query()
    .point(key)
    .execute()
  };

  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].second, 1);

  query()
    .put(key, 2)
    .execute();

  result = query()
    .point(key)
    .execute();

  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].second, 2);
}

TEST_F(QueryBuilderTest, CanRemove) {
  TimeSeriesKey key{5'000, "metric", {}};

  query()
    .put(key, 1)
    .execute();

  auto result{query()
    .point(key)
    .execute()
  };

  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].second, 1);

  query()
    .remove(key)
    .execute();

  result = query()
    .point(key)
    .execute();

  EXPECT_EQ(result.size(), 0);
}