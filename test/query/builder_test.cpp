#include "gtest/gtest.h"
#include "query/builder.h"

class QueryBuilderTest : public ::testing::Test {
protected:
  void SetUp() override {
    lsm_tree_ = std::make_unique<LSMTree<int>>("/Users/vkay/Dev/vkdb/output");
    query_builder_ = std::make_unique<QueryBuilder<int>>(*lsm_tree_);

    for (Timestamp i{0}; i < 10'000; ++i) {
      TimeSeriesKey key{i, "metric", {}};
      lsm_tree_->put(key, i);
    }
  }

  std::unique_ptr<LSMTree<int>> lsm_tree_;
  std::unique_ptr<QueryBuilder<int>> query_builder_;
};

TEST_F(QueryBuilderTest, CanPointQuery) {
  TimeSeriesKey key{5'000, "metric", {}};

  auto result{query_builder_
    ->point(key)
    .execute()
  };

  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].second, 5'000);
}

TEST_F(QueryBuilderTest, CanRangeQuery) {
  TimeSeriesKey start{5'000, "metric", {}};
  TimeSeriesKey end{10'000, "metric", {}};

  auto result{query_builder_
    ->range(start, end)
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

  auto result{query_builder_
    ->filterByTag("tag1", "value1")
    .execute()
  };

  EXPECT_EQ(result.size(), 3);
  EXPECT_EQ(result[0].second, 1);
  EXPECT_EQ(result[1].second, 2);
  EXPECT_EQ(result[2].second, 3);
}

TEST_F(QueryBuilderTest, CanFilterByMetric) {
  TimeSeriesKey key1{5'000, "metric1", {}};
  TimeSeriesKey key2{5'001, "metric1", {}};
  TimeSeriesKey key3{5'002, "metric1", {}};

  lsm_tree_->put(key1, 1);
  lsm_tree_->put(key2, 2);
  lsm_tree_->put(key3, 3);

  auto result{query_builder_
    ->filterByMetric("metric1")
    .execute()
  };

  EXPECT_EQ(result.size(), 3);
  EXPECT_EQ(result[0].second, 1);
  EXPECT_EQ(result[1].second, 2);
  EXPECT_EQ(result[2].second, 3);
}

TEST_F(QueryBuilderTest, CanFilterByTimestamp) {
  auto result{query_builder_
    ->filterByTimestamp(5'000)
    .execute()
  };

  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].second, 5'000);
}

TEST_F(QueryBuilderTest, CanFilterByTagAndMetric) {
  TimeSeriesKey key1{5'000, "metric1", {{"tag1", "value1"}}};
  TimeSeriesKey key2{5'001, "metric2", {{"tag1", "value1"}}};
  TimeSeriesKey key3{5'002, "metric2", {{"tag1", "value1"}}};

  lsm_tree_->put(key1, 1);
  lsm_tree_->put(key2, 2);
  lsm_tree_->put(key3, 3);

  auto result{query_builder_
    ->filterByTag("tag1", "value1")
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

  auto result{query_builder_
    ->filterByTag("tag1", "val1")
    .filterByMetric("metric2")
    .filterByTimestamp(5002)
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

  auto result{query_builder_
    ->range(range_start, range_end)
    .filterByTag("tag1", "val1")
    .filterByMetric("metric2")
    .filterByTimestamp(5002)
    .execute()
  };

  EXPECT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].second, 3);
  EXPECT_EQ(result[1].second, 4);
}