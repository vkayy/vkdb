#include "gtest/gtest.h"
#include "query/builder.h"

using namespace vkdb;

class QueryBuilderTest : public ::testing::Test {
protected:
  void SetUp() override {
    lsm_tree_ = std::make_unique<LSMTree<int>>("/Users/vkay/Dev/vkdb/output");

    for (Timestamp i{0}; i < 10'000; ++i) {
      TimeSeriesKey key{i, "metric", {}};
      lsm_tree_->put(key, i);
    }

    tag_columns_.insert("tag1");
    tag_columns_.insert("tag2");
    tag_columns_.insert("tag3");
  }

  QueryBuilder<int> query() {
    return QueryBuilder<int>(*lsm_tree_, tag_columns_);
  }

  std::set<TagKey> tag_columns_;
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

TEST_F(QueryBuilderTest, CanGetCountWithoutFilters) {
  auto result{query()
    .count()
  };

  EXPECT_EQ(result, 10'000);
}

TEST_F(QueryBuilderTest, CanGetCountWithFilters) {
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
    .count()
  };

  EXPECT_EQ(result, 3);
}

TEST_F(QueryBuilderTest, CanGetSumWithoutFilters) {
  auto result{query()
    .sum()
  };

  EXPECT_EQ(result, 49'995'000);
}

TEST_F(QueryBuilderTest, CanGetSumWithFilters) {
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
    .sum()
  };

  EXPECT_EQ(result, 9);
}

TEST_F(QueryBuilderTest, CanGetAvgWithoutFilters) {
  auto result{query()
    .avg()
  };

  EXPECT_DOUBLE_EQ(result, 4'999.5);
}

TEST_F(QueryBuilderTest, CanGetAvgWithFilters) {
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
    .avg()
  };

  EXPECT_DOUBLE_EQ(result, 3);
}

TEST_F(QueryBuilderTest, CanGetMinWithoutFilters) {
  auto result{query()
    .min()
  };

  EXPECT_EQ(result, 0);
}

TEST_F(QueryBuilderTest, CanGetMinWithFilters) {
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
    .min()
  };

  EXPECT_EQ(result, 2);
}

TEST_F(QueryBuilderTest, CanGetMaxWithoutFilters) {
  auto result{query()
    .max()
  };

  EXPECT_EQ(result, 9'999);
}

TEST_F(QueryBuilderTest, CanGetMaxWithFilters) {
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
    .max()
  };

  EXPECT_EQ(result, 4);
}

TEST_F(QueryBuilderTest, CanAggregateOverPointQueries) {
  TimeSeriesKey key1{5'000, "metric", {}};

  lsm_tree_->put(key1, 1);

  auto count_result{query()
    .point(key1)
    .count()
  };

  auto sum_result{query()
    .point(key1)
    .sum()
  };

  auto avg_result{query()
    .point(key1)
    .avg()
  };

  auto min_result{query()
    .point(key1)
    .min()
  };

  auto max_result{query()
    .point(key1)
    .max()
  };

  EXPECT_EQ(count_result, 1);
  EXPECT_EQ(sum_result, 1);
  EXPECT_DOUBLE_EQ(avg_result, 1);
  EXPECT_EQ(min_result, 1);
  EXPECT_EQ(max_result, 1);
}

TEST_F(QueryBuilderTest, ThrowsWhenQueryTypeIsNone) {
  EXPECT_THROW(query().execute(), std::runtime_error);
}

TEST_F(QueryBuilderTest, ThrowsWhenSettingQueryTypeMoreThanOnce) {
  TimeSeriesKey key{5'000, "metric", {}};
  EXPECT_THROW(query().point(key).point(key).execute(), std::runtime_error);
}

TEST_F(QueryBuilderTest, ThrowsWhenAggregatingOnAnInappropriateQuery) {
  TimeSeriesKey key{5'000, "metric", {}};
  EXPECT_THROW(auto result{query().put(key, 1).sum()}, std::runtime_error);
}

TEST_F(QueryBuilderTest, ThrowsWhenAggregatingOnEmptyRangeExceptCount) {
  Metric metric{"non-existent-metric"};
  EXPECT_THROW(
    auto result{query().filterByMetric(metric).sum()},
    std::runtime_error
  );
  EXPECT_THROW(
    auto result{query().filterByMetric(metric).avg()},
    std::runtime_error
  );
  EXPECT_THROW(
    auto result{query().filterByMetric(metric).min()},
    std::runtime_error
  );
  EXPECT_THROW(
    auto result{query().filterByMetric(metric).max()},
    std::runtime_error
  );
  EXPECT_EQ(
    query().filterByMetric(metric).count(),
    0
  );
}

TEST_F(QueryBuilderTest, ThrowsWhenFilteringOnInvalidTags) {
  EXPECT_THROW(
    query().filterByTag("invalid-tag", "value").execute(),
    std::runtime_error
  );
}