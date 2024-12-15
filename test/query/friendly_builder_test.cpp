#include "gtest/gtest.h"
#include "query/friendly_builder.h"

using namespace vkdb;

class FriendlyQueryBuilderTest : public ::testing::Test {
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

  FriendlyQueryBuilder<int> query() {
    return FriendlyQueryBuilder<int>(*lsm_tree_, tag_columns_);
  }

  std::unordered_set<TagKey> tag_columns_;
  std::unique_ptr<LSMTree<int>> lsm_tree_;
};

TEST_F(FriendlyQueryBuilderTest, CanGetQuery) {
  auto result{query()
    .get(0, "metric", {})
    .execute()
  };

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].value, 0);
}

TEST_F(FriendlyQueryBuilderTest, CanBetweenQuery) {
  auto result{query()
    .between(5'000, 10'000)
    .execute()
  };

  ASSERT_EQ(result.size(), 5'000);
  for (Timestamp i{0}; i < 5'000; ++i) {
    EXPECT_EQ(result[i].value, i + 5'000);
  }
}

TEST_F(FriendlyQueryBuilderTest, CanWhereMetricIsQuery) {
  auto result{query()
    .whereMetricIs("metric")
    .execute()
  };

  ASSERT_EQ(result.size(), 10'000);
  for (Timestamp i{0}; i < 10'000; ++i) {
    EXPECT_EQ(result[i].value, i);
  }
}

TEST_F(FriendlyQueryBuilderTest, CanWhereMetricIsAnyOfQuery) {
  auto result{query()
    .whereMetricIsAnyOf("metric", "metric2")
    .execute()
  };

  ASSERT_EQ(result.size(), 10'000);
  for (Timestamp i{0}; i < 10'000; ++i) {
    EXPECT_EQ(result[i].value, i);
  }
}

TEST_F(FriendlyQueryBuilderTest, CanWhereTimestampIsQuery) {
  auto result{query()
    .whereTimestampIs(5'000)
    .execute()
  };

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].value, 5'000);
}

TEST_F(FriendlyQueryBuilderTest, CanWhereTimestampIsAnyOfQuery) {
  auto result{query()
    .whereTimestampIsAnyOf(5'000, 5'001)
    .execute()
  };

  ASSERT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].value, 5'000);
  EXPECT_EQ(result[1].value, 5'001);
}

TEST_F(FriendlyQueryBuilderTest, CanWhereTagsContainQuery) {
  TimeSeriesKey key1{0, "metric", {{"tag1", "value1"}}};
  TimeSeriesKey key2{1, "metric", {{"tag2", "value2"}}};

  lsm_tree_->put(key1, 0);
  lsm_tree_->put(key2, 1);

  auto result{query()
    .whereTagsContain({"tag1", "value1"})
    .execute()
  };

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].value, 0);
}

TEST_F(FriendlyQueryBuilderTest, CanWhereTagsContainAnyOfQuery) {
  TimeSeriesKey key1{0, "metric", {{"tag1", "value1"}}};
  TimeSeriesKey key2{1, "metric", {{"tag2", "value2"}}};

  lsm_tree_->put(key1, 0);
  lsm_tree_->put(key2, 1);

  auto result{query()
    .whereTagsContainAnyOf(
      std::make_pair("tag1", "value1"),
      std::make_pair("tag2", "value2")
    )
    .execute()
  };

  ASSERT_EQ(result.size(), 2);
  EXPECT_EQ(result[0].value, 0);
  EXPECT_EQ(result[1].value, 1);
}

TEST_F(FriendlyQueryBuilderTest, CanWhereTagsContainAllOfQuery) {
  TimeSeriesKey key1{0, "metric", {{"tag1", "value1"}, {"tag2", "value2"}}};
  TimeSeriesKey key2{1, "metric", {{"tag1", "value1"}, {"tag3", "value3"}}};

  lsm_tree_->put(key1, 0);
  lsm_tree_->put(key2, 1);

  auto result{query()
    .whereTagsContainAllOf(
      std::make_pair("tag1", "value1"),
      std::make_pair("tag2", "value2")
    )
    .execute()
  };

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].value, 0);
}

TEST_F(FriendlyQueryBuilderTest, CanPutAndGetQuery) {
  auto result{query()
    .put(10'000, "metric", {}, 10'000)
    .execute()
  };

  ASSERT_EQ(result.size(), 0);

  auto get_result{query()
    .get(10'000, "metric", {})
    .execute()
  };

  ASSERT_EQ(get_result.size(), 1);
  EXPECT_EQ(get_result[0].value, 10'000);
}

TEST_F(FriendlyQueryBuilderTest, CanRemoveQuery) {
  auto result{query()
    .remove(0, "metric", {})
    .execute()
  };

  ASSERT_EQ(result.size(), 0);

  auto get_result{query()
    .get(0, "metric", {})
    .execute()
  };

  ASSERT_EQ(get_result.size(), 0);
}

TEST_F(FriendlyQueryBuilderTest, CanCountQuery) {
  auto count{query()
    .count()
  };

  EXPECT_EQ(count, 10'000);
}

TEST_F(FriendlyQueryBuilderTest, CanSumQuery) {
  auto sum{query()
    .sum()
  };

  EXPECT_EQ(sum, 49'995'000);
}

TEST_F(FriendlyQueryBuilderTest, CanAvgQuery) {
  auto avg{query()
    .avg()
  };

  EXPECT_DOUBLE_EQ(avg, 4'999.5);
}

TEST_F(FriendlyQueryBuilderTest, CanMinQuery) {
  auto min{query()
    .min()
  };

  EXPECT_EQ(min, 0);
}

TEST_F(FriendlyQueryBuilderTest, CanMaxQuery) {
  auto max{query()
    .max()
  };

  EXPECT_EQ(max, 9'999);
}

TEST_F(FriendlyQueryBuilderTest, ThrowsWhenInvalidMetric) {
  EXPECT_THROW(
    query()
      .put(0, "", {}, 0)
      .execute(),
    std::runtime_error
  );

  EXPECT_THROW(
    query()
      .put(0, std::string(TimeSeriesKey::MAX_METRIC_LENGTH, '\0'), {}, 0)
      .execute(),
    std::runtime_error
  );
}