#include "gtest/gtest.h"
#include <vkdb/table.h>

using namespace vkdb;

class TableTest : public ::testing::Test {
protected:
  void SetUp() override {
    table_ = std::make_unique<Table>("test_db", "table");

    table_->addTagColumn("tag1");
    table_->addTagColumn("tag2");
    table_->addTagColumn("tag3");
  }

  void TearDown() override {
    table_->clear();
  }

  std::unique_ptr<Table> table_;
};

TEST_F(TableTest, CanAddTagColumn) {
  EXPECT_NO_THROW(table_->addTagColumn("tag4"));
  EXPECT_THROW(table_->addTagColumn("tag4"), std::runtime_error);
}

TEST_F(TableTest, CanRemoveTagColumn) {
  EXPECT_NO_THROW(table_->removeTagColumn("tag1"));
  EXPECT_THROW(table_->removeTagColumn("tag1"), std::runtime_error);
}

TEST_F(TableTest, CanQueryData) {
  ASSERT_NO_THROW(table_->addTagColumn("region"));

  for (Timestamp i{0}; i < 10'000; ++i) {
    table_->query()
      .put(i, "temperature", {{"region", "ldn"}}, 0.5 * i)
      .execute();
  }

  auto result{table_->query()
    .between(2'500, 7'500)
    .whereMetricIs("temperature")
    .whereTagsContain({"region", "ldn"})
    .avg()
  };

  EXPECT_DOUBLE_EQ(result, 2'500.0);
}

TEST_F(TableTest, CanQueryDataWithMultipleMetrics) {
  ASSERT_NO_THROW(table_->addTagColumn("region"));

  for (Timestamp i{0}; i < 10'000; ++i) {
    table_->query()
      .put(i, "temperature", {{"region", "ldn"}}, 0.5 * i)
      .execute();
    table_->query()
      .put(i, "humidity", {{"region", "ldn"}}, 0.25 * i)
      .execute();
  }

  auto result{table_->query()
    .between(2'500, 7'500)
    .whereMetricIsAnyOf("temperature", "humidity")
    .whereTagsContain({"region", "ldn"})
    .avg()
  };

  EXPECT_DOUBLE_EQ(result, 1'875.0);
}

TEST_F(TableTest, CanQueryDataWithMultipleMetricsAndTags) {
  ASSERT_NO_THROW(table_->addTagColumn("region"));
  ASSERT_NO_THROW(table_->addTagColumn("device"));

  for (Timestamp i{0}; i < 10'000; ++i) {
    table_->query()
      .put(i, "temperature", {{"region", "ldn"}, {"device", "sensor1"}}, 0.5 * i)
      .execute();
    table_->query()
      .put(i, "humidity", {{"region", "ldn"}, {"device", "sensor1"}}, 0.25 * i)
      .execute();
  }

  auto result{table_->query()
    .between(2'500, 7'500)
    .whereMetricIsAnyOf("temperature", "humidity")
    .whereTagsContainAllOf(
      std::make_pair("region", "ldn"),
      std::make_pair("device", "sensor1")
    )
    .avg()
  };

  EXPECT_DOUBLE_EQ(result, 1'875.0);
}

TEST_F(TableTest, CanGetTableName) {
  auto name{table_->name()};

  EXPECT_EQ(name, "table");
}