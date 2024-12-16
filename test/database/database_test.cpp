#include "gtest/gtest.h"
#include <vkdb/database.h>

using namespace vkdb;

class DatabaseTest : public ::testing::Test {
protected:
  void SetUp() override {
    database_ = std::make_unique<Database>("test_db");
  }

  void TearDown() override {
    database_->clear();
  }

  std::unique_ptr<Database> database_;
};

TEST_F(DatabaseTest, CanCreateTable) {
  database_->createTable("sensor_data");
  EXPECT_NO_THROW(std::ignore = database_->getTable("sensor_data"));
}

TEST_F(DatabaseTest, CanDropTable) {
  database_->createTable("sensor_data");
  EXPECT_NO_THROW(database_->dropTable("sensor_data"));
}

TEST_F(DatabaseTest, CanGetTable) {
  database_->createTable("sensor_data");
  EXPECT_EQ(database_->getTable("sensor_data").name(), "sensor_data");
}

TEST_F(DatabaseTest, CanGetName) {
  EXPECT_EQ(database_->name(), "test_db");
}

TEST_F(DatabaseTest, CanExecuteSelectDataPointQuery) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.addTagColumn("region");
  table.addTagColumn("city");

  table.query()
    .put(
      123456,
      "temperature",
      {{"region", "eu"}, {"city", "london"}},
      25.3
    ).execute();

  std::string query{
    "SELECT DATA temperature "
    "FROM sensor_data "
    "AT 123456 "
    "WHERE region=eu city=london;"
  };

  auto result_str{database_->executeQuery(query)};
  auto result{datapointsFromString<double>(result_str)};

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].timestamp, 123456);
  EXPECT_EQ(result[0].metric, "temperature");
  ASSERT_EQ(result[0].tags.size(), 2);
  EXPECT_EQ(result[0].tags.at("region"), "eu");
  EXPECT_EQ(result[0].tags.at("city"), "london");
  EXPECT_DOUBLE_EQ(result[0].value, 25.3);
}

TEST_F(DatabaseTest, CanExecuteSelectDataRangeQuery) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.addTagColumn("region");
  table.addTagColumn("city");

  table.query()
    .put(
      123456,
      "temperature",
      {{"region", "eu"}, {"city", "london"}},
      25.3
    ).execute();

  std::string query{
    "SELECT DATA temperature "
    "FROM sensor_data "
    "BETWEEN 100000 AND 200000 "
    "WHERE region=eu city=london;"
  };

  auto result_str{database_->executeQuery(query)};
  auto result{datapointsFromString<double>(result_str)};

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].timestamp, 123456);
  EXPECT_EQ(result[0].metric, "temperature");
  ASSERT_EQ(result[0].tags.size(), 2);
  EXPECT_EQ(result[0].tags.at("region"), "eu");
  EXPECT_EQ(result[0].tags.at("city"), "london");
  EXPECT_DOUBLE_EQ(result[0].value, 25.3);
}

TEST_F(DatabaseTest, CanExecuteSelectAggregateQuery) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.addTagColumn("region");
  table.addTagColumn("city");

  table.query()
    .put(
      123456,
      "temperature",
      {{"region", "eu"}, {"city", "london"}},
      25.3
    ).execute();

  auto count_result{database_->executeQuery(
    "SELECT COUNT temperature "
    "FROM sensor_data "
    "AT 123456 "
    "WHERE region=eu city=london;"
  )};

  auto sum_result{database_->executeQuery(
    "SELECT SUM temperature "
    "FROM sensor_data "
    "BETWEEN 100000 AND 200000 "
    "WHERE region=eu city=london;"
  )};

  auto avg_result{database_->executeQuery(
    "SELECT AVG temperature "
    "FROM sensor_data "
    "BETWEEN 100000 AND 200000 "
    "WHERE region=eu city=london;"
  )};

  auto min_result{database_->executeQuery(
    "SELECT MIN temperature "
    "FROM sensor_data "
    "ALL "
    "WHERE region=eu city=london;"
  )};

  auto max_result{database_->executeQuery(
    "SELECT MAX temperature "
    "FROM sensor_data "
    "BETWEEN 100000 AND 200000 "
    "WHERE region=eu city=london;"
  )};

  EXPECT_EQ(std::stoull(count_result), 1);
  EXPECT_DOUBLE_EQ(std::stod(sum_result), 25.3);
  EXPECT_DOUBLE_EQ(std::stod(avg_result), 25.3);
  EXPECT_DOUBLE_EQ(std::stod(min_result), 25.3);
  EXPECT_DOUBLE_EQ(std::stod(max_result), 25.3);
}

TEST_F(DatabaseTest, CanExecutePutQuery) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.addTagColumn("region");
  table.addTagColumn("city");

  std::string query{
    "PUT temperature 123456 25.3 "
    "INTO sensor_data region=eu city=london"
  };

  database_->executeQuery(query);

  auto result{table.query()
    .get(123456, "temperature", {{"region", "eu"}, {"city", "london"}})
    .execute()
  };

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].timestamp, 123456);
  EXPECT_EQ(result[0].metric, "temperature");
  ASSERT_EQ(result[0].tags.size(), 2);
  EXPECT_EQ(result[0].tags.at("region"), "eu");
  EXPECT_EQ(result[0].tags.at("city"), "london");
  EXPECT_DOUBLE_EQ(result[0].value, 25.3);
}

TEST_F(DatabaseTest, CanExecuteDeleteQuery) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.addTagColumn("region");
  table.addTagColumn("city");

  table.query()
    .put(
      123456,
      "temperature",
      {{"region", "eu"}, {"city", "london"}},
      25.3
    ).execute();
  
  auto result{table.query()
    .get(123456, "temperature", {{"region", "eu"}, {"city", "london"}})
    .execute()
  };

  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].timestamp, 123456);
  EXPECT_EQ(result[0].metric, "temperature");
  ASSERT_EQ(result[0].tags.size(), 2);
  EXPECT_EQ(result[0].tags.at("region"), "eu");
  EXPECT_EQ(result[0].tags.at("city"), "london");

  std::string query{
    "DELETE temperature 123456 "
    "FROM sensor_data "
    "region=eu city=london;"
  };

  database_->executeQuery(query);

  result = table.query()
    .get(123456, "temperature", {{"region", "eu"}, {"city", "london"}})
    .execute();

  EXPECT_TRUE(result.empty());
}

TEST_F(DatabaseTest, ThrowsWhenCreatingExistingTable) {
  database_->createTable("sensor_data");
  EXPECT_THROW(database_->createTable("sensor_data"), std::runtime_error);
}

TEST_F(DatabaseTest, ThrowsWhenGettingNonExistentTable) {
  EXPECT_THROW(
    std::ignore = database_->getTable("sensor_data"),
    std::runtime_error
  );
}

TEST_F(DatabaseTest, ThrowsWhenDroppingNonExistentTable) {
  EXPECT_THROW(database_->dropTable("sensor_data"), std::runtime_error);
}