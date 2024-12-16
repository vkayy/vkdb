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

  template <uint32_t N>
  void testMalformedQueries(
    const std::string& base,
    const std::vector<std::string>& after,
    const std::unordered_set<uint32_t> exceptions = {}
  ) {
    for (auto mask{0}; mask < (1 << N) - 1; ++mask) {
      if (exceptions.contains(mask)) {
        continue;
      }
      auto query{base};
      for (auto idx{0}; idx < N; ++idx) {
        if (mask & (1 << idx)) {
          query += " " + after[idx];
        }
      }
      query += ";";
      EXPECT_THROW(database_->executeQuery(query), std::runtime_error)
        << "Query: " << query << "\nMask: 0b" << std::bitset<N>(mask);
    }
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

TEST_F(DatabaseTest, CanExecuteCreateTableQuery) {
  std::string query{
    "CREATE TABLE sensor_data;"
  };

  EXPECT_NO_THROW(database_->executeQuery(query));
  EXPECT_NO_THROW(std::ignore = database_->getTable("sensor_data"));
}

TEST_F(DatabaseTest, CanExecuteCreateTableWithTagsQuery) {
  std::string query{
    "CREATE TABLE sensor_data TAGS region city;"
  };

  EXPECT_NO_THROW(database_->executeQuery(query));
  auto& table{database_->getTable("sensor_data")};

  EXPECT_EQ(table.tagColumns().size(), 2);
  EXPECT_TRUE(table.tagColumns().contains("region"));
  EXPECT_TRUE(table.tagColumns().contains("city"));
}

TEST_F(DatabaseTest, CanExecuteDropTableQuery) {
  database_->createTable("sensor_data");

  std::string query{
    "DROP TABLE sensor_data;"
  };

  EXPECT_NO_THROW(database_->executeQuery(query));
  EXPECT_THROW(
    std::ignore = database_->getTable("sensor_data"),
    std::runtime_error
  );
}

TEST_F(DatabaseTest, CanExecuteAddTagsQueryWithOneTag) {
  database_->createTable("sensor_data");

  std::string query{
    "ADD TAGS location TO sensor_data;"
  };

  EXPECT_NO_THROW(database_->executeQuery(query));
  auto& table{database_->getTable("sensor_data")};

  EXPECT_EQ(table.tagColumns().size(), 1);
  EXPECT_TRUE(table.tagColumns().contains("location"));
}

TEST_F(DatabaseTest, CanExecuteAddTagsQueryWithMultipleTags) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  std::string query{
    "ADD TAGS location type status TO sensor_data;"
  };

  EXPECT_NO_THROW(database_->executeQuery(query));

  EXPECT_EQ(table.tagColumns().size(), 3);
  EXPECT_TRUE(table.tagColumns().contains("location"));
  EXPECT_TRUE(table.tagColumns().contains("type"));
  EXPECT_TRUE(table.tagColumns().contains("status"));
}

TEST_F(DatabaseTest, CanExecuteRemoveTagsQueryWithOneTag) {
  database_->createTable("sensor_data");

  auto& table{database_->getTable("sensor_data")};
  table.addTagColumn("location");

  std::string query{
    "REMOVE TAGS location FROM sensor_data;"
  };

  EXPECT_NO_THROW(database_->executeQuery(query));

  EXPECT_EQ(table.tagColumns().size(), 0);
}

TEST_F(DatabaseTest, CanExecuteRemoveTagsQueryWithMultipleTags) {
  database_->createTable("sensor_data");

  auto& table{database_->getTable("sensor_data")};
  table.addTagColumn("location");
  table.addTagColumn("type");
  table.addTagColumn("status");

  std::string query{
    "REMOVE TAGS location type status FROM sensor_data;"
  };

  EXPECT_NO_THROW(database_->executeQuery(query));

  EXPECT_EQ(table.tagColumns().size(), 0);
}

TEST_F(DatabaseTest, ThrowsWhenMalformedCreateTableQuery) {
  testMalformedQueries<4>(
    "CREATE",
    {"TABLE", "sensor_data", "TAGS", "tag"},
    {0b1001, 0b0011}
  );
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

TEST_F(DatabaseTest, ThrowsWhenMalformedAddTagsQuery) {
  database_->createTable("table");
  testMalformedQueries<4>("ADD", {"TAGS", "tag", "TO", "table"});
}

TEST_F(DatabaseTest, ThrowsWhenAddingTagsToNonExistentTable) {
  std::string query{
    "ADD TAGS location type status TO sensor_data;"
  };

  EXPECT_THROW(database_->executeQuery(query), std::runtime_error);
}

TEST_F(DatabaseTest, ThrowsWhenAddingTagsToTableWithExistingTags) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.addTagColumn("type");

  std::string query{
    "ADD TAGS type status TO sensor_data;"
  };

  EXPECT_THROW(database_->executeQuery(query), std::runtime_error);
}


TEST_F(DatabaseTest, ThrowsWhenMalformedRemoveTagsQuery) {
  database_->createTable("table");
  database_->getTable("table").addTagColumn("tag");
  testMalformedQueries<4>("REMOVE", {"TAGS", "tag", "FROM", "table"});
}

TEST_F(DatabaseTest, ThrowsWhenRemovingTagsFromNonExistentTable) {
  std::string query{
    "REMOVE TAGS location type status FROM sensor_data;"
  };

  EXPECT_THROW(database_->executeQuery(query), std::runtime_error);
}

TEST_F(DatabaseTest, ThrowsWhenRemovingTagsFromTableWithNonExistentTags) {
  database_->createTable("sensor_data");

  std::string query{
    "REMOVE TAGS location type status FROM sensor_data;"
  };

  EXPECT_THROW(database_->executeQuery(query), std::runtime_error);
}

TEST_F(DatabaseTest, ThrowsWhenMalformedSelectDataQuery) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.addTagColumn("region");

  testMalformedQueries<8>(
    "SELECT",
    {
      "DATA",
      "temperature",
      "FROM",
      "sensor_data",
      "AT",
      "123456",
      "WHERE",
      "region=eu"
    }, {0b00111111, 0b10011111}
  );

  testMalformedQueries<10>(
    "SELECT",
    {
      "DATA",
      "temperature",
      "FROM",
      "sensor_data",
      "BETWEEN",
      "123456",
      "AND",
      "123457",
      "WHERE",
      "region=eu"
    }, {0b0011111111}
  );

  testMalformedQueries<7>(
    "SELECT",
    {
      "DATA",
      "temperature",
      "FROM",
      "sensor_data",
      "ALL",
      "WHERE",
      "region=eu"
    }, {0b0011111}
  );
}

TEST_F(DatabaseTest, ThrowsWhenSelectingDataFromNonExistentTable) {
  std::string query{
    "SELECT DATA temperature "
    "FROM sensor_data "
    "AT 123456 "
    "WHERE region=eu;"
  };

  EXPECT_THROW(database_->executeQuery(query), std::runtime_error);
}

TEST_F(DatabaseTest, ThrowsWhenSelectingDataWithNonExistentTag) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  std::string query{
    "SELECT DATA temperature "
    "FROM sensor_data "
    "AT 123456 "
    "WHERE region=eu;"
  };

  EXPECT_THROW(database_->executeQuery(query), std::runtime_error);
}

TEST_F(DatabaseTest, ThrowsWhenMalformedSelectAggregateQuery) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.addTagColumn("region");

  table.query().put(123456, "temperature", {{"region", "eu"}}, 25.3).execute();

  testMalformedQueries<8>(
    "SELECT",
    {
      "AVG",
      "temperature",
      "FROM",
      "sensor_data",
      "AT",
      "123456",
      "WHERE",
      "region=eu"
    }, {0b00111111, 0b10011111}
  );

  testMalformedQueries<10>(
    "SELECT",
    {
      "AVG",
      "temperature",
      "FROM",
      "sensor_data",
      "BETWEEN",
      "123456",
      "AND",
      "123457",
      "WHERE",
      "region=eu"
    }, {0b0011111111}
  );

  testMalformedQueries<7>(
    "SELECT",
    {
      "AVG",
      "temperature",
      "FROM",
      "sensor_data",
      "ALL",
      "WHERE",
      "region=eu"
    }, {0b0011111}
  );
}


TEST_F(DatabaseTest, ThrowsWhenSelectingAggregateFromNonExistentTable) {
  std::string query{
    "SELECT AVG temperature "
    "FROM sensor_data "
    "AT 123456 "
    "WHERE region=eu;"
  };

  EXPECT_THROW(database_->executeQuery(query), std::runtime_error);
}

TEST_F(DatabaseTest, ThrowsWhenSelectingAggregateWithNonExistentTag) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  std::string query{
    "SELECT AVG temperature "
    "FROM sensor_data "
    "AT 123456 "
    "WHERE region=eu;"
  };

  EXPECT_THROW(database_->executeQuery(query), std::runtime_error);
}

TEST_F(DatabaseTest, ThrowsWhenMalformedPutQuery) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.addTagColumn("region");

  testMalformedQueries<6>(
    "PUT",
    {"temperature", "123456", "25.3", "INTO", "sensor_data", "region=eu"}
  );
}

TEST_F(DatabaseTest, ThrowsWhenPuttingDataIntoNonExistentTable) {
  std::string query{
    "PUT temperature 123456 25.3 INTO sensor_data region=eu;"
  };

  EXPECT_THROW(database_->executeQuery(query), std::runtime_error);
}

TEST_F(DatabaseTest, ThrowsWhenPuttingDataWithMissingTags) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.addTagColumn("region");

  std::string query{
    "PUT temperature 123456 25.3 INTO sensor_data;"
  };

  EXPECT_THROW(database_->executeQuery(query), std::runtime_error);
}

TEST_F(DatabaseTest, ThrowsWhenPuttingDataWithNonExistentTags) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.addTagColumn("region");

  std::string query{
    "PUT temperature 123456 25.3 INTO sensor_data region=eu city=london;"
  };

  EXPECT_THROW(database_->executeQuery(query), std::runtime_error);
}

TEST_F(DatabaseTest, ThrowsWhenMalformedDeleteQuery) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.addTagColumn("region");
  table.query()
    .put(123456, "temperature", {{"region", "eu"}}, 25.3)
    .execute();

  testMalformedQueries<5>(
    "DELETE",
    {"temperature", "123456", "FROM", "sensor_data", "region=eu"}
  );
}

TEST_F(DatabaseTest, ThrowsWhenDeletingDataFromNonExistentTable) {
  std::string query{
    "DELETE temperature 123456 FROM sensor_data region=eu;"
  };

  EXPECT_THROW(database_->executeQuery(query), std::runtime_error);
}


TEST_F(DatabaseTest, ThrowsWhenDeletingDataWithMissingTags) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.addTagColumn("region");
  table.query()
    .put(123456, "temperature", {{"region", "eu"}}, 25.3)
    .execute();
  
  std::string query{
    "DELETE temperature 123456 FROM sensor_data;"
  };

  EXPECT_THROW(database_->executeQuery(query), std::runtime_error);
}

TEST_F(DatabaseTest, ThrowsWhenDeletingDataWithNonExistentTags) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.addTagColumn("region");
  table.query()
    .put(123456, "temperature", {{"region", "eu"}}, 25.3)
    .execute();
  
  std::string query{
    "DELETE temperature 123456 FROM sensor_data region=eu city=london;"
  };

  EXPECT_THROW(database_->executeQuery(query), std::runtime_error);
}