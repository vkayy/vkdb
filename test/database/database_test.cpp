#include "gtest/gtest.h"
#include <vkdb/database.h>
#include <vkdb/string.h>

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

TEST_F(DatabaseTest, CanGetPath) {
  EXPECT_EQ(database_->path(), DATABASE_DIRECTORY / "test_db");
}

TEST_F(DatabaseTest, CanRunCreateQuery) {
  database_->run("CREATE TABLE table TAGS tag;");
  EXPECT_NO_THROW(std::ignore = database_->getTable("table"));
}

TEST_F(DatabaseTest, CanRunDropQuery) {
  database_->run("CREATE TABLE table TAGS tag;");
  database_->run("DROP TABLE table;");
  EXPECT_THROW(std::ignore = database_->getTable("table"), std::runtime_error);
}

TEST_F(DatabaseTest, CanRunAddQuery) {
  database_->createTable("sensor_data");
  database_->run("ADD TAGS temperature TO sensor_data;");
  EXPECT_TRUE(
    database_->getTable("sensor_data").tagColumns().contains("temperature")
  );
}

TEST_F(DatabaseTest, CanRunRemoveQuery) {
  database_->createTable("sensor_data");
  database_->run("ADD TAGS temperature TO sensor_data;");
  database_->run("REMOVE TAGS temperature FROM sensor_data;");
  EXPECT_FALSE(
    database_->getTable("sensor_data").tagColumns().contains("temperature")
  );
}

TEST_F(DatabaseTest, CanRunSelectDataAllQuery) {
  database_->createTable("sensor_data");
  database_->getTable("sensor_data").query()
    .put(1, "temperature", {}, 20.0)
    .execute();
  std::stringstream result;
  database_->run("SELECT DATA temperature FROM sensor_data ALL;", result);

  std::vector<DataPoint<double>> expected_datapoints{
    {1, "temperature", {}, 20.0}
  };

  EXPECT_EQ(
    result.str(),
    datapointsToString<double>(expected_datapoints) + "\n"
  );
}

TEST_F(DatabaseTest, CanRunSelectDataAllWhereQuery) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.addTagColumn("id").query()
    .put(1, "temperature", {{"id", "two"}}, 20.0)
    .execute();

  table.query()
    .put(2, "temperature", {{"id", "one"}}, 25.0)
    .execute();

  std::stringstream result;
  database_->run("SELECT DATA temperature FROM sensor_data ALL WHERE id=one;", result);

  std::vector<DataPoint<double>> expected_datapoints{
    {2, "temperature", {{"id", "one"}}, 25.0}
  };

  EXPECT_EQ(
    result.str(),
    datapointsToString<double>(expected_datapoints) + "\n"
  );
}

TEST_F(DatabaseTest, CanRunSelectDataBetweenQuery) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.query()
    .put(1, "temperature", {}, 20.0)
    .execute();

  table.query()
    .put(2, "temperature", {}, 25.0)
    .execute();

  std::stringstream result;
  database_->run("SELECT DATA temperature FROM sensor_data BETWEEN 1 AND 2;", result);

  std::vector<DataPoint<double>> expected_datapoints{
    {1, "temperature", {}, 20.0},
    {2, "temperature", {}, 25.0}
  };

  EXPECT_EQ(
    result.str(),
    datapointsToString<double>(expected_datapoints) + "\n"
  );
}

TEST_F(DatabaseTest, CanRunSelectDataBetweenWhereQuery) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.addTagColumn("id").query()
    .put(1, "temperature", {{"id", "two"}}, 20.0)
    .execute();

  table.query()
    .put(2, "temperature", {{"id", "one"}}, 25.0)
    .execute();

  std::stringstream result;
  database_->run("SELECT DATA temperature FROM sensor_data BETWEEN 1 AND 2 WHERE id=one;", result);

  std::vector<DataPoint<double>> expected_datapoints{
    {2, "temperature", {{"id", "one"}}, 25.0}
  };

  EXPECT_EQ(
    result.str(),
    datapointsToString<double>(expected_datapoints) + "\n"
  );
}

TEST_F(DatabaseTest, CanRunSelectDataAtQuery) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.query()
    .put(1, "temperature", {}, 20.0)
    .execute();

  table.query()
    .put(2, "temperature", {}, 25.0)
    .execute();

  std::stringstream result;
  database_->run("SELECT DATA temperature FROM sensor_data AT 1;", result);

  std::vector<DataPoint<double>> expected_datapoints{
    {1, "temperature", {}, 20.0}
  };

  EXPECT_EQ(
    result.str(),
    datapointsToString<double>(expected_datapoints) + "\n"
  );
}

TEST_F(DatabaseTest, CanRunSelectDataAtWhereQuery) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.addTagColumn("id").query()
    .put(1, "temperature", {{"id", "two"}}, 20.0)
    .execute();

  table.query()
    .put(2, "temperature", {{"id", "one"}}, 25.0)
    .execute();

  std::stringstream result;
  database_->run("SELECT DATA temperature FROM sensor_data AT 2 WHERE id=one;", result);

  std::vector<DataPoint<double>> expected_datapoints{
    {2, "temperature", {{"id", "one"}}, 25.0}
  };

  EXPECT_EQ(
    result.str(),
    datapointsToString<double>(expected_datapoints) + "\n"
  );
}
TEST_F(DatabaseTest, CanRunSelectAggregateAllQuery) {
  database_->createTable("sensor_data");
  database_->getTable("sensor_data").query()
    .put(1, "temperature", {}, 20.0)
    .execute();
  std::stringstream result;
  database_->run("SELECT AVG temperature FROM sensor_data ALL;", result);

  EXPECT_DOUBLE_EQ(std::stod(result.str()), 20.0);
}

TEST_F(DatabaseTest, CanRunSelectAggregateAllWhereQuery) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.addTagColumn("id").query()
    .put(1, "temperature", {{"id", "two"}}, 20.0)
    .execute();

  table.query()
    .put(2, "temperature", {{"id", "one"}}, 25.0)
    .execute();

  std::stringstream result;
  database_->run("SELECT AVG temperature FROM sensor_data ALL WHERE id=one;", result);

  EXPECT_DOUBLE_EQ(std::stod(result.str()), 25.0);
}

TEST_F(DatabaseTest, CanRunSelectAggregateBetweenQuery) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.query()
    .put(1, "temperature", {}, 20.0)
    .execute();

  table.query()
    .put(2, "temperature", {}, 25.0)
    .execute();

  std::stringstream result;
  database_->run("SELECT AVG temperature FROM sensor_data BETWEEN 1 AND 2;", result);

  EXPECT_DOUBLE_EQ(std::stod(result.str()), 22.5);
}

TEST_F(DatabaseTest, CanRunSelectAggregateBetweenWhereQuery) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.addTagColumn("id").query()
    .put(1, "temperature", {{"id", "two"}}, 20.0)
    .execute();

  table.query()
    .put(2, "temperature", {{"id", "one"}}, 25.0)
    .execute();

  std::stringstream result;
  database_->run("SELECT AVG temperature FROM sensor_data BETWEEN 1 AND 2 WHERE id=one;", result);

  EXPECT_DOUBLE_EQ(std::stod(result.str()), 25.0);
}

TEST_F(DatabaseTest, CanRunSelectAggregateAtQuery) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.query()
    .put(1, "temperature", {}, 20.0)
    .execute();

  table.query()
    .put(2, "temperature", {}, 25.0)
    .execute();

  std::stringstream result;
  database_->run("SELECT AVG temperature FROM sensor_data AT 1;", result);

  EXPECT_DOUBLE_EQ(std::stod(result.str()), 20.0);
}

TEST_F(DatabaseTest, CanRunSelectAggregateAtWhereQuery) {
  database_->createTable("sensor_data");
  auto& table{database_->getTable("sensor_data")};

  table.addTagColumn("id").query()
    .put(1, "temperature", {{"id", "two"}}, 20.0)
    .execute();

  table.query()
    .put(2, "temperature", {{"id", "one"}}, 25.0)
    .execute();

  std::stringstream result;
  database_->run("SELECT AVG temperature FROM sensor_data AT 2 WHERE id=one;", result);

  EXPECT_DOUBLE_EQ(std::stod(result.str()), 25.0);
}

TEST_F(DatabaseTest, CanPutDataWithoutTags) {
  database_->createTable("sensor_data");
  database_->run("PUT temperature 10 20.0 INTO sensor_data;");

  auto result{database_->getTable("sensor_data").query()
    .get(10, "temperature", {})
    .execute()
  };

  ASSERT_EQ(result.size(), 1);
  EXPECT_DOUBLE_EQ(result[0].value, 20.0);
}

TEST_F(DatabaseTest, CanPutDataWithTags) {
  database_->createTable("sensor_data").addTagColumn("location");
  database_->run("PUT temperature 10 20.0 INTO sensor_data TAGS location=room1;");

  auto result{database_->getTable("sensor_data").query()
    .get(10, "temperature", {{"location", "room1"}})
    .execute()
  };

  ASSERT_EQ(result.size(), 1);
  EXPECT_DOUBLE_EQ(result[0].value, 20.0);
}

TEST_F(DatabaseTest, CanDeleteDataWithoutTags) {
  database_->createTable("sensor_data");
  database_->run("PUT temperature 10 20.0 INTO sensor_data;");
  database_->run("DELETE temperature 10 FROM sensor_data;");

  auto result{database_->getTable("sensor_data").query()
    .get(10, "temperature", {})
    .execute()
  };

  EXPECT_TRUE(result.empty());
}

TEST_F(DatabaseTest, CanDeleteDataWithTags) {
  database_->createTable("sensor_data").addTagColumn("location");
  database_->run("PUT temperature 10 20.0 INTO sensor_data TAGS location=room1;");
  database_->run("DELETE temperature 10 FROM sensor_data TAGS location=room1;");

  auto result{database_->getTable("sensor_data").query()
    .get(10, "temperature", {{"location", "room1"}})
    .execute()
  };

  EXPECT_TRUE(result.empty());
}