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

TEST_F(DatabaseTest, CanGetPath) {
  EXPECT_EQ(database_->path(), DATABASE_DIRECTORY / "test_db");
}