#include "gtest/gtest.h"
#include "database/database.h"

class DatabaseTest : public ::testing::Test {
protected:
  void SetUp() override {
    database_ = std::make_unique<Database>();
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

