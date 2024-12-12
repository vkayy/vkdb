#include "gtest/gtest.h"
#include "storage/sstable.h"

class SSTableTest : public ::testing::Test {
protected:
  void SetUp() override {
    file_path_ = "test.sstable";
    sstable_ = std::make_unique<SSTable<int>>(file_path_);
    mem_table_ = std::make_unique<MemTable<int>>();
  }

  void TearDown() override {
    std::remove(file_path_.c_str());
  }

  FilePath file_path_;
  std::unique_ptr<SSTable<int>> sstable_;
  std::unique_ptr<MemTable<int>> mem_table_;
};

TEST_F(SSTableTest, CanWriteMemTableToFile) {
  TimeSeriesKey key1{1, "metric1", {}};
  TimeSeriesKey key2{2, "metric2", {}};
  TimeSeriesKey key3{3, "metric3", {}};

  mem_table_->put(key1, 1);
  mem_table_->put(key2, 2);
  mem_table_->put(key3, 3);

  sstable_->writeMemTableToFile(*mem_table_);

  std::ifstream file{file_path_};
  std::string str;
  std::getline(file, str);

  auto expected_str{
    "3"
    "[{00000000000000000001}{metric1}{}|1]"
    "[{00000000000000000002}{metric2}{}|2]"
    "[{00000000000000000003}{metric3}{}|3]"
  };

  EXPECT_EQ(str, expected_str);
}

TEST_F(SSTableTest, CanGetFromSSTable) {
  TimeSeriesKey key1{1, "metric1", {}};
  TimeSeriesKey key2{2, "metric2", {}};
  TimeSeriesKey key3{3, "metric3", {}};

  mem_table_->put(key1, 1);
  mem_table_->put(key2, 2);
  mem_table_->put(key3, 3);

  sstable_->writeMemTableToFile(*mem_table_);

  auto value1{sstable_->get(key1)};
  auto value2{sstable_->get(key2)};
  auto value3{sstable_->get(key3)};

  EXPECT_EQ(value1, 1);
  EXPECT_EQ(value2, 2);
  EXPECT_EQ(value3, 3);
}