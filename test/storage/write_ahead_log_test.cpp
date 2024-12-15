#include "gtest/gtest.h"
#include "storage/write_ahead_log.h"

using namespace vkdb;

class WriteAheadLogTest : public ::testing::Test {
protected:
  void SetUp() override {
    lsm_tree_path_ = "/Users/vkay/Dev/vkdb/output/wal_test";
    std::filesystem::create_directories(lsm_tree_path_);
    wal_ = std::make_unique<WriteAheadLog<int>>(lsm_tree_path_);
  }

  void TearDown() override {
    std::filesystem::remove_all(lsm_tree_path_);
  }

  FilePath lsm_tree_path_;
  std::unique_ptr<WriteAheadLog<int>> wal_;
};

TEST_F(WriteAheadLogTest, CanAppendRecord) {
  TimeSeriesKey key{1, "metric", {}};
  WALRecord<int> record{WALRecordType::Put, TimeSeriesEntry<int>{key, 1}};
  
  wal_->append(record);

  std::ifstream file{wal_->path()};
  std::string line;
  std::getline(file, line);

  EXPECT_EQ(line, "0 [{00000000000000000001}{metric}{}|1]");
  file.close();
}

TEST_F(WriteAheadLogTest, CanReplayRecord) {
  TimeSeriesKey key{1, "metric", {}};
  WALRecord<int> record{WALRecordType::Put, TimeSeriesEntry<int>{key, 1}};
  wal_->append(record);

  LSMTree<int> lsm_tree{lsm_tree_path_};
  wal_->replay(lsm_tree);

  auto entry{lsm_tree.get(key)};
  EXPECT_EQ(entry.value(), 1);
}

TEST_F(WriteAheadLogTest, ThrowsWhenUnableToOpenFileAndFileExists) {
  std::ofstream wal_file{wal_->path()};
  ASSERT_TRUE(wal_file.is_open());
  wal_file.close();

  std::filesystem::permissions(
    wal_->path(),
    std::filesystem::perms::none,
    std::filesystem::perm_options::replace
  );

  LSMTree<int> lsm_tree{lsm_tree_path_};
  EXPECT_THROW(lsm_tree.replayWAL(), std::runtime_error);

  
  std::filesystem::permissions(
    wal_->path(),
    std::filesystem::perms::all,
    std::filesystem::perm_options::replace
  );
}
