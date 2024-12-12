#include "gtest/gtest.h"
#include "storage/lsm_tree.h"

class LSMTreeTest : public ::testing::Test {
protected:
  void SetUp() override {
    directory_ = "/Users/vkay/Dev/vkdb/output";
    lsm_tree_ = std::make_unique<LSMTree<int>>(directory_);
  }

  FilePath directory_;
  std::unique_ptr<LSMTree<int>> lsm_tree_;
};

TEST_F(LSMTreeTest, CanPutAndGetWithoutFlushing) {
  for (Timestamp i{0}; i < 100; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->put(key, i);
  }

  for (Timestamp i{0}; i < 100; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    auto value{lsm_tree_->get(key)};
    EXPECT_EQ(value, i);
  }

  for (Timestamp i{0}; i < 100; ++i) {
    TimeSeriesKey key{i + 100, "metric", {}};
    auto value{lsm_tree_->get(key)};
    EXPECT_EQ(value, std::nullopt);
  }

  EXPECT_EQ(lsm_tree_->sstableCount(), 0);
}

TEST_F(LSMTreeTest, CanUpdateValuesOfKeysWithoutFlushing) {
  for (Timestamp i{0}; i < 100; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->put(key, i);
  }

  for (Timestamp i{0}; i < 100; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->put(key, i + 1);
  }

  for (Timestamp i{0}; i < 100; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    auto value{lsm_tree_->get(key)};
    EXPECT_EQ(value, i + 1);
  }

  EXPECT_EQ(lsm_tree_->sstableCount(), 0);
}

TEST_F(LSMTreeTest, CanPutAndGetWithFlushing) {
  for (Timestamp i{0}; i < 10'000; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->put(key, i);
  }

  for (Timestamp i{0}; i < 10'000; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    auto value{lsm_tree_->get(key)};
    EXPECT_EQ(value, i);
  }

  for (Timestamp i{0}; i < 10'000; ++i) {
    TimeSeriesKey key{i + 10'000, "metric", {}};
    auto value{lsm_tree_->get(key)};
    EXPECT_EQ(value, std::nullopt);
  }

  EXPECT_EQ(lsm_tree_->sstableCount(), 10);
}

TEST_F(LSMTreeTest, CanUpdateValuesOfKeysWithFlushing) {
  for (Timestamp i{0}; i < 5'000; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->put(key, i);
  }

  for (Timestamp i{0}; i < 5'000; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->put(key, i + 1);
  }

  for (Timestamp i{0}; i < 5'000; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    auto value{lsm_tree_->get(key)};
    EXPECT_EQ(value, i + 1);
  }

  EXPECT_EQ(lsm_tree_->sstableCount(), 10);
}