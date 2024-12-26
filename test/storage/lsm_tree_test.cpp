#include "gtest/gtest.h"
#include <vkdb/lsm_tree.h>

using namespace vkdb;

class LSMTreeTest : public ::testing::Test {
protected:
  void SetUp() override {
    directory_ = "/Users/vkay/Dev/vkdb/output";
    lsm_tree_ = std::make_unique<LSMTree<int>>(directory_);
  }

  void TearDown() override {
    lsm_tree_->clear();
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

TEST_F(LSMTreeTest, CanGetRangeOfEntriesWithoutFlushing) {
  for (Timestamp i{0}; i < 100; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->put(key, i);
  }

  auto entries{lsm_tree_->getRange(
    TimeSeriesKey{0, "metric", {}},
    TimeSeriesKey{100, "metric", {}},
    [](const auto&) { return true; }
  )
  };

  EXPECT_EQ(entries.size(), 100);
  for (Timestamp i{0}; i < 100; ++i) {
    EXPECT_EQ(entries[i].second, i);
  }

  auto empty_entries{lsm_tree_->getRange(
    TimeSeriesKey{100, "metric", {}},
    TimeSeriesKey{200, "metric", {}},
    [](const auto&) { return true; }
  )
  };
  EXPECT_EQ(empty_entries.size(), 0);
}

TEST_F(LSMTreeTest, CanGetRangeOfEntriesWithFlushing) {
  for (Timestamp i{0}; i < 10'000; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->put(key, i);
  }

  auto entries{lsm_tree_->getRange(
    TimeSeriesKey{0, "metric", {}},
    TimeSeriesKey{10'000, "metric", {}},
    [](const auto&) { return true; }
  )
  };

  EXPECT_EQ(entries.size(), 10'000);
  for (Timestamp i{0}; i < 10'000; ++i) {
    EXPECT_EQ(entries[i].second, i);
  }

  auto empty_entries{lsm_tree_->getRange(
    TimeSeriesKey{10'000, "metric", {}},
    TimeSeriesKey{20'000, "metric", {}},
    [](const auto&) { return true; }
  )
  };
  EXPECT_EQ(empty_entries.size(), 0);
}

TEST_F(LSMTreeTest, CanGetRangeOfEntriesWithFlushingAndUpdates) {
  for (Timestamp i{0}; i < 5'000; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->put(key, i);
  }

  for (Timestamp i{0}; i < 5'000; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->put(key, i + 1);
  }

  auto entries{lsm_tree_->getRange(
    TimeSeriesKey{0, "metric", {}},
    TimeSeriesKey{5'000, "metric", {}},
    [](const auto&) { return true; }
  )
  };

  EXPECT_EQ(entries.size(), 5'000);
  for (Timestamp i{0}; i < 5'000; ++i) {
    EXPECT_EQ(entries[i].second, i + 1);
  }

  auto empty_entries{lsm_tree_->getRange(
    TimeSeriesKey{5'000, "metric", {}},
    TimeSeriesKey{10'000, "metric", {}},
    [](const auto&) { return true; }
  )
  };
  EXPECT_EQ(empty_entries.size(), 0);
}

TEST_F(LSMTreeTest, CanGetRangeOfEntriesWithFlushingAndDeletions) {
  for (Timestamp i{0}; i < 5'000; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->put(key, i);
  }

  for (Timestamp i{0}; i < 5'000; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->remove(key);
  }

  auto entries{lsm_tree_->getRange(
    TimeSeriesKey{0, "metric", {}},
    TimeSeriesKey{5'000, "metric", {}},
    [](const auto&) { return true; }
  )
  };
  EXPECT_EQ(entries.size(), 0);

  auto empty_entries{lsm_tree_->getRange(
    TimeSeriesKey{5'000, "metric", {}},
    TimeSeriesKey{10'000, "metric", {}},
    [](const auto&) { return true; }
  )
  };
  EXPECT_EQ(empty_entries.size(), 0);
}

TEST_F(LSMTreeTest, CanGetRangeOfEntriesWithFlushingAndFilter) {
  for (Timestamp i{0}; i < 10'000; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->put(key, i);
  }

  auto entries{lsm_tree_->getRange(
    TimeSeriesKey{0, "metric", {}},
    TimeSeriesKey{10'000, "metric", {}},
    [](const auto& key) {
      return key.timestamp() % 2 == 0;
    })
  };

  EXPECT_EQ(entries.size(), 5'000);
  for (Timestamp i{0}; i < 5'000; ++i) {
    EXPECT_EQ(entries[i].second, 2 * i);
  }
}

TEST_F(LSMTreeTest, CanGetRangeInParallelOfEntriesWithoutFlushing) {
  for (Timestamp i{0}; i < 100; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->put(key, i);
  }

  auto entries{lsm_tree_->getRangeParallel(
    TimeSeriesKey{0, "metric", {}},
    TimeSeriesKey{100, "metric", {}},
    [](const auto&) { return true; }
  )};

  EXPECT_EQ(entries.size(), 100);
  for (Timestamp i{0}; i < 100; ++i) {
    EXPECT_EQ(entries[i].second, i);
  }

  auto empty_entries{lsm_tree_->getRangeParallel(
    TimeSeriesKey{100, "metric", {}},
    TimeSeriesKey{200, "metric", {}},
    [](const auto&) { return true; }
  )};
  EXPECT_EQ(empty_entries.size(), 0);
}

TEST_F(LSMTreeTest, CanGetRangeInParallelOfEntriesWithFlushing) {
  for (Timestamp i{0}; i < 10'000; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->put(key, i);
  }

  auto entries{lsm_tree_->getRangeParallel(
    TimeSeriesKey{0, "metric", {}},
    TimeSeriesKey{10'000, "metric", {}},
    [](const auto&) { return true; }
  )};

  EXPECT_EQ(entries.size(), 10'000);
  for (Timestamp i{0}; i < 10'000; ++i) {
    EXPECT_EQ(entries[i].second, i);
  }

  auto empty_entries{lsm_tree_->getRangeParallel(
    TimeSeriesKey{10'000, "metric", {}},
    TimeSeriesKey{20'000, "metric", {}},
    [](const auto&) { return true; }
  )};
  EXPECT_EQ(empty_entries.size(), 0);
}

TEST_F(LSMTreeTest, CanGetRangeInParallelOfEntriesWithFlushingAndUpdates) {
  for (Timestamp i{0}; i < 5'000; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->put(key, i);
  }

  for (Timestamp i{0}; i < 5'000; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->put(key, i + 1);
  }

  auto entries{lsm_tree_->getRangeParallel(
    TimeSeriesKey{0, "metric", {}},
    TimeSeriesKey{5'000, "metric", {}},
    [](const auto&) { return true; }
  )};

  EXPECT_EQ(entries.size(), 5'000);
  for (Timestamp i{0}; i < 5'000; ++i) {
    EXPECT_EQ(entries[i].second, i + 1);
  }

  auto empty_entries{lsm_tree_->getRangeParallel(
    TimeSeriesKey{5'000, "metric", {}},
    TimeSeriesKey{10'000, "metric", {}},
    [](const auto&) { return true; }
  )};
  EXPECT_EQ(empty_entries.size(), 0);
}

TEST_F(LSMTreeTest, CanGetRangeInParallelOfEntriesWithFlushingAndDeletions) {
  for (Timestamp i{0}; i < 5'000; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->put(key, i);
  }

  for (Timestamp i{0}; i < 5'000; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->remove(key);
  }

  auto entries{lsm_tree_->getRangeParallel(
    TimeSeriesKey{0, "metric", {}},
    TimeSeriesKey{5'000, "metric", {}},
    [](const auto&) { return true; }
  )};
  EXPECT_EQ(entries.size(), 0);

  auto empty_entries{lsm_tree_->getRangeParallel(
    TimeSeriesKey{5'000, "metric", {}},
    TimeSeriesKey{10'000, "metric", {}},
    [](const auto&) { return true; }
  )};
  EXPECT_EQ(empty_entries.size(), 0);
}

TEST_F(LSMTreeTest, CanGetRangeInParallelOfEntriesWithFlushingAndFilter) {
  for (Timestamp i{0}; i < 10'000; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->put(key, i);
  }

  auto entries{lsm_tree_->getRangeParallel(
    TimeSeriesKey{0, "metric", {}},
    TimeSeriesKey{10'000, "metric", {}},
    [](const TimeSeriesKey& key) {
      return key.timestamp() % 2 == 0;
    })
  };

  EXPECT_EQ(entries.size(), 5'000);
  for (Timestamp i{0}; i < 5'000; ++i) {
    EXPECT_EQ(entries[i].second, 2 * i);
  }
}

TEST_F(LSMTreeTest, CanReplayWriteAheadLog) {
  for (Timestamp i{0}; i < 100; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->put(key, i);
  }

  lsm_tree_.reset();
  lsm_tree_ = std::make_unique<LSMTree<int>>(directory_);
  lsm_tree_->replayWAL();

  for (Timestamp i{0}; i < 100; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    auto value = lsm_tree_->get(key);
    EXPECT_EQ(value, i);
  }

  for (Timestamp i{100}; i < 200; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    auto value = lsm_tree_->get(key);
    EXPECT_EQ(value, std::nullopt);
  }

  EXPECT_EQ(lsm_tree_->sstableCount(), 0);
}

TEST_F(LSMTreeTest, CanPutWithFlushing) {
  for (Timestamp i{0}; i < 100'000; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->put(key, i);
  }

  EXPECT_EQ(lsm_tree_->sstableCount(), 100);
}

TEST_F(LSMTreeTest, CanPutWithFlushingAndGetDifferentKeys) {
  for (Timestamp i{0}; i < 100'000; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->put(key, i);
  }

  for (Timestamp i{0}; i < 100'000; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    auto value{lsm_tree_->get(key)};
    EXPECT_EQ(value, i);
  }

  EXPECT_EQ(lsm_tree_->sstableCount(), 100);
}

TEST_F(LSMTreeTest, CanPutWithFlushingAndGetRepeatedKey) {
  for (Timestamp i{0}; i < 100'000; ++i) {
    TimeSeriesKey key{i, "metric", {}};
    lsm_tree_->put(key, i);
  }

  for (auto i{0}; i < 100'000; ++i) {
    TimeSeriesKey key{0, "metric", {}};
    auto value{lsm_tree_->get(key)};
    EXPECT_EQ(value, 0);
  }

  EXPECT_EQ(lsm_tree_->sstableCount(), 100);
}

