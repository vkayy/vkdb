#include "gtest/gtest.h"
#include "storage/lru_cache.h"

class LRUCacheTest : public ::testing::Test {
protected:
  static constexpr LRUCacheCapacity CACHE_CAPACITY{3};

  void SetUp() override {
    cache = std::make_unique<LRUCache<int, int>>(CACHE_CAPACITY);
  }

  std::unique_ptr<LRUCache<int, int>> cache;
};

TEST_F(LRUCacheTest, ValuesPutInCacheCanBeRetrieved) {
  cache->put(1, 1);
  cache->put(2, 2);
  EXPECT_EQ(cache->get(1), 1);
  EXPECT_EQ(cache->get(2), 2);
}

TEST_F(LRUCacheTest, ValuesPutInCacheCanBeUpdated) {
  cache->put(1, 1);
  cache->put(2, 2);
  cache->put(1, 10);
  EXPECT_EQ(cache->get(1), 10);
  EXPECT_EQ(cache->get(2), 2);
}

TEST_F(LRUCacheTest, LeastRecentlyUsedValueIsEvictedWhenCapacityIsReached) {
  cache->put(1, 1);
  cache->put(2, 2);
  cache->put(3, 3);
  EXPECT_EQ(cache->get(1), 1);
  cache->put(4, 4);
  EXPECT_EQ(cache->get(1), 1);
  EXPECT_EQ(cache->get(2), std::nullopt);
  EXPECT_EQ(cache->get(3), 3);
  EXPECT_EQ(cache->get(4), 4);
}