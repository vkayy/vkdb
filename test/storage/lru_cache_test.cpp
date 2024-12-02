#include "gtest/gtest.h"
#include "storage/lru_cache.h"

class LRUCacheTest : public ::testing::Test {
protected:
  using Cache = LRUCache<int, int>;
  
  static constexpr CacheCapacity CACHE_CAPACITY{3};

  void SetUp() override {
    cache_ = std::make_unique<Cache>(CACHE_CAPACITY);
  }

  std::unique_ptr<Cache> cache_;
};

TEST_F(LRUCacheTest, CanObtainValueWhenKeyExists) {
  cache_->put(1, 1);
  cache_->put(2, 2);

  auto get1{cache_->get(1)};
  auto get2{cache_->get(2)};

  EXPECT_EQ(get1, 1);
  EXPECT_EQ(get2, 2);
}

TEST_F(LRUCacheTest, CanUpdateValueWhenKeyExists) {
  cache_->put(1, 1);
  cache_->put(1, 2);

  auto get1{cache_->get(1)};

  EXPECT_EQ(get1, 2);
}

TEST_F(LRUCacheTest, EvictsLeastRecentlyUsedWhenFull) {
  cache_->put(1, 1);
  cache_->put(2, 2);
  cache_->put(3, 3);
  cache_->put(4, 4);

  auto contains1{cache_->contains(1)};
  auto contains2{cache_->contains(2)};
  auto contains3{cache_->contains(3)};
  auto contains4{cache_->contains(4)};

  EXPECT_FALSE(contains1);
  EXPECT_TRUE(contains2);
  EXPECT_TRUE(contains3);
  EXPECT_TRUE(contains4);
}

TEST_F(LRUCacheTest, DoesNotModifyWhenCheckingKeyExists) {
  cache_->put(1, 1);
  cache_->put(2, 2);
  cache_->put(3, 3);

  auto contains4{cache_->contains(4)};
  auto contains1{cache_->contains(1)};
  auto contains2{cache_->contains(2)};
  auto contains3{cache_->contains(3)};

  EXPECT_FALSE(contains4);
  EXPECT_TRUE(contains1);
  EXPECT_TRUE(contains2);
  EXPECT_TRUE(contains3);
}

TEST_F(LRUCacheTest, ReturnsCacheCapacityWhenCapacityChecked) {
  auto capacity{cache_->capacity()};

  EXPECT_EQ(capacity, CACHE_CAPACITY);
}

TEST_F(LRUCacheTest, ReturnsNumberOfElementsWhenSizeChecked) {
  cache_->put(1, 1);
  cache_->put(2, 2);
  
  auto size{cache_->size()};

  EXPECT_EQ(size, 2);
}

TEST_F(LRUCacheTest, ContentsEmptiedWhenCacheCleared) {
  cache_->put(1, 1);
  cache_->put(2, 2);
  cache_->clear();

  auto get1{cache_->get(1)};
  auto get2{cache_->get(2)};

  EXPECT_EQ(get1, std::nullopt);
  EXPECT_EQ(get2, std::nullopt);
}
