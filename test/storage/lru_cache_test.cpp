#include "storage/lru_cache.hpp"
#include "gtest/gtest.h"

TEST(LRUCacheTest, PutsAndGets) {
    LRUCache<int, std::string> cache(3);

    cache.put(1, "one");
    cache.put(2, "two");
    cache.put(3, "three");

    EXPECT_EQ(*cache.get(1), "one");
    EXPECT_EQ(*cache.get(2), "two");
    EXPECT_EQ(*cache.get(3), "three");
}

TEST(LRUCacheTest, HandlesEviction) {
    LRUCache<int, std::string> cache(3);

    cache.put(1, "one");
    cache.put(2, "two");
    cache.put(3, "three");

    cache.put(4, "four");

    EXPECT_EQ(cache.get(1), nullptr);
    EXPECT_EQ(*cache.get(2), "two");
    EXPECT_EQ(*cache.get(3), "three");
    EXPECT_EQ(*cache.get(4), "four");
}

TEST(LRUCacheTest, UpdatesValue) {
    LRUCache<int, std::string> cache(3);

    cache.put(1, "one");
    cache.put(2, "two");
    cache.put(3, "three");

    cache.put(2, "TWO");

    EXPECT_EQ(*cache.get(2), "TWO");
    EXPECT_EQ(*cache.get(1), "one");
    EXPECT_EQ(*cache.get(3), "three");
}

TEST(LRUCacheTest, OrdersAccessUpdates) {
    LRUCache<int, std::string> cache(3);

    cache.put(1, "one");
    cache.put(2, "two");
    cache.put(3, "three");

    cache.get(1);

    cache.put(4, "four");

    EXPECT_EQ(cache.get(2), nullptr);
    EXPECT_EQ(*cache.get(1), "one");
    EXPECT_EQ(*cache.get(3), "three");
    EXPECT_EQ(*cache.get(4), "four");
}

TEST(LRUCacheTest, CacheHitsAndMisses) {
    LRUCache<int, std::string> cache(2);

    cache.put(1, "one");
    cache.put(2, "two");

    EXPECT_NE(cache.get(1), nullptr);
    EXPECT_EQ(*cache.get(1), "one");

    EXPECT_EQ(cache.get(3), nullptr);
}

TEST(LRUCacheTest, EvictsCorrectElement) {
    LRUCache<int, std::string> cache(2);

    cache.put(1, "one");
    cache.put(2, "two");

    cache.get(1);

    cache.put(3, "three");

    EXPECT_EQ(cache.get(2), nullptr);
    EXPECT_EQ(*cache.get(1), "one");
    EXPECT_EQ(*cache.get(3), "three");
}
