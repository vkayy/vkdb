#include "storage/skip_list.hpp"
#include "utils/random.hpp"
#include <gtest/gtest.h>
#include <thread>

TEST(SkipListTest, InsertsAndSearchesForKeyValuePairs) {
    SkipList<int32_t, int32_t> skiplist;
    std::vector<std::pair<int32_t, int32_t>> pairs;

    int32_t random_key = generateRandomInt32(1, 10000);
    for (size_t i = 0; i < 100; ++i) {
        int32_t random_value = generateRandomInt32(1, 10000);
        skiplist.insert(random_key * i, {random_value, std::time_t(nullptr)});
        pairs.push_back({random_key * i, random_value});
    }

    for (const auto &[key, value] : pairs) {
        TimestampedValue<int32_t> *search_result = skiplist.findWaitFree(key);
        EXPECT_TRUE(search_result != nullptr);
        EXPECT_EQ(value, search_result->value.value());
    }
}

TEST(SkipListTest, SerializesAndDeserializesSkipList) {
    SkipList<int32_t, std::string> skiplist;
    std::vector<std::pair<int32_t, std::string>> pairs;

    int32_t random_key = generateRandomInt32(1, 10000);
    for (size_t i = 0; i < 100; ++i) {
        std::string random_value = generateRandomString(100);
        skiplist.insert(random_key * i, {random_value, std::time_t(nullptr)});
        pairs.push_back({random_key * i, random_value});
    }

    for (const auto &[key, value] : pairs) {
        TimestampedValue<std::string> *search_result = skiplist.findWaitFree(key);
        EXPECT_TRUE(search_result != nullptr);
        EXPECT_EQ(value, search_result->value.value());
    }

    const std::string filename = "./skiplist_test_output";
    skiplist.serialize(filename);

    SkipList<int32_t, std::string> deserialized_skiplist;
    deserialized_skiplist.deserialize(filename);

    for (size_t i = 0; i < 100; ++i) {
        auto [key, value] = pairs[i];
        TimestampedValue<std::string> *search_result = deserialized_skiplist.findWaitFree(key);
        EXPECT_TRUE(search_result != nullptr);
        EXPECT_EQ(value, search_result->value.value());
    }

    std::filesystem::remove(filename.c_str());
}

TEST(SkipListTest, SearchesForNonExistentKeys) {
    SkipList<int32_t, int32_t> skiplist;

    int32_t random_key = generateRandomInt32(1, 10000);
    for (size_t i = 0; i < 100; ++i) {
        int32_t random_value = generateRandomInt32(1, 10000);
        skiplist.insert(random_key * i, {random_value, std::time_t(nullptr)});
    }

    int32_t non_existent_key = generateRandomInt32(10001, 20000);
    TimestampedValue<int32_t> *value = skiplist.findWaitFree(non_existent_key);
    EXPECT_EQ(value, nullptr);
}

TEST(SkipListTest, HandlesConcurrentInsertsAndSearches) {
    SkipList<int32_t, std::string> skiplist;
    const int32_t num_threads = 10;
    const int32_t num_operations_per_thread = 1000;
    std::atomic<bool> start_flag(false);
    std::vector<std::thread> threads;
    std::vector<std::pair<int32_t, std::string>> inserted_pairs;

    int32_t random_key = generateRandomInt32(1, 100000);
    for (int32_t i = 0; i < num_threads * num_operations_per_thread; ++i) {
        std::string random_value = generateRandomString(100);
        inserted_pairs.push_back({random_key * i, random_value});
    }

    auto insert_and_search = [&](int32_t thread_id) {
        while (!start_flag) {
            std::this_thread::yield();
        }

        for (int32_t i = thread_id * num_operations_per_thread; i < (thread_id + 1) * num_operations_per_thread; ++i) {
            auto [key, value] = inserted_pairs[i];
            skiplist.insert(key, {value, std::time_t(nullptr)});

            if (generateRandomInt32(1, 100) <= 50) {
                TimestampedValue<std::string> *search_result = skiplist.findWaitFree(key);
                EXPECT_TRUE(search_result != nullptr);
                if (search_result != nullptr) {
                    EXPECT_EQ(value, search_result->value.value());
                }
            }
        }
    };

    for (int32_t i = 0; i < num_threads; ++i) {
        threads.emplace_back(insert_and_search, i);
    }

    start_flag.store(true);

    for (auto &thread : threads) {
        thread.join();
    }

    for (const auto &[key, value] : inserted_pairs) {
        TimestampedValue<std::string> *search_result = skiplist.findWaitFree(key);
        EXPECT_TRUE(search_result != nullptr);
        if (search_result != nullptr) {
            EXPECT_EQ(value, search_result->value.value());
        }
    }
}

TEST(SkipListTest, UpdatesKeys) {
    SkipList<int32_t, std::string> skiplist;

    skiplist.insert(1, {"value1", std::time_t(nullptr)});
    skiplist.insert(2, {"value2", std::time_t(nullptr)});
    skiplist.insert(3, {"value3", std::time_t(nullptr)});

    skiplist.insert(2, {"new_value2", std::time_t(nullptr)});
    skiplist.insert(3, {"new_value3", std::time_t(nullptr)});

    TimestampedValue<std::string> *value = skiplist.findWaitFree(1);
    EXPECT_TRUE(value != nullptr);
    EXPECT_EQ(value->value.value(), "value1");

    value = skiplist.findWaitFree(2);
    EXPECT_TRUE(value != nullptr);
    EXPECT_EQ(value->value.value(), "new_value2");

    value = skiplist.findWaitFree(3);
    EXPECT_TRUE(value != nullptr);
    EXPECT_EQ(value->value.value(), "new_value3");

    value = skiplist.findWaitFree(4);
    EXPECT_TRUE(value == nullptr);
}

TEST(SkipListTest, HandlesConcurrentUpdates) {
    SkipList<int32_t, std::string> skiplist;
    const int32_t num_threads = 10;
    const int32_t num_operations_per_thread = 1000;
    std::atomic<bool> start_flag(false);
    std::vector<std::thread> threads;
    std::map<int32_t, std::string> expected_values; // To store expected final values

    auto update = [&](int32_t thread_id) {
        while (!start_flag.load()) {
            std::this_thread::yield();
        }

        for (int32_t i = thread_id * num_operations_per_thread; i < (thread_id + 1) * num_operations_per_thread; ++i) {
            int32_t key = i;
            std::string value = generateRandomString(100);
            skiplist.insert(key, {value, std::time_t(nullptr)});
            expected_values[key] = value;
        }
    };

    for (int32_t i = 0; i < num_threads; ++i) {
        threads.emplace_back(update, i);
    }

    start_flag.store(true);

    for (auto &thread : threads) {
        thread.join();
    }

    for (const auto &[key, expected_value] : expected_values) {
        TimestampedValue<std::string> *search_result = skiplist.findWaitFree(key);
        EXPECT_TRUE(search_result != nullptr);
        if (search_result != nullptr) {
            EXPECT_EQ(expected_value, search_result->value.value());
        }
    }
}

TEST(SkipListIteratorTest, IteratorTraversal) {
    SkipList<int32_t, std::string> skiplist;
    skiplist.insert(1, {"one", std::time_t(nullptr)});
    skiplist.insert(2, {"two", std::time_t(nullptr)});
    skiplist.insert(3, {"three", std::time_t(nullptr)});
    skiplist.insert(4, {"four", std::time_t(nullptr)});
    skiplist.insert(5, {"five", std::time_t(nullptr)});

    std::vector<std::pair<int32_t, std::string>> expected{
        {1, "one"},
        {2, "two"},
        {3, "three"},
        {4, "four"},
        {5, "five"}};

    auto it = skiplist.begin();
    auto expectedIt = expected.begin();

    while (it != skiplist.end() && expectedIt != expected.end()) {
        EXPECT_EQ(it->first, expectedIt->first);
        EXPECT_EQ(it->second.value.value(), expectedIt->second);
        ++it;
        ++expectedIt;
    }

    EXPECT_EQ(it, skiplist.end());
    EXPECT_EQ(expectedIt, expected.end());
}

TEST(SkipListIteratorTest, EmptyIterator) {
    SkipList<int32_t, std::string> emptyList;
    auto it = emptyList.begin();
    EXPECT_EQ(it, emptyList.end());
}

TEST(SkipListIteratorTest, IteratorFind) {
    SkipList<int32_t, std::string> skiplist;
    skiplist.insert(1, {"one", std::time_t(nullptr)});
    skiplist.insert(2, {"two", std::time_t(nullptr)});
    skiplist.insert(3, {"three", std::time_t(nullptr)});
    skiplist.insert(4, {"four", std::time_t(nullptr)});
    skiplist.insert(5, {"five", std::time_t(nullptr)});

    auto it = skiplist.begin();

    while (it != skiplist.end()) {
        if (it->first == 3) {
            EXPECT_EQ(it->second.value.value(), "three");
            break;
        }
        ++it;
    }
    EXPECT_NE(it, skiplist.end());
}

TEST(SkipListIteratorTest, IteratorLoop) {
    SkipList<int32_t, std::string> skiplist;
    skiplist.insert(1, {"one", std::time_t(nullptr)});
    skiplist.insert(2, {"two", std::time_t(nullptr)});
    skiplist.insert(3, {"three", std::time_t(nullptr)});
    skiplist.insert(4, {"four", std::time_t(nullptr)});
    skiplist.insert(5, {"five", std::time_t(nullptr)});

    std::vector<int32_t> keys;
    for (auto it = skiplist.begin(); it != skiplist.end(); ++it) {
        keys.push_back(it->first);
    }

    std::vector<int32_t> expectedKeys{1, 2, 3, 4, 5};
    EXPECT_EQ(keys, expectedKeys);
}
