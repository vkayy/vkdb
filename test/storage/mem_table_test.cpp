#include "storage/mem_table.hpp"
#include "utils/random.hpp"
#include <gtest/gtest.h>
#include <thread>

TEST(MemTableTest, PutsAndGetsKeyValuePairs) {
    MemTable<int32_t, std::string> memtable;
    std::vector<std::pair<int32_t, std::string>> pairs;

    int32_t random_key = generateRandomInt32(0, 10000);
    for (size_t i = 0; i < 100; ++i) {
        std::string random_value = generateRandomString(100);
        memtable.put(random_key * i, random_value);
        pairs.push_back({random_key * i, random_value});
    }

    for (size_t i = 0; i < 100; ++i) {
        auto [key, value] = pairs[i];
        EXPECT_EQ(memtable.get(key), value);
    }
}

TEST(MemTableTest, SerializesAndDeserializesMemTable) {
    MemTable<int32_t, std::string> memtable;
    std::vector<std::pair<int32_t, std::string>> pairs;

    int32_t random_key = generateRandomInt32(1, 10000);
    for (size_t i = 0; i < 100; ++i) {
        std::string random_value = generateRandomString(100);
        memtable.put(random_key * i, random_value);
        pairs.push_back({random_key * i, random_value});
    }

    const std::string filename = "./memtable_test_output";
    memtable.serialize(filename);

    MemTable<int32_t, std::string> deserialized_memtable;
    deserialized_memtable.deserialize(filename);

    for (size_t i = 0; i < 100; ++i) {
        auto [key, value] = pairs[i];
        EXPECT_EQ(deserialized_memtable.get(key), value);
    }

    std::remove(filename.c_str());
}

TEST(MemTableTest, HandlesConcurrentPutsAndGets) {
    MemTable<int32_t, std::string> memtable;
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

    auto put_and_get = [&](int32_t thread_id) {
        while (!start_flag) {
            std::this_thread::yield();
        }

        for (int32_t i = thread_id * num_operations_per_thread; i < (thread_id + 1) * num_operations_per_thread; ++i) {
            auto [key, value] = inserted_pairs[i];
            memtable.put(key, value);

            if (generateRandomInt32(1, 100) <= 50) {
                std::string retrieved_value = memtable.get(key);
                EXPECT_EQ(value, retrieved_value);
            }
        }
    };

    for (int32_t i = 0; i < num_threads; ++i) {
        threads.emplace_back(put_and_get, i);
    }

    start_flag.store(true);

    for (auto &thread : threads) {
        thread.join();
    }

    for (const auto &[key, value] : inserted_pairs) {
        std::string retrieved_value = memtable.get(key);
        EXPECT_EQ(value, retrieved_value);
    }
}

TEST(MemTableTest, HandlesConcurrentUpdates) {
    MemTable<int32_t, std::string> memtable;
    const int32_t num_threads = 10;
    const int32_t num_operations_per_thread = 1000;
    std::atomic<bool> start_flag(false);
    std::vector<std::thread> threads;
    std::map<int32_t, std::string> expected_values;

    auto update = [&](int32_t thread_id) {
        while (!start_flag.load()) {
            std::this_thread::yield();
        }

        for (int32_t i = thread_id * num_operations_per_thread; i < (thread_id + 1) * num_operations_per_thread; ++i) {
            int32_t key = i;
            std::string value = generateRandomString(100);
            memtable.put(key, value);
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
        std::string search_result = memtable.get(key);
        EXPECT_EQ(expected_value, search_result);
    }
}

TEST(MemTableTest, RemovesKeyValuePairs) {
    MemTable<int32_t, std::string> memtable;
    int32_t key = generateRandomInt32(1, 10000);
    std::string value = generateRandomString(100);

    memtable.put(key, value);
    EXPECT_EQ(memtable.get(key), value);

    memtable.remove(key);
    EXPECT_THROW(memtable.get(key), std::runtime_error);
}

TEST(MemTableTest, HandlesConcurrentRemovalsAndGets) {
    MemTable<int32_t, std::string> memtable;
    const int32_t num_threads = 10;
    const int32_t num_operations_per_thread = 100;
    std::atomic<bool> start_flag(false);
    std::vector<std::thread> threads;
    std::vector<int32_t> keys;

    for (int32_t i = 0; i < num_threads * num_operations_per_thread; ++i) {
        keys.push_back(i);
        memtable.put(i, "value_" + std::to_string(i));
    }

    auto remove_and_get = [&](int32_t thread_id) {
        while (!start_flag.load()) {
            std::this_thread::yield();
        }

        for (int32_t i = thread_id * num_operations_per_thread; i < (thread_id + 1) * num_operations_per_thread; ++i) {
            memtable.remove(keys[i]);
            EXPECT_THROW(memtable.get(keys[i]), std::runtime_error);
        }
    };

    for (int32_t i = 0; i < num_threads; ++i) {
        threads.emplace_back(remove_and_get, i);
    }

    start_flag.store(true);

    for (auto &thread : threads) {
        thread.join();
    }
}

TEST(MemTableTest, HandlesTombstoneSerializationAndDeserialization) {
    MemTable<int32_t, std::string> memtable;

    int32_t key = generateRandomInt32(1, 10000);
    std::string value = generateRandomString(100);
    memtable.put(key, value);

    memtable.remove(key);
    EXPECT_THROW(memtable.get(key), std::runtime_error);

    const std::string filename = "./memtable_remove_test_output";
    memtable.serialize(filename);

    MemTable<int32_t, std::string> deserialized_memtable;
    deserialized_memtable.deserialize(filename);

    EXPECT_THROW(deserialized_memtable.get(key), std::runtime_error);

    std::remove(filename.c_str());
}
