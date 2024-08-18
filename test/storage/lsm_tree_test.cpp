#include "storage/lsm_tree.hpp"
#include "utils/random.hpp"
#include <filesystem>
#include <gtest/gtest.h>

TEST(LSMTreeTest, PutsAndGets) {
    LSMTree<int32_t, std::string> lsm_tree("./wal_test.log");
    int32_t random_key = generateRandomInt32(1, 10000);
    std::string random_value = generateRandomString(100);

    lsm_tree.put(random_key, random_value, std::time_t(nullptr));

    auto retrieved_value = lsm_tree.get(random_key);
    EXPECT_TRUE(retrieved_value.value.has_value());
    EXPECT_EQ(retrieved_value.value.value(), random_value);

    std::filesystem::remove("./wal_test.log");
}

TEST(LSMTreeTest, RemovesKey) {
    LSMTree<int32_t, std::string> lsm_tree("./wal_test.log");
    int32_t random_key = generateRandomInt32(1, 10000);
    std::string random_value = generateRandomString(100);

    lsm_tree.put(random_key, random_value, std::time_t(nullptr));
    lsm_tree.remove(random_key);

    auto retrieved_value = lsm_tree.get(random_key);
    EXPECT_FALSE(retrieved_value.value.has_value());

    std::filesystem::remove("./wal_test.log");
}

TEST(LSMTreeTest, HandlesNonExistentKey) {
    LSMTree<int32_t, std::string> lsm_tree("./wal_test.log");
    int32_t random_key = generateRandomInt32(1, 10000);

    auto retrieved_value = lsm_tree.get(random_key);
    EXPECT_FALSE(retrieved_value.value.has_value());

    std::filesystem::remove("./wal_test.log");
}

TEST(LSMTreeTest, FlushesMemTableToSSTable) {
    LSMTree<int32_t, std::string> lsm_tree("./wal_test.log");
    const int32_t num_pairs = 10000;

    for (int32_t i = 0; i < num_pairs; ++i) {
        int32_t key = i;
        std::string value = generateRandomString(100);
        lsm_tree.put(key, value, std::time_t(nullptr));
    }

    for (int32_t i = 0; i < num_pairs; ++i) {
        auto retrieved_value = lsm_tree.get(i);
        EXPECT_TRUE(retrieved_value.value.has_value());
    }

    std::filesystem::remove("./wal_test.log");
    std::filesystem::remove("./sstable_0.db");
    std::filesystem::remove("./sstable_0.db.meta");
}

TEST(LSMTreeTest, RecoversFromWAL) {
    LSMTree<int32_t, std::string> lsm_tree("./wal_test.log");
    int32_t random_key = generateRandomInt32(1, 10000);
    std::string random_value = generateRandomString(100);

    lsm_tree.put(random_key, random_value, std::time_t(nullptr));

    LSMTree<int32_t, std::string> recovered_lsm_tree("./wal_test.log");
    recovered_lsm_tree.recover();

    auto retrieved_value = recovered_lsm_tree.get(random_key);
    EXPECT_TRUE(retrieved_value.value.has_value());
    EXPECT_EQ(retrieved_value.value.value(), random_value);

    std::filesystem::remove("./wal_test.log");
}

TEST(LSMTreeTest, HandlesConcurrentOperations) {
    LSMTree<int32_t, std::string> lsm_tree("./wal_test.log");
    const int32_t num_threads = 4;
    const int32_t ops_per_thread = 10000;

    auto worker = [&](int thread_id) {
        for (int32_t i = 0; i < ops_per_thread; ++i) {
            int32_t key = thread_id * ops_per_thread + i;
            std::string value = generateRandomString(100);
            lsm_tree.put(key, value, std::time_t(nullptr));

            auto retrieved_value = lsm_tree.get(key);
            EXPECT_TRUE(retrieved_value.value.has_value());
            EXPECT_EQ(retrieved_value.value.value(), value);

            if (i % 10 == 0) {
                lsm_tree.remove(key);
                auto removed_value = lsm_tree.get(key);
                EXPECT_FALSE(removed_value.value.has_value());
            }
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker, i);
    }

    for (auto &thread : threads) {
        thread.join();
    }

    std::filesystem::remove("./wal_test.log");
    for (int i = 0; i < 10; ++i) {
        std::filesystem::remove("./sstable_" + std::to_string(i) + ".db");
        std::filesystem::remove("./sstable_" + std::to_string(i) + ".db.meta");
    }
}
