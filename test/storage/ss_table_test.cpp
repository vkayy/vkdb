#include "storage/mem_table.hpp"
#include "storage/ss_table.hpp"
#include "utils/random.hpp"
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>

TEST(SSTableTest, FlushFromMemTableAndRetrieve) {
    MemTable<int32_t, std::string> memtable;
    int32_t random_key = generateRandomInt32(1, 10000);
    std::string random_value = generateRandomString(100);

    memtable.put(random_key, random_value);

    SSTable<int32_t, std::string> sstable("./sstable_test_output");
    sstable.flushFromMemTable(memtable);

    auto retrieved_value = sstable.get(random_key);
    EXPECT_TRUE(retrieved_value.has_value());
    EXPECT_EQ(retrieved_value.value(), random_value);

    std::filesystem::remove("./sstable_test_output");
    std::filesystem::remove("./sstable_test_output.idx");
}

TEST(SSTableTest, HandlesNonExistentKey) {
    MemTable<int32_t, std::string> memtable;
    int32_t random_key = generateRandomInt32(1, 10000);
    std::string random_value = generateRandomString(100);

    memtable.put(random_key, random_value);

    SSTable<int32_t, std::string> sstable("./sstable_test_output");
    sstable.flushFromMemTable(memtable);

    auto non_existent_key = random_key + 1;
    auto retrieved_value = sstable.get(non_existent_key);
    EXPECT_FALSE(retrieved_value.has_value());

    std::filesystem::remove("./sstable_test_output");
    std::filesystem::remove("./sstable_test_output.idx");
}

TEST(SSTableTest, HandlesDeletedKey) {
    MemTable<int32_t, std::string> memtable;
    int32_t random_key = generateRandomInt32(1, 10000);
    std::string random_value = generateRandomString(10);

    memtable.put(random_key, random_value);
    memtable.remove(random_key);
    EXPECT_THROW(memtable.get(random_key), std::runtime_error);

    SSTable<int32_t, std::string> sstable("./sstable_test_output");
    sstable.flushFromMemTable(memtable);

    auto retrieved_value = sstable.get(random_key);
    EXPECT_FALSE(retrieved_value.has_value());

    std::filesystem::remove("./sstable_test_output");
    std::filesystem::remove("./sstable_test_output.idx");
}

TEST(SSTableTest, DeserializesMetadataAndRetrieves) {
    MemTable<int32_t, std::string> memtable;
    int32_t random_key1 = generateRandomInt32(1, 10000);
    std::string random_value1 = generateRandomString(100);
    int32_t random_key2 = generateRandomInt32(10001, 20000);
    std::string random_value2 = generateRandomString(100);

    memtable.put(random_key1, random_value1);
    memtable.put(random_key2, random_value2);

    SSTable<int32_t, std::string> sstable("./sstable_test_output");
    sstable.flushFromMemTable(memtable);

    SSTable<int32_t, std::string> sstable_loaded("./sstable_test_output");
    sstable_loaded.deserializeMetadata();

    auto retrieved_value1 = sstable_loaded.get(random_key1);
    EXPECT_TRUE(retrieved_value1.has_value());
    EXPECT_EQ(retrieved_value1.value(), random_value1);

    auto retrieved_value2 = sstable_loaded.get(random_key2);
    EXPECT_TRUE(retrieved_value2.has_value());
    EXPECT_EQ(retrieved_value2.value(), random_value2);

    EXPECT_EQ(sstable_loaded.getMinKey(), random_key1);
    EXPECT_EQ(sstable_loaded.getMaxKey(), random_key2);

    std::filesystem::remove("./sstable_test_output");
    std::filesystem::remove("./sstable_test_output.idx");
}

TEST(SSTableTest, HandlesLargeNumberOfPairs) {
    MemTable<int32_t, std::string> memtable;
    const int32_t num_pairs = 10000;
    std::vector<std::pair<int32_t, std::string>> pairs;

    for (int32_t i = 0; i < num_pairs; ++i) {
        int32_t key = i;
        std::string value = generateRandomString(100);
        memtable.put(key, value);
        pairs.push_back({key, value});
    }

    SSTable<int32_t, std::string> sstable("./sstable_test_output");
    sstable.flushFromMemTable(memtable);

    for (const auto &[key, value] : pairs) {
        auto retrieved_value = sstable.get(key);
        EXPECT_TRUE(retrieved_value.has_value());
        EXPECT_EQ(retrieved_value.value(), value);
    }

    std::filesystem::remove("./sstable_test_output");
    std::filesystem::remove("./sstable_test_output.idx");
}
