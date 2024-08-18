#include "storage/bloom_filter.hpp"
#include "storage/mem_table.hpp"
#include "storage/ss_table.hpp"
#include "utils/random.hpp"
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>

template <typename TKey, typename TValue>
std::unique_ptr<SSTable<TKey, TValue>> createAndPopulateSSTable(const std::string &filename, const std::vector<std::pair<TKey, TValue>> &pairs) {
    MemTable<TKey, TValue> memtable;
    BloomFilter<TKey> bloom_filter(pairs.size(), 0.01);
    KeyRange<TKey> key_range;
    for (const auto &[key, value] : pairs) {
        memtable.put(key, {value, std::time_t(nullptr)});
    }
    auto sstable = std::make_unique<SSTable<TKey, TValue>>(filename);
    sstable->flushFromMemTable(memtable, bloom_filter, key_range);
    return sstable;
}

TEST(SSTableTest, FlushFromMemTableAndRetrieve) {
    MemTable<int32_t, std::string> memtable;
    int32_t random_key = generateRandomInt32(1, 10000);
    std::string random_value = generateRandomString(100);

    memtable.put(random_key, {random_value, std::time_t(nullptr)});

    BloomFilter<int32_t> bloom_filter(10, 0.01);
    bloom_filter.insert(random_key);
    KeyRange<int32_t> key_range;
    key_range.updateKeyRange(random_key);
    SSTable<int32_t, std::string> sstable("./sstable_test_output");

    sstable.flushFromMemTable(memtable, bloom_filter, key_range);

    auto retrieved_value = sstable.get(random_key);
    EXPECT_TRUE(retrieved_value.value.has_value());
    EXPECT_EQ(retrieved_value.value.value(), random_value);

    std::filesystem::remove("./sstable_test_output");
    std::filesystem::remove("./sstable_test_output.idx");
}

TEST(SSTableTest, HandlesNonExistentKey) {
    MemTable<int32_t, std::string> memtable;
    int32_t random_key = generateRandomInt32(1, 10000);
    std::string random_value = generateRandomString(100);

    memtable.put(random_key, {random_value, std::time_t(nullptr)});

    BloomFilter<int32_t> bloom_filter(10, 0.01);
    KeyRange<int32_t> key_range;
    SSTable<int32_t, std::string> sstable("./sstable_test_output");

    sstable.flushFromMemTable(memtable, bloom_filter, key_range);

    auto non_existent_key = random_key + 1;
    EXPECT_THROW(sstable.get(non_existent_key), std::runtime_error);
    std::filesystem::remove("./sstable_test_output");
    std::filesystem::remove("./sstable_test_output.idx");
}

TEST(SSTableTest, HandlesDeletedKey) {
    MemTable<int32_t, std::string> memtable;
    int32_t random_key = generateRandomInt32(1, 10000);
    std::string random_value = generateRandomString(10);

    memtable.put(random_key, {random_value, std::time_t(nullptr)});
    memtable.remove(random_key);
    EXPECT_THROW(memtable.get(random_key), std::runtime_error);

    BloomFilter<int32_t> bloom_filter(10, 0.01);
    KeyRange<int32_t> key_range;
    SSTable<int32_t, std::string> sstable("./sstable_test_output");

    sstable.flushFromMemTable(memtable, bloom_filter, key_range);

    EXPECT_THROW(sstable.get(random_key), std::runtime_error);

    std::filesystem::remove("./sstable_test_output");
    std::filesystem::remove("./sstable_test_output.idx");
}

TEST(SSTableTest, DeserializesMetadataAndRetrieves) {
    MemTable<int32_t, std::string> memtable;
    int32_t random_key1 = generateRandomInt32(1, 10000);
    std::string random_value1 = generateRandomString(100);
    int32_t random_key2 = generateRandomInt32(10001, 20000);
    std::string random_value2 = generateRandomString(100);

    memtable.put(random_key1, {random_value1, std::time_t(nullptr)});
    memtable.put(random_key2, {random_value2, std::time_t(nullptr)});

    BloomFilter<int32_t> bloom_filter(1000, 0.01);
    KeyRange<int32_t> key_range;
    SSTable<int32_t, std::string> sstable("./sstable_test_output");

    sstable.flushFromMemTable(memtable, bloom_filter, key_range);

    SSTable<int32_t, std::string> sstable_loaded("./sstable_test_output");

    auto retrieved_value1 = sstable_loaded.get(random_key1);
    EXPECT_TRUE(retrieved_value1.value.has_value());
    EXPECT_EQ(retrieved_value1.value.value(), random_value1);

    auto retrieved_value2 = sstable_loaded.get(random_key2);
    EXPECT_TRUE(retrieved_value2.value.has_value());
    EXPECT_EQ(retrieved_value2.value.value(), random_value2);

    EXPECT_EQ(key_range.getMinKey(), random_key1);
    EXPECT_EQ(key_range.getMaxKey(), random_key2);

    std::filesystem::remove("./sstable_test_output");
    std::filesystem::remove("./sstable_test_output.idx");
}

TEST(SSTableTest, HandlesLargeNumberOfPairs) {
    MemTable<int32_t, std::string> memtable;
    const int32_t num_pairs = 1000;
    std::vector<std::pair<int32_t, std::string>> pairs;

    for (int32_t i = 0; i < num_pairs; ++i) {
        int32_t key = i;
        std::string value = generateRandomString(100);
        memtable.put(key, {value, std::time_t(nullptr)});
        pairs.push_back({key, value});
    }

    BloomFilter<int32_t> bloom_filter(1000, 0.01);
    KeyRange<int32_t> key_range;
    SSTable<int32_t, std::string> sstable("./sstable_test_output");

    sstable.flushFromMemTable(memtable, bloom_filter, key_range);

    for (const auto &[key, value] : pairs) {
        auto retrieved_value = sstable.get(key);
        EXPECT_TRUE(retrieved_value.value.has_value());
        EXPECT_EQ(retrieved_value.value.value(), value);
    }

    std::filesystem::remove("./sstable_test_output");
    std::filesystem::remove("./sstable_test_output.idx");
}

TEST(SSTableMergeTest, MergesTwoNonOverlappingSSTables) {
    std::vector<std::pair<int32_t, std::string>> pairs1 = {
        {1, "value1"}, {3, "value3"}, {5, "value5"}};
    std::vector<std::pair<int32_t, std::string>> pairs2 = {
        {7, "value7"}, {9, "value9"}, {11, "value11"}};

    auto sstable1 = createAndPopulateSSTable<int32_t, std::string>("./sstable1", pairs1);
    auto sstable2 = createAndPopulateSSTable<int32_t, std::string>("./sstable2", pairs2);

    BloomFilter<int32_t> merged_bloom_filter(pairs1.size() + pairs2.size(), 0.01);
    KeyRange<int32_t> merged_key_range;
    auto merged_sstable = sstable1->merge(*sstable2, merged_bloom_filter, merged_key_range);

    for (const auto &[key, value] : pairs1) {
        EXPECT_TRUE(merged_bloom_filter.mightContain(key));
        EXPECT_EQ(merged_sstable->get(key).value.value(), value);
    }

    for (const auto &[key, value] : pairs2) {
        EXPECT_TRUE(merged_bloom_filter.mightContain(key));
        EXPECT_EQ(merged_sstable->get(key).value.value(), value);
    }

    EXPECT_EQ(merged_key_range.getMinKey(), 1);
    EXPECT_EQ(merged_key_range.getMaxKey(), 11);

    std::filesystem::remove("./sstable1");
    std::filesystem::remove("./sstable1.idx");
    std::filesystem::remove("./sstable2");
    std::filesystem::remove("./sstable2.idx");
    std::filesystem::remove("./sstable1_merged");
    std::filesystem::remove("./sstable1_merged.idx");
}

TEST(SSTableMergeTest, MergesTwoOverlappingSSTables) {
    std::vector<std::pair<int32_t, std::string>> pairs1 = {
        {1, "value1"}, {3, "value3"}, {5, "value5"}};
    std::vector<std::pair<int32_t, std::string>> pairs2 = {
        {4, "value4"}, {5, "value6"}, {6, "value6"}};

    auto sstable1 = createAndPopulateSSTable<int32_t, std::string>("./sstable1", pairs1);
    auto sstable2 = createAndPopulateSSTable<int32_t, std::string>("./sstable2", pairs2);

    BloomFilter<int32_t> merged_bloom_filter(pairs1.size() + pairs2.size(), 0.01);
    KeyRange<int32_t> merged_key_range;
    auto merged_sstable = sstable1->merge(*sstable2, merged_bloom_filter, merged_key_range);

    std::map<int32_t, std::string> expected_values = {
        {1, "value1"}, {3, "value3"}, {4, "value4"}, {5, "value6"}, {6, "value6"}};

    for (const auto &[key, value] : expected_values) {
        EXPECT_EQ(merged_sstable->get(key).value.value(), value);
    }

    EXPECT_EQ(merged_key_range.getMinKey(), 1);
    EXPECT_EQ(merged_key_range.getMaxKey(), 6);

    std::filesystem::remove("./sstable1");
    std::filesystem::remove("./sstable1.idx");
    std::filesystem::remove("./sstable2");
    std::filesystem::remove("./sstable2.idx");
    std::filesystem::remove("./sstable1_merged");
    std::filesystem::remove("./sstable1_merged.idx");
}

TEST(SSTableMergeTest, HandlesEmptySSTables) {
    std::vector<std::pair<int32_t, std::string>> pairs1 = {
        {1, "value1"}, {3, "value3"}};

    auto sstable1 = createAndPopulateSSTable<int32_t, std::string>("./sstable1", pairs1);
    auto sstable2 = std::make_unique<SSTable<int32_t, std::string>>("./sstable2");

    BloomFilter<int32_t> merged_bloom_filter(10, 0.01);
    KeyRange<int32_t> merged_key_range;
    auto merged_sstable = sstable1->merge(*sstable2, merged_bloom_filter, merged_key_range);

    for (const auto &[key, value] : pairs1) {
        EXPECT_EQ(merged_sstable->get(key).value.value(), value);
    }

    EXPECT_EQ(merged_key_range.getMinKey(), 1);
    EXPECT_EQ(merged_key_range.getMaxKey(), 3);

    std::filesystem::remove("./sstable1");
    std::filesystem::remove("./sstable1.idx");
    std::filesystem::remove("./sstable2");
    std::filesystem::remove("./sstable2.idx");
    std::filesystem::remove("./sstable1_merged");
    std::filesystem::remove("./sstable1_merged.idx");
}

TEST(SSTableMergeTest, MergesWithDuplicateKeys) {
    std::vector<std::pair<int32_t, std::string>> pairs1 = {
        {1, "value1"}, {2, "value2"}, {3, "value3"}};
    std::vector<std::pair<int32_t, std::string>> pairs2 = {
        {2, "value4"}, {3, "value5"}, {4, "value6"}};

    auto sstable1 = createAndPopulateSSTable<int32_t, std::string>("./sstable1", pairs1);
    auto sstable2 = createAndPopulateSSTable<int32_t, std::string>("./sstable2", pairs2);

    BloomFilter<int32_t> merged_bloom_filter(pairs1.size() + pairs2.size(), 0.01);
    KeyRange<int32_t> merged_key_range;
    auto merged_sstable = sstable1->merge(*sstable2, merged_bloom_filter, merged_key_range);

    std::map<int32_t, std::string> expected_values = {
        {1, "value1"}, {2, "value4"}, {3, "value5"}, {4, "value6"}};

    for (const auto &[key, value] : expected_values) {
        EXPECT_EQ(merged_sstable->get(key).value.value(), value);
    }

    EXPECT_EQ(merged_key_range.getMinKey(), 1);
    EXPECT_EQ(merged_key_range.getMaxKey(), 4);

    std::filesystem::remove("./sstable1");
    std::filesystem::remove("./sstable1.idx");
    std::filesystem::remove("./sstable2");
    std::filesystem::remove("./sstable2.idx");
    std::filesystem::remove("./sstable1_merged");
    std::filesystem::remove("./sstable1_merged.idx");
}
