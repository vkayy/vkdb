#include "storage/bloom_filter.hpp"
#include "utils/random.hpp"
#include <cmath>
#include <fstream>
#include <gtest/gtest.h>

TEST(BloomFilterTest, InsertsAndChecksElements) {
    BloomFilter<int32_t> filter(1000, 0.01);
    int32_t element1 = generateRandomInt32(1, 10000);
    int32_t element2 = generateRandomInt32(10001, 20000);

    filter.insert(element1);

    EXPECT_TRUE(filter.mightContain(element1));
    EXPECT_FALSE(filter.mightContain(element2));
}

TEST(BloomFilterTest, HandlesMultipleInsertions) {
    BloomFilter<int32_t> filter(10000, 0.01);
    const int num_elements = 1000;

    for (int i = 0; i < num_elements; ++i) {
        int32_t element = generateRandomInt32(1, 1000000);
        filter.insert(element);
        EXPECT_TRUE(filter.mightContain(element));
    }
}

TEST(BloomFilterTest, MaintainsLowFalsePositiveRate) {
    const int num_elements = 1000;
    const int num_tests = 10000;
    const double target_false_positive_rate = 0.01;

    BloomFilter<int32_t> filter(num_elements, target_false_positive_rate);
    int false_positives = 0;

    std::vector<int32_t> inserted_elements;
    for (int i = 0; i < num_elements; ++i) {
        int32_t element = generateRandomInt32(1, 1000000);
        filter.insert(element);
        inserted_elements.push_back(element);
    }

    for (int i = 0; i < num_tests; ++i) {
        int32_t test_element = generateRandomInt32(1000001, 2000000);
        if (filter.mightContain(test_element)) {
            false_positives++;
        }
    }

    double false_positive_rate = static_cast<double>(false_positives) / num_tests;
    std::cout << "False positive rate: " << false_positive_rate << std::endl;
    EXPECT_LT(false_positive_rate, target_false_positive_rate * 2);
}

TEST(BloomFilterTest, SerializesAndDeserializes) {
    BloomFilter<int32_t> filter(1000, 0.01);
    const int num_elements = 100;

    std::vector<int32_t> inserted_elements;
    for (int i = 0; i < num_elements; ++i) {
        int32_t element = generateRandomInt32(1, 1000000);
        filter.insert(element);
        inserted_elements.push_back(element);
    }

    std::ofstream ofs("bloom_filter_test.bin", std::ios::binary);
    filter.serialize(ofs);
    ofs.close();

    BloomFilter<int32_t> deserialized_filter;
    std::ifstream ifs("bloom_filter_test.bin", std::ios::binary);
    deserialized_filter.deserialize(ifs);
    ifs.close();

    for (const auto &element : inserted_elements) {
        EXPECT_TRUE(deserialized_filter.mightContain(element));
    }

    std::filesystem::remove("bloom_filter_test.bin");
}

TEST(BloomFilterTest, HandlesEmptyFilter) {
    BloomFilter<int32_t> filter(100, 0.01);
    int32_t element = generateRandomInt32(1, 10000);

    EXPECT_FALSE(filter.mightContain(element));
}

TEST(BloomFilterTest, PerformsWithLargeNumberOfElements) {
    BloomFilter<int32_t> filter(1000000, 0.01);
    const int num_elements = 100000;

    for (int i = 0; i < num_elements; ++i) {
        int32_t element = generateRandomInt32(1, 1000000);
        filter.insert(element);
        EXPECT_TRUE(filter.mightContain(element));
    }
}
