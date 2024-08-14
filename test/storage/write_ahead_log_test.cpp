#include "storage/write_ahead_log.hpp"
#include <fstream>
#include <gtest/gtest.h>
#include <mutex>
#include <thread>

/**
 * @brief Creates a temporary filename with static counter.
 *
 * @return std::string The filename.
 */
static std::string generate_log_file_name() {
    static int counter = 0;
    return "test_log_" + std::to_string(counter++) + ".log";
}

TEST(WriteAheadLogTest, ConstructorOpensFile) {
    std::string log_file_path = generate_log_file_name();
    EXPECT_NO_THROW(WriteAheadLog log(log_file_path));
}

TEST(WriteAheadLogTest, AppendsLogEntry) {
    std::string log_file_path = generate_log_file_name();
    WriteAheadLog wal(log_file_path);

    EXPECT_NO_THROW(wal.appendLog("Test entry 1"));
    EXPECT_NO_THROW(wal.appendLog("Test entry 2"));

    std::ifstream file(log_file_path);
    ASSERT_TRUE(file.is_open());

    std::string line;
    std::getline(file, line);
    EXPECT_EQ(line, "Test entry 1");

    std::getline(file, line);
    EXPECT_EQ(line, "Test entry 2");
}

TEST(WriteAheadLogTest, FlushesLog) {
    std::string log_file_path = generate_log_file_name();
    WriteAheadLog wal(log_file_path);

    wal.appendLog("Flushed entry");
    EXPECT_NO_THROW(wal.flushLog());

    std::ifstream file(log_file_path);
    ASSERT_TRUE(file.is_open());

    std::string line;
    std::getline(file, line);
    EXPECT_EQ(line, "Flushed entry");
}

TEST(WriteAheadLogTest, RecoversFromLog) {
    std::string log_file_path = generate_log_file_name();

    WriteAheadLog wal(log_file_path);
    wal.appendLog("Recovery entry 1");
    wal.appendLog("Recovery entry 2");
    wal.flushLog();

    std::ofstream empty_file(log_file_path, std::ios::trunc);
    empty_file.close();

    WriteAheadLog new_wal(log_file_path);
    EXPECT_NO_THROW(new_wal.recoverFromLog());

    std::ifstream file(log_file_path);
    ASSERT_TRUE(file.is_open());

    std::string line;
    std::getline(file, line);
    EXPECT_TRUE(line.empty());
}

TEST(WriteAheadLogTest, HandlesConcurrentAppending) {
    std::string log_file_path = generate_log_file_name();
    WriteAheadLog wal(log_file_path);

    const int num_threads = 10;
    auto append_task = [&]() {
        for (int i = 0; i < 10; ++i) {
            std::string entry = "Concurrent entry " + std::to_string(i);
            wal.appendLog(entry);
        }
    };

    std::thread threads[num_threads];
    for (int i = 0; i < num_threads; ++i) {
        threads[i] = std::thread(append_task);
    }
    for (int i = 0; i < num_threads; ++i) {
        threads[i].join();
    }

    std::ifstream file(log_file_path);
    EXPECT_TRUE(file.is_open());

    std::string line;
    int entry_count = 0;
    while (std::getline(file, line)) {
        ++entry_count;
    }

    EXPECT_GE(entry_count, num_threads * 10);
}
