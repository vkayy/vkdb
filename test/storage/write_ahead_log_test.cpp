#include "storage/write_ahead_log.hpp"
#include <gtest/gtest.h>
#include <optional>
#include <string>

/**
 * @brief Generate a unique log file name.
 *
 * @return `std::string` The generated log file name.
 */
static std::string generate_log_file_name() {
    static int counter = 0;
    return "test_log_" + std::to_string(counter++) + ".log";
}

TEST(WriteAheadLogTest, AppendsLogEntry) {
    std::string log_file_path = generate_log_file_name();
    WriteAheadLog<int32_t, std::string> wal(log_file_path);

    EXPECT_NO_THROW(wal.appendLog(1, std::optional<std::string>("Test entry 1")));
    EXPECT_NO_THROW(wal.appendLog(2, std::optional<std::string>("Test entry 2")));

    wal.flushLog();

    std::ifstream file(log_file_path, std::ios::binary);
    ASSERT_TRUE(file.is_open());

    size_t size;
    std::time_t timestamp;
    int32_t key;
    bool has_value;
    std::string value;

    file.read(reinterpret_cast<char *>(&size), sizeof(size));
    file.read(reinterpret_cast<char *>(&timestamp), size);
    file.read(reinterpret_cast<char *>(&size), sizeof(size));
    file.read(reinterpret_cast<char *>(&key), size);
    file.read(reinterpret_cast<char *>(&size), sizeof(size));
    file.read(reinterpret_cast<char *>(&has_value), size);

    EXPECT_EQ(key, 1);

    if (has_value) {
        file.read(reinterpret_cast<char *>(&size), sizeof(size));
        value.resize(size);
        file.read(&value[0], size);
        EXPECT_EQ(value, std::string("Test entry 1"));
    }

    file.read(reinterpret_cast<char *>(&size), sizeof(size));
    file.read(reinterpret_cast<char *>(&timestamp), size);
    file.read(reinterpret_cast<char *>(&size), sizeof(size));
    file.read(reinterpret_cast<char *>(&key), size);
    file.read(reinterpret_cast<char *>(&size), sizeof(size));
    file.read(reinterpret_cast<char *>(&has_value), size);

    EXPECT_EQ(key, 2);

    if (has_value) {
        file.read(reinterpret_cast<char *>(&size), sizeof(size));
        value.resize(size);
        file.read(&value[0], size);
        EXPECT_EQ(value, std::string("Test entry 2"));
    }

    file.close();
}

TEST(WriteAheadLogTest, HandlesOptionalValues) {
    std::string log_file_path = generate_log_file_name();
    WriteAheadLog<int32_t, std::string> wal(log_file_path);

    EXPECT_NO_THROW(wal.appendLog(1, std::nullopt));
    EXPECT_NO_THROW(wal.appendLog(2, std::optional<std::string>("Test entry 2")));

    wal.flushLog();

    std::ifstream file(log_file_path, std::ios::binary);
    ASSERT_TRUE(file.is_open());

    size_t size;
    std::time_t timestamp;
    int32_t key;
    bool has_value;
    std::string value;

    file.read(reinterpret_cast<char *>(&size), sizeof(size));
    file.read(reinterpret_cast<char *>(&timestamp), size);
    file.read(reinterpret_cast<char *>(&size), sizeof(size));
    file.read(reinterpret_cast<char *>(&key), size);
    file.read(reinterpret_cast<char *>(&size), sizeof(size));
    file.read(reinterpret_cast<char *>(&has_value), size);

    EXPECT_EQ(key, 1);
    EXPECT_FALSE(has_value);

    file.read(reinterpret_cast<char *>(&size), sizeof(size));
    file.read(reinterpret_cast<char *>(&timestamp), size);
    file.read(reinterpret_cast<char *>(&size), sizeof(size));
    file.read(reinterpret_cast<char *>(&key), size);
    file.read(reinterpret_cast<char *>(&size), sizeof(size));
    file.read(reinterpret_cast<char *>(&has_value), size);

    EXPECT_EQ(key, 2);

    if (has_value) {
        file.read(reinterpret_cast<char *>(&size), sizeof(size));
        value.resize(size);
        file.read(&value[0], size);
        EXPECT_EQ(value, std::string("Test entry 2"));
    }

    file.close();
}
