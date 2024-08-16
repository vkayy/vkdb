#ifndef WRITE_AHEAD_LOG_HPP
#define WRITE_AHEAD_LOG_HPP

#include "mem_table.hpp"
#include "utils/serialize.hpp"
#include <ctime>
#include <fstream>
#include <iostream>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>

/**
 * @brief A write-ahead log for key-value storage.
 *
 * @tparam TKey The type of the key.
 * @tparam TValue The type of the value.
 */
template <typename TKey, typename TValue>
class WriteAheadLog {
private:
    /**
     * @brief A write-ahead log entry with a timestamp, key, and optional value.
     *
     */
    struct WriteAheadLogEntry {
        std::time_t timestamp;       // The timestamp of the log entry.
        TKey key;                    // The key of the log entry.
        std::optional<TValue> value; // The optional value of the log entry.
    };

    /**
     * @brief Serialize a write-ahead log entry to the log stream.
     *
     * @param entry The log entry to serialize.
     */
    void serializeWALEntry(const WriteAheadLogEntry &entry) {
        serializeValue(log_stream, entry.timestamp);
        serializeValue(log_stream, entry.key);

        bool has_value = entry.value.has_value();
        serializeValue(log_stream, has_value);

        if (has_value) {
            serializeValue(log_stream, entry.value.value());
        }
    }

    /**
     * @brief Deserialize a write-ahead log entry from the recovery stream.
     *
     * @param recovery_stream The recovery stream to read from.
     * @param entry The log entry to deserialize into.
     */
    void deserializeWALEntry(std::ifstream &recovery_stream, WriteAheadLogEntry &entry) {
        deserializeValue(recovery_stream, entry.timestamp);
        deserializeValue(recovery_stream, entry.key);

        bool has_value;
        deserializeValue(recovery_stream, has_value);

        if (has_value) {
            TValue value;
            deserializeValue(recovery_stream, value);
            entry.value = value;
            return;
        }

        entry.value.reset();
    }

    std::string log_file_path; // The file path of the log file.
    std::ofstream log_stream;  // The `ofstream` of the log file.
    std::mutex log_mutex;      // The mutex for concurrent log file modification.

public:
    /**
     * @brief Construct a new write-ahead log object with the given log file path.
     *
     * @param log_file_path The path to the log file.
     * @throws `std::runtime_error` if unable to open log file.
     */
    WriteAheadLog(const std::string &log_file_path)
        : log_file_path(log_file_path), log_stream(log_file_path, std::ios::app | std::ios::binary) {
        if (!log_stream.is_open()) {
            throw std::runtime_error("unable to open log file");
        }
    }

    /**
     * @brief Destroy the write-ahead log object.
     *
     * If the log stream is still open, it is closed.
     */
    ~WriteAheadLog() {
        if (log_stream.is_open()) {
            log_stream.close();
        }
    }

    /**
     * @brief Append a log entry to the log file.
     *
     * @param entry The log entry to append.
     * @throws `std::runtime_error` if unable to write to log file.
     */
    void appendLog(const TKey &key, const std::optional<TValue> &value) {
        std::lock_guard<std::mutex> lock(log_mutex);

        WriteAheadLogEntry entry({std::time(nullptr), key, value});
        serializeWALEntry(entry);
        log_stream.flush();

        if (!log_stream.good()) {
            throw std::runtime_error("unable to write to log file");
        }
    }

    /**
     * @brief Recover from the log file by reading all entries.
     *
     * Closes the log stream and reopens the log file as a recovery stream,
     * reading entry-by-entry to process the log.
     *
     * @throws `std::runtime_error` if unable to reopen log file or entries are out of order.
     */
    void recoverFromLog(std::unique_ptr<MemTable<TKey, TValue>> &memtable) {
        std::lock_guard<std::mutex> lock(log_mutex);
        std::ifstream recovery_stream(log_file_path, std::ios::binary);
        if (!recovery_stream.is_open()) {
            throw std::runtime_error("unable to open log file for recovery");
        }

        std::time_t last_timestamp = 0;

        while (recovery_stream.peek() != EOF) {
            WriteAheadLogEntry entry;
            deserializeWALEntry(recovery_stream, entry);
            if (entry.timestamp < last_timestamp) {
                throw std::runtime_error("log entries are not in chronological order");
            }
            memtable->put(entry.key, entry.value);
            last_timestamp = entry.timestamp;
        }
        recovery_stream.close();
    }
};

#endif // WRITE_AHEAD_LOG_HPP
