#include "storage/write_ahead_log.hpp"
#include <fstream>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>

/**
 * @brief Construct a new write-ahead log object with the given log file path.
 *
 * @param log_file_path The path to the log file.
 * @throws `std::runtime_error` if unable to open log file.
 */
WriteAheadLog::WriteAheadLog(const std::string &log_file_path)
    : log_file_path(log_file_path), log_stream(log_file_path, std::ios::app | std::ios::out) {
    if (!log_stream.is_open()) {
        throw std::runtime_error("unable to open log file");
    }
}

/**
 * @brief Destroy the write-ahead log object.
 *
 * If the log stream is still open, it is closed.
 */
WriteAheadLog::~WriteAheadLog() {
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
void WriteAheadLog::append_log(const std::string &entry) {
    std::lock_guard<std::mutex> lock(log_mutex);
    log_stream << entry << std::endl;
    if (!log_stream.good()) {
        throw std::runtime_error("unable to write to log file");
    }
}

/**
 * @brief Flush the log file to ensure all entries are written.
 *
 * @throws `std::runtime_error` if unable to flush log file.
 */
void WriteAheadLog::flush_log() {
    std::lock_guard<std::mutex> lock(log_mutex);
    log_stream.flush();
    if (!log_stream.good()) {
        throw std::runtime_error("unable to flush log file");
    }
}

/**
 * @brief Recover from the log file by reading all entries.
 *
 * Closes the log stream and reopens the log file as a recovery
 * stream, reading line-by-line to process the log.
 *
 * @throws `std::runtime_error` if unable to reopen log file.
 */
void WriteAheadLog::recover_from_log() {
    std::lock_guard<std::mutex> lock(log_mutex);
    log_stream.close();
    std::ifstream recovery_stream(log_file_path, std::ios::in);
    if (!recovery_stream.is_open()) {
        throw std::runtime_error("unable to open log file for recovery");
    }
    std::string line;
    while (std::getline(recovery_stream, line)) {
        // TODO: process the log to recover entries, where each record
        // is erased after it is processed (a checkpoint).
    }
    recovery_stream.close();
}
