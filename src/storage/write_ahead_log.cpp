#include "storage/write_ahead_log.hpp"
#include <fstream>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>

WriteAheadLog::WriteAheadLog(const std::string &log_file_path)
    : log_file_path(log_file_path), log_stream(log_file_path, std::ios::app | std::ios::out) {
    if (!log_stream.is_open()) {
        throw std::runtime_error("unable to open log file");
    }
}

WriteAheadLog::~WriteAheadLog() {
    if (log_stream.is_open()) {
        log_stream.close();
    }
}

void WriteAheadLog::append_log(const std::string &entry) {
    std::lock_guard<std::mutex> lock(log_mutex);
    log_stream << entry << std::endl;
    if (!log_stream.good()) {
        throw std::runtime_error("unable to write to log file");
    }
}

void WriteAheadLog::flush_log() {
    std::lock_guard<std::mutex> lock(log_mutex);
    log_stream.flush();
    if (!log_stream.good()) {
        throw std::runtime_error("unable to flush log file");
    }
}

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
