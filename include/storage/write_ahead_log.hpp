#ifndef WRITE_AHEAD_LOG_HPP
#define WRITE_AHEAD_LOG_HPP

#include <fstream>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>

class WriteAheadLog {
public:
    WriteAheadLog(const std::string &log_file_path);
    ~WriteAheadLog();

    void append_log(const std::string &entry);
    void flush_log();
    void recover_from_log();

private:
    std::string log_file_path; // The file path of the log file.
    std::ofstream log_stream;  // The `ofstream` of the log file.
    std::mutex log_mutex;      // The mutex for concurrent log file modification.
};

#endif // WRITE_AHEAD_LOG_HPP
