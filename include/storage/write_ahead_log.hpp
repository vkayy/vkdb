#ifndef WRITE_AHEAD_LOG_HPP
#define WRITE_AHEAD_LOG_HPP

#include <fstream>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>

class WriteAheadLog {
public:
    /**
     * @brief Construct a new write-ahead log object with the given log file path.
     *
     * @param log_file_path The path to the log file.
     * @throws `std::runtime_error` if unable to open log file.
     */
    WriteAheadLog(const std::string &log_file_path);

    /**
     * @brief Destroy the write-ahead log object.
     *
     * If the log stream is still open, it is closed.
     */
    ~WriteAheadLog();

    /**
     * @brief Append a log entry to the log file.
     *
     * @param entry The log entry to append.
     * @throws `std::runtime_error` if unable to write to log file.
     */
    void appendLog(const std::string &entry);

    /**
     * @brief Flush the log file to ensure all entries are written.
     *
     * @throws `std::runtime_error` if unable to flush log file.
     */
    void flushLog();

    /**
     * @brief Recover from the log file by reading all entries.
     *
     * Closes the log stream and reopens the log file as a recovery
     * stream, reading line-by-line to process the log.
     *
     * @throws `std::runtime_error` if unable to reopen log file.
     */
    void recoverFromLog();

private:
    std::string log_file_path; // The file path of the log file.
    std::ofstream log_stream;  // The `ofstream` of the log file.
    std::mutex log_mutex;      // The mutex for concurrent log file modification.
};

#endif // WRITE_AHEAD_LOG_HPP
