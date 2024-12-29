#ifndef STORAGE_WRITE_AHEAD_LOG_H
#define STORAGE_WRITE_AHEAD_LOG_H

#include <vkdb/lsm_tree.h>
#include <vkdb/wal_lsm.h>

namespace vkdb {
/**
 * @brief Write-ahead log filename.
 * 
 */
const FilePath WAL_FILENAME{"wal.log"};

/**
 * @brief Write-ahead log.
 * 
 * @tparam TValue Value type.
 */
template <ArithmeticNoCVRefQuals TValue>
class WriteAheadLog {
public:
  /**
   * @brief Deleted default constructor.
   * 
   */
  WriteAheadLog() = delete;

  /**
   * @brief Construct a new WriteAheadLog object given the path of the LSM
   * tree.
   * 
   * @param lsm_tree_path Path.
   */
  explicit WriteAheadLog(FilePath lsm_tree_path) noexcept
    : path_{lsm_tree_path / WAL_FILENAME} {}

  /**
   * @brief Move-construct a WriteAheadLog object.
   * 
   */
  WriteAheadLog(WriteAheadLog&&) noexcept = default;
  
  /**
   * @brief Move-assign a WriteAheadLog object.
   * 
   */
  WriteAheadLog& operator=(WriteAheadLog&&) noexcept = default;

  /**
   * @brief Deleted copy constructor.
   * 
   */
  WriteAheadLog(const WriteAheadLog&) = delete;
  
  /**
   * @brief Deleted copy assignment operator.
   * 
   */
  WriteAheadLog& operator=(const WriteAheadLog&) = delete;

  /**
   * @brief Destroy the WriteAheadLog object.
   * 
   */
  ~WriteAheadLog() noexcept = default;

  /**
   * @brief Append a WAL record to the write-ahead log.
   * 
   * @param record WAL record.
   * 
   * @throw std::runtime_error If the file cannot be opened.
   */
  void append(const WALRecord<TValue>& record) {
    std::ofstream file{path_, std::ios::app};
    if (!file.is_open()) {
      throw std::runtime_error{
        "WriteAheadLog::append(): Unable to open file "
        + std::string(path_) + "."
      };
    }
    file << std::to_string(static_cast<int>(record.type));
    file << " " << entryToString(record.entry) << "\n";
    file.close();
  }

  /**
   * @brief Replay the write-ahead log on the LSM tree.
   * 
   * @param lsm_tree LSM tree.
   * 
   * @throw std::runtime_error If the file cannot be opened.
   */
  void replay(LSMTree<TValue>& lsm_tree) {
    if (!std::filesystem::exists(path_)) {
      return;
    }
    std::ifstream file{path_};
    if (!file.is_open()) {
      throw std::runtime_error{
        "WriteAheadLog::replay(): Unable to open file "
        + std::string(path_) + "."
      };
    }

    std::string line;
    while (std::getline(file, line)) {
      std::istringstream iss{line};

      std::string type_str;
      iss >> type_str;
      WALRecordType type{std::stoi(type_str)};

      std::string entry_str;
      iss >> entry_str;
      TimeSeriesEntry<TValue> entry{
        entryFromString<TValue>(entry_str.substr(1))
      };

      switch (type) {
      case WALRecordType::PUT:
        lsm_tree.put(entry.first, entry.second.value(), false);
        break;
      case WALRecordType::REMOVE:
        lsm_tree.remove(entry.first, false);
        break;
      }
    }
    file.close();
  }

  /**
   * @brief Clear the write-ahead log.
   * 
   * @throw std::runtime_error If the file cannot be opened.
   */
  void clear() {
    std::ofstream file{path_};
    if (!file.is_open()) {
      throw std::runtime_error{
        "WriteAheadLog::clear(): Unable to open file "
        + std::string(path_) + "."
      };
    }
    file.close();
  }

  /**
   * @brief Get the path of the write-ahead log.
   * 
   * @return FilePath Path.
   */
  [[nodiscard]] FilePath path() const noexcept {
    return path_;
  }

private:
  /**
   * @brief Path.
   * 
   */
  FilePath path_;
};
}  // namespace vkdb

#endif // STORAGE_WRITE_AHEAD_LOG_H