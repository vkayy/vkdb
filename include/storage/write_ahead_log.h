#ifndef STORAGE_WRITE_AHEAD_LOG_H
#define STORAGE_WRITE_AHEAD_LOG_H

#include "storage/lsm_tree.h"
#include "storage/wal_lsm.h"

namespace vkdb {
template <ArithmeticNoCVRefQuals TValue>
class WriteAheadLog {
public:
  WriteAheadLog() = delete;

  explicit WriteAheadLog(FilePath lsm_tree_path)
    : path_{lsm_tree_path + "/wal.log"} {}

  WriteAheadLog(WriteAheadLog&&) noexcept = default;
  WriteAheadLog& operator=(WriteAheadLog&&) noexcept = default;

  WriteAheadLog(const WriteAheadLog&) = delete;
  WriteAheadLog& operator=(const WriteAheadLog&) = delete;

  ~WriteAheadLog() = default;

  void append(const WALRecord<TValue>& record) {
    std::ofstream file{path_, std::ios::app};
    if (!file.is_open()) {
      throw std::runtime_error{
        "WriteAheadLog::append(): Unable to open file."
      };
    }
    file << std::to_string(static_cast<int>(record.type));
    file << " " << entryToString(record.entry) << "\n";
    file.close();
  }

  void replay(LSMTree<TValue>& lsm_tree) {
    std::ifstream file{path_};
    if (!file.is_open()) {
      throw std::runtime_error{
        "WriteAheadLog::replay(): Unable to open file."
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
      case WALRecordType::Put:
        lsm_tree.put(entry.first, entry.second.value());
        break;
      case WALRecordType::Remove:
        lsm_tree.remove(entry.first);
        break;
      }
    }
    file.close();
  }

  void clear() {
    std::ofstream file{path_};
    if (!file.is_open()) {
      throw std::runtime_error{
        "WriteAheadLog::clear(): Unable to open file."
      };
    }
    file.close();
  }

  [[nodiscard]] FilePath path() const noexcept {
    return path_;
  }
private:
  FilePath path_;
};
}  // namespace vkdb

#endif // STORAGE_WRITE_AHEAD_LOG_H