#ifndef DATABASE_TABLE_H
#define DATABASE_TABLE_H

#include "utils/concepts.h"
#include "storage/lsm_tree.h"
#include "query/friendly_builder.h"
#include <set>

namespace vkdb {
const FilePath VKDB_DATABASE_DIRECTORY{"/Users/vkay/Dev/vkdb/output/"};

using DatabaseName = std::string;
using TableName = std::string;

class Table {
public:
  Table() = delete;

  explicit Table(const FilePath& db_path, const TableName& name)
    : name_{name}
    , db_path_{db_path}
    , storage_engine_{path()} {
      std::filesystem::create_directories(path());
      storage_engine_.replayWAL();
    }

  Table(Table&&) noexcept = default;
  Table& operator=(Table&&) noexcept = default;

  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;

  ~Table() = default;

  void setTagColumns(const TagColumns& tag_columns) {
    tag_columns_ = tag_columns;
  }

  bool addTagColumn(const TagKey& tag_column) {
    return tag_columns_.insert(tag_column).second;
  }

  bool removeTagColumn(const TagKey& tag_column) {
    return tag_columns_.erase(tag_column) > 0;
  }

  void clear() const noexcept {
    std::filesystem::remove_all(path());
    std::filesystem::create_directories(path());
  }

  [[nodiscard]] FriendlyQueryBuilder<double> query() {
    return FriendlyQueryBuilder<double>(storage_engine_, tag_columns_);
  }

  [[nodiscard]] TableName name() const noexcept {
    return name_;
  }

  [[nodiscard]] FilePath path() const noexcept {
    return db_path_ + "/" + name_;
  }
  
private:
  using StorageEngine = LSMTree<double>;

  DatabaseName db_path_;
  TableName name_;
  TagColumns tag_columns_;
  StorageEngine storage_engine_;
};
}  // namespace vkdb

#endif // DATABASE_TABLE_H