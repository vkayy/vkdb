#ifndef DATABASE_TABLE_H
#define DATABASE_TABLE_H

#include "utils/concepts.h"
#include "storage/lsm_tree.h"
#include "query/friendly_builder.h"
#include <set>

namespace vkdb {
using TableName = std::string;

class Table {
public:
  Table() = delete;

  explicit Table(const TableName& name)
    : name_{name}
    , storage_engine_{"/Users/vkay/Dev/vkdb/output/" + name} {}

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

  [[nodiscard]] FriendlyQueryBuilder<double> query() {
    return FriendlyQueryBuilder<double>(storage_engine_, tag_columns_);
  }

  [[nodiscard]] TableName name() const noexcept {
    return name_;
  }
  
private:
  using StorageEngine = LSMTree<double>;

  TableName name_;
  TagColumns tag_columns_;
  StorageEngine storage_engine_;
};
}  // namespace vkdb

#endif // DATABASE_TABLE_H