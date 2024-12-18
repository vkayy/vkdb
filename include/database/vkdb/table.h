#ifndef DATABASE_TABLE_H
#define DATABASE_TABLE_H

#include <vkdb/concepts.h>
#include <vkdb/lsm_tree.h>
#include <vkdb/friendly_builder.h>
#include <fstream>

namespace vkdb {
static const FilePath TAG_COLUMNS_FILENAME{"tag_columns.metadata"};

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
      load_tag_columns();
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

  void addTagColumn(const TagKey& tag_column) {
    auto inserted{tag_columns_.insert(tag_column).second};
    if (!inserted) {
      throw std::runtime_error{
        "Table::addTagColumn(): Tag column '" + tag_column
        + "' already exists in '" + name_ + "'." 
      };
    }
    save_tag_columns();
  }

  void removeTagColumn(const TagKey& tag_column) {
    auto removed{tag_columns_.erase(tag_column) > 0};
    if (!removed) {
      throw std::runtime_error{
        "Table::removeTagColumn(): Tag column '" + tag_column
        + "' does not exist in '" + name_ + "'."
      };
    }
    save_tag_columns();
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

  [[nodiscard]] TagColumns tagColumns() const noexcept {
    return tag_columns_;
  }

  [[nodiscard]] FilePath path() const noexcept {
    return db_path_ / FilePath{name_};
  }
  
  [[nodiscard]] bool beenPopulated() const noexcept {
    return !storage_engine_.empty();
  }

private:
  using StorageEngine = LSMTree<double>;

  void save_tag_columns() const {
    std::ofstream file{tag_columns_path()};
    if (!file.is_open()) {
      throw std::runtime_error{
        "Table::save_tag_columns(): Unable to open file "
        + std::string(tag_columns_path()) + "."
      };
    }
    for (const auto& column : tag_columns_) {
      file << column << "\n";
    }
    file.close();
  }

  void load_tag_columns() {
    tag_columns_.clear();
    if (!std::filesystem::exists(tag_columns_path())) {
      return;
    }
    std::ifstream file{tag_columns_path()};
    if (!file.is_open()) {
      throw std::runtime_error{
        "Table::load_tag_columns(): Unable to open file "
        + std::string(tag_columns_path()) + "."
      };
    }
    std::string column;
    while (std::getline(file, column)) {
      if (!column.empty()) {
        tag_columns_.insert(column);
      }
    }
    file.close();
  }

  [[nodiscard]] FilePath tag_columns_path() const noexcept {
    return path() / TAG_COLUMNS_FILENAME;
  }

  DatabaseName db_path_;
  TableName name_;
  TagColumns tag_columns_;
  StorageEngine storage_engine_;
};
}  // namespace vkdb

#endif // DATABASE_TABLE_H
