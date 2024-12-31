#include <vkdb/table.h>
#include <filesystem>
#include <stdexcept>

namespace vkdb {
Table::Table(const FilePath& db_path, const TableName& name)
  : name_{name}
  , db_path_{db_path}
  , storage_engine_{path()} {
  load();
}

Table& Table::setTagColumns(const TagColumns& tag_columns) noexcept {
  tag_columns_ = tag_columns;
  return *this;
}

Table& Table::addTagColumn(const TagKey& tag_column) {
  if (been_populated()) {
    throw std::runtime_error{
      "Table::addTagColumn(): Table '" + name_
      + "' has previously been populated with data."
    };
  }
  auto inserted{tag_columns_.insert(tag_column).second};
  if (!inserted) {
    throw std::runtime_error{
      "Table::addTagColumn(): Tag column '" + tag_column
      + "' already exists in '" + name_ + "'."
    };
  }
  save_tag_columns();
  return *this;
}

Table& Table::removeTagColumn(const TagKey& tag_column) {
  if (been_populated()) {
    throw std::runtime_error{
      "Table::removeTagColumn(): Table '" + name_
      + "' has previously been populated with data."
    };
  }
  auto removed{tag_columns_.erase(tag_column) > 0};
  if (!removed) {
    throw std::runtime_error{
      "Table::removeTagColumn(): Tag column '" + tag_column
      + "' does not exist in '" + name_ + "'."
    };
  }
  save_tag_columns();
  return *this;
}

void Table::clear() const noexcept {
  std::filesystem::remove_all(path());
  std::filesystem::create_directories(path());
}

FriendlyQueryBuilder<double> Table::query() noexcept {
  return FriendlyQueryBuilder<double>(storage_engine_, tag_columns_);
}

TableName Table::name() const noexcept {
  return name_;
}

TagColumns Table::tagColumns() const noexcept {
  return tag_columns_;
}

FilePath Table::path() const noexcept {
  return db_path_ / FilePath{name_};
}

bool Table::been_populated() const noexcept {
  return !storage_engine_.empty();
}

void Table::save_tag_columns() const {
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

void Table::load_tag_columns() {
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

FilePath Table::tag_columns_path() const noexcept {
  return path() / TAG_COLUMNS_FILENAME;
}

void Table::load() {
  std::filesystem::create_directories(path());
  load_tag_columns();
  storage_engine_.replayWAL();
}

} // namespace vkdb