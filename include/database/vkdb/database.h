#ifndef DATABASE_DATABASE_H
#define DATABASE_DATABASE_H

#include <vkdb/table.h>

namespace vkdb {
static const FilePath DATABASE_DIRECTORY{""};

using DatabaseName = std::string;

class Database {
public:
  Database() = delete;

  explicit Database(DatabaseName name)
    : name_{std::move(name)} {
    load();
  }

  Database(Database&&) noexcept = default;
  Database& operator=(Database&&) noexcept = default;

  Database(const Database&) = delete;
  Database& operator=(const Database&) = delete;

  ~Database() = default;

  void createTable(const TableName& table_name) {
    if (table_map_.contains(table_name)) {
      throw std::runtime_error{
        "Database::createTable(): Table '" + table_name + "' already exists."
      };
    }
    table_map_.emplace(table_name, Table{path(), table_name});
    std::filesystem::create_directories(table_map_.at(table_name).path());
  }

  [[nodiscard]] Table& getTable(const TableName& table_name) {
    if (!table_map_.contains(table_name)) {
      throw std::runtime_error{
        "Database::getTable(): Table '" + table_name + "' does not exist."
      };
    }
    return table_map_.at(table_name);
  }

  void dropTable(const TableName& table_name) {
    if (!table_map_.contains(table_name)) {
      throw std::runtime_error{
        "Database::dropTable(): Table '" + table_name + "' does not exist."
      };
    }
    std::filesystem::remove_all(table_map_.at(table_name).path());
    table_map_.erase(table_name);
  }

  void clear() {
    std::filesystem::remove_all(path());
  }

  [[nodiscard]] DatabaseName name() const noexcept {
    return name_;
  }

  [[nodiscard]] FilePath path() const noexcept {
    return DATABASE_DIRECTORY / name_;
  }

private:
  using TableMap = std::unordered_map<TableName, Table>;

  void load() {
    const auto db_path{path()};
    if (!std::filesystem::exists(db_path)) {
      std::filesystem::create_directories(db_path);
      return;
    }

    for (const auto& entry : std::filesystem::directory_iterator(db_path)) {
      if (entry.is_directory()) {
        auto table_name{entry.path().filename().string()};
        table_map_.emplace(table_name, Table{path(), table_name});
      }
    }
  }      

  TableMap table_map_;
  DatabaseName name_;
};
}  // namespace vkdb

#endif // DATABASE_DATABASE_H