#ifndef DATABASE_DATABASE_H
#define DATABASE_DATABASE_H

#include "database/table.h"

namespace vkdb {
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
    if (tables_.contains(table_name)) {
      throw std::runtime_error{
        "Database::createTable(): Table already exists."
      };
    }
    tables_.emplace(table_name, Table{get_directory(), table_name});
    std::filesystem::create_directories(tables_.at(table_name).getDirectory());
  }

  [[nodiscard]] Table& getTable(const TableName& table_name) {
    if (!tables_.contains(table_name)) {
      throw std::runtime_error{
        "Database::getTable(): Table does not exist."
      };
    }
    return tables_.at(table_name);
  }

  void dropTable(const TableName& table_name) {
    if (!tables_.contains(table_name)) {
      throw std::runtime_error{
        "Database::dropTable(): Table does not exist."
      };
    }
    std::filesystem::remove_all(tables_.at(table_name).getDirectory());
    tables_.erase(table_name);
  }

  void clear() {
    std::filesystem::remove_all(get_directory());
  }

  [[nodiscard]] DatabaseName name() const noexcept {
    return name_;
  }

private:
  void load() {
    const std::filesystem::path db_path{get_directory()};
    if (!std::filesystem::exists(db_path)) {
      std::filesystem::create_directories(db_path);
      return;
    }

    for (const auto& entry : std::filesystem::directory_iterator(db_path)) {
      if (entry.is_directory()) {
        auto table_name{entry.path().filename().string()};
        tables_.emplace(table_name, Table{get_directory(), table_name});
      }
    }
  }

  FilePath get_directory() const noexcept {
    return VKDB_DATABASE_DIRECTORY + name_;
  }

  std::unordered_map<TableName, Table> tables_;
  DatabaseName name_;
};
}  // namespace vkdb

#endif // DATABASE_DATABASE_H