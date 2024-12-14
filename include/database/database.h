#ifndef DATABASE_DATABASE_H
#define DATABASE_DATABASE_H

#include "database/table.h"

namespace vkdb {
using DatabaseName = std::string;
class Database {
public:
  Database() = delete;

  explicit Database(DatabaseName name)
    : name_{std::move(name)} {}

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
    tables_.emplace(table_name, Table{name_, table_name});
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
    tables_.erase(table_name);
  }
private:
  std::unordered_map<TableName, Table> tables_;
  DatabaseName name_;
};
}  // namespace vkdb

#endif // DATABASE_DATABASE_H