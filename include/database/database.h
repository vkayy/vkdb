#ifndef DATABASE_DATABASE_H
#define DATABASE_DATABASE_H

#include "database/table.h"

class Database {
public:
  Database() = default;

  Database(Database&&) noexcept = default;
  Database& operator=(Database&&) noexcept = default;

  Database(const Database&) = delete;
  Database& operator=(const Database&) = delete;

  ~Database() = default;

  void createTable(const TableName& name) {
    if (tables_.contains(name)) {
      throw std::runtime_error{
        "Database::createTable(): Table already exists."
      };
    }
    tables_.emplace(name, Table{name});
  }

  [[nodiscard]] Table& getTable(const TableName& name) {
    if (!tables_.contains(name)) {
      throw std::runtime_error{
        "Database::getTable(): Table does not exist."
      };
    }
    return tables_.at(name);
  }

  void dropTable(const TableName& name) {
    if (!tables_.contains(name)) {
      throw std::runtime_error{
        "Database::dropTable(): Table does not exist."
      };
    }
    tables_.erase(name);
  }
private:
  std::unordered_map<TableName, Table> tables_;
};

#endif // DATABASE_DATABASE_H