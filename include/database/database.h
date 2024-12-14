#ifndef DATABASE_DATABASE_H
#define DATABASE_DATABASE_H

#include "database/table.h"

namespace vkdb {
class Database {
public:
  Database() = delete;

  explicit Database(std::string name)
    : name_{std::move(name)} {}

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
    tables_.emplace(name, Table{name_, name});
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
  std::string name_;
};
}  // namespace vkdb

#endif // DATABASE_DATABASE_H