#ifndef DATABASE_DATABASE_H
#define DATABASE_DATABASE_H

#include <vkdb/vq.h>
#include <vkdb/table.h>

namespace vkdb {
static const FilePath DATABASE_DIRECTORY{""};

using DatabaseName = std::string;

class Database {
public:
  using size_type = uint64_t;
  using error_callback = std::function<void(Token, const std::string&)>;
  using runtime_error_callback = std::function<void(const RuntimeError&)>;

  Database() = delete;

  explicit Database(
    DatabaseName name,
    error_callback error = VQ::error,
    runtime_error_callback runtime_error = VQ::runtimeError
  );

  Database(Database&&) noexcept = default;
  Database& operator=(Database&&) noexcept = default;

  Database(const Database&) = delete;
  Database& operator=(const Database&) = delete;

  ~Database() = default;

  Table& createTable(const TableName& table_name);
  [[nodiscard]] Table& getTable(const TableName& table_name);
  void dropTable(const TableName& table_name);
  void clear();

  [[nodiscard]] DatabaseName name() const noexcept;
  [[nodiscard]] FilePath path() const noexcept;

  Database& run(const std::string& source, std::ostream& stream = std::cout);
  Database& runFile(const std::filesystem::path path, std::ostream& stream = std::cout);
  Database& runPrompt();

private:
  using TableMap = std::unordered_map<TableName, Table>;

  void error(Token token, const std::string& message);
  void runtime_error(const RuntimeError& error);
  void report(
    size_type line,
    const std::string& where,
    const std::string& message
  );

  void load();

  TableMap table_map_;
  DatabaseName name_;
  bool had_error_;
  bool had_runtime_error_;
  error_callback callback_;
  runtime_error_callback runtime_callback_;
};
}  // namespace vkdb

#endif // DATABASE_DATABASE_H