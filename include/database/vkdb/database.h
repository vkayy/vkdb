#ifndef DATABASE_DATABASE_H
#define DATABASE_DATABASE_H

#include <vkdb/vq.h>
#include <vkdb/table.h>

namespace vkdb {
/**
 * @brief The directory where databases are stored.
 * 
 */
static const FilePath DATABASE_DIRECTORY{"_vkdb_database_directory"};

/**
 * @brief Represents a database in vkdb.
 * 
 */
class Database {
public:
  using size_type = uint64_t;
  using error_callback = std::function<void(Token, const std::string&)>;
  using runtime_error_callback = std::function<void(const RuntimeError&)>;

  /**
   * @brief Deleted default constructor.
   * 
   */
  Database() = delete;

  /**
   * @brief Construct a new Database object.
   * @details The constructor will load the database if it exists.
   * 
   * @param name Name of the database.
   * @param error Error callback.
   * @param runtime_error Runtime error callback.
   * 
   * @throw std::runtime_error If loading the database fails.
   */
  explicit Database(
    DatabaseName name,
    error_callback error = VQ::error,
    runtime_error_callback runtime_error = VQ::runtimeError
  );

  /**
   * @brief Move-construct a new Database object.
   * 
   */
  Database(Database&&) noexcept = default;

  /**
   * @brief Move-assign a new Database object.
   * 
   */
  Database& operator=(Database&&) noexcept = default;

  /**
   * @brief Deleted copy constructor.
   * 
   */
  Database(const Database&) = delete;

  /**
   * @brief Deleted copy assignment operator.
   * 
   */
  Database& operator=(const Database&) = delete;

  /**
   * @brief Destroy the Database object.
   * 
   */
  ~Database() noexcept = default;

  /**
   * @brief Create a Table object.
   * @details Inserts new Table object into the table map and creates the
   * directory for the table.
   * 
   * @param table_name Name of the table.
   * @return Table& Reference to the created table.
   * 
   * @throw std::runtime_error If the table already exists.
   */
  Table& createTable(const TableName& table_name);

  /**
   * @brief Get the Table object.
   * 
   * @param table_name Name of the table.
   * @return Table& Reference to the table.
   * 
   * @throw std::runtime_error If the table does not exist.
   */
  [[nodiscard]] Table& getTable(const TableName& table_name);

  /**
   * @brief Drop the Table object.
   * @details Removes the table directory and erases the table from the table
   * map.
   * 
   * @param table_name Name of the table.
   * 
   * @throw std::runtime_error If the table does not exist.
   */
  void dropTable(const TableName& table_name);

  /**
   * @brief Clear the database.
   * @details Removes the database directory and all its contents.
   * 
   * @throw std::runtime_error If the database directory cannot be removed.
   */
  void clear();

  /**
   * @brief Get the name of the database.
   * 
   * @return DatabaseName Name of the database.
   */
  [[nodiscard]] DatabaseName name() const noexcept;

  /**
   * @brief Get the path to the database directory.
   * 
   * @return FilePath Path to the database directory.
   */
  [[nodiscard]] FilePath path() const noexcept;

  /**
   * @brief Get the names of the tables in the database.
   * 
   * @return std::vector<TableName> Names of the tables.
   */
  [[nodiscard]] std::vector<TableName> tables() const noexcept;

  /**
   * @brief Run a source string.
   * @details The source string is lexed, parsed, and interpreted.
   * 
   * @param source Source string.
   * @param stream Output stream.
   * @return Database& Reference to this Database object.
   */
  Database& run(
    const std::string& source,
    std::ostream& stream = std::cout
  ) noexcept;

  /**
   * @brief Run a file.
   * @details The file is read, lexed, parsed, and interpreted.
   * 
   * @param path Path to the file.
   * @param stream Output stream.
   * @return Database& Reference to this Database object.
   */
  Database& runFile(
    const std::filesystem::path path,
    std::ostream& stream = std::cout
  ) noexcept;

  /**
   * @brief Run the prompt.
   * @details The user can enter queries and commands interactively.
   * 
   * @return Database& Reference to this Database object.
   */
  Database& runPrompt() noexcept;

private:
  /**
   * @brief Type alias for a map from table names to Table objects.
   * 
   */
  using TableMap = std::unordered_map<TableName, Table>;

  /**
   * @brief Handle an error.
   * @details Reports the error and sets the had_error_ flag.
   * 
   * @param token Token where the error occurred.
   * @param message Error message.
   */
  void error(Token token, const std::string& message) noexcept;

  /**
   * @brief Handle a runtime error.
   * @details Reports the runtime error and sets the had_runtime_error_ flag.
   * 
   * @param error Runtime error.
   */
  void runtime_error(const RuntimeError& error) noexcept;

  /**
   * @brief Report an error.
   * 
   * @param line Line number.
   * @param where Where the error occurred.
   * @param message Error message.
   */
  void report(
    size_type line,
    const std::string& where,
    const std::string& message
  ) const noexcept;

  /**
   * @brief Load the database.
   * @details If the database directory does not exist, it is created. Each
   * existing table is loaded into the table map.
   * 
   * @throw std::runtime_error If loading the tables fails.
   */
  void load();

  /**
   * @brief Map from table names to Table objects.
   * 
   */
  TableMap table_map_;

  /**
   * @brief Name of the database.
   * 
   */
  DatabaseName name_;

  /**
   * @brief Flag for errors.
   * 
   */
  bool had_error_;

  /**
   * @brief Flag for runtime errors.
   * 
   */
  bool had_runtime_error_;

  /**
   * @brief Error callback.
   * 
   */
  error_callback callback_;

  /**
   * @brief Runtime error callback.
   * 
   */
  runtime_error_callback runtime_callback_;
};
}  // namespace vkdb

#endif // DATABASE_DATABASE_H