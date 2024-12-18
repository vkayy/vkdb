#include <vkdb/database.h>

#include <vkdb/database.h>
#include <vkdb/interpreter.h>
#include <vkdb/lexer.h>
#include <vkdb/parser.h>

namespace vkdb {

Database::Database(
  DatabaseName name,
  error_callback error,
  runtime_error_callback runtime_error
)
  : name_{std::move(name)}
  , callback_{std::move(error)}
  , runtime_callback_{std::move(runtime_error)}
  , had_error_{false}
  , had_runtime_error_{false} {
  load();
}

Table& Database::createTable(const TableName& table_name) {
  if (table_map_.contains(table_name)) {
    throw std::runtime_error{
      "Database::createTable(): Table '" + table_name + "' already exists."
    };
  }
  table_map_.emplace(table_name, Table{path(), table_name});
  std::filesystem::create_directories(table_map_.at(table_name).path());
  return table_map_.at(table_name);
}

Table& Database::getTable(const TableName& table_name) {
  if (!table_map_.contains(table_name)) {
    throw std::runtime_error{
      "Database::getTable(): Table '" + table_name + "' does not exist."
    };
  }
  return table_map_.at(table_name);
}

void Database::dropTable(const TableName& table_name) {
  if (!table_map_.contains(table_name)) {
    throw std::runtime_error{
      "Database::dropTable(): Table '" + table_name + "' does not exist."
    };
  }
  std::filesystem::remove_all(table_map_.at(table_name).path());
  table_map_.erase(table_name);
}

void Database::clear() {
  std::filesystem::remove_all(path());
}

DatabaseName Database::name() const noexcept {
  return name_;
}

FilePath Database::path() const noexcept {
  return DATABASE_DIRECTORY / name_;
}

Database& Database::run(
  const std::string& source,
  std::ostream& stream
) {
  Lexer lexer{source};
  auto tokens{lexer.tokenize()};

  Parser parser{tokens, [this](Token token, const std::string& message) {
    error(token, message);
  }};
  auto expr{parser.parse()};

  if (had_error_) {
    return *this;
  }
  
  Interpreter interpreter{*this, [this](const RuntimeError& error) {
    runtime_error(error);
  }};
  interpreter.interpret(expr.value(), stream);

  return *this;
}

Database& Database::runFile(
  const std::filesystem::path path,
  std::ostream& stream
) {
  if (path.extension() != ".vq") {
    std::cerr << "\033[1;32mDatabase::runFile(): File extension cannot be "
    << path.extension() << ", must be .vq.\033[0m\n";
    return *this;
  }
  std::ifstream file{path};
  if (!file.is_open()) {
    std::cerr << "\033[1;32mDatabase::runFile(): Unable to open file "
    << path << ".\033[0m\n";
    return *this;
  }
  std::stringstream buffer;
  buffer << file.rdbuf();
  run(buffer.str(), stream);

  if (had_error_) {
    exit(65);
  }

  if (had_runtime_error_) {
    exit(70);
  }

  return *this;
}

Database& Database::runPrompt() {
  std::cout << "\033[1;31mwelcome to the vq repl! :)\033[0m\n";
  std::cout << "\033[1;31m(on database '" << name_ << "')\033[0m\n";
  std::string line;
  while (true) {
    std::cout << "\033[1;34m(vq) >> \033[0m";
    if (!std::getline(std::cin, line) || line.empty()) {
      break;
    };
    run(line);
    had_error_ = false;
  }
  return *this;
}

void Database::error(Token token, const std::string& message) {
  if (token.type() == TokenType::END_OF_FILE) {
    report(token.line(), "at end", message);
  } else {
    report(token.line(), "at '" + token.lexeme() + "'", message);
  }
  had_error_ = true;
}

void Database::runtime_error(const RuntimeError& error) {
  std::cerr << "\033[1;32m[line " << error.token().line();
  std::cerr << "] Runtime error: " << error.message() << "\033[0m\n";
  had_runtime_error_ = true;
}

void Database::report(
  size_type line,
  const std::string& where,
  const std::string& message
) {
  std::cerr << "\033[1;32m[line " << line << "] Parse error ";
  std::cerr << where << ": " << message << "\033[0m\n";
}

void Database::load() {
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
}  // namespace vkdb