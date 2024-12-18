#ifndef QUERY_TOKEN_H
#define QUERY_TOKEN_H

#include <string>
#include <unordered_map>
#include <unordered_set>

namespace vkdb {
using Lexeme = std::string;

enum class TokenType {
  SELECT, PUT, DELETE, CREATE, DROP, ADD, REMOVE,
  DATA, AVG, SUM, COUNT, MIN, MAX,
  TABLE, TAGS, ALL, BETWEEN, AND, AT, WHERE, FROM, INTO, TO,
  EQUAL, COMMA, SEMICOLON,
  IDENTIFIER, NUMBER,
  END_OF_FILE, UNKNOWN
};

static const std::unordered_set<TokenType> QUERY_BASE_WORDS{
  TokenType::SELECT, TokenType::PUT, TokenType::DELETE,
  TokenType::CREATE, TokenType::DROP, TokenType::ADD, TokenType::REMOVE
};

static const std::unordered_map<TokenType, std::string> TOKEN_TYPE_TO_STRING{
  {TokenType::SELECT, "SELECT"},
  {TokenType::PUT, "PUT"},
  {TokenType::DELETE, "DELETE"},
  {TokenType::CREATE, "CREATE"},
  {TokenType::DROP, "DROP"},
  {TokenType::ADD, "ADD"},
  {TokenType::REMOVE, "REMOVE"},
  {TokenType::DATA, "DATA"},
  {TokenType::AVG, "AVG"},
  {TokenType::SUM, "SUM"},
  {TokenType::COUNT, "COUNT"},
  {TokenType::MIN, "MIN"},
  {TokenType::MAX, "MAX"},
  {TokenType::TABLE, "TABLE"},
  {TokenType::TAGS, "TAGS"},
  {TokenType::ALL, "ALL"},
  {TokenType::BETWEEN, "BETWEEN"},
  {TokenType::AND, "AND"},
  {TokenType::AT, "AT"},
  {TokenType::WHERE, "WHERE"},
  {TokenType::FROM, "FROM"},
  {TokenType::INTO, "INTO"},
  {TokenType::TO, "TO"},
  {TokenType::EQUAL, "EQUAL"},
  {TokenType::COMMA, "COMMA"},
  {TokenType::SEMICOLON, "SEMICOLON"},
  {TokenType::IDENTIFIER, "IDENTIFIER"},
  {TokenType::NUMBER, "NUMBER"},
  {TokenType::END_OF_FILE, "END_OF_FILE"},
  {TokenType::UNKNOWN, "UNKNOWN"}
};

class Token {
public:
  using size_type = uint64_t;

  Token() = delete;

  explicit Token(TokenType type, const Lexeme& lexeme, size_type line,
        size_type column)
    : type_{type}, lexeme_{lexeme}, line_{line}, column_{column} {}
  
  Token(Token&&) noexcept = default;
  Token& operator=(Token&&) noexcept = default;
  
  Token(const Token&) noexcept = default;
  Token& operator=(const Token&) noexcept = default;

  ~Token() = default;

  [[nodiscard]] bool operator==(const Token& other) const noexcept {
    return type_ == other.type() &&
      lexeme_ == other.lexeme() &&
      line_ == other.line() &&
      column_ == other.column();
  }
  
  [[nodiscard]] TokenType type() const noexcept {
    return type_;
  }

  [[nodiscard]] Lexeme lexeme() const noexcept {
    return lexeme_;
  }

  [[nodiscard]] size_type line() const noexcept {
    return line_;
  }

  [[nodiscard]] size_type column() const noexcept {
    return column_;
  }

  [[nodiscard]] std::string toString() const noexcept {
    auto type_str{TOKEN_TYPE_TO_STRING.at(type_)};
    return type_str + " "
      + lexeme_ + " "
      + std::to_string(line_) + " "
      + std::to_string(column_);
  }

private:
  TokenType type_;
  Lexeme lexeme_;
  size_type line_;
  size_type column_;
};
}  // namespace vkdb

#endif // QUERY_TOKEN_H