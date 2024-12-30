#ifndef QUERY_TOKEN_H
#define QUERY_TOKEN_H

#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace vkdb {
/**
 * @brief Type alias for a string.
 * 
 */
using Lexeme = std::string;

/**
 * @brief Enumeration of token types.
 * 
 */
enum class TokenType {
  SELECT, PUT, DELETE, CREATE, DROP, ADD, REMOVE,
  DATA, AVG, SUM, COUNT, MIN, MAX,
  TABLE, TABLES, TAGS, ALL, BETWEEN, AND, AT, WHERE, FROM, INTO, TO,
  EQUAL, COMMA, SEMICOLON,
  IDENTIFIER, NUMBER,
  END_OF_FILE, UNKNOWN
};

/**
 * @brief Set of base words for queries.
 * 
 */
static const std::unordered_set<TokenType> QUERY_BASE_WORDS{
  TokenType::SELECT, TokenType::PUT, TokenType::DELETE,
  TokenType::CREATE, TokenType::DROP, TokenType::ADD, TokenType::REMOVE,
  TokenType::TABLES
};

/**
 * @brief Mapping from token type to string.
 * 
 */
static const std::unordered_map<TokenType, std::string> TOKEN_TYPE_TO_STRING{
  {TokenType::SELECT, "SELECT"},
  {TokenType::PUT, "PUT"},
  {TokenType::DELETE, "DELETE"},
  {TokenType::CREATE, "CREATE"},
  {TokenType::DROP, "DROP"},
  {TokenType::ADD, "ADD"},
  {TokenType::REMOVE, "REMOVE"},
  {TokenType::TABLES, "TABLES"},
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

/**
 * @brief Represents a token.
 * 
 */
class Token {
public:
  using size_type = uint64_t;

  /**
   * @brief Deleted default constructor.
   * 
   */
  Token() = delete;

  /**
   * @brief Construct a Token object from the given type, lexeme, line, and
   * column.
   * 
   * @param type The type of the token.
   * @param lexeme The lexeme of the token.
   * @param line The line number of the token.
   * @param column The column number of the token.
   */
  explicit Token(
    TokenType type,
    const Lexeme& lexeme,
    size_type line,
    size_type column
  ) noexcept;

  /**
   * @brief Move-construct a Token object.
   * 
   */
  Token(Token&&) noexcept = default;

  /**
   * @brief Move-assign a Token object.
   * 
   */
  Token& operator=(Token&&) noexcept = default;

  /**
   * @brief Copy-construct a Token object.
   * 
   */
  Token(const Token&) noexcept = default;

  /**
   * @brief Copy-assign a Token object.
   * 
   */
  Token& operator=(const Token&) noexcept = default;

  /**
   * @brief Destroy the Token object.
   * 
   */
  ~Token() noexcept = default;

  /**
   * @brief Equality operator for Token objects.
   * 
   * @param other The other Token object.
   * @return true if the Token objects are equal.
   * @return false if the Token objects are not equal.
   */
  [[nodiscard]] bool operator==(const Token& other) const noexcept;

  /**
   * @brief Get the type of the token.
   * 
   * @return TokenType The type of the token.
   */
  [[nodiscard]] TokenType type() const noexcept;

  /**
   * @brief Get the lexeme of the token.
   * 
   * @return Lexeme The lexeme of the token.
   */
  [[nodiscard]] Lexeme lexeme() const noexcept;

  /**
   * @brief Get the line number of the token.
   * 
   * @return size_type The line number of the token.
   */
  [[nodiscard]] size_type line() const noexcept;

  /**
   * @brief Get the column number of the token.
   * 
   * @return size_type The column number of the token.
   */
  [[nodiscard]] size_type column() const noexcept;

  /**
   * @brief Get the string representation of the token.
   * 
   * @return std::string The string representation of the token.
   */
  [[nodiscard]] std::string str() const noexcept;

private:
  /**
   * @brief The type of the token.
   * 
   */
  TokenType type_;

  /**
   * @brief The lexeme of the token.
   * 
   */
  Lexeme lexeme_;

  /**
   * @brief The line number of the token.
   * 
   */
  size_type line_;

  /**
   * @brief The column number of the token.
   * 
   */
  size_type column_;
};
}  // namespace vkdb

#endif // QUERY_TOKEN_H