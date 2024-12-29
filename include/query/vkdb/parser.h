#ifndef QUERY_PARSER_H
#define QUERY_PARSER_H

#include <vkdb/expr.h>
#include <vkdb/token.h>
#include <vkdb/concepts.h>
#include <iostream>
#include <memory>
#include <vector>
#include <functional>
#include <optional>

namespace vkdb {
/**
 * @brief Exception class for parse errors.
 */
class ParseError {};

/**
 * @brief Parser for vq.
 */
class Parser {
public:
  using size_type = uint64_t;
  using error_callback = std::function<void(const Token&, const std::string&)>;

  /**
   * @brief Deleted default constructor.
   */
  Parser() = delete;

  /**
   * @brief Constructs a Parser with a list of tokens and an optional error callback.
   * @details Defaults the error callback to an empty lambda.
   * 
   * @param tokens The list of tokens to parse.
   * @param callback The error callback function.
   */
  Parser(
    const std::vector<Token>& tokens,
    error_callback callback = [](const Token&, const std::string&) {}
  ) noexcept;

  /**
   * @brief Parses the tokens into an expression.
   * 
   * @return An optional expression if parsing is successful.
   */
  [[nodiscard]] std::optional<Expr> parse() noexcept;

private:
  /**
   * @brief Creates a ParseError with the given token and message.
   * 
   * @param token The token where the error occurred.
   * @param message The error message.
   * @return The ParseError object.
   */
  [[nodiscard]] ParseError error(Token token, const std::string& message);

  /**
   * @brief Synchronizes the parser state after an error.
   */
  void synchronise();

  /**
   * @brief Checks if there are remaining tokens to parse.
   * 
   * @return True if there are remaining tokens, false otherwise.
   */
  [[nodiscard]] bool tokens_remaining() const noexcept;

  /**
   * @brief Peeks at the previous token.
   * 
   * @return The previous token.
   * 
   * @throws std::exception If there are no previous tokens.
   */
  [[nodiscard]] Token peek_back() const;

  /**
   * @brief Peeks at the current token.
   * 
   * @return The current token.
   * 
   * @throws std::exception If there are no tokens to peek.
   */
  [[nodiscard]] Token peek() const;

  /**
   * @brief Advances to the next token.
   * 
   * @return The current token before advancing.
   */
  Token advance() noexcept;

  /**
   * @brief Checks if the current token matches the given type.
   * 
   * @param type The token type to check.
   * @return True if the current token matches the type, false otherwise.
   */
  [[nodiscard]] bool check(TokenType type) const noexcept;

  /**
   * @brief Matches the current token with any of the given types.
   * 
   * @tparam TokenTypes The token types to match.
   * @param types The token types to match.
   * @return True if the current token matches any of the types, false otherwise.
   */
  template <AllSameNoCVRefQuals<TokenType>... TokenTypes>
  [[nodiscard]] bool match(TokenTypes... types) noexcept;

  /**
   * @brief Consumes the current token if it matches the given type.
   * 
   * @param type The token type to consume.
   * @param message The error message if the token does not match.
   * @return The consumed token.
   * 
   * @throws ParseError If the token does not match the given type.
   */
  Token consume(TokenType type, const std::string& message);

  /**
   * @brief Parses an expression.
   * 
   * @return The parsed expression.
   * 
   * @throws ParseError If the expression cannot be parsed.
   */
  [[nodiscard]] Expr parse_expression();

  /**
   * @brief Parses a query.
   * 
   * @return The parsed query.
   * 
   * @throws ParseError If the query cannot be parsed.
   */
  [[nodiscard]] Query parse_query();

  /**
   * @brief Parses a select query.
   * 
   * @return The parsed select query.
   * 
   * @throws ParseError If the select query cannot be parsed.
   */
  [[nodiscard]] SelectQuery parse_select_query();

  /**
   * @brief Parses a put query.
   * 
   * @return The parsed put query.
   * 
   * @throws ParseError If the put query cannot be parsed.
   */
  [[nodiscard]] PutQuery parse_put_query();

  /**
   * @brief Parses a delete query.
   * 
   * @return The parsed delete query.
   * 
   * @throws ParseError If the delete query cannot be parsed.
   */
  [[nodiscard]] DeleteQuery parse_delete_query();

  /**
   * @brief Parses a create query.
   * 
   * @return The parsed create query.
   * 
   * @throws ParseError If the create query cannot be parsed.
   */
  [[nodiscard]] CreateQuery parse_create_query();

  /**
   * @brief Parses a drop query.
   * 
   * @return The parsed drop query.
   * 
   * @throws ParseError If the drop query cannot be parsed.
   */
  [[nodiscard]] DropQuery parse_drop_query();

  /**
   * @brief Parses an add query.
   * 
   * @return The parsed add query.
   * 
   * @throws ParseError If the add query cannot be parsed.
   */
  [[nodiscard]] AddQuery parse_add_query();

  /**
   * @brief Parses a remove query.
   * 
   * @return The parsed remove query.
   * 
   * @throws ParseError If the remove query cannot be parsed.
   */
  [[nodiscard]] RemoveQuery parse_remove_query();

  /**
   * @brief Parses a tables query.
   * 
   * @return The parsed tables query.
   * 
   * @throws ParseError If the tables query cannot be parsed.
   */
  [[nodiscard]] TablesQuery parse_tables_query();

  /**
   * @brief Parses a select type.
   * 
   * @return The parsed select type.
   * 
   * @throws ParseError If the select type cannot be parsed.
   */
  [[nodiscard]] SelectType parse_select_type();

  /**
   * @brief Parses a select clause.
   * 
   * @return The parsed select clause.
   * 
   * @throws ParseError If the select clause cannot be parsed.
   */
  [[nodiscard]] SelectClause parse_select_clause();
  /**
   * @brief Parses an all clause.
   * 
   * @return The parsed all clause.
   * 
   * @throws ParseError If the all clause cannot be parsed.
   */
  [[nodiscard]] AllClause parse_all_clause();

  /**
   * @brief Parses a between clause.
   * 
   * @return The parsed between clause.
   * 
   * @throws ParseError If the between clause cannot be parsed.
   */
  [[nodiscard]] BetweenClause parse_between_clause();

  /**
   * @brief Parses an at clause.
   * 
   * @return The parsed at clause.
   * 
   * @throws ParseError If the at clause cannot be parsed.
   */
  [[nodiscard]] AtClause parse_at_clause();

  /**
   * @brief Parses a where clause.
   * 
   * @return The parsed where clause.
   * 
   * @throws ParseError If the where clause cannot be parsed.
   */
  [[nodiscard]] WhereClause parse_where_clause();

  /**
   * @brief Parses a tag list expression.
   * 
   * @return The parsed tag list expression.
   * 
   * @throws ParseError If the tag list expression cannot be parsed.
   */
  [[nodiscard]] TagListExpr parse_tag_list();

  /**
   * @brief Parses a tag expression.
   * 
   * @return The parsed tag expression.
   * 
   * @throws ParseError If the tag expression cannot be parsed.
   */
  [[nodiscard]] TagExpr parse_tag();

  /**
   * @brief Parses a tag columns expression.
   * 
   * @return The parsed tag columns expression.
   * 
   * @throws ParseError If the tag columns expression cannot be parsed.
   */
  [[nodiscard]] TagColumnsExpr parse_tag_columns();

  /**
   * @brief Parses a tag key expression.
   * 
   * @return The parsed tag key expression.
   * 
   * @throws ParseError If the tag key expression cannot be parsed.
   */
  [[nodiscard]] TagKeyExpr parse_tag_key();

  /**
   * @brief Parses a tag value expression.
   * 
   * @return The parsed tag value expression.
   * 
   * @throws ParseError If the tag value expression cannot be parsed.
   */
  [[nodiscard]] TagValueExpr parse_tag_value();

  /**
   * @brief Parses a metric expression.
   * 
   * @return The parsed metric expression.
   * 
   * @throws ParseError If the metric expression cannot be parsed.
   */
  [[nodiscard]] MetricExpr parse_metric();

  /**
   * @brief Parses a table name expression.
   * 
   * @return The parsed table name expression.
   * 
   * @throws ParseError If the table name expression cannot be parsed.
   */
  [[nodiscard]] TableNameExpr parse_table_name();

  /**
   * @brief Parses a timestamp expression.
   * 
   * @return The parsed timestamp expression.
   * 
   * @throws ParseError If the timestamp expression cannot be parsed.
   */
  [[nodiscard]] TimestampExpr parse_timestamp();

  /**
   * @brief Parses a value expression.
   * 
   * @return The parsed value expression.
   * 
   * @throws ParseError If the value expression cannot be parsed.
   */
  [[nodiscard]] ValueExpr parse_value();

  /**
   * @brief The list of tokens to parse.
   */
  const std::vector<Token> tokens_;

  /**
   * @brief The error callback function.
   */
  error_callback callback_;

  /**
   * @brief The current position in the token list.
   */
  size_type position_;
};

}  // namespace vkdb

#endif // QUERY_PARSER_H