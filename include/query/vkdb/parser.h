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
class ParseError {};

class Parser {
public:
  using size_type = uint64_t;
  using error_callback = std::function<void(const Token&, const std::string&)>;

  Parser() = delete;

  Parser(
    const std::vector<Token>& tokens,
    error_callback callback = [](const Token&, const std::string&) {}
  ) noexcept;

  [[nodiscard]] std::optional<Expr> parse();

private:
  [[nodiscard]] ParseError error(Token token, const std::string& message);
  void synchronise();

  [[nodiscard]] bool tokens_remaining() const noexcept;
  [[nodiscard]] Token peek_back() const;
  [[nodiscard]] Token peek() const;
  Token advance() noexcept;

  [[nodiscard]] bool check(TokenType type) const noexcept;

  template <AllSameNoCVRefQuals<TokenType>... TokenTypes>
  [[nodiscard]] bool match(TokenTypes... types) noexcept;

  Token consume(TokenType type, const std::string& message);

  [[nodiscard]] Expr parse_expression();
  [[nodiscard]] Query parse_query();
  [[nodiscard]] SelectQuery parse_select_query();
  [[nodiscard]] PutQuery parse_put_query();
  [[nodiscard]] DeleteQuery parse_delete_query();
  [[nodiscard]] CreateQuery parse_create_query();
  [[nodiscard]] DropQuery parse_drop_query();
  [[nodiscard]] AddQuery parse_add_query();
  [[nodiscard]] RemoveQuery parse_remove_query();
  [[nodiscard]] TablesQuery parse_tables_query();
  [[nodiscard]] SelectType parse_select_type();
  [[nodiscard]] SelectClause parse_select_clause();
  [[nodiscard]] AllClause parse_all_clause();
  [[nodiscard]] BetweenClause parse_between_clause();
  [[nodiscard]] AtClause parse_at_clause();
  [[nodiscard]] WhereClause parse_where_clause();
  [[nodiscard]] TagListExpr parse_tag_list();
  [[nodiscard]] TagExpr parse_tag();
  [[nodiscard]] TagColumnsExpr parse_tag_columns();
  [[nodiscard]] TagKeyExpr parse_tag_key();
  [[nodiscard]] TagValueExpr parse_tag_value();
  [[nodiscard]] MetricExpr parse_metric();
  [[nodiscard]] TableNameExpr parse_table_name();
  [[nodiscard]] TimestampExpr parse_timestamp();
  [[nodiscard]] ValueExpr parse_value();

  const std::vector<Token> tokens_;
  error_callback callback_;
  size_type position_;
};
}  // namespace vkdb

#endif // QUERY_PARSER_H