#ifndef QUERY_PARSER_H
#define QUERY_PARSER_H

#include <vkdb/expr.h>
#include <vkdb/token.h>
#include <vkdb/concepts.h>
#include <iostream>
#include <memory>

namespace vkdb {
class Parser {
public:
  using size_type = uint64_t;
  using error_callback = std::function<void(const Token&, const std::string&)>;

  Parser() = delete;

  Parser(
    const std::vector<Token>& tokens,
    error_callback callback = [](const Token&, const std::string&) {}
  ) : tokens_{tokens}, callback_{callback}, position_{0} {}

  [[nodiscard]] std::optional<Expr> parse() {
    try {
      return parse_expression();
    } catch (const ParseError&) {
      return std::nullopt;
    }
  }

private:
  class ParseError {};
  
  [[nodiscard]] ParseError error(Token token, const std::string& message) {
    callback_(token, message);
    return ParseError{};
  }

  void synchronise() {
    advance();
    while (tokens_remaining()) {
      if (peek_back().type() == TokenType::SEMICOLON) {
        return;
      }
      if (QUERY_BASE_WORDS.contains(peek().type())) {
        return;
      }
      advance();
    }
  }

  [[nodiscard]] bool tokens_remaining() const noexcept {
    return position_ < tokens_.size();
  }
  
  [[nodiscard]] Token peek_back() const {
    return tokens_[position_ - 1];
  }
  
  [[nodiscard]] Token peek() const {
    return tokens_[position_];
  }

  Token advance() noexcept {
    if (tokens_remaining()) {
      ++position_;
    }
    return peek_back();
  }

  [[nodiscard]] bool check(TokenType type) const noexcept {
    if (!tokens_remaining()) {
      return false;
    }
    return peek().type() == type;
  }

  template <AllSameNoCVRefQuals<TokenType>... TokenTypes>
  [[nodiscard]] bool match(TokenTypes... types) noexcept {
    for (const auto& type : {types...}) {
      if (check(type)) {
        advance();
        return true;
      }
    }
    return false;
  }

  Token consume(TokenType type, const std::string& message) {
    if (check(type)) {
      return advance();
    }
    throw error(peek(), message);
  }

  [[nodiscard]] Expr parse_expression() {
    auto query{parse_query()};
    consume(TokenType::SEMICOLON, "Expected semicolon.");
    return query;
  }

  [[nodiscard]] Query parse_query() {
    switch (peek().type()) {
    case TokenType::SELECT:
      return parse_select_query();
    case TokenType::PUT:
      return parse_put_query();
    case TokenType::DELETE:
      return parse_delete_query();
    case TokenType::CREATE:
      return parse_create_query();
    case TokenType::DROP:
      return parse_drop_query();
    case TokenType::ADD:
      return parse_add_query();
    case TokenType::REMOVE:
      return parse_remove_query();
    default:
      throw error(peek(), "Expected query keyword.");
    }
  }

  [[nodiscard]] SelectQuery parse_select_query() {
    consume(TokenType::SELECT, "Expected SELECT.");
    auto select_type{parse_select_type()};
    auto metric{parse_metric()};
    consume(TokenType::FROM, "Expected FROM.");
    auto table_name{parse_table_name()};
    auto select_clause{parse_select_clause()};
    return {
      select_type,
      metric,
      table_name,
      select_clause
    };
  }

  [[nodiscard]] PutQuery parse_put_query() {
    consume(TokenType::PUT, "Expected PUT");
    auto metric{parse_metric()};
    auto timestamp{parse_timestamp()};
    auto value{parse_value()};
    consume(TokenType::INTO, "Expected INTO.");
    auto table_name{parse_table_name()};
    TagList tag_list;
    if (match(TokenType::TAGS)) {
      tag_list = parse_tag_list();
    }
    return {
      metric,
      timestamp,
      value,
      table_name,
      tag_list
    };
  }

  [[nodiscard]] DeleteQuery parse_delete_query() {
    consume(TokenType::DELETE, "Expected DELETE.");
    auto metric{parse_metric()};
    auto timestamp{parse_timestamp()};
    consume(TokenType::FROM, "Expected FROM.");
    auto table_name{parse_table_name()};
    TagList tag_list;
    if (match(TokenType::TAGS)) {
      tag_list = parse_tag_list();
    }
    return {
      metric,
      timestamp,
      table_name,
      tag_list
    };
  }

  [[nodiscard]] CreateQuery parse_create_query() {
    consume(TokenType::CREATE, "Expected CREATE.");
    consume(TokenType::TABLE, "Expected TABLE.");
    auto table_name{parse_table_name()};
    TagColumns tag_columns;
    if (match(TokenType::TAGS)) {
      tag_columns = parse_tag_columns();
    }
    return {
      table_name,
      tag_columns
    };
  }

  [[nodiscard]] DropQuery parse_drop_query() {
    consume(TokenType::DROP, "Expected DROP.");
    consume(TokenType::TABLE, "Expected TABLE.");
    auto table_name{parse_table_name()};
    return {table_name};
  }

  [[nodiscard]] AddQuery parse_add_query() {
    consume(TokenType::ADD, "Expected ADD.");
    consume(TokenType::TAGS, "Expected TAGS.");
    auto tag_columns{parse_tag_columns()};
    consume(TokenType::TO, "Expected TO.");
    auto table_name{parse_table_name()};
    return {
      tag_columns,
      table_name
    };
  }

  [[nodiscard]] RemoveQuery parse_remove_query() {
    consume(TokenType::REMOVE, "Expected REMOVE.");
    consume(TokenType::TAGS, "Expected TAGS.");
    auto tag_columns{parse_tag_columns()};
    consume(TokenType::FROM, "Expected FROM.");
    auto table_name{parse_table_name()};
    return {
      tag_columns,
      table_name
    };
  }

  [[nodiscard]] SelectType parse_select_type() {
    auto select_type{[this]() {
      switch (peek().type()) {
      case TokenType::DATA:
        advance();
        return SelectType::DATA;
      case TokenType::AVG:
        advance();
        return SelectType::AVG;
      case TokenType::SUM:
        advance();
        return SelectType::SUM;
      case TokenType::COUNT:
        advance();
        return SelectType::COUNT;
      case TokenType::MIN:
        advance();
        return SelectType::MIN;
      case TokenType::MAX:
        advance();
        return SelectType::MAX;
      default:
        throw error(peek(), "Expected select type.");
      }
    }()};
    return select_type;
  }

  [[nodiscard]] SelectClause parse_select_clause() {
    switch (peek().type()) {
    case TokenType::ALL:
      return parse_all_clause();
    case TokenType::BETWEEN:
      return parse_between_clause();
    case TokenType::AT:
      return parse_at_clause();
    default:
      throw error(peek(), "Expected select clause.");
    }
  }

  [[nodiscard]] AllClause parse_all_clause() {
    consume(TokenType::ALL, "Expected ALL.");
    WhereClause where_clause;
    if (check(TokenType::WHERE)) {
      where_clause = parse_where_clause();
    }
    return {where_clause};
  }

  [[nodiscard]] BetweenClause parse_between_clause() {
    consume(TokenType::BETWEEN, "Expected BETWEEN.");
    auto start_timestamp{parse_timestamp()};
    consume(TokenType::AND, "Expected AND.");
    auto end_timestamp{parse_timestamp()};
    WhereClause where_clause;
    if (check(TokenType::WHERE)) {
      where_clause = parse_where_clause();
    }
    return {
      start_timestamp,
      end_timestamp,
      where_clause
    };
  }

  [[nodiscard]] AtClause parse_at_clause() {
    consume(TokenType::AT, "Expected AT.");
    auto timestamp{parse_timestamp()};
    WhereClause where_clause;
    if (check(TokenType::WHERE)) {
      where_clause = parse_where_clause();
    }
    return {
      timestamp,
      where_clause
    };
  }

  [[nodiscard]] WhereClause parse_where_clause() {
    consume(TokenType::WHERE, "Expected WHERE.");
    auto tag_list{parse_tag_list()};
    return {tag_list};
  }

  [[nodiscard]] TagList parse_tag_list() {
    std::vector<Tag> tags;
    do {
      tags.push_back(parse_tag());
    } while (match(TokenType::COMMA));
    return {tags};
  }

  [[nodiscard]] Tag parse_tag() {
    auto tag_key{parse_tag_key()};
    consume(TokenType::EQUAL, "Expected '='.");
    auto tag_value{parse_tag_value()};
    return {
      tag_key,
      tag_value
    };
  }

  [[nodiscard]] TagColumns parse_tag_columns() {
    std::vector<TagKey> keys;
    do {
      keys.push_back(parse_tag_key());
    } while (match(TokenType::COMMA));
    return {keys};
  }

  [[nodiscard]] TagKey parse_tag_key() {
    auto tag_key{consume(TokenType::IDENTIFIER, "Expected tag key.")};
    return {tag_key.lexeme()};
  }

  [[nodiscard]] TagValue parse_tag_value() {
    auto tag_value{consume(TokenType::IDENTIFIER, "Expected tag value.")};
    return {tag_value.lexeme()};
  }

  [[nodiscard]] Metric parse_metric() {
    auto metric{consume(TokenType::IDENTIFIER, "Expected metric.")};
    return {metric.lexeme()};
  }

  [[nodiscard]] TableName parse_table_name() {
    auto table_name{consume(TokenType::IDENTIFIER, "Expected table name.")};
    return {table_name.lexeme()};
  }

  [[nodiscard]] Timestamp parse_timestamp() {
    auto timestamp{consume(TokenType::NUMBER, "Expected timestamp.")};
    return {timestamp.lexeme()};
  }

  [[nodiscard]] Value parse_value() {
    auto value{consume(TokenType::NUMBER, "Expected value.")};
    return {value.lexeme()};
  }

  const std::vector<Token> tokens_;
  error_callback callback_;
  size_type position_;
};
}  // namespace vkdb

#endif // QUERY_PARSER_H