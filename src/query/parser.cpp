#include <vkdb/parser.h>
#include <vkdb/token.h>

namespace vkdb {
Parser::Parser(
  const std::vector<Token>& tokens,
  error_callback callback
) noexcept
  : tokens_{tokens}, callback_{callback}, position_{0} {}

std::optional<Expr> Parser::parse() {
  try {
    return parse_expression();
  } catch (const ParseError&) {
    return std::nullopt;
  }
}

ParseError Parser::error(Token token, const std::string& message) {
  callback_(token, message);
  return ParseError{};
}

void Parser::synchronise() {
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

bool Parser::tokens_remaining() const noexcept {
  return position_ < tokens_.size();
}

Token Parser::peek_back() const {
  return tokens_[position_ - 1];
}

Token Parser::peek() const {
  return tokens_[position_];
}

Token Parser::advance() noexcept {
  if (tokens_remaining()) {
    ++position_;
  }
  return peek_back();
}

bool Parser::check(TokenType type) const noexcept {
  if (!tokens_remaining()) {
    return false;
  }
  return peek().type() == type;
}

template <AllSameNoCVRefQuals<TokenType>... TokenTypes>
bool Parser::match(TokenTypes... types) noexcept {
  for (const auto& type : {types...}) {
    if (check(type)) {
      advance();
      return true;
    }
  }
  return false;
}

Token Parser::consume(TokenType type, const std::string& message) {
  if (check(type)) {
    return advance();
  }
  throw error(peek(), message);
}

Expr Parser::parse_expression() {
  Expr expr{};
  do {
    expr.push_back(parse_query());
    consume(TokenType::SEMICOLON, "Expected semicolon.");
  } while (tokens_remaining() && !check(TokenType::END_OF_FILE));
  return expr;
}

Query Parser::parse_query() {
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
      throw error(peek(), "Expected query base word.");
  }
}

SelectQuery Parser::parse_select_query() {
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

PutQuery Parser::parse_put_query() {
  consume(TokenType::PUT, "Expected PUT");
  auto metric{parse_metric()};
  auto timestamp{parse_timestamp()};
  auto value{parse_value()};
  consume(TokenType::INTO, "Expected INTO.");
  auto table_name{parse_table_name()};
  TagListExpr tag_list;
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

DeleteQuery Parser::parse_delete_query() {
  consume(TokenType::DELETE, "Expected DELETE.");
  auto metric{parse_metric()};
  auto timestamp{parse_timestamp()};
  consume(TokenType::FROM, "Expected FROM.");
  auto table_name{parse_table_name()};
  TagListExpr tag_list;
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

CreateQuery Parser::parse_create_query() {
  consume(TokenType::CREATE, "Expected CREATE.");
  consume(TokenType::TABLE, "Expected TABLE.");
  auto table_name{parse_table_name()};
  TagColumnsExpr tag_columns;
  if (match(TokenType::TAGS)) {
    tag_columns = parse_tag_columns();
  }
  return {
    table_name,
    tag_columns
  };
}

DropQuery Parser::parse_drop_query() {
  consume(TokenType::DROP, "Expected DROP.");
  consume(TokenType::TABLE, "Expected TABLE.");
  auto table_name{parse_table_name()};
  return {table_name};
}

AddQuery Parser::parse_add_query() {
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

RemoveQuery Parser::parse_remove_query() {
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

SelectType Parser::parse_select_type() {
  auto select_type{[this]() -> SelectType {
    switch (peek().type()) {
      case TokenType::DATA:
        advance();
        return SelectTypeDataExpr{peek()};
      case TokenType::COUNT:
        advance();
        return SelectTypeCountExpr{peek()};
      case TokenType::AVG:
        advance();
        return SelectTypeAvgExpr{peek()};
      case TokenType::SUM:
        advance();
        return SelectTypeSumExpr{peek()};
      case TokenType::MIN:
        advance();
        return SelectTypeMinExpr{peek()};
      case TokenType::MAX:
        advance();
        return SelectTypeMaxExpr{peek()};
      default:
        throw error(peek(), "Expected select type.");
    }
  }()};
  return select_type;
}

SelectClause Parser::parse_select_clause() {
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

AllClause Parser::parse_all_clause() {
  consume(TokenType::ALL, "Expected ALL.");
  WhereClause where_clause;
  if (check(TokenType::WHERE)) {
    where_clause = parse_where_clause();
  }
  return {where_clause};
}

BetweenClause Parser::parse_between_clause() {
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

AtClause Parser::parse_at_clause() {
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

WhereClause Parser::parse_where_clause() {
  consume(TokenType::WHERE, "Expected WHERE.");
  auto tag_list{parse_tag_list()};
  return {tag_list};
}

TagListExpr Parser::parse_tag_list() {
  std::vector<TagExpr> tags;
  do {
    tags.push_back(parse_tag());
  } while (match(TokenType::COMMA));
  return {tags};
}

TagExpr Parser::parse_tag() {
  auto tag_key{parse_tag_key()};
  consume(TokenType::EQUAL, "Expected '='.");
  auto tag_value{parse_tag_value()};
  return {
    tag_key,
    tag_value
  };
}

TagColumnsExpr Parser::parse_tag_columns() {
  std::vector<TagKeyExpr> keys;
  do {
    keys.push_back(parse_tag_key());
  } while (match(TokenType::COMMA));
  return {keys};
}

TagKeyExpr Parser::parse_tag_key() {
  auto tag_key{consume(TokenType::IDENTIFIER, "Expected tag key.")};
  return {tag_key};
}

TagValueExpr Parser::parse_tag_value() {
  auto tag_value{consume(TokenType::IDENTIFIER, "Expected tag value.")};
  return {tag_value};
}

MetricExpr Parser::parse_metric() {
  auto metric{consume(TokenType::IDENTIFIER, "Expected metric.")};
  return {metric};
}

TableNameExpr Parser::parse_table_name() {
  auto table_name{consume(TokenType::IDENTIFIER, "Expected table name.")};
  return {table_name};
}

TimestampExpr Parser::parse_timestamp() {
  auto timestamp{consume(TokenType::NUMBER, "Expected timestamp.")};
  return {timestamp};
}

ValueExpr Parser::parse_value() {
  auto value{consume(TokenType::NUMBER, "Expected value.")};
  return {value};
}
} // namespace vkdb