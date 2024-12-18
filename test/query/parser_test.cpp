#include "gtest/gtest.h"
#include <vkdb/parser.h>

using namespace vkdb;

static Token make_token(TokenType type, const std::string& lexeme) {
  return Token{type, lexeme, 1, 1};
}

TEST(ParserTest, CanParseCreateQuery) {
  std::vector<Token> tokens{
    make_token(TokenType::CREATE, "CREATE"),
    make_token(TokenType::TABLE, "TABLE"),
    make_token(TokenType::IDENTIFIER, "table_name"),
    make_token(TokenType::SEMICOLON, ";")
  };

  Parser parser{tokens};
  auto create_query{parser.parse()};
  ASSERT_TRUE(create_query.has_value());

  auto create_query_ptr{std::get_if<CreateQuery>(&create_query.value()[0])};
  ASSERT_NE(create_query_ptr, nullptr);

  EXPECT_EQ(create_query_ptr->table_name.token.lexeme(), "table_name");
}
TEST(ParserTest, CanParseCreateQueryWithTags) {
  std::vector<Token> tokens{
    make_token(TokenType::CREATE, "CREATE"),
    make_token(TokenType::TABLE, "TABLE"),
    make_token(TokenType::IDENTIFIER, "table_name"),
    make_token(TokenType::TAGS, "TAGS"),
    make_token(TokenType::IDENTIFIER, "tag1"),
    make_token(TokenType::COMMA, ","),
    make_token(TokenType::IDENTIFIER, "tag2"),
    make_token(TokenType::SEMICOLON, ";"),
  };

  Parser parser{tokens};
  auto create_query{parser.parse()};
  ASSERT_TRUE(create_query.has_value());

  auto create_query_ptr{std::get_if<CreateQuery>(&create_query.value()[0])};
  ASSERT_NE(create_query_ptr, nullptr);
  EXPECT_EQ(create_query_ptr->table_name.token.lexeme(), "table_name");

  ASSERT_TRUE(create_query_ptr->tag_columns.has_value());
  ASSERT_EQ(create_query_ptr->tag_columns->keys.size(), 2);
  EXPECT_EQ(create_query_ptr->tag_columns->keys[0].token.lexeme(), "tag1");
  EXPECT_EQ(create_query_ptr->tag_columns->keys[1].token.lexeme(), "tag2");
}

TEST(ParserTest, CanParseDropQuery) {
  std::vector<Token> tokens{
    make_token(TokenType::DROP, "DROP"),
    make_token(TokenType::TABLE, "TABLE"),
    make_token(TokenType::IDENTIFIER, "table_name"),
    make_token(TokenType::SEMICOLON, ";"),
  };

  Parser parser{tokens};
  auto drop_query{parser.parse()};
  ASSERT_TRUE(drop_query.has_value());

  auto drop_query_ptr{std::get_if<DropQuery>(&drop_query.value()[0])};
  ASSERT_NE(drop_query_ptr, nullptr);

  EXPECT_EQ(drop_query_ptr->table_name.token.lexeme(), "table_name");
}

TEST(ParserTest, CanParseAddQuery) {
  std::vector<Token> tokens{
    make_token(TokenType::ADD, "ADD"),
    make_token(TokenType::TAGS, "TAGS"),
    make_token(TokenType::IDENTIFIER, "tag1"),
    make_token(TokenType::COMMA, ","),
    make_token(TokenType::IDENTIFIER, "tag2"),
    make_token(TokenType::TO, "TO"),
    make_token(TokenType::IDENTIFIER, "table_name"),
    make_token(TokenType::SEMICOLON, ";"),
  };

  Parser parser{tokens};
  auto add_query{parser.parse()};
  ASSERT_TRUE(add_query.has_value());

  auto add_query_ptr{std::get_if<AddQuery>(&add_query.value()[0])};
  ASSERT_NE(add_query_ptr, nullptr);

  EXPECT_EQ(add_query_ptr->table_name.token.lexeme(), "table_name");

  ASSERT_EQ(add_query_ptr->tag_columns.keys.size(), 2);
  EXPECT_EQ(add_query_ptr->tag_columns.keys[0].token.lexeme(), "tag1");
  EXPECT_EQ(add_query_ptr->tag_columns.keys[1].token.lexeme(), "tag2");
}

TEST(ParserTest, CanParseRemoveQuery) {
  std::vector<Token> tokens{
    make_token(TokenType::REMOVE, "REMOVE"),
    make_token(TokenType::TAGS, "TAGS"),
    make_token(TokenType::IDENTIFIER, "tag1"),
    make_token(TokenType::COMMA, ","),
    make_token(TokenType::IDENTIFIER, "tag2"),
    make_token(TokenType::FROM, "FROM"),
    make_token(TokenType::IDENTIFIER, "table_name"),
    make_token(TokenType::SEMICOLON, ";"),
  };

  Parser parser{tokens};
  auto remove_query{parser.parse()};
  ASSERT_TRUE(remove_query.has_value());

  auto remove_query_ptr{std::get_if<RemoveQuery>(&remove_query.value()[0])};
  ASSERT_NE(remove_query_ptr, nullptr);

  EXPECT_EQ(remove_query_ptr->table_name.token.lexeme(), "table_name");

  ASSERT_EQ(remove_query_ptr->tag_columns.keys.size(), 2);
  EXPECT_EQ(remove_query_ptr->tag_columns.keys[0].token.lexeme(), "tag1");
  EXPECT_EQ(remove_query_ptr->tag_columns.keys[1].token.lexeme(), "tag2");
}

TEST(ParserTest, CanParseSelectDataAllQuery) {
  std::vector<Token> tokens{
    make_token(TokenType::SELECT, "SELECT"),
    make_token(TokenType::DATA, "DATA"),
    make_token(TokenType::IDENTIFIER, "metric"),
    make_token(TokenType::FROM, "FROM"),
    make_token(TokenType::IDENTIFIER, "table"),
    make_token(TokenType::ALL, "ALL"),
    make_token(TokenType::SEMICOLON, ";"),
  };

  Parser parser{tokens};
  auto select_query{parser.parse()};
  ASSERT_TRUE(select_query.has_value());

  auto select_query_ptr{std::get_if<SelectQuery>(&select_query.value()[0])};
  ASSERT_NE(select_query_ptr, nullptr);

  EXPECT_EQ(select_query_ptr->type.index(), 0);
  EXPECT_EQ(select_query_ptr->metric.token.lexeme(), "metric");
  EXPECT_EQ(select_query_ptr->table_name.token.lexeme(), "table");

  ASSERT_NE(std::get_if<AllClause>(&select_query_ptr->clause), nullptr);
}

TEST(ParserTest, CanParseSelectDataBetweenQuery) {
  std::vector<Token> tokens{
    make_token(TokenType::SELECT, "SELECT"),
    make_token(TokenType::DATA, "DATA"),
    make_token(TokenType::IDENTIFIER, "metric"),
    make_token(TokenType::FROM, "FROM"),
    make_token(TokenType::IDENTIFIER, "table"),
    make_token(TokenType::BETWEEN, "BETWEEN"),
    make_token(TokenType::NUMBER, "10"),
    make_token(TokenType::AND, "AND"),
    make_token(TokenType::NUMBER, "20"),
    make_token(TokenType::SEMICOLON, ";"),
  };

  Parser parser{tokens};
  auto select_query{parser.parse()};
  ASSERT_TRUE(select_query.has_value());

  auto select_query_ptr{std::get_if<SelectQuery>(&select_query.value()[0])};
  ASSERT_NE(select_query_ptr, nullptr);

  EXPECT_EQ(select_query_ptr->type.index(), 0);
  EXPECT_EQ(select_query_ptr->metric.token.lexeme(), "metric");
  EXPECT_EQ(select_query_ptr->table_name.token.lexeme(), "table");

  auto between_clause{std::get_if<BetweenClause>(&select_query_ptr->clause)};
  ASSERT_NE(between_clause, nullptr);
  EXPECT_EQ(between_clause->start.token.lexeme(), "10");
  EXPECT_EQ(between_clause->end.token.lexeme(), "20");
}

TEST(ParserTest, CanParseSelectDataAtQuery) {
  std::vector<Token> tokens{
    make_token(TokenType::SELECT, "SELECT"),
    make_token(TokenType::DATA, "DATA"),
    make_token(TokenType::IDENTIFIER, "metric"),
    make_token(TokenType::FROM, "FROM"),
    make_token(TokenType::IDENTIFIER, "table"),
    make_token(TokenType::AT, "AT"),
    make_token(TokenType::NUMBER, "15"),
    make_token(TokenType::SEMICOLON, ";"),
  };

  Parser parser{tokens};
  auto select_query{parser.parse()};
  ASSERT_TRUE(select_query.has_value());

  auto select_query_ptr{std::get_if<SelectQuery>(&select_query.value()[0])};
  ASSERT_NE(select_query_ptr, nullptr);

  EXPECT_EQ(select_query_ptr->type.index(), 0);
  EXPECT_EQ(select_query_ptr->metric.token.lexeme(), "metric");
  EXPECT_EQ(select_query_ptr->table_name.token.lexeme(), "table");

  auto at_clause{std::get_if<AtClause>(&select_query_ptr->clause)};
  ASSERT_NE(at_clause, nullptr);
  EXPECT_EQ(at_clause->timestamp.token.lexeme(), "15");
}

TEST(ParserTest, CanParseSelectDataAllWhereQuery) {
  std::vector<Token> tokens{
    make_token(TokenType::SELECT, "SELECT"),
    make_token(TokenType::DATA, "DATA"),
    make_token(TokenType::IDENTIFIER, "metric"),
    make_token(TokenType::FROM, "FROM"),
    make_token(TokenType::IDENTIFIER, "table"),
    make_token(TokenType::ALL, "ALL"),
    make_token(TokenType::WHERE, "WHERE"),
    make_token(TokenType::IDENTIFIER, "tag1"),
    make_token(TokenType::EQUAL, "="),
    make_token(TokenType::IDENTIFIER, "value1"),
    make_token(TokenType::COMMA, ","),
    make_token(TokenType::IDENTIFIER, "tag2"),
    make_token(TokenType::EQUAL, "="),
    make_token(TokenType::IDENTIFIER, "value2"),
    make_token(TokenType::SEMICOLON, ";"),
  };

  Parser parser{tokens};
  auto select_query{parser.parse()};
  ASSERT_TRUE(select_query.has_value());

  auto select_query_ptr{std::get_if<SelectQuery>(&select_query.value()[0])};
  ASSERT_NE(select_query_ptr, nullptr);

  EXPECT_EQ(select_query_ptr->type.index(), 0);
  EXPECT_EQ(select_query_ptr->metric.token.lexeme(), "metric");
  EXPECT_EQ(select_query_ptr->table_name.token.lexeme(), "table");

  auto all_clause{std::get_if<AllClause>(&select_query_ptr->clause)};
  ASSERT_NE(all_clause, nullptr);
  
  ASSERT_TRUE(all_clause->where_clause.has_value());
  auto tag_list{all_clause->where_clause->tag_list};

  ASSERT_EQ(tag_list.tags.size(), 2);
  EXPECT_EQ(tag_list.tags[0].key.token.lexeme(), "tag1");
  EXPECT_EQ(tag_list.tags[0].value.token.lexeme(), "value1");
  EXPECT_EQ(tag_list.tags[1].key.token.lexeme(), "tag2");
  EXPECT_EQ(tag_list.tags[1].value.token.lexeme(), "value2");
}

TEST(ParserTest, CanParseSelectDataBetweenWhereQuery) {
  std::vector<Token> tokens{
    make_token(TokenType::SELECT, "SELECT"),
    make_token(TokenType::DATA, "DATA"),
    make_token(TokenType::IDENTIFIER, "metric"),
    make_token(TokenType::FROM, "FROM"),
    make_token(TokenType::IDENTIFIER, "table"),
    make_token(TokenType::BETWEEN, "BETWEEN"),
    make_token(TokenType::NUMBER, "10"),
    make_token(TokenType::AND, "AND"),
    make_token(TokenType::NUMBER, "20"),
    make_token(TokenType::WHERE, "WHERE"),
    make_token(TokenType::IDENTIFIER, "tag1"),
    make_token(TokenType::EQUAL, "="),
    make_token(TokenType::IDENTIFIER, "value1"),
    make_token(TokenType::COMMA, ","),
    make_token(TokenType::IDENTIFIER, "tag2"),
    make_token(TokenType::EQUAL, "="),
    make_token(TokenType::IDENTIFIER, "value2"),
    make_token(TokenType::SEMICOLON, ";"),
  };

  Parser parser{tokens};
  auto select_query{parser.parse()};
  ASSERT_TRUE(select_query.has_value());

  auto select_query_ptr{std::get_if<SelectQuery>(&select_query.value()[0])};
  ASSERT_NE(select_query_ptr, nullptr);

  EXPECT_EQ(select_query_ptr->type.index(), 0);
  EXPECT_EQ(select_query_ptr->metric.token.lexeme(), "metric");
  EXPECT_EQ(select_query_ptr->table_name.token.lexeme(), "table");

  auto between_clause{std::get_if<BetweenClause>(&select_query_ptr->clause)};
  ASSERT_NE(between_clause, nullptr);
  EXPECT_EQ(between_clause->start.token.lexeme(), "10");
  EXPECT_EQ(between_clause->end.token.lexeme(), "20");

  ASSERT_TRUE(between_clause->where_clause.has_value());
  auto tag_list{between_clause->where_clause->tag_list};

  ASSERT_EQ(tag_list.tags.size(), 2);
  EXPECT_EQ(tag_list.tags[0].key.token.lexeme(), "tag1");
  EXPECT_EQ(tag_list.tags[0].value.token.lexeme(), "value1");
  EXPECT_EQ(tag_list.tags[1].key.token.lexeme(), "tag2");
  EXPECT_EQ(tag_list.tags[1].value.token.lexeme(), "value2");
}

TEST(ParserTest, CanParseSelectDataAtWhereQuery) {
  std::vector<Token> tokens{
    make_token(TokenType::SELECT, "SELECT"),
    make_token(TokenType::DATA, "DATA"),
    make_token(TokenType::IDENTIFIER, "metric"),
    make_token(TokenType::FROM, "FROM"),
    make_token(TokenType::IDENTIFIER, "table"),
    make_token(TokenType::AT, "AT"),
    make_token(TokenType::NUMBER, "15"),
    make_token(TokenType::WHERE, "WHERE"),
    make_token(TokenType::IDENTIFIER, "tag1"),
    make_token(TokenType::EQUAL, "="),
    make_token(TokenType::IDENTIFIER, "value1"),
    make_token(TokenType::COMMA, ","),
    make_token(TokenType::IDENTIFIER, "tag2"),
    make_token(TokenType::EQUAL, "="),
    make_token(TokenType::IDENTIFIER, "value2"),
    make_token(TokenType::SEMICOLON, ";"),
  };

  Parser parser{tokens};
  auto select_query{parser.parse()};
  ASSERT_TRUE(select_query.has_value());

  auto select_query_ptr{std::get_if<SelectQuery>(&select_query.value()[0])};
  ASSERT_NE(select_query_ptr, nullptr);

  EXPECT_EQ(select_query_ptr->type.index(), 0);
  EXPECT_EQ(select_query_ptr->metric.token.lexeme(), "metric");
  EXPECT_EQ(select_query_ptr->table_name.token.lexeme(), "table");

  auto at_clause{std::get_if<AtClause>(&select_query_ptr->clause)};
  ASSERT_NE(at_clause, nullptr);
  EXPECT_EQ(at_clause->timestamp.token.lexeme(), "15");

  ASSERT_TRUE(at_clause->where_clause.has_value());
  auto tag_list{at_clause->where_clause->tag_list};

  ASSERT_EQ(tag_list.tags.size(), 2);
  EXPECT_EQ(tag_list.tags[0].key.token.lexeme(), "tag1");
  EXPECT_EQ(tag_list.tags[0].value.token.lexeme(), "value1");
  EXPECT_EQ(tag_list.tags[1].key.token.lexeme(), "tag2");
  EXPECT_EQ(tag_list.tags[1].value.token.lexeme(), "value2");
}

TEST(ParserTest, CanParsePutQuery) {
  std::vector<Token> tokens{
    make_token(TokenType::PUT, "PUT"),
    make_token(TokenType::IDENTIFIER, "metric"),
    make_token(TokenType::NUMBER, "10"),
    make_token(TokenType::NUMBER, "20"),
    make_token(TokenType::INTO, "INTO"),
    make_token(TokenType::IDENTIFIER, "table"),
    make_token(TokenType::SEMICOLON, ";"),
  };

  Parser parser{tokens};
  auto put_query{parser.parse()};
  ASSERT_TRUE(put_query.has_value());

  auto put_query_ptr{std::get_if<PutQuery>(&put_query.value()[0])};
  ASSERT_NE(put_query_ptr, nullptr);

  EXPECT_EQ(put_query_ptr->metric.token.lexeme(), "metric");
  EXPECT_EQ(put_query_ptr->timestamp.token.lexeme(), "10");
  EXPECT_EQ(put_query_ptr->value.token.lexeme(), "20");
  EXPECT_EQ(put_query_ptr->table_name.token.lexeme(), "table");
}

TEST(ParserTest, CanParsePutWithTagsQuery) {
  std::vector<Token> tokens{
    make_token(TokenType::PUT, "PUT"),
    make_token(TokenType::IDENTIFIER, "metric"),
    make_token(TokenType::NUMBER, "10"),
    make_token(TokenType::NUMBER, "20"),
    make_token(TokenType::INTO, "INTO"),
    make_token(TokenType::IDENTIFIER, "table"),
    make_token(TokenType::TAGS, "TAGS"),
    make_token(TokenType::IDENTIFIER, "tag1"),
    make_token(TokenType::EQUAL, "="),
    make_token(TokenType::IDENTIFIER, "value1"),
    make_token(TokenType::COMMA, ","),
    make_token(TokenType::IDENTIFIER, "tag2"),
    make_token(TokenType::EQUAL, "="),
    make_token(TokenType::IDENTIFIER, "value2"),
    make_token(TokenType::SEMICOLON, ";"),
  };

  Parser parser{tokens};
  auto put_query{parser.parse()};
  ASSERT_TRUE(put_query.has_value());

  auto put_query_ptr{std::get_if<PutQuery>(&put_query.value()[0])};
  ASSERT_NE(put_query_ptr, nullptr);

  EXPECT_EQ(put_query_ptr->metric.token.lexeme(), "metric");
  EXPECT_EQ(put_query_ptr->timestamp.token.lexeme(), "10");
  EXPECT_EQ(put_query_ptr->value.token.lexeme(), "20");
  EXPECT_EQ(put_query_ptr->table_name.token.lexeme(), "table");

  auto tag_list{put_query_ptr->tag_list};
  ASSERT_TRUE(put_query_ptr->tag_list.has_value());

  ASSERT_EQ(tag_list->tags.size(), 2);
  EXPECT_EQ(tag_list->tags[0].key.token.lexeme(), "tag1");
  EXPECT_EQ(tag_list->tags[0].value.token.lexeme(), "value1");
  EXPECT_EQ(tag_list->tags[1].key.token.lexeme(), "tag2");
  EXPECT_EQ(tag_list->tags[1].value.token.lexeme(), "value2");
}

TEST(ParserTest, CanParseDeleteQuery) {
  std::vector<Token> tokens{
    make_token(TokenType::DELETE, "DELETE"),
    make_token(TokenType::IDENTIFIER, "metric"),
    make_token(TokenType::NUMBER, "10"),
    make_token(TokenType::FROM, "FROM"),
    make_token(TokenType::IDENTIFIER, "table"),
    make_token(TokenType::SEMICOLON, ";"),
  };

  Parser parser{tokens};
  auto delete_query{parser.parse()};
  ASSERT_TRUE(delete_query.has_value());

  auto delete_query_ptr{std::get_if<DeleteQuery>(&delete_query.value()[0])};
  ASSERT_NE(delete_query_ptr, nullptr);

  EXPECT_EQ(delete_query_ptr->metric.token.lexeme(), "metric");
  EXPECT_EQ(delete_query_ptr->timestamp.token.lexeme(), "10");
  EXPECT_EQ(delete_query_ptr->table_name.token.lexeme(), "table");
}

TEST(ParserTest, CanParseDeleteWithTagsQuery) {
  std::vector<Token> tokens{
    make_token(TokenType::DELETE, "DELETE"),
    make_token(TokenType::IDENTIFIER, "metric"),
    make_token(TokenType::NUMBER, "10"),
    make_token(TokenType::FROM, "FROM"),
    make_token(TokenType::IDENTIFIER, "table"),
    make_token(TokenType::TAGS, "TAGS"),
    make_token(TokenType::IDENTIFIER, "tag1"),
    make_token(TokenType::EQUAL, "="),
    make_token(TokenType::IDENTIFIER, "value1"),
    make_token(TokenType::COMMA, ","),
    make_token(TokenType::IDENTIFIER, "tag2"),
    make_token(TokenType::EQUAL, "="),
    make_token(TokenType::IDENTIFIER, "value2"),
    make_token(TokenType::SEMICOLON, ";"),
  };

  Parser parser{tokens};
  auto delete_query{parser.parse()};
  ASSERT_TRUE(delete_query.has_value());

  auto delete_query_ptr{std::get_if<DeleteQuery>(&delete_query.value()[0])};
  ASSERT_NE(delete_query_ptr, nullptr);

  EXPECT_EQ(delete_query_ptr->metric.token.lexeme(), "metric");
  EXPECT_EQ(delete_query_ptr->timestamp.token.lexeme(), "10");
  EXPECT_EQ(delete_query_ptr->table_name.token.lexeme(), "table");

  auto tag_list{delete_query_ptr->tag_list};
  ASSERT_TRUE(delete_query_ptr->tag_list.has_value());

  ASSERT_EQ(tag_list->tags.size(), 2);
  EXPECT_EQ(tag_list->tags[0].key.token.lexeme(), "tag1");
  EXPECT_EQ(tag_list->tags[0].value.token.lexeme(), "value1");
  EXPECT_EQ(tag_list->tags[1].key.token.lexeme(), "tag2");
  EXPECT_EQ(tag_list->tags[1].value.token.lexeme(), "value2");
}

TEST(ParserTest, CanParseMultipleQueries) {
  std::vector<Token> tokens{
    make_token(TokenType::CREATE, "CREATE"),
    make_token(TokenType::TABLE, "TABLE"),
    make_token(TokenType::IDENTIFIER, "table_name"),
    make_token(TokenType::TAGS, "TAGS"),
    make_token(TokenType::IDENTIFIER, "tag1"),
    make_token(TokenType::COMMA, ","),
    make_token(TokenType::IDENTIFIER, "tag2"),
    make_token(TokenType::SEMICOLON, ";"),
    make_token(TokenType::PUT, "PUT"),
    make_token(TokenType::IDENTIFIER, "metric"),
    make_token(TokenType::NUMBER, "10"),
    make_token(TokenType::NUMBER, "20"),
    make_token(TokenType::INTO, "INTO"),
    make_token(TokenType::IDENTIFIER, "table"),
    make_token(TokenType::TAGS, "TAGS"),
    make_token(TokenType::IDENTIFIER, "tag1"),
    make_token(TokenType::EQUAL, "="),
    make_token(TokenType::IDENTIFIER, "value1"),
    make_token(TokenType::COMMA, ","),
    make_token(TokenType::IDENTIFIER, "tag2"),
    make_token(TokenType::EQUAL, "="),
    make_token(TokenType::IDENTIFIER, "value2"),
    make_token(TokenType::SEMICOLON, ";"),
    make_token(TokenType::DELETE, "DELETE"),
    make_token(TokenType::IDENTIFIER, "metric"),
    make_token(TokenType::NUMBER, "10"),
    make_token(TokenType::FROM, "FROM"),
    make_token(TokenType::IDENTIFIER, "table"),
    make_token(TokenType::TAGS, "TAGS"),
    make_token(TokenType::IDENTIFIER, "tag1"),
    make_token(TokenType::EQUAL, "="),
    make_token(TokenType::IDENTIFIER, "value1"),
    make_token(TokenType::COMMA, ","),
    make_token(TokenType::IDENTIFIER, "tag2"),
    make_token(TokenType::EQUAL, "="),
    make_token(TokenType::IDENTIFIER, "value2"),
    make_token(TokenType::SEMICOLON, ";"),
  };

  Parser parser{tokens};
  auto all_queries{parser.parse()};
  ASSERT_TRUE(all_queries.has_value());
  ASSERT_EQ(all_queries->size(), 3);

  auto create_query_ptr{std::get_if<CreateQuery>(&all_queries.value()[0])};
  ASSERT_NE(create_query_ptr, nullptr);
  EXPECT_EQ(create_query_ptr->table_name.token.lexeme(), "table_name");

  auto put_query_ptr{std::get_if<PutQuery>(&all_queries.value()[1])};
  ASSERT_NE(put_query_ptr, nullptr);

  EXPECT_EQ(put_query_ptr->metric.token.lexeme(), "metric");
  EXPECT_EQ(put_query_ptr->timestamp.token.lexeme(), "10");
  EXPECT_EQ(put_query_ptr->value.token.lexeme(), "20");
  EXPECT_EQ(put_query_ptr->table_name.token.lexeme(), "table");

  auto tag_list{put_query_ptr->tag_list};
  ASSERT_TRUE(put_query_ptr->tag_list.has_value());

  ASSERT_EQ(tag_list->tags.size(), 2);
  EXPECT_EQ(tag_list->tags[0].key.token.lexeme(), "tag1");
  EXPECT_EQ(tag_list->tags[0].value.token.lexeme(), "value1");
  EXPECT_EQ(tag_list->tags[1].key.token.lexeme(), "tag2");
  EXPECT_EQ(tag_list->tags[1].value.token.lexeme(), "value2");

  auto delete_query_ptr{std::get_if<DeleteQuery>(&all_queries.value()[2])};
  ASSERT_NE(delete_query_ptr, nullptr);

  EXPECT_EQ(delete_query_ptr->metric.token.lexeme(), "metric");
  EXPECT_EQ(delete_query_ptr->timestamp.token.lexeme(), "10");
  EXPECT_EQ(delete_query_ptr->table_name.token.lexeme(), "table");
  
  tag_list = delete_query_ptr->tag_list;
  ASSERT_TRUE(delete_query_ptr->tag_list.has_value());

  ASSERT_EQ(tag_list->tags.size(), 2);
  EXPECT_EQ(tag_list->tags[0].key.token.lexeme(), "tag1");
  EXPECT_EQ(tag_list->tags[0].value.token.lexeme(), "value1");
  EXPECT_EQ(tag_list->tags[1].key.token.lexeme(), "tag2");
  EXPECT_EQ(tag_list->tags[1].value.token.lexeme(), "value2");
}