#include "gtest/gtest.h"
#include <vkdb/lexer.h>

using namespace vkdb;

TEST(LexerTest, CanTokenizeInputWithAllLexemes) {
  Lexer lexer{
    "SELECT PUT DELETE CREATE DROP ADD REMOVE "
    "DATA AVG SUM COUNT MIN MAX "
    "TABLE TAGS ALL BETWEEN AND AT WHERE FROM INTO TO "
    "= , ; "
    "id123 123id 123 -123 123.00 -123.00"
  };

  auto tokens{lexer.tokenize()};

  ASSERT_EQ(tokens.size(), 34); 

  EXPECT_EQ(tokens[0], Token(TokenType::SELECT, "SELECT", 1, 1));
  EXPECT_EQ(tokens[1], Token(TokenType::PUT, "PUT", 1, 8));
  EXPECT_EQ(tokens[2], Token(TokenType::DELETE, "DELETE", 1, 12));
  EXPECT_EQ(tokens[3], Token(TokenType::CREATE, "CREATE", 1, 19));
  EXPECT_EQ(tokens[4], Token(TokenType::DROP, "DROP", 1, 26));
  EXPECT_EQ(tokens[5], Token(TokenType::ADD, "ADD", 1, 31));
  EXPECT_EQ(tokens[6], Token(TokenType::REMOVE, "REMOVE", 1, 35));

  EXPECT_EQ(tokens[7], Token(TokenType::DATA, "DATA", 1, 42));
  EXPECT_EQ(tokens[8], Token(TokenType::AVG, "AVG", 1, 47));
  EXPECT_EQ(tokens[9], Token(TokenType::SUM, "SUM", 1, 51));
  EXPECT_EQ(tokens[10], Token(TokenType::COUNT, "COUNT", 1, 55));
  EXPECT_EQ(tokens[11], Token(TokenType::MIN, "MIN", 1, 61));
  EXPECT_EQ(tokens[12], Token(TokenType::MAX, "MAX", 1, 65));

  EXPECT_EQ(tokens[13], Token(TokenType::TABLE, "TABLE", 1, 69));
  EXPECT_EQ(tokens[14], Token(TokenType::TAGS, "TAGS", 1, 75));
  EXPECT_EQ(tokens[15], Token(TokenType::ALL, "ALL", 1, 80));
  EXPECT_EQ(tokens[16], Token(TokenType::BETWEEN, "BETWEEN", 1, 84));
  EXPECT_EQ(tokens[17], Token(TokenType::AND, "AND", 1, 92));
  EXPECT_EQ(tokens[18], Token(TokenType::AT, "AT", 1, 96));
  EXPECT_EQ(tokens[19], Token(TokenType::WHERE, "WHERE", 1, 99));
  EXPECT_EQ(tokens[20], Token(TokenType::FROM, "FROM", 1, 105));
  EXPECT_EQ(tokens[21], Token(TokenType::INTO, "INTO", 1, 110));
  EXPECT_EQ(tokens[22], Token(TokenType::TO, "TO", 1, 115));

  EXPECT_EQ(tokens[23], Token(TokenType::EQUAL, "=", 1, 118));
  EXPECT_EQ(tokens[24], Token(TokenType::COMMA, ",", 1, 120));
  EXPECT_EQ(tokens[25], Token(TokenType::SEMICOLON, ";", 1, 122));

  EXPECT_EQ(tokens[26], Token(TokenType::IDENTIFIER, "id123", 1, 124));
  EXPECT_EQ(tokens[27], Token(TokenType::NUMBER, "123", 1, 130));
  EXPECT_EQ(tokens[28], Token(TokenType::IDENTIFIER, "id", 1, 133));
  EXPECT_EQ(tokens[29], Token(TokenType::NUMBER, "123", 1, 136));
  EXPECT_EQ(tokens[30], Token(TokenType::NUMBER, "-123", 1, 140));
  EXPECT_EQ(tokens[31], Token(TokenType::NUMBER, "123.00", 1, 145));
  EXPECT_EQ(tokens[32], Token(TokenType::NUMBER, "-123.00", 1, 152));

  EXPECT_EQ(tokens.back(), Token(TokenType::END_OF_FILE, "", 1, 159));
}

TEST(LexerTest, HandlesEmptyInput) {
  Lexer lexer{""};
  auto tokens{lexer.tokenize()};
  
  ASSERT_EQ(tokens.size(), 1);
  EXPECT_EQ(tokens[0], Token(TokenType::END_OF_FILE, "", 1, 1));
}

TEST(LexerTest, HandlesWhitespaceOnlyInput) {
  Lexer lexer{"    \n\t   "};
  auto tokens{lexer.tokenize()};

  ASSERT_EQ(tokens.size(), 1);
  EXPECT_EQ(tokens[0], Token(TokenType::END_OF_FILE, "", 2, 5));
}

TEST(LexerTest, HandlesUnknownCharacters) {
  Lexer lexer{"SELECT # @ $"};
  auto tokens{lexer.tokenize()};

  ASSERT_EQ(tokens.size(), 5);
  EXPECT_EQ(tokens[0], Token(TokenType::SELECT, "SELECT", 1, 1));
  EXPECT_EQ(tokens[1], Token(TokenType::UNKNOWN, "#", 1, 8));
  EXPECT_EQ(tokens[2], Token(TokenType::UNKNOWN, "@", 1, 10));
  EXPECT_EQ(tokens[3], Token(TokenType::UNKNOWN, "$", 1, 12));
  EXPECT_EQ(tokens[4], Token(TokenType::END_OF_FILE, "", 1, 13));
}

TEST(LexerTest, HandlesMalformedNumbers) {
  Lexer lexer{"123..45 -123. invalid123. --"};
  auto tokens{lexer.tokenize()};

  ASSERT_EQ(tokens.size(), 11);
  EXPECT_EQ(tokens[0], Token(TokenType::NUMBER, "123", 1, 1));
  EXPECT_EQ(tokens[1], Token(TokenType::UNKNOWN, ".", 1, 4));
  EXPECT_EQ(tokens[2], Token(TokenType::UNKNOWN, ".", 1, 5));
  EXPECT_EQ(tokens[3], Token(TokenType::NUMBER, "45", 1, 6));
  EXPECT_EQ(tokens[4], Token(TokenType::NUMBER, "-123", 1, 9));
  EXPECT_EQ(tokens[5], Token(TokenType::UNKNOWN, ".", 1, 13));
  EXPECT_EQ(tokens[6], Token(TokenType::IDENTIFIER, "invalid123", 1, 15));
  EXPECT_EQ(tokens[7], Token(TokenType::UNKNOWN, ".", 1, 25));
  EXPECT_EQ(tokens[8], Token(TokenType::UNKNOWN, "-", 1, 27));
  EXPECT_EQ(tokens[9], Token(TokenType::UNKNOWN, "-", 1, 28));
  EXPECT_EQ(tokens[10], Token(TokenType::END_OF_FILE, "", 1, 29));
}

TEST(LexerTest, HandlesLongInput) {
  std::string long_input(10000, 'a');
  Lexer lexer{long_input};
  auto tokens{lexer.tokenize()};

  ASSERT_EQ(tokens.size(), 2);
  EXPECT_EQ(tokens[0], Token(TokenType::IDENTIFIER, long_input, 1, 1));
  EXPECT_EQ(tokens[1], Token(TokenType::END_OF_FILE, "", 1, 10001));
}

TEST(LexerTest, HandlesMixedLexemesWithWhitespace) {
  Lexer lexer{"SELECT 123 ; my_table\nWHERE = -456.78"};
  auto tokens{lexer.tokenize()};

  ASSERT_EQ(tokens.size(), 8);
  EXPECT_EQ(tokens[0], Token(TokenType::SELECT, "SELECT", 1, 1));
  EXPECT_EQ(tokens[1], Token(TokenType::NUMBER, "123", 1, 8));
  EXPECT_EQ(tokens[2], Token(TokenType::SEMICOLON, ";", 1, 12));
  EXPECT_EQ(tokens[3], Token(TokenType::IDENTIFIER, "my_table", 1, 14));
  EXPECT_EQ(tokens[4], Token(TokenType::WHERE, "WHERE", 2, 1));
  EXPECT_EQ(tokens[5], Token(TokenType::EQUAL, "=", 2, 7));
  EXPECT_EQ(tokens[6], Token(TokenType::NUMBER, "-456.78", 2, 9));
  EXPECT_EQ(tokens[7], Token(TokenType::END_OF_FILE, "", 2, 16));
}

TEST(LexerTest, HandlesTagListSyntax) {
  Lexer lexer{"key1=value1 key2=value2"};
  auto tokens{lexer.tokenize()};

  ASSERT_EQ(tokens.size(), 7);
  EXPECT_EQ(tokens[0], Token(TokenType::IDENTIFIER, "key1", 1, 1));
  EXPECT_EQ(tokens[1], Token(TokenType::EQUAL, "=", 1, 5));
  EXPECT_EQ(tokens[2], Token(TokenType::IDENTIFIER, "value1", 1, 6));
  EXPECT_EQ(tokens[3], Token(TokenType::IDENTIFIER, "key2", 1, 13));
  EXPECT_EQ(tokens[4], Token(TokenType::EQUAL, "=", 1, 17));
  EXPECT_EQ(tokens[5], Token(TokenType::IDENTIFIER, "value2", 1, 18));
  EXPECT_EQ(tokens[6], Token(TokenType::END_OF_FILE, "", 1, 24));
}

TEST(LexerTest, HandlesMixedCaseKeywords) {
  Lexer lexer{"select PUT SeLeCt"};
  auto tokens{lexer.tokenize()};

  ASSERT_EQ(tokens.size(), 4);
  EXPECT_EQ(tokens[0], Token(TokenType::IDENTIFIER, "select", 1, 1));
  EXPECT_EQ(tokens[1], Token(TokenType::PUT, "PUT", 1, 8));
  EXPECT_EQ(tokens[2], Token(TokenType::IDENTIFIER, "SeLeCt", 1, 12));
  EXPECT_EQ(tokens[3], Token(TokenType::END_OF_FILE, "", 1, 18));
}

TEST(LexerTest, HandlesComments) {
  Lexer lexer{"SELECT // This is a comment\nPUT"};
  auto tokens{lexer.tokenize()};

  ASSERT_EQ(tokens.size(), 3);
  EXPECT_EQ(tokens[0], Token(TokenType::SELECT, "SELECT", 1, 1));
  EXPECT_EQ(tokens[1], Token(TokenType::PUT, "PUT", 2, 1));
  EXPECT_EQ(tokens[2], Token(TokenType::END_OF_FILE, "", 2, 4));
}