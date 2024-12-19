#include "gtest/gtest.h"
#include <vkdb/printer.h>

using namespace vkdb;

static Token make_token(TokenType type, const std::string& lexeme) {
  return Token{type, lexeme, 1, 1};
}

TEST(PrinterTest, CanPrintSelectQuery) {
  Expr select_query{SelectQuery{
    SelectTypeDataExpr{make_token(TokenType::DATA, "DATA")},
    MetricExpr{make_token(TokenType::IDENTIFIER, "metric")},
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table_name")},
    SelectClause{AllClause{WhereClause{TagListExpr{TagListExpr{
      {{TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag")},
      TagValueExpr{make_token(TokenType::IDENTIFIER, "value")}}}
    }}}}}
  }};

  Printer printer;
  auto result{printer.print(select_query)};
  EXPECT_EQ(result, "SELECT DATA metric FROM table_name ALL WHERE tag=value;");
}

TEST(PrinterTest, CanPrintPutQuery) {
  Expr put_query{PutQuery{
    MetricExpr{make_token(TokenType::IDENTIFIER, "metric")},
    TimestampExpr{make_token(TokenType::NUMBER, "15")},
    ValueExpr{make_token(TokenType::NUMBER, "10")},
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table_name")},
    TagListExpr{TagListExpr{
      {{TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag")},
      TagValueExpr{make_token(TokenType::IDENTIFIER, "value")}}}
    }}
  }};

  Printer printer;
  auto result{printer.print(put_query)};
  EXPECT_EQ(result, "PUT metric 15 10 INTO table_name TAGS tag=value;");
}

TEST(PrinterTest, CanPrintDeleteQueries) {
  Expr delete_query{DeleteQuery{
    MetricExpr{make_token(TokenType::IDENTIFIER, "metric")},
    TimestampExpr{make_token(TokenType::NUMBER, "15")},
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table_name")},
    TagListExpr{TagListExpr{
      {{TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag")},
      TagValueExpr{make_token(TokenType::IDENTIFIER, "value")}}}
    }}
  }};

  Printer printer;
  auto result{printer.print(delete_query)};
  EXPECT_EQ(result, "DELETE metric 15 FROM table_name TAGS tag=value;");
}

TEST(PrinterTest, CanPrintCreateQuery) {
  Expr create_query{CreateQuery{
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table_name")},
    TagColumnsExpr{std::vector<TagKeyExpr>{
      TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag1")},
      TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag2")}
    }}
  }};

  Printer printer;
  auto result{printer.print(create_query)};
  EXPECT_EQ(result, "CREATE TABLE table_name TAGS tag1, tag2;");
}

TEST(PrinterTest, CanPrintDropQuery) {
  Expr drop_query{DropQuery{
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table_name")}
  }};

  Printer printer;
  auto result{printer.print(drop_query)};
  EXPECT_EQ(result, "DROP TABLE table_name;");
}

TEST(PrinterTest, CanPrintAddQuery) {
  Expr add_query{AddQuery{
    TagColumnsExpr{std::vector<TagKeyExpr>{
      TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag1")},
      TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag2")}}
    },
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table_name")}
  }};

  Printer printer;
  auto result{printer.print(add_query)};
  EXPECT_EQ(result, "ADD TAGS tag1, tag2 TO table_name;");
}

TEST(PrinterTest, CanPrintRemoveQuery) {
  Expr remove_query{RemoveQuery{
    TagColumnsExpr{std::vector<TagKeyExpr>{
      TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag1")},
      TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag2")}
    }},
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table_name")}
  }};

  Printer printer;
  auto result{printer.print(remove_query)};
  EXPECT_EQ(result, "REMOVE TAGS tag1, tag2 FROM table_name;");
}

TEST(PrinterTest, CanPrintTablesQuery) {
  Expr tables_query{TablesQuery{}};

  Printer printer;
  auto result{printer.print(tables_query)};
  EXPECT_EQ(result, "TABLES;");
}

TEST(PrinterTest, CanPrintMultipleQueries) {
  Expr all_queries{
  SelectQuery{
    SelectTypeDataExpr{make_token(TokenType::DATA, "DATA")},
    MetricExpr{make_token(TokenType::IDENTIFIER, "metric")},
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table_name")},
    AllClause{WhereClause{TagListExpr{TagListExpr{
      {{TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag")},
      TagValueExpr{make_token(TokenType::IDENTIFIER, "value")}}}
    }}}}
  },
  PutQuery{
    MetricExpr{make_token(TokenType::IDENTIFIER, "metric")},
    TimestampExpr{make_token(TokenType::NUMBER, "15")},
    ValueExpr{make_token(TokenType::NUMBER, "10")},
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table_name")},
    TagListExpr{TagListExpr{
      {{TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag")},
      TagValueExpr{make_token(TokenType::IDENTIFIER, "value")}}}
    }}
  },
  DeleteQuery{
    MetricExpr{make_token(TokenType::IDENTIFIER, "metric")},
    TimestampExpr{make_token(TokenType::NUMBER, "15")},
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table_name")},
    TagListExpr{TagListExpr{
      {{TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag")},
      TagValueExpr{make_token(TokenType::IDENTIFIER, "value")}}}
    }}
  },
  CreateQuery{
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table_name")},
    TagColumnsExpr{std::vector<TagKeyExpr>{TagKeyExpr{
      make_token(TokenType::IDENTIFIER, "tag1")}, TagKeyExpr{
      make_token(TokenType::IDENTIFIER, "tag2")}
    }}
  },
  DropQuery{
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table_name")}
  },
  AddQuery{
    TagColumnsExpr{std::vector<TagKeyExpr>{
      TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag1")},
      TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag2")}}
    },
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table_name")}
  }, 
  RemoveQuery{
    TagColumnsExpr{std::vector<TagKeyExpr>{
      TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag1")},
      TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag2")}
    }},
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table_name")}
  }};

  Printer printer;
  auto result{printer.print(all_queries)};

  std::string expected{
    "SELECT DATA metric FROM table_name ALL WHERE tag=value;"
    "PUT metric 15 10 INTO table_name TAGS tag=value;"
    "DELETE metric 15 FROM table_name TAGS tag=value;"
    "CREATE TABLE table_name TAGS tag1, tag2;"
    "DROP TABLE table_name;"
    "ADD TAGS tag1, tag2 TO table_name;"
    "REMOVE TAGS tag1, tag2 FROM table_name;"
  };
}