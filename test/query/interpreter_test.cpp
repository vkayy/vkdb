#include "gtest/gtest.h"
#include <vkdb/interpreter.h>

using namespace vkdb;

static Token make_token(TokenType type, const std::string& lexeme) {
  return Token{type, lexeme, 1, 1};
}

class InterpreterTest : public ::testing::Test {
protected:
  void SetUp() override {
    database_ = std::make_unique<Database>(INTERPRETER_DEFAULT_DATABASE);
  }

  void TearDown() override {
    database_->clear();
  }

  std::unique_ptr<Database> database_;
};

TEST_F(InterpreterTest, CanInterpretSelectDataAllWhereQuery) {
  database_->createTable("table");

  auto& table{database_->getTable("table")};
  table.addTagColumn("tag1");
  table.addTagColumn("tag2");

  table.query()
    .put(1, "metric", {{"tag1", "value1"}, {"tag2", "value2"}}, 10)
    .execute();

  Expr expr{SelectQuery{
    SelectTypeDataExpr{make_token(TokenType::DATA, "DATA")},
    MetricExpr{make_token(TokenType::IDENTIFIER, "metric")},
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table")},
    AllClause{WhereClause{TagListExpr{{
      TagExpr{
        TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag1")},
        TagValueExpr{make_token(TokenType::IDENTIFIER, "value1")}
      },
      TagExpr{
        TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag2")},
        TagValueExpr{make_token(TokenType::IDENTIFIER, "value2")}
      }}
    }}}
  }};

  Interpreter interpreter{*database_};
  std::ostringstream stream;
  interpreter.interpret(expr, stream);

  std::vector<DataPoint<double>> expected_datapoints{
    {1, "metric", {{"tag1", "value1"}, {"tag2", "value2"}}, 10}
  };

  EXPECT_EQ(stream.str(), datapointsToString<double>(expected_datapoints) + "\n");
}
TEST_F(InterpreterTest, CanInterpretSelectDataBetweenWhereQuery) {
  database_->createTable("table");

  auto& table{database_->getTable("table")};
  table.addTagColumn("tag1");
  table.addTagColumn("tag2");

  table.query()
    .put(1, "metric", {{"tag1", "value1"}, {"tag2", "value2"}}, 10)
    .execute();

  table.query()
    .put(2, "metric", {{"tag1", "value1"}, {"tag2", "value2"}}, 20)
    .execute();

  Expr expr{SelectQuery{
    SelectTypeDataExpr{make_token(TokenType::DATA, "DATA")},
    MetricExpr{make_token(TokenType::IDENTIFIER, "metric")},
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table")},
    BetweenClause{
      make_token(TokenType::NUMBER, "1"),
      make_token(TokenType::NUMBER, "2"),
      WhereClause{TagListExpr{{
        TagExpr{
          TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag1")},
          TagValueExpr{make_token(TokenType::IDENTIFIER, "value1")}
        },
        TagExpr{
          TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag2")},
          TagValueExpr{make_token(TokenType::IDENTIFIER, "value2")}
        }
      }}}}
    }};

  Interpreter interpreter{*database_};
  std::ostringstream stream;
  interpreter.interpret(expr, stream);

  std::vector<DataPoint<double>> expected_datapoints{
    {1, "metric", {{"tag1", "value1"}, {"tag2", "value2"}}, 10},
    {2, "metric", {{"tag1", "value1"}, {"tag2", "value2"}}, 20}
  };

  EXPECT_EQ(stream.str(), datapointsToString<double>(expected_datapoints) + "\n");
}

TEST_F(InterpreterTest, CanInterpretSelectDataAtWhereQuery) {
  database_->createTable("table");

  auto& table{database_->getTable("table")};
  table.addTagColumn("tag1");
  table.addTagColumn("tag2");

  table.query()
    .put(1, "metric", {{"tag1", "value1"}, {"tag2", "value2"}}, 10)
    .execute();

  Expr expr{SelectQuery{
    SelectTypeDataExpr{make_token(TokenType::DATA, "DATA")},
    MetricExpr{make_token(TokenType::IDENTIFIER, "metric")},
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table")},
    AtClause{
      make_token(TokenType::NUMBER, "1"),
      WhereClause{TagListExpr{{
        TagExpr{
          TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag1")},
          TagValueExpr{make_token(TokenType::IDENTIFIER, "value1")}
        },
        TagExpr{
          TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag2")},
          TagValueExpr{make_token(TokenType::IDENTIFIER, "value2")}
        }}
    }}}
  }};

  Interpreter interpreter{*database_};
  std::ostringstream stream;
  interpreter.interpret(expr, stream);

  std::vector<DataPoint<double>> expected_datapoints{
    {1, "metric", {{"tag1", "value1"}, {"tag2", "value2"}}, 10}
  };

  EXPECT_EQ(stream.str(), datapointsToString<double>(expected_datapoints) + "\n");
}

TEST_F(InterpreterTest, CanInterpretPutWithTagsQuery) {
  database_->createTable("table");

  auto& table{database_->getTable("table")};
  table.addTagColumn("tag1");
  table.addTagColumn("tag2");

  Expr expr{PutQuery{
    make_token(TokenType::IDENTIFIER, "metric"),
    make_token(TokenType::NUMBER, "1"),
    make_token(TokenType::NUMBER, "10"),
    make_token(TokenType::IDENTIFIER, "table"),
    TagListExpr{{{
      make_token(TokenType::IDENTIFIER, "tag1"),
      make_token(TokenType::IDENTIFIER, "value1")
    },
    {
      make_token(TokenType::IDENTIFIER, "tag2"),
      make_token(TokenType::IDENTIFIER, "value2")
    }}}
  }};

  Interpreter interpreter{*database_};
  interpreter.interpret(expr);

  std::vector<DataPoint<double>> expected_datapoints{
    {1, "metric", {{"tag1", "value1"}, {"tag2", "value2"}}, 10}
  };

  auto result{table.query()
    .whereTagsContainAllOf(Tag{"tag1", "value1"}, Tag{"tag2", "value2"})
    .execute()};

  EXPECT_EQ(
    datapointsToString<double>(result),
    datapointsToString<double>(expected_datapoints)
  );
}

TEST_F(InterpreterTest, CanInterpretDeleteQuery) {
  database_->createTable("table");

  auto& table{database_->getTable("table")};
  table.addTagColumn("tag1");
  table.addTagColumn("tag2");

  table.query()
    .put(1, "metric", {{"tag1", "value1"}, {"tag2", "value2"}}, 10)
    .execute();

  Expr expr{DeleteQuery{
    make_token(TokenType::IDENTIFIER, "metric"),
    make_token(TokenType::NUMBER, "1"),
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table")},
    TagListExpr{{
      TagExpr{
        TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag1")},
        TagValueExpr{make_token(TokenType::IDENTIFIER, "value1")}
      },
      TagExpr{
        TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag2")},
        TagValueExpr{make_token(TokenType::IDENTIFIER, "value2")}
      }}
    }
  }};

  Interpreter interpreter{*database_};
  interpreter.interpret(expr);

  auto result{table.query()
    .whereTagsContainAllOf(Tag{"tag1", "value1"}, Tag{"tag2", "value2"})
    .execute()};

  EXPECT_TRUE(result.empty());
}

TEST_F(InterpreterTest, CanInterpretCreateQuery) {
  Expr expr{CreateQuery{
    TableNameExpr{make_token(TokenType::IDENTIFIER, "new_table")},
    TagColumnsExpr{{
      TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag1")},
      TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag2")}
    }}
  }};

  Interpreter interpreter{*database_};
  interpreter.interpret(expr);

  EXPECT_NO_THROW(std::ignore = database_->getTable("new_table"));
}

TEST_F(InterpreterTest, CanInterpretDropQuery) {
  database_->createTable("table");

  Expr expr{DropQuery{
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table")}
  }};

  Interpreter interpreter{*database_};
  interpreter.interpret(expr);

  EXPECT_THROW(std::ignore = database_->getTable("table"), std::runtime_error);
}

TEST_F(InterpreterTest, CanInterpretAddQuery) {
  database_->createTable("table");

  auto& table{database_->getTable("table")};
  table.addTagColumn("tag1");

  Expr expr{AddQuery{
    TagColumnsExpr{{TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag2")}}},
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table")}
  }};

  Interpreter interpreter{*database_};
  interpreter.interpret(expr);

  EXPECT_THROW(table.addTagColumn("tag2"), std::runtime_error);
}

TEST_F(InterpreterTest, CanInterpretRemoveQuery) {
  database_->createTable("table");

  auto& table{database_->getTable("table")};
  table.addTagColumn("tag1");
  table.addTagColumn("tag2");

  Expr expr{RemoveQuery{
    TagColumnsExpr{{TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag2")}}},
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table")}
  }};

  Interpreter interpreter{*database_};
  interpreter.interpret(expr);

  EXPECT_THROW(table.removeTagColumn("tag2"), std::runtime_error);
}

TEST_F(InterpreterTest, CanInterpretMultipleQueries) {
  Expr create_expr{CreateQuery{
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table")},
    TagColumnsExpr{{
      TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag1")},
      TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag2")}
    }}
  }};
  Interpreter interpreter{*database_};
  interpreter.interpret(create_expr);

  Expr drop_expr{DropQuery{
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table")}
  }};
  interpreter.interpret(drop_expr);

  interpreter.interpret(create_expr);
  
  Expr add_expr{AddQuery{AddQuery{
    TagColumnsExpr{{TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag3")}}},
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table")}
  }}};
  interpreter.interpret(add_expr);

  Expr remove_expr{RemoveQuery{
    TagColumnsExpr{{TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag3")}}},
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table")}
  }};
  interpreter.interpret(remove_expr);

  Expr put_expr1{PutQuery{
    make_token(TokenType::IDENTIFIER, "metric"),
    make_token(TokenType::NUMBER, "1"),
    make_token(TokenType::NUMBER, "10"),
    make_token(TokenType::IDENTIFIER, "table"),
    TagListExpr{{{
      make_token(TokenType::IDENTIFIER, "tag1"),
      make_token(TokenType::IDENTIFIER, "value1")
    },
    {
      make_token(TokenType::IDENTIFIER, "tag2"),
      make_token(TokenType::IDENTIFIER, "value2")
    }}}
  }};
  interpreter.interpret(put_expr1);

  Expr put_expr2{PutQuery{
    make_token(TokenType::IDENTIFIER, "metric"),
    make_token(TokenType::NUMBER, "2"),
    make_token(TokenType::NUMBER, "20"),
    make_token(TokenType::IDENTIFIER, "table"),
    TagListExpr{{{
      make_token(TokenType::IDENTIFIER, "tag1"),
      make_token(TokenType::IDENTIFIER, "value1")
    },
    {
      make_token(TokenType::IDENTIFIER, "tag2"),
      make_token(TokenType::IDENTIFIER, "value2")
    }}}
  }};
  interpreter.interpret(put_expr2);

  Expr delete_expr{DeleteQuery{
    make_token(TokenType::IDENTIFIER, "metric"),
    make_token(TokenType::NUMBER, "1"),
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table")},
    TagListExpr{{
      TagExpr{
        TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag1")},
        TagValueExpr{make_token(TokenType::IDENTIFIER, "value1")}
      },
      TagExpr{
        TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag2")},
        TagValueExpr{make_token(TokenType::IDENTIFIER, "value2")}
      }}
    }
  }};
  interpreter.interpret(delete_expr);

  Expr select_expr{SelectQuery{
    SelectTypeDataExpr{make_token(TokenType::DATA, "DATA")},
    MetricExpr{make_token(TokenType::IDENTIFIER, "metric")},
    TableNameExpr{make_token(TokenType::IDENTIFIER, "table")},
    AllClause{WhereClause{TagListExpr{{
      TagExpr{
        TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag1")},
        TagValueExpr{make_token(TokenType::IDENTIFIER, "value1")}
      },
      TagExpr{
        TagKeyExpr{make_token(TokenType::IDENTIFIER, "tag2")},
        TagValueExpr{make_token(TokenType::IDENTIFIER, "value2")}
      }}
    }}}
  }};

  std::ostringstream stream;
  interpreter.interpret(select_expr, stream);

  std::vector<DataPoint<double>> expected_datapoints{
    {2, "metric", {{"tag1", "value1"}, {"tag2", "value2"}}, 20}
  };

  EXPECT_EQ(stream.str(), datapointsToString<double>(expected_datapoints) + "\n");
}