#include "gtest/gtest.h"
#include <vkdb/printer.h>

using namespace vkdb;

TEST(PrinterTest, CanPrintCreateQuery) {
  CreateQuery create_query{
    TableName{"table_name"},
    TagColumns{std::vector<TagKey>{TagKey{"tag1"}, TagKey{"tag2"}}}
  };

  Printer printer;
  auto result{printer.print(create_query)};
  EXPECT_EQ(result, "CREATE TABLE table_name TAGS tag1, tag2;");
}

TEST(PrinterTest, CanPrintDropQuery) {
  DropQuery drop_query{TableName{"table_name"}};

  Printer printer;
  auto result{printer.print(drop_query)};
  EXPECT_EQ(result, "DROP TABLE table_name;");
}

TEST(PrinterTest, CanPrintAddQuery) {
  AddQuery add_query{
    TagColumns{std::vector<TagKey>{TagKey{"tag1"}, TagKey{"tag2"}}},
    TableName{"table_name"}
  };

  Printer printer;
  auto result{printer.print(add_query)};
  EXPECT_EQ(result, "ADD TAGS tag1, tag2 TO table_name;");
}

TEST(PrinterTest, CanPrintRemoveQuery) {
  RemoveQuery remove_query{
    TagColumns{std::vector<TagKey>{TagKey{"tag1"}, TagKey{"tag2"}}},
    TableName{"table_name"}
  };

  Printer printer;
  auto result{printer.print(remove_query)};
  EXPECT_EQ(result, "REMOVE TAGS tag1, tag2 FROM table_name;");
}