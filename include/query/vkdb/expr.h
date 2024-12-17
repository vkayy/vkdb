#ifndef QUERY_EXPR_H
#define QUERY_EXPR_H

#include <string>
#include <vector>
#include <optional>
#include <variant>

namespace vkdb {
struct Metric {
  std::string identifier;
};

struct TableName {
  std::string identifier;
};

struct Timestamp {
  std::string number;
};

struct Value {
  std::string number;
};

struct TagKey {
  std::string identifier;
};

struct TagColumns {
  std::vector<TagKey> keys;
};

struct TagValue {
  std::string identifier;
};

struct Tag {
  TagKey key;
  TagValue value;
};

struct TagList {
  std::vector<Tag> tags;
};

struct WhereClause {
  TagList tag_list;
};

struct AllClause {
  std::optional<WhereClause> where_clause;
};

struct BetweenClause {
  Timestamp start;
  Timestamp end;
  std::optional<WhereClause> where_clause;
};

struct AtClause {
  Timestamp timestamp;
  std::optional<WhereClause> where_clause;
};

enum class SelectType {
  DATA, AVG, SUM, COUNT, MIN, MAX
};

using SelectClause = std::variant<
  AllClause,
  BetweenClause,
  AtClause
>;

struct SelectQuery {
  SelectType type;
  Metric metric;
  TableName table_name;
  SelectClause clause;
};

struct PutQuery {
  Metric metric;
  Timestamp timestamp;
  Value value;
  TableName table_name;
  std::optional<TagList> tag_list;
};

struct DeleteQuery {
  Metric metric;
  Timestamp timestamp;
  TableName table_name;
  std::optional<TagList> tag_list;
};

struct CreateQuery {
  TableName table_name;
  std::optional<TagColumns> tag_columns;
};

struct DropQuery {
  TableName table_name;
};

struct AddQuery {
  TagColumns tag_columns;
  TableName table_name;
};

struct RemoveQuery {
  TagColumns tag_columns;
  TableName table_name;
};

using Query = std::variant<
  SelectQuery, PutQuery, DeleteQuery,
  CreateQuery, DropQuery, 
  AddQuery, RemoveQuery
>;

using Expr = Query;
}  // namespace vkdb

#endif // QUERY_EXPR_H
