#ifndef QUERY_EXPR_H
#define QUERY_EXPR_H

#include <vkdb/token.h>
#include <string>
#include <vector>
#include <optional>
#include <variant>

namespace vkdb {
struct MetricExpr {
  Token token;
};

struct TableNameExpr {
  Token token;
};

struct TimestampExpr {
  Token token;
};

struct ValueExpr {
  Token token;
};

struct TagKeyExpr {
  Token token;
};

struct TagColumnsExpr {
  std::vector<TagKeyExpr> keys;
};

struct TagValueExpr {
  Token token;
};

struct TagExpr {
  TagKeyExpr key;
  TagValueExpr value;
};

struct TagListExpr {
  std::vector<TagExpr> tags;
};

struct WhereClause {
  TagListExpr tag_list;
};

struct AllClause {
  std::optional<WhereClause> where_clause;
};

struct BetweenClause {
  TimestampExpr start;
  TimestampExpr end;
  std::optional<WhereClause> where_clause;
};

struct AtClause {
  TimestampExpr timestamp;
  std::optional<WhereClause> where_clause;
};

struct SelectTypeDataExpr {
  Token token;
};

struct SelectTypeCountExpr {
  Token token;
};

struct SelectTypeAvgExpr {
  Token token;
};

struct SelectTypeSumExpr {
  Token token;
};

struct SelectTypeMinExpr {
  Token token;
};

struct SelectTypeMaxExpr {
  Token token;
};

using SelectType = std::variant<
  SelectTypeDataExpr,
  SelectTypeCountExpr,
  SelectTypeAvgExpr,
  SelectTypeSumExpr,
  SelectTypeMinExpr,
  SelectTypeMaxExpr
>;  

using SelectClause = std::variant<
  AllClause,
  BetweenClause,
  AtClause
>;

struct SelectQuery {
  SelectType type;
  MetricExpr metric;
  TableNameExpr table_name;
  SelectClause clause;
};

struct PutQuery {
  MetricExpr metric;
  TimestampExpr timestamp;
  ValueExpr value;
  TableNameExpr table_name;
  std::optional<TagListExpr> tag_list;
};

struct DeleteQuery {
  MetricExpr metric;
  TimestampExpr timestamp;
  TableNameExpr table_name;
  std::optional<TagListExpr> tag_list;
};

struct CreateQuery {
  TableNameExpr table_name;
  std::optional<TagColumnsExpr> tag_columns;
};

struct DropQuery {
  TableNameExpr table_name;
};

struct AddQuery {
  TagColumnsExpr tag_columns;
  TableNameExpr table_name;
};

struct RemoveQuery {
  TagColumnsExpr tag_columns;
  TableNameExpr table_name;
};

using Query = std::variant<
  SelectQuery, PutQuery, DeleteQuery,
  CreateQuery, DropQuery, 
  AddQuery, RemoveQuery
>;

using Expr = std::vector<Query>;
}  // namespace vkdb

#endif // QUERY_EXPR_H
