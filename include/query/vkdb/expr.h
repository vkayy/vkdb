#ifndef QUERY_EXPR_H
#define QUERY_EXPR_H

#include <vkdb/token.h>
#include <string>
#include <vector>
#include <optional>
#include <variant>

namespace vkdb {
/**
 * @brief Metric expression.
 * 
 */
struct MetricExpr {
  /**
   * @brief Token for the metric.
   * 
   */
  Token token;
};

/**
 * @brief Table name expression.
 * 
 */
struct TableNameExpr {
  /**
   * @brief Token for the table name.
   * 
   */
  Token token;
};

/**
 * @brief Timestamp expression.
 * 
 */
struct TimestampExpr {
  /**
   * @brief Token for the timestamp.
   * 
   */
  Token token;
};

/**
 * @brief Value expression.
 * 
 */
struct ValueExpr {
  /**
   * @brief Token for the value.
   * 
   */
  Token token;
};

/**
 * @brief Tag key expression.
 * 
 */
struct TagKeyExpr {
  /**
   * @brief Token for the tag key.
   * 
   */
  Token token;
};

/**
 * @brief Tag columns expression.
 * 
 */
struct TagColumnsExpr {
  /**
   * @brief Vector of tag key expressions.
   * 
   */
  std::vector<TagKeyExpr> keys;
};

/**
 * @brief Tag value expression.
 * 
 */
struct TagValueExpr {
  /**
   * @brief Token for the tag value.
   * 
   */
  Token token;
};

/**
 * @brief Tag expression.
 * 
 */
struct TagExpr {
  /**
   * @brief Tag key expression.
   * 
   */
  TagKeyExpr key;

  /**
   * @brief Tag value expression.
   * 
   */
  TagValueExpr value;
};

/**
 * @brief Tag list expression.
 * 
 */
struct TagListExpr {
  /**
   * @brief Vector of tag expressions.
   * 
   */
  std::vector<TagExpr> tags;
};

/**
 * @brief Where clause.
 * 
 */
struct WhereClause {
  /**
   * @brief Tag list expression.
   * 
   */
  TagListExpr tag_list;
};

/**
 * @brief All clause.
 * 
 */
struct AllClause {
  /**
   * @brief Optional where clause.
   * 
   */
  std::optional<WhereClause> where_clause;
};

/**
 * @brief Between clause.
 * 
 */
struct BetweenClause {
  /**
   * @brief Start timestamp expression.
   * 
   */
  TimestampExpr start;

  /**
   * @brief End timestamp expression.
   * 
   */
  TimestampExpr end;

  /**
   * @brief Optional where clause.
   * 
   */
  std::optional<WhereClause> where_clause;
};

/**
 * @brief At clause.
 * 
 */
struct AtClause {
  /**
   * @brief Timestamp expression.
   * 
   */
  TimestampExpr timestamp;

  /**
   * @brief Optional where clause.
   * 
   */
  std::optional<WhereClause> where_clause;
};

/**
 * @brief Select type data expression.
 * 
 */
struct SelectTypeDataExpr {
  /**
   * @brief Token for the select type data.
   * 
   */
  Token token;
};

/**
 * @brief Select type count expression.
 * 
 */
struct SelectTypeCountExpr {
  /**
   * @brief Token for the select type count.
   * 
   */
  Token token;
};

/**
 * @brief Select type average expression.
 * 
 */
struct SelectTypeAvgExpr {
  /**
   * @brief Token for the select type average.
   * 
   */
  Token token;
};

/**
 * @brief Select type sum expression.
 * 
 */
struct SelectTypeSumExpr {
  /**
   * @brief Token for the select type sum.
   * 
   */
  Token token;
};

/**
 * @brief Select type minimum expression.
 * 
 */
struct SelectTypeMinExpr {
  /**
   * @brief Token for the select type minimum.
   * 
   */
  Token token;
};

/**
 * @brief Select type maximum expression.
 * 
 */
struct SelectTypeMaxExpr {
  /**
   * @brief Token for the select type maximum.
   * 
   */
  Token token;
};

/**
 * @brief Select type.
 * @details Variant of select type data, count, average, sum, minimum, and
 * maximum expressions.
 * 
 */
using SelectType = std::variant<
  SelectTypeDataExpr,
  SelectTypeCountExpr,
  SelectTypeAvgExpr,
  SelectTypeSumExpr,
  SelectTypeMinExpr,
  SelectTypeMaxExpr
>;  

/**
 * @brief Select clause.
 * @details Variant of all, between, and at clauses.
 * 
 */
using SelectClause = std::variant<
  AllClause,
  BetweenClause,
  AtClause
>;

/**
 * @brief Select query.
 * 
 */
struct SelectQuery {
  /**
   * @brief Select type.
   * 
   */
  SelectType type;

  /**
   * @brief Metric expression.
   * 
   */
  MetricExpr metric;

  /**
   * @brief Table name expression.
   * 
   */
  TableNameExpr table_name;

  /**
   * @brief Select clause.
   * 
   */
  SelectClause clause;
};


/**
 * @brief Put query.
 * 
 */
struct PutQuery {
  /**
   * @brief Metric expression.
   * 
   */
  MetricExpr metric;

  /**
   * @brief Timestamp expression.
   * 
   */
  TimestampExpr timestamp;

  /**
   * @brief Value expression.
   * 
   */
  ValueExpr value;

  /**
   * @brief Table name expression.
   * 
   */
  TableNameExpr table_name;

  /**
   * @brief Optional tag list expression.
   * 
   */
  std::optional<TagListExpr> tag_list;
};

/**
 * @brief Delete query.
 * 
 */
struct DeleteQuery {
  /**
   * @brief Metric expression.
   * 
   */
  MetricExpr metric;

  /**
   * @brief Timestamp expression.
   * 
   */
  TimestampExpr timestamp;

  /**
   * @brief Table name expression.
   * 
   */
  TableNameExpr table_name;

  /**
   * @brief Optional tag list expression.
   * 
   */
  std::optional<TagListExpr> tag_list;
};

/**
 * @brief Create query.
 * 
 */
struct CreateQuery {
  /**
   * @brief Table name expression.
   * 
   */
  TableNameExpr table_name;

  /**
   * @brief Optional tag columns expression.
   * 
   */
  std::optional<TagColumnsExpr> tag_columns;
};

/**
 * @brief Drop query.
 * 
 */
struct DropQuery {
  /**
   * @brief Table name expression.
   * 
   */
  TableNameExpr table_name;
};

/**
 * @brief Add query.
 * 
 */
struct AddQuery {
  /**
   * @brief Tag columns expression.
   * 
   */
  TagColumnsExpr tag_columns;

  /**
   * @brief Table name expression.
   * 
   */
  TableNameExpr table_name;
};

/**
 * @brief Remove query.
 * 
 */
struct RemoveQuery {
  /**
   * @brief Tag columns expression.
   * 
   */
  TagColumnsExpr tag_columns;

  /**
   * @brief Table name expression.
   * 
   */
  TableNameExpr table_name;
};

/**
 * @brief Tables query.
 * 
 */
struct TablesQuery {
  /**
   * @brief Token for the tables query.
   * 
   */
  Token token;
};

/**
 * @brief Query expression.
 * @details Variant of select, put, delete, create, drop, add, remove, and
 * tables queries.
 * 
 */
using Query = std::variant<
  SelectQuery, PutQuery, DeleteQuery,
  CreateQuery, DropQuery, 
  AddQuery, RemoveQuery,
  TablesQuery
>;

/**
 * @brief Type alias for a vector of queries.
 * 
 */
using Expr = std::vector<Query>;
}  // namespace vkdb

#endif // QUERY_EXPR_H
