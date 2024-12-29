#ifndef QUERY_INTERPRETER_H
#define QUERY_INTERPRETER_H

#include <vkdb/friendly_builder.h>
#include <vkdb/expr.h>
#include <sstream>
#include <iostream>
#include <string>
#include <variant>

namespace vkdb {
class Database;

/**
 * @brief Type alias for Metric.
 * 
 */
using MetricExprResult = Metric;

/**
 * @brief Type alias for string.
 * 
 */
using TableNameExprResult = std::string;

/**
 * @brief Type alias for Timestamp.
 * 
 */
using TimestampExprResult = Timestamp;

/**
 * @brief Type alias for double.
 * 
 */
using ValueExprResult = double;

/**
 * @brief Type alias for TagKey.
 * 
 */
using TagKeyExprResult = TagKey;

/**
 * @brief Type alias for TagValue.
 * 
 */
using TagValueExprResult = TagValue;

/**
 * @brief Type alias for Tag.
 * 
 */
using TagExprResult = Tag;

/**
 * @brief Type alias for TagTable.
 * 
 */
using TagListExprResult = TagTable;

/**
 * @brief Type alias for TagColumns.
 * 
 */
using TagColumnsExprResult = TagColumns;

/**
 * @brief Type alias for a vector of DataPoint<double>.
 * 
 */
using SelectDataResult = std::vector<DataPoint<double>>;

/**
 * @brief Type alias for double.
 * 
 */
using SelectDoubleResult = double;

/**
 * @brief Type alias for uint64_t.
 * 
 */
using SelectCountResult = uint64_t;

/**
 * @brief Select result.
 * @details Variant of select data, double, and count results.
 * 
 */
using SelectResult = std::variant<
  SelectDataResult,
  SelectDoubleResult,
  SelectCountResult
>;

/**
 * @brief Type alias for SelectType.
 * 
 */
using SelectTypeResult = SelectType;

/**
 * @brief Type alias for TagListExprResult.
 * 
 */
using WhereClauseResult = TagListExprResult;

/**
 * @brief Type alias for optional WhereClauseResult.
 * 
 */
using AllClauseResult = std::optional<WhereClauseResult>;

/**
 * @brief Between clause result.
 * @details Tuple of start timestamp, end timestamp, and optional where clause
 * result.
 * 
 */
using BetweenClauseResult = std::tuple<
  TimestampExprResult,
  TimestampExprResult,
  std::optional<WhereClauseResult>
>;

/**
 * @brief Type alias for pair of TimestampExprResult and optional
 * WhereClauseResult.
 * 
 */
using AtClauseResult = std::pair<
  TimestampExprResult,
  std::optional<WhereClauseResult>
>;

/**
 * @brief Select clause result.
 * @details Variant of all, between, and at clause results.
 * 
 */
using SelectClauseResult = std::variant<
  AllClauseResult,
  BetweenClauseResult,
  AtClauseResult
>;

/**
 * @brief Type alias for void.
 * 
 */
using PutResult = void;

/**
 * @brief Type alias for void.
 * 
 */
using DeleteResult = void;

/**
 * @brief Type alias for void.
 * 
 */
using CreateResult = void;

/**
 * @brief Type alias for void.
 * 
 */
using DropResult = void;

/**
 * @brief Type alias for void.
 * 
 */
using AddResult = void;

/**
 * @brief Type alias for void.
 * 
 */
using RemoveResult = void;

/**
 * @brief Type alias for vector of strings.
 * 
 */
using TablesResult = std::vector<std::string>;

/**
 * @brief Output result.
 * @details Variant of select and tables results.
 * 
 */
using OutputResult = std::variant<SelectResult, TablesResult>;

/**
 * @brief Type alias for optional OutputResult.
 * 
 */
using Result = std::optional<OutputResult>;

/**
 * @brief Type alias for vector of results.
 * 
 */
using Results = std::vector<Result>;

/**
 * @brief Database name used by standalone interpreter.
 * 
 */
static const std::string INTERPRETER_DEFAULT_DATABASE{"interpreter_default"};

/**
 * @brief Runtime error.
 * 
 */
class RuntimeError {
public:
  /**
   * @brief Construct a new Runtime Error object.
   * 
   * @param token Token.
   * @param message Message.
   */
  RuntimeError(Token token, const std::string& message) noexcept;

  /**
   * @brief Get the token.
   * 
   * @return Token Token.
   */
  [[nodiscard]] Token token() const noexcept;

  /**
   * @brief Get the message.
   * 
   * @return std::string Message.
   */
  [[nodiscard]] std::string message() const noexcept;

private:
  /**
   * @brief Token.
   * 
   */
  Token token_;

  /**
   * @brief Message.
   * 
   */
  std::string message_;
};

/**
 * @brief Interpreter for vq.
 * 
 */
class Interpreter {
public:
  using error_callback = std::function<void(const RuntimeError&)>;
  
  /**
   * @brief Construct a new Interpreter object.
   * @details Defaults the callback to an empty lambda.
   * 
   * @param database Database.
   * @param callback Error callback.
   */
  explicit Interpreter(
    Database& database,
    error_callback callback = [](const RuntimeError&) {}
  ) noexcept;

  /**
   * @brief Interpret the expression.
   * @details Defaults the output stream to std::cout.
   * 
   * @param expr Expression.
   * @param stream Stream.
   */
  void interpret(
    const Expr& expr,
    std::ostream& stream = std::cout
  ) const noexcept;

private:
  /**
   * @brief Convert the output result to a string.
   * 
   * @param result Output result.
   * @return std::string String.
   */
  [[nodiscard]] std::string to_string(const OutputResult& result) const;

  /**
   * @brief Convert the select result to a string.
   * 
   * @param result Select result.
   * @return std::string String.
   */
  [[nodiscard]] std::string to_string(const SelectResult& result) const;

  /**
   * @brief Convert the tables result to a string.
   * 
   * @param result Tables result.
   * @return std::string String.
   */
  [[nodiscard]] std::string to_string(const TablesResult& result) const;

  /**
   * @brief Visit the expression.
   * @details Interprets the expression.
   * 
   * @param expr Expression.
   * @return Results Results.
   */
  [[nodiscard]] Results visit(const Expr& expr) const;

  /**
   * @brief Visit the query.
   * @details Interprets the query.
   * 
   * @param query Query.
   * @return Result Result.
   */
  [[nodiscard]] Result visit(const Query& query) const;
  
  /**
   * @brief Add an optional tag list to the query builder.
   * 
   * @param query_builder Query builder.
   * @param tag_list Optional tag list.
   */
  static void add_optional_tag_list(
    FriendlyQueryBuilder<double> &query_builder,
    const std::optional<TagListExprResult>& tag_list
  ) noexcept;
  
  /**
   * @brief Handle the select type.
   * 
   * @param query_builder Query builder.
   * @param type Select type.
   * @return SelectResult Select result.
   * 
   * @throws RuntimeError If select query fails.
   */
  [[nodiscard]]
  static SelectResult handle_select_type(
    FriendlyQueryBuilder<double> &query_builder,
    SelectType type
  );

  /**
   * @brief Visit the select query.
   * @details Interprets the select query.
   * 
   * @param query Select query.
   * @return SelectResult Select result.
   * 
   * @throws RuntimeError If select query fails.
   */
  [[nodiscard]] SelectResult visit(const SelectQuery& query) const;

  /**
   * @brief Visit the put query.
   * @details Interprets the put query.
   * 
   * @param query Put query.
   * 
   * @throws RuntimeError If put query fails.
   */
  PutResult visit(const PutQuery& query) const;

  /**
   * @brief Visit the delete query.
   * @details Interprets the delete query.
   * 
   * @param query Delete query.
   * 
   * @throws RuntimeError If delete query fails.
   */
  DeleteResult visit(const DeleteQuery& query) const;

  /**
   * @brief Visit the create query.
   * @details Interprets the create query.
   * 
   * @param query Create query.
   * 
   * @throws RuntimeError If create query fails.
   */
  CreateResult visit(const CreateQuery& query) const;

  /**
   * @brief Visit the drop query.
   * @details Interprets the drop query.
   * 
   * @param query Drop query.
   * 
   * @throws RuntimeError If drop query fails.
   */
  DropResult visit(const DropQuery& query) const;

  /**
   * @brief Visit the add query.
   * @details Interprets the add query.
   * 
   * @param query Add query.
   * 
   * @throws RuntimeError If add query fails.
   */
  AddResult visit(const AddQuery& query) const;

  /**
   * @brief Visit the remove query.
   * @details Interprets the remove query.
   * 
   * @param query Remove query.
   * 
   * @throws RuntimeError If remove query fails.
   */
  RemoveResult visit(const RemoveQuery& query) const;

  /**
   * @brief Visit the tables query.
   * @details Interprets the tables query.
   * 
   * @param query Tables query.
   * @return TablesResult Tables result.
   * 
   * @throws RuntimeError If creating the tables result fails.
   */
  [[nodiscard]] TablesResult visit(const TablesQuery& query) const;

  /**
   * @brief Visit the all clause.
   * @details Interprets the all clause.
   * 
   * @param clause All clause.
   * @return AllClauseResult All clause result.
   * 
   * @throws std::exception If visiting the where clause fails.
   */
  [[nodiscard]] AllClauseResult visit(const AllClause& clause) const;

  /**
   * @brief Visit the between clause.
   * @details Interprets the between clause.
   * 
   * @param clause Between clause.
   * @return BetweenClauseResult Between clause result.
   * 
   * @throws std::exception If visiting the start or end or where clause fails.
   */
  [[nodiscard]] BetweenClauseResult visit(const BetweenClause& clause) const;

  /**
   * @brief Visit the at clause.
   * @details Interprets the at clause.
   * 
   * @param clause At clause.
   * @return AtClauseResult At clause result.
   * 
   * @throws std::exception If visiting the timestamp or where clause fails.
   */
  [[nodiscard]] AtClauseResult visit(const AtClause& clause) const;

  /**
   * @brief Visit the where clause.
   * 
   * @param clause Where clause.
   * @return WhereClauseResult Where clause result.
   * 
   * @throws std::exception If visiting the tag list fails. 
   */
  [[nodiscard]] WhereClauseResult visit(const WhereClause& clause) const;

  /**
   * @brief Visit the select type.
   * 
   * @param type Select type.
   * @return SelectTypeResult Select type result.
   */
  [[nodiscard]] SelectTypeResult visit(const SelectType& type) const noexcept;

  /**
   * @brief Visit the metric expression.
   * 
   * @param metric Metric expression.
   * @return MetricExprResult Metric expression result.
   */
  [[nodiscard]] MetricExprResult visit(
    const MetricExpr& metric
  ) const noexcept;

  /**
   * @brief Visit the table name expression.
   * 
   * @param table_name Table name expression.
   * @return TableNameExprResult Table name expression result.
   */
  [[nodiscard]] TableNameExprResult visit(
    const TableNameExpr& table_name
  ) const noexcept;
  
  /**
   * @brief Visit the tag key expression.
   * 
   * @param tag_key Tag key expression.
   * @return TagKeyExprResult Tag key expression result.
   */
  [[nodiscard]] TagKeyExprResult visit(
    const TagKeyExpr& tag_key
  ) const noexcept;

  /**
   * @brief Visit the tag value expression.
   * 
   * @param tag_value Tag value expression.
   * @return TagValueExprResult Tag value expression result.
   */
  [[nodiscard]] TagValueExprResult visit(
    const TagValueExpr& tag_value
  ) const noexcept;

  /**
   * @brief Visit the tag expression.
   * 
   * @param tag Tag expression.
   * @return TagExprResult Tag expression result.
   */
  [[nodiscard]] TagExprResult visit(const TagExpr& tag) const noexcept;

  /**
   * @brief Visit the tag list expression.
   * 
   * @param tag_list Tag list expression.
   * @return TagListExprResult Tag list expression result.
   * 
   * @throws std::exception If creating the tag list result fails.
   */
  [[nodiscard]] TagListExprResult visit(const TagListExpr& tag_list) const;

  /**
   * @brief Visit the tag columns expression.
   * 
   * @param tag_columns Tag columns expression.
   * @return TagColumnsExprResult Tag columns expression result.
   * 
   * @throws std::exception If creating the tag columns result fails.
   */
  [[nodiscard]] TagColumnsExprResult visit(
    const TagColumnsExpr& tag_columns
  ) const;

  /**
   * @brief Visit the timestamp expression.
   * 
   * @param timestamp Timestamp expression.
   * @return TimestampExprResult Timestamp expression result.
   * 
   * @throws std::exception If creating the timestamp result fails.
   */
  [[nodiscard]] TimestampExprResult visit(
    const TimestampExpr& timestamp
  ) const;

  /**
   * @brief Visit the value expression.
   * 
   * @param value Value expression.
   * @return ValueExprResult Value expression result.
   * 
   * @throws std::exception If creating the value result fails.
   */
  [[nodiscard]] ValueExprResult visit(const ValueExpr& value) const;

  /**
   * @brief Error callback.
   * 
   */
  error_callback callback_;

  /**
   * @brief Reference to the database.
   * 
   */
  Database& database_;
};

}  // namespace vkdb

#endif // QUERY_INTERPRETER_H