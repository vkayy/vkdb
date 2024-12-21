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

using MetricExprResult = Metric;
using TableNameExprResult = std::string;
using TimestampExprResult = Timestamp;
using ValueExprResult = double;
using TagKeyExprResult = TagKey;
using TagValueExprResult = TagValue;
using TagExprResult = Tag;
using TagListExprResult = TagTable;
using TagColumnsExprResult = TagColumns;

using SelectDataResult = std::vector<DataPoint<double>>;
using SelectDoubleResult = double;
using SelectCountResult = uint64_t;

using SelectResult = std::variant<
  SelectDataResult,
  SelectDoubleResult,
  SelectCountResult
>;

using SelectTypeResult = SelectType;
using WhereClauseResult = TagListExprResult;
using AllClauseResult = std::optional<WhereClauseResult>;
using BetweenClauseResult = std::tuple<
  TimestampExprResult,
  TimestampExprResult,
  std::optional<WhereClauseResult>
>;
using AtClauseResult = std::pair<
  TimestampExprResult,
  std::optional<WhereClauseResult>
>;
using SelectClauseResult = std::variant<
  AllClauseResult,
  BetweenClauseResult,
  AtClauseResult
>;

using PutResult = void;
using DeleteResult = void;
using CreateResult = void;
using DropResult = void;
using AddResult = void;
using RemoveResult = void;
using TablesResult = std::vector<std::string>;

using OutputResult = std::variant<
  SelectResult,
  TablesResult
>;
using Result = std::optional<OutputResult>;
using Results = std::vector<Result>;

static const std::string INTERPRETER_DEFAULT_DATABASE{"interpreter_default"};

class RuntimeError {
public:
  RuntimeError(Token token, const std::string& message) noexcept;
  [[nodiscard]] Token token() const;
  [[nodiscard]] std::string message() const;
private:
  Token token_;
  std::string message_;
};

class Interpreter {
public:
  using error_callback = std::function<void(const RuntimeError&)>;
  
  explicit Interpreter(
    Database& database,
    error_callback callback = [](const RuntimeError&) {}
  ) noexcept;

  void interpret(const Expr& expr, std::ostream& stream = std::cout) const;
private:
  [[nodiscard]] std::string to_string(const OutputResult& result) const;
  [[nodiscard]] std::string to_string(const SelectResult& result) const;
  [[nodiscard]] std::string to_string(const TablesResult& result) const;
  [[nodiscard]] Results visit(const Expr& expr) const;
  [[nodiscard]] Result visit(const Query& query) const;
  
  static void add_optional_tag_list(
    FriendlyQueryBuilder<double> &query_builder,
    const std::optional<TagListExprResult>& tag_list
  ) noexcept;
  
  static SelectResult handle_select_type(
    FriendlyQueryBuilder<double> &query_builder,
    SelectType type
  );

  [[nodiscard]] SelectResult visit(const SelectQuery& query) const;
  PutResult visit(const PutQuery& query) const;
  DeleteResult visit(const DeleteQuery& query) const;
  CreateResult visit(const CreateQuery& query) const;
  DropResult visit(const DropQuery& query) const;
  AddResult visit(const AddQuery& query) const;
  RemoveResult visit(const RemoveQuery& query) const;
  [[nodiscard]] TablesResult visit(const TablesQuery& query) const;
  [[nodiscard]] AllClauseResult visit(const AllClause& clause) const;
  [[nodiscard]] BetweenClauseResult visit(const BetweenClause& clause) const;
  [[nodiscard]] AtClauseResult visit(const AtClause& clause) const;
  [[nodiscard]] WhereClauseResult visit(const WhereClause& clause) const;
  [[nodiscard]] SelectTypeResult visit(const SelectType& type) const;
  [[nodiscard]] MetricExprResult visit(const MetricExpr& metric) const;
  [[nodiscard]] TableNameExprResult visit(const TableNameExpr& table_name) const;
  [[nodiscard]] TagKeyExprResult visit(const TagKeyExpr& tag_key) const;
  [[nodiscard]] TagValueExprResult visit(const TagValueExpr& tag_value) const;
  [[nodiscard]] TagExprResult visit(const TagExpr& tag) const;
  [[nodiscard]] TagListExprResult visit(const TagListExpr& tag_list) const;
  [[nodiscard]] TagColumnsExprResult visit(const TagColumnsExpr& tag_columns) const;
  [[nodiscard]] TimestampExprResult visit(const TimestampExpr& timestamp) const;
  [[nodiscard]] ValueExprResult visit(const ValueExpr& value) const;

  error_callback callback_;
  Database& database_;
};

}  // namespace vkdb

#endif // QUERY_INTERPRETER_H