#ifndef QUERY_INTERPRETER_H
#define QUERY_INTERPRETER_H

#include <vkdb/expr.h>
#include <vkdb/database.h>
#include <vkdb/friendly_builder.h>
#include <sstream>
#include <iostream>
#include <string>
#include <variant>

namespace vkdb {
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

using Result = std::optional<SelectResult>;
using Results = std::vector<Result>;

static const std::string INTERPRETER_DEFAULT_DATABASE{"interpreter_default"};

class RuntimeError {
public:
  RuntimeError(Token token, const std::string& message) noexcept
    : token_{token}, message_{message} {}
  
  [[nodiscard]] Token token() const {
    return token_;
  }

  [[nodiscard]] std::string message() const {
    return message_;
  }
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
  ) noexcept
    : database_{database}, callback_{callback} {}

  void interpret(const Expr& expr, std::ostream& stream = std::cout) const {
    try{
      auto results{visit(expr)};
      for (const auto& result : results) {
        if (result.has_value()) {
          stream << to_string(result.value()) << '\n';
        }
      }
    } catch (const RuntimeError& error) {
      callback_(error);
    }
  }

private:
  [[nodiscard]] std::string to_string(const SelectResult& result) const {
    return std::visit([this](auto&& result) -> std::string {
      using R = std::decay_t<decltype(result)>;
      if constexpr (std::is_same_v<R, SelectDataResult>) {
        return datapointsToString<double>(result);
      } else if constexpr (std::is_same_v<R, SelectDoubleResult>) {
        return std::to_string(result);
      } else if constexpr (std::is_same_v<R, SelectCountResult>) {
        return std::to_string(result);
      }
    }, result);
  }

  [[nodiscard]] Results visit(const Expr& expr) const {
    try {
      Results results{};
      for (const auto& query : expr) {
        results.push_back(visit(query));
      }
      return results;
    } catch (const RuntimeError& e) {
      throw e;
    }
  }

  [[nodiscard]] Result visit(const Query& query) const {
    try {
      return std::visit([this](auto&& query) -> Result {
        using Q = std::decay_t<decltype(query)>;
        if constexpr (std::is_same_v<Q, SelectQuery>) {
          return visit(query);
        } else if constexpr (std::is_same_v<Q, PutQuery>) {
          visit(query);
          return std::nullopt;
        } else if constexpr (std::is_same_v<Q, DeleteQuery>) {
          visit(query);
          return std::nullopt;
        } else if constexpr (std::is_same_v<Q, CreateQuery>) {
          visit(query);
          return std::nullopt;
        } else if constexpr (std::is_same_v<Q, DropQuery>) {
          visit(query);
          return std::nullopt;
        } else if constexpr (std::is_same_v<Q, AddQuery>) {
          visit(query);
          return std::nullopt;
        } else if constexpr (std::is_same_v<Q, RemoveQuery>) {
          visit(query);
          return std::nullopt;
        }
      }, query);
    } catch (const RuntimeError& e) {
      throw e;
    }
  }

  static auto add_optional_tag_list(
    auto &query_builder,
    const std::optional<TagListExprResult>& tag_list
  ) noexcept {
    if (tag_list.has_value()) {
      for (const auto& [key, value] : tag_list.value()) {
        std::ignore = query_builder.whereTagsContain({key, value});
      }
    }
  }

  static SelectResult handle_select_type(
    auto &query_builder, SelectType type
  ) {
    return std::visit([&query_builder](auto&& type) -> SelectResult {
      using T = std::decay_t<decltype(type)>;
      try {
        if constexpr (std::is_same_v<T, SelectTypeDataExpr>) {
          return query_builder.execute();
        } else if constexpr (std::is_same_v<T, SelectTypeCountExpr>) {
          return query_builder.count();
        } else if constexpr (std::is_same_v<T, SelectTypeAvgExpr>) {
          return query_builder.avg();
        } else if constexpr (std::is_same_v<T, SelectTypeSumExpr>) {
          return query_builder.sum();
        } else if constexpr (std::is_same_v<T, SelectTypeMinExpr>) {
          return query_builder.min();
        } else if constexpr (std::is_same_v<T, SelectTypeMaxExpr>) {
          return query_builder.max();
        }
      } catch (const std::exception& e) {
      throw RuntimeError{type.token, e.what()};
      }
    }, type);
  }

  [[nodiscard]] SelectResult visit(const SelectQuery& query) const {
    try {
      auto type_result{visit(query.type)};
      auto metric_result{visit(query.metric)};
      auto table_name_result{visit(query.table_name)};
      auto& table{database_.getTable(table_name_result)};
      auto query_builder{table.query()
        .whereMetricIs(metric_result)
      };
      std::visit([this, &query_builder](auto&& select_clause) -> void {
        using C = std::decay_t<decltype(select_clause)>;
        if constexpr (std::is_same_v<C, AllClause>) {
          auto all_clause_result{visit(select_clause)};
          add_optional_tag_list(query_builder, all_clause_result);
        } else if constexpr (std::is_same_v<C, BetweenClause>) {
          auto between_clause_result{visit(select_clause)};
          std::ignore = query_builder.whereTimestampBetween(
            std::get<0>(between_clause_result),
            std::get<1>(between_clause_result)
          );
          add_optional_tag_list(
            query_builder,
            std::get<2>(between_clause_result)
          );
        } else if constexpr (std::is_same_v<C, AtClause>) {
          auto at_clause_result{visit(select_clause)};
          std::ignore = query_builder.whereTimestampIs(at_clause_result.first);
          add_optional_tag_list(query_builder, at_clause_result.second);
        }
      }, query.clause);
      return handle_select_type(query_builder, type_result);
    } catch (const RuntimeError& e) {
      throw e;
    }
}

  PutResult visit(const PutQuery& query) const {
    try {
      auto metric_result{visit(query.metric)};
      auto timestamp_result{visit(query.timestamp)};
      auto value_result{visit(query.value)};
      auto table_name_result{visit(query.table_name)};

      auto& table{database_.getTable(table_name_result)};
      if (!query.tag_list.has_value()) {
        table.query()
          .put(timestamp_result, metric_result, {}, value_result)
          .execute();
      }

      auto tag_list_result{visit(query.tag_list.value())};
      table.query()
        .put(timestamp_result, metric_result, tag_list_result, value_result)
        .execute();
    } catch (const std::exception& e) {
      throw RuntimeError{query.metric.token, e.what()};
    }
  }

  DeleteResult visit(const DeleteQuery& query) const {
    try {
      auto metric_result{visit(query.metric)};
      auto timestamp_result{visit(query.timestamp)};
      auto table_name_result{visit(query.table_name)};
      
      auto& table{database_.getTable(table_name_result)};
      if (!query.tag_list.has_value()) {
        table.query()
          .remove(timestamp_result, metric_result, {})
          .execute();
      }
      auto tag_list_result{visit(query.tag_list.value())};
      table.query()
        .remove(timestamp_result, metric_result, tag_list_result)
        .execute();
    } catch (const std::exception& e) {
      throw RuntimeError{query.metric.token, e.what()};
    }
  }

  CreateResult visit(const CreateQuery& query) const {
    try {
      auto table_name_result{visit(query.table_name)};
      database_.createTable(table_name_result);
      if (query.tag_columns.has_value()) {
        auto tag_columns_result{visit(query.tag_columns.value())};
        database_
          .getTable(table_name_result)
          .setTagColumns(tag_columns_result);
      }
    } catch (const std::exception& e) {
      throw RuntimeError{query.table_name.token, e.what()};
    }
  }

  DropResult visit(const DropQuery& query) const {
    try {
      auto table_name_result{visit(query.table_name)};
      database_.dropTable(table_name_result);
    } catch (const std::exception& e) {
      throw RuntimeError{query.table_name.token, e.what()};
    }
  }

  AddResult visit(const AddQuery& query) const {
    try {
      auto tag_columns_result{visit(query.tag_columns)};
      auto table_name_result{visit(query.table_name)};
      auto& table{database_.getTable(table_name_result)};
      for (const auto& tag_key : tag_columns_result) {
          table.addTagColumn(tag_key);
      }
    } catch (const std::exception& e) {
      throw RuntimeError{query.table_name.token, e.what()};
    }
  }

  RemoveResult visit(const RemoveQuery& query) const {
    try {
      auto tag_columns_result{visit(query.tag_columns)};
      auto table_name_result{visit(query.table_name)};
      auto& table{database_.getTable(table_name_result)};
      for (const auto& tag_key : tag_columns_result) {
          table.removeTagColumn(tag_key);
      }
    } catch (const std::exception& e) {
      throw RuntimeError{query.table_name.token, e.what()};
    }
  }

  [[nodiscard]] AllClauseResult visit(const AllClause& clause) const {
    AllClauseResult all_clause_result{};
    if (clause.where_clause.has_value()) {
      all_clause_result = visit(clause.where_clause.value());
    }
    return all_clause_result;
  }

  [[nodiscard]] BetweenClauseResult visit(const BetweenClause& clause) const {
    BetweenClauseResult between_clause_result{};
    try {
      std::get<0>(between_clause_result) = visit(clause.start);
      std::get<1>(between_clause_result) = visit(clause.end);
      if (clause.where_clause.has_value()) {
        std::get<2>(between_clause_result) = visit(clause.where_clause.value());
      }
      return between_clause_result;
    } catch (const std::exception& e) {
      throw RuntimeError{clause.start.token, e.what()};
    }
  }

  [[nodiscard]] AtClauseResult visit(const AtClause& clause) const {
    AtClauseResult at_clause_result{};
    try {
      at_clause_result.first = visit(clause.timestamp);
      if (clause.where_clause.has_value()) {
        at_clause_result.second = visit(clause.where_clause.value());
      }
      return at_clause_result;
    } catch (const std::exception& e) {
      throw RuntimeError{clause.timestamp.token, e.what()};
    }
  }

  [[nodiscard]] WhereClauseResult visit(const WhereClause& clause) const {
    return visit(clause.tag_list);
  }

  [[nodiscard]] SelectTypeResult visit(const SelectType& type) const {
    return type;
  }

  [[nodiscard]] MetricExprResult visit(const MetricExpr& metric) const {
    return metric.token.lexeme();
  }

  [[nodiscard]] TableNameExprResult visit(const TableNameExpr& table_name) const {
    return table_name.token.lexeme();
  }

  [[nodiscard]] TagKeyExprResult visit(const TagKeyExpr& tag_key) const {
    return tag_key.token.lexeme();
  }

  [[nodiscard]] TagValueExprResult visit(const TagValueExpr& tag_value) const {
    return tag_value.token.lexeme();
  }

  [[nodiscard]] TagExprResult visit(const TagExpr& tag) const {
    auto tag_key_result{visit(tag.key)};
    auto tag_value_result{visit(tag.value)};
    return {tag_key_result, tag_value_result};
  }

  [[nodiscard]] TagListExprResult visit(const TagListExpr& tag_list) const {
    TagListExprResult tag_list_result;
    for (const auto& tag : tag_list.tags) {
      tag_list_result.emplace(visit(tag));
    }
    return tag_list_result;
  }

  [[nodiscard]] TagColumnsExprResult visit(const TagColumnsExpr& tag_columns) const {
    TagColumnsExprResult tag_columns_result;
    for (const auto& tag_key : tag_columns.keys) {
      tag_columns_result.emplace(visit(tag_key));
    }
    return tag_columns_result;
  }

  [[nodiscard]] TimestampExprResult visit(const TimestampExpr& timestamp) const {
    try {
      return std::stoull(timestamp.token.lexeme());
    } catch (const std::exception& e) {
      throw RuntimeError{timestamp.token, "Invalid timestamp."};
    }
  }

  [[nodiscard]] ValueExprResult visit(const ValueExpr& value) const {
    try {
      return std::stod(value.token.lexeme());
    } catch (const std::exception& e) {
      throw RuntimeError{value.token, "Invalid value."};
    }
  }

  error_callback callback_;
  Database& database_;
};
}  // namespace vkdb

#endif // QUERY_INTERPRETER_H