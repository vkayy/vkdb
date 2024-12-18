#ifndef QUERY_PRINTER_H
#define QUERY_PRINTER_H

#include <vkdb/expr.h>
#include <sstream>
#include <string>
#include <variant>

namespace vkdb {
class Printer {
public:
  [[nodiscard]] std::string print(const Expr& expr) noexcept {
    std::visit([this](auto&& query) {
      using Q = std::decay_t<decltype(query)>;
      if constexpr (std::is_same_v<Q, SelectQuery>) {
        visit(query);
      } else if constexpr (std::is_same_v<Q, PutQuery>) {
        visit(query);
      } else if constexpr (std::is_same_v<Q, DeleteQuery>) {
        visit(query);
      } else if constexpr (std::is_same_v<Q, CreateQuery>) {
        visit(query);
      } else if constexpr (std::is_same_v<Q, DropQuery>) {
        visit(query);
      } else if constexpr (std::is_same_v<Q, AddQuery>) {
        visit(query);
      } else if constexpr (std::is_same_v<Q, RemoveQuery>) {
        visit(query);
      }
    }, expr);
    output_ << ";";
    return output_.str();
  }

private:
  void visit(const SelectQuery& query) noexcept {
    output_ << "SELECT ";
    visit(query.type);
    output_ << " ";
    visit(query.metric);
    output_ << " FROM ";
    visit(query.table_name);
    output_ << " ";
    std::visit([this](auto&& select_clause) {
      using C = std::decay_t<decltype(select_clause)>;
      if constexpr (std::is_same_v<C, AllClause>) {
        visit(select_clause);
      } else if constexpr (std::is_same_v<C, BetweenClause>) {
        visit(select_clause);
      } else if constexpr (std::is_same_v<C, AtClause>) {
        visit(select_clause);
      }
    }, query.clause);
  }

  void visit(const PutQuery& query) noexcept {
    output_ << "PUT ";
    visit(query.metric);
    visit(query.timestamp);
    visit(query.value);
    output_ << " INTO ";
    visit(query.table_name);
    if (query.tag_list.has_value()) {
      output_ << " TAGS ";
      visit(query.tag_list.value());
    }
  }

  void visit(const DeleteQuery& query) noexcept {
    output_ << "DELETE ";
    visit(query.metric);
    output_ << " AT ";
    visit(query.timestamp);
    output_ << " FROM ";
    visit(query.table_name);
    if (query.tag_list.has_value()) {
      output_ << " TAGS ";
      visit(query.tag_list.value());
    }
  }

  void visit(const CreateQuery& query) noexcept {
    output_ << "CREATE TABLE ";
    visit(query.table_name);
    if (query.tag_columns.has_value()) {
      output_ << " TAGS ";
      visit(query.tag_columns.value());
    }
  }

  void visit(const DropQuery& query) noexcept {
    output_ << "DROP TABLE ";
    visit(query.table_name);
  }

  void visit(const AddQuery& query) noexcept {
    output_ << "ADD TAGS ";
    visit(query.tag_columns);
    output_ << " TO ";
    visit(query.table_name);
  }

  void visit(const RemoveQuery& query) noexcept {
    output_ << "REMOVE TAGS ";
    visit(query.tag_columns);
    output_ << " FROM ";
    visit(query.table_name);
  }

  void visit(const AllClause& clause) noexcept {
    output_ << "ALL";
    if (clause.where_clause) {
      output_ << " ";
      visit(clause.where_clause.value());
    }
  }

  void visit(const BetweenClause& clause) noexcept {
    output_ << "BETWEEN ";
    visit(clause.start);
    output_ << " AND ";
    visit(clause.end);
    if (clause.where_clause) {
      output_ << " ";
      visit(clause.where_clause.value());
    }
  }

  void visit(const AtClause& clause) noexcept {
    output_ << "AT ";
    visit(clause.timestamp);
    if (clause.where_clause) {
      output_ << " ";
      visit(clause.where_clause.value());
    }
  }

  void visit(const WhereClause& clause) noexcept {
    output_ << "WHERE ";
    visit(clause.tag_list);
  }

  void visit(const SelectType& type) noexcept {
    switch (type) {
    case SelectType::DATA:
      output_ << "DATA";
      break;
    case SelectType::AVG:
      output_ << "AVG";
      break;
    case SelectType::SUM:
      output_ << "SUM";
      break;
    case SelectType::COUNT:
      output_ << "COUNT";
      break;
    case SelectType::MIN:
      output_ << "MIN";
      break;
    case SelectType::MAX:
      output_ << "MAX";
      break;
    }
  }

  void visit(const Metric& metric) noexcept {
    output_ << metric.identifier;
  }

  void visit(const TableName& table_name) noexcept {
    output_ << table_name.identifier;
  }

  void visit(const TagKey& tag_key) noexcept {
    output_ << tag_key.identifier;
  }

  void visit(const TagValue& tag_value) noexcept {
    output_ << tag_value.identifier;
  }

  void visit(const Tag& tag) noexcept {
    visit(tag.key);
    output_ << "=";
    visit(tag.value);
  }

  void visit(const TagList& tag_list) noexcept {
    auto first{true};
    for (const auto& tag : tag_list.tags) {
      if (!first) {
        output_ << ", ";
      }
      visit(tag);
      first = false;
    }
  }

  void visit(const TagColumns& tag_columns) noexcept {
    auto first{true};
    for (const auto& tag_key : tag_columns.keys) {
      if (!first) {
        output_ << ", ";
      }
      visit(tag_key);
      first = false;
    }
  }

  void visit(const Timestamp& timestamp) noexcept {
    output_ << timestamp.number;
  }

  void visit(const Value& value) noexcept {
    output_ << value.number;
  }

  std::stringstream output_;
};
}  // namespace vkdb

#endif // QUERY_PRINTER_H