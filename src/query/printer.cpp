#include <vkdb/printer.h>

namespace vkdb {
std::string Printer::print(const Expr& expr) noexcept {
  for (const auto& query : expr) {
    visit(query);
  }
  return output_.str();
}

void Printer::visit(const Query& query) noexcept {
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
  }, query);
  output_ << ";";
}

void Printer::visit(const SelectQuery& query) noexcept {
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

void Printer::visit(const PutQuery& query) noexcept {
  output_ << "PUT ";
  visit(query.metric);
  output_ << " ";
  visit(query.timestamp);
  output_ << " ";
  visit(query.value);
  output_ << " INTO ";
  visit(query.table_name);
  if (query.tag_list.has_value()) {
    output_ << " TAGS ";
    visit(query.tag_list.value());
  }
}

void Printer::visit(const DeleteQuery& query) noexcept {
  output_ << "DELETE ";
  visit(query.metric);
  output_ << " ";
  visit(query.timestamp);
  output_ << " FROM ";
  visit(query.table_name);
  if (query.tag_list.has_value()) {
    output_ << " TAGS ";
    visit(query.tag_list.value());
  }
}

void Printer::visit(const CreateQuery& query) noexcept {
  output_ << "CREATE TABLE ";
  visit(query.table_name);
  if (query.tag_columns.has_value()) {
    output_ << " TAGS ";
    visit(query.tag_columns.value());
  }
}

void Printer::visit(const DropQuery& query) noexcept {
  output_ << "DROP TABLE ";
  visit(query.table_name);
}

void Printer::visit(const AddQuery& query) noexcept {
  output_ << "ADD TAGS ";
  visit(query.tag_columns);
  output_ << " TO ";
  visit(query.table_name);
}

void Printer::visit(const RemoveQuery& query) noexcept {
  output_ << "REMOVE TAGS ";
  visit(query.tag_columns);
  output_ << " FROM ";
  visit(query.table_name);
}

void Printer::visit(const AllClause& clause) noexcept {
  output_ << "ALL";
  if (clause.where_clause) {
    output_ << " ";
    visit(clause.where_clause.value());
  }
}

void Printer::visit(const BetweenClause& clause) noexcept {
  output_ << "BETWEEN ";
  visit(clause.start);
  output_ << " AND ";
  visit(clause.end);
  if (clause.where_clause) {
    output_ << " ";
    visit(clause.where_clause.value());
  }
}

void Printer::visit(const AtClause& clause) noexcept {
  output_ << "AT ";
  visit(clause.timestamp);
  if (clause.where_clause) {
    output_ << " ";
    visit(clause.where_clause.value());
  }
}

void Printer::visit(const WhereClause& clause) noexcept {
  output_ << "WHERE ";
  visit(clause.tag_list);
}

void Printer::visit(const SelectType& type) noexcept {
  std::visit([this](auto&& type) {
    using T = std::decay_t<decltype(type)>;
    if constexpr (std::is_same_v<T, SelectTypeDataExpr>) {
      output_ << "DATA";
    } else if constexpr (std::is_same_v<T, SelectTypeCountExpr>) {
      output_ << "COUNT";
    } else if constexpr (std::is_same_v<T, SelectTypeAvgExpr>) {
      output_ << "AVG";
    } else if constexpr (std::is_same_v<T, SelectTypeSumExpr>) {
      output_ << "SUM";
    } else if constexpr (std::is_same_v<T, SelectTypeMinExpr>) {
      output_ << "MIN";
    } else if constexpr (std::is_same_v<T, SelectTypeMaxExpr>) {
      output_ << "MAX";
    }
  }, type);
}

void Printer::visit(const MetricExpr& metric) noexcept {
  output_ << metric.token.lexeme();
}

void Printer::visit(const TableNameExpr& table_name) noexcept {
  output_ << table_name.token.lexeme();
}

void Printer::visit(const TagKeyExpr& tag_key) noexcept {
  output_ << tag_key.token.lexeme();
}

void Printer::visit(const TagValueExpr& tag_value) noexcept {
  output_ << tag_value.token.lexeme();
}

void Printer::visit(const TagExpr& tag) noexcept {
  visit(tag.key);
  output_ << "=";
  visit(tag.value);
}

void Printer::visit(const TagListExpr& tag_list) noexcept {
  auto first{true};
  for (const auto& tag : tag_list.tags) {
    if (!first) {
      output_ << ", ";
    }
    visit(tag);
    first = false;
  }
}

void Printer::visit(const TagColumnsExpr& tag_columns) noexcept {
  auto first{true};
  for (const auto& tag_key : tag_columns.keys) {
    if (!first) {
      output_ << ", ";
    }
    visit(tag_key);
    first = false;
  }
}

void Printer::visit(const TimestampExpr& timestamp) noexcept {
  output_ << timestamp.token.lexeme();
}

void Printer::visit(const ValueExpr& value) noexcept {
  output_ << value.token.lexeme();
}
}  // namespace vkdb
