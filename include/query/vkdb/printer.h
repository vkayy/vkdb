#ifndef QUERY_PRINTER_H
#define QUERY_PRINTER_H

#include <vkdb/expr.h>
#include <sstream>
#include <string>
#include <variant>

namespace vkdb {
class Printer {
public:
  [[nodiscard]] std::string print(const Expr& expr) noexcept;

private:
  void visit(const Query& query) noexcept;
  void visit(const SelectQuery& query) noexcept;
  void visit(const PutQuery& query) noexcept;
  void visit(const DeleteQuery& query) noexcept;
  void visit(const CreateQuery& query) noexcept;
  void visit(const DropQuery& query) noexcept;
  void visit(const AddQuery& query) noexcept;
  void visit(const RemoveQuery& query) noexcept;
  void visit(const TablesQuery& query) noexcept;
  void visit(const AllClause& clause) noexcept;
  void visit(const BetweenClause& clause) noexcept;
  void visit(const AtClause& clause) noexcept;
  void visit(const WhereClause& clause) noexcept;
  void visit(const SelectType& type) noexcept;
  void visit(const MetricExpr& metric) noexcept;
  void visit(const TableNameExpr& table_name) noexcept;
  void visit(const TagKeyExpr& tag_key) noexcept;
  void visit(const TagValueExpr& tag_value) noexcept;
  void visit(const TagExpr& tag) noexcept;
  void visit(const TagListExpr& tag_list) noexcept;
  void visit(const TagColumnsExpr& tag_columns) noexcept;
  void visit(const TimestampExpr& timestamp) noexcept;
  void visit(const ValueExpr& value) noexcept;

  std::stringstream output_;
};
}  // namespace vkdb

#endif // QUERY_PRINTER_H
