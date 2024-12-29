#ifndef QUERY_PRINTER_H
#define QUERY_PRINTER_H

#include <vkdb/expr.h>
#include <sstream>
#include <string>
#include <variant>

namespace vkdb {
/**
 * @brief Printer for vq.
 * 
 */
class Printer {
public:
  /**
   * @brief Prints the given expression.
   * 
   * @param expr The expression to print.
   * @return A string representation of the expression.
   */
  [[nodiscard]] std::string print(const Expr& expr) noexcept;

private:
  /**
   * @brief Visits a query.
   * 
   * @param query The query to visit.
   */
  void visit(const Query& query) noexcept;

  /**
   * @brief Visits a select query.
   * 
   * @param query The select query to visit.
   */
  void visit(const SelectQuery& query) noexcept;

  /**
   * @brief Visits a put query.
   * 
   * @param query The put query to visit.
   */
  void visit(const PutQuery& query) noexcept;

  /**
   * @brief Visits a delete query.
   * 
   * @param query The delete query to visit.
   */
  void visit(const DeleteQuery& query) noexcept;

  /**
   * @brief Visits a create query.
   * 
   * @param query The create query to visit.
   */
  void visit(const CreateQuery& query) noexcept;

  /**
   * @brief Visits a drop query.
   * 
   * @param query The drop query to visit.
   */
  void visit(const DropQuery& query) noexcept;

  /**
   * @brief Visits an add query.
   * 
   * @param query The add query to visit.
   */
  void visit(const AddQuery& query) noexcept;

  /**
   * @brief Visits a remove query.
   * 
   * @param query The remove query to visit.
   */
  void visit(const RemoveQuery& query) noexcept;

  /**
   * @brief Visits a tables query.
   * 
   * @param query The tables query to visit.
   */
  void visit(const TablesQuery& query) noexcept;

  /**
   * @brief Visits an all clause.
   * 
   * @param clause The all clause to visit.
   */
  void visit(const AllClause& clause) noexcept;

  /**
   * @brief Visits a between clause.
   * 
   * @param clause The between clause to visit.
   */
  void visit(const BetweenClause& clause) noexcept;

  /**
   * @brief Visits an at clause.
   * 
   * @param clause The at clause to visit.
   */
  void visit(const AtClause& clause) noexcept;

  /**
   * @brief Visits a where clause.
   * 
   * @param clause The where clause to visit.
   */
  void visit(const WhereClause& clause) noexcept;

  /**
   * @brief Visits a select type.
   * 
   * @param type The select type to visit.
   */
  void visit(const SelectType& type) noexcept;

  /**
   * @brief Visits a metric expression.
   * 
   * @param metric The metric expression to visit.
   */
  void visit(const MetricExpr& metric) noexcept;

  /**
   * @brief Visits a table name expression.
   * 
   * @param table_name The table name expression to visit.
   */
  void visit(const TableNameExpr& table_name) noexcept;

  /**
   * @brief Visits a tag key expression.
   * 
   * @param tag_key The tag key expression to visit.
   */
  void visit(const TagKeyExpr& tag_key) noexcept;

  /**
   * @brief Visits a tag value expression.
   * 
   * @param tag_value The tag value expression to visit.
   */
  void visit(const TagValueExpr& tag_value) noexcept;

  /**
   * @brief Visits a tag expression.
   * 
   * @param tag The tag expression to visit.
   */
  void visit(const TagExpr& tag) noexcept;

  /**
   * @brief Visits a tag list expression.
   * 
   * @param tag_list The tag list expression to visit.
   */
  void visit(const TagListExpr& tag_list) noexcept;

  /**
   * @brief Visits a tag columns expression.
   * 
   * @param tag_columns The tag columns expression to visit.
   */
  void visit(const TagColumnsExpr& tag_columns) noexcept;

  /**
   * @brief Visits a timestamp expression.
   * 
   * @param timestamp The timestamp expression to visit.
   */
  void visit(const TimestampExpr& timestamp) noexcept;

  /**
   * @brief Visits a value expression.
   * 
   * @param value The value expression to visit.
   */
  void visit(const ValueExpr& value) noexcept;

  /**
   * @brief The output stream.
   * 
   */
  std::stringstream output_;
};
}  // namespace vkdb

#endif // QUERY_PRINTER_H
