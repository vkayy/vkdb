#ifndef DATABASE_DATABASE_H
#define DATABASE_DATABASE_H

#include <vkdb/table.h>

namespace vkdb {
static const FilePath DATABASE_DIRECTORY{""};

using DatabaseName = std::string;
using QueryResult = std::string;

static const std::unordered_set<std::string> AGGREGATIONS{
  "AVG", "SUM", "COUNT", "MIN", "MAX"
};

static const std::unordered_set<std::string> SELECT_TYPES{
  "DATA", "AVG", "SUM", "COUNT", "MIN", "MAX"
};

class Database {
public:
  Database() = delete;

  explicit Database(DatabaseName name)
    : name_{std::move(name)} {
    load();
  }

  Database(Database&&) noexcept = default;
  Database& operator=(Database&&) noexcept = default;

  Database(const Database&) = delete;
  Database& operator=(const Database&) = delete;

  ~Database() = default;

  void createTable(const TableName& table_name) {
    if (tables_.contains(table_name)) {
      throw std::runtime_error{
        "Database::createTable(): Table already exists."
      };
    }
    tables_.emplace(table_name, Table{path(), table_name});
    std::filesystem::create_directories(tables_.at(table_name).path());
  }

  [[nodiscard]] Table& getTable(const TableName& table_name) {
    if (!tables_.contains(table_name)) {
      throw std::runtime_error{
        "Database::getTable(): Table does not exist."
      };
    }
    return tables_.at(table_name);
  }

  void dropTable(const TableName& table_name) {
    if (!tables_.contains(table_name)) {
      throw std::runtime_error{
        "Database::dropTable(): Table does not exist."
      };
    }
    std::filesystem::remove_all(tables_.at(table_name).path());
    tables_.erase(table_name);
  }

  QueryResult executeQuery(const std::string& query) {
    std::istringstream stream{query};
    std::string command;
    stream >> command;

    if (command == "SELECT") {
      return handle_select(stream);
    } else if (command == "PUT") {
      return handle_put(stream);
    } else if (command == "DELETE") {
      return handle_delete(stream);
    } else {
      throw std::runtime_error{
        "Database::executeQuery(): Unknown query command."
      };
    }
  }

  QueryResult executeFile(const FilePath& file_path) {
    std::ifstream file{file_path};
    if (!file.is_open()) {
      throw std::runtime_error{
        "Database::executeFile(): Unable to open file."
      };
    }

    std::string query;
    std::string result;
    while (std::getline(file, query)) {
      result += executeQuery(query) + '\n';
    }

    return result;
  }
  
  void clear() {
    std::filesystem::remove_all(path());
  }

  [[nodiscard]] DatabaseName name() const noexcept {
    return name_;
  }

  [[nodiscard]] FilePath path() const noexcept {
    return DATABASE_DIRECTORY / name_;
  }

private:
  void load() {
    const auto db_path{path()};
    if (!std::filesystem::exists(db_path)) {
      std::filesystem::create_directories(db_path);
      return;
    }

    for (const auto& entry : std::filesystem::directory_iterator(db_path)) {
      if (entry.is_directory()) {
        auto table_name{entry.path().filename().string()};
        tables_.emplace(table_name, Table{path(), table_name});
      }
    }
  }

  [[nodiscard]] Tag split_tag_string(const std::string& tag_str) {
    auto sep{tag_str.find('=')};
    if (sep == std::string::npos) {
      throw std::runtime_error{
        "Database::split_tag_string(): Invalid tag string."
      };
    }

    auto tag{tag_str.substr(0, sep)};
    auto value{tag_str.substr(sep + 1)};
    if (!value.empty() && value.back() == ';') {
      value.pop_back();
    }

    return {tag, value};
  }

  [[nodiscard]] QueryResult handle_aggregate(
    auto& query,
    const std::string& agg
  ) {
    if (agg == "AVG") {
      return std::to_string(query.avg());
    }
    if (agg == "SUM") {
      return std::to_string(query.sum());
    }
    if (agg == "COUNT") {
      return std::to_string(query.count());
    }
    if (agg == "MIN") {
      return std::to_string(query.min());
    }
    if (agg == "MAX") {
      return std::to_string(query.max());
    }
    throw std::runtime_error{
      "Database::handle_aggregate(): Unknown aggregation."
    };
  }

  void parse_tags(std::istringstream& stream, auto& query) {
    std::string where;
    if (stream >> where && where == "WHERE") {
      std::string tag_condition;
      while (stream >> tag_condition) {
        auto [tag, value] = split_tag_string(tag_condition);
        std::ignore = query.whereTagsContain({tag, value});
      }
    }
  }

  [[nodiscard]] std::tuple<std::string, std::string, Table&> parse_select(
    std::istringstream& stream
  ) {
    std::string select_type, metric, from_str, table_name;
    stream >> select_type >> metric >> from_str >> table_name;

    if (from_str != "FROM") {
      throw std::runtime_error{
        "Database::parse_select(): Missing FROM keyword."
      };
    }

    auto& table{getTable(table_name)};
    return {select_type, metric, table};
  }

  [[nodiscard]] QueryResult handle_all_clause(
    std::istringstream& stream,
    auto& query,
    const std::string& select_type
  ) {
    parse_tags(stream, query);

    if (select_type == "DATA") {
      auto results = query.execute();
      return datapointsToString<double>(results);
    }

    return handle_aggregate(query, select_type);
  }

  [[nodiscard]] QueryResult handle_between_clause(
    std::istringstream& stream,
    auto& query,
    const std::string& select_type
  ) {
    Timestamp start, end;
    std::string and_str;

    stream >> start >> and_str >> end;

    if (and_str != "AND") {
      throw std::runtime_error{
        "Database::handle_between_clause(): Invalid BETWEEN syntax."
      };
    }

    std::ignore = query.between(start, end);
    parse_tags(stream, query);

    if (select_type == "DATA") {
      auto results = query.execute();
      return datapointsToString<double>(results);
    }

    return handle_aggregate(query, select_type);
  }

  [[nodiscard]] QueryResult handle_at_clause(
    std::istringstream& stream,
    auto& query,
    const std::string& select_type
  ) {
    Timestamp at_time;
    stream >> at_time;

    std::ignore = query.whereTimestampIs(at_time);
    parse_tags(stream, query);

    if (select_type == "DATA") {
      auto results = query.execute();
      return datapointsToString<double>(results);
    }

    return handle_aggregate(query, select_type);
  }

  [[nodiscard]] QueryResult handle_select(std::istringstream& stream) {
    auto [select_type, metric, table] = parse_select(stream);

    if (!SELECT_TYPES.contains(select_type)) {
      throw std::runtime_error{
        "Database::handle_select(): Unknown SELECT type."
      };
    }

    auto query{table.query().whereMetricIs(metric)};

    std::string clause;
    stream >> clause;
    
    if (clause == "ALL") {
      return handle_all_clause(stream, query, select_type);
    }

    if (clause == "BETWEEN") {
      return handle_between_clause(stream, query, select_type);
    }

    if (clause == "AT") {
      return handle_at_clause(stream, query, select_type);
    }

    throw std::runtime_error{
      "Database::handle_select(): Invalid SELECT syntax."
    };
  }

  [[nodiscard]] QueryResult handle_put(std::istringstream& stream) {
    std::string metric, into_str, table_name, tag_value;
    Timestamp timestamp;
    double value;

    stream >> metric >> timestamp >> value >> into_str >> table_name;
    if (into_str != "INTO") {
      throw std::runtime_error{
        "Database::handle_put(): Invalid PUT syntax."
      };
    }

    TagTable tag_table;
    while (stream >> tag_value) {
      auto [tag, val] = split_tag_string(tag_value);
      tag_table[tag] = val;
    }

    auto& table = getTable(table_name);
    table.query().put(timestamp, metric, tag_table, value).execute();

    return {};
  }

  [[nodiscard]] QueryResult handle_delete(std::istringstream& stream) {
    std::string metric, from_str, table_name, tag;
    Timestamp timestamp;

    stream >> metric >> timestamp >> from_str >> table_name;
    if (from_str != "FROM") {
      throw std::runtime_error{
        "Database::handle_delete(): Invalid DELETE syntax."
      };
    }

    TagTable tag_table;
    while (stream >> tag) {
      auto [tag_key, tag_value] = split_tag_string(tag);
      tag_table[tag_key] = tag_value;
    }

    auto& table = getTable(table_name);
    table.query().remove(timestamp, metric, tag_table).execute();

    return {};
  }

  std::unordered_map<TableName, Table> tables_;
  DatabaseName name_;
};
}  // namespace vkdb

#endif // DATABASE_DATABASE_H