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

static const std::unordered_set<std::string> KEYWORDS{
  "SELECT", "PUT", "DELETE", "CREATE", "DROP", "ADD", "REMOVE",
  "TABLE", "TAGS", "ALL", "WHERE", "BETWEEN", "AND", "AT",
  "FROM", "TO", "DATA", "AVG", "SUM", "COUNT", "MIN", "MAX"
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
      return parse_select_clause(stream);
    } else if (command == "PUT") {
      return parse_put_clause(stream);
    } else if (command == "DELETE") {
      return parse_delete_clause(stream);
    } else if (command == "CREATE") {
      std::string sub_command;          
      stream >> sub_command;
      if (sub_command == "TABLE") {
        return parse_create_clause(stream);
      }
    } else if (command == "DROP") {
      std::string sub_command;
      stream >> sub_command;
      if (sub_command == "TABLE") {
        return parse_drop_clause(stream);
      }
    } else if (command == "ADD") {
      std::string sub_command;
      stream >> sub_command;
      if (sub_command == "TAGS") {
        return parse_add_clause(stream);
      }
    } else if (command == "REMOVE") {
      std::string tags_str;
      stream >> tags_str;
      if (tags_str == "TAGS") {
        return parse_remove_clause(stream);
      }
    }

    throw std::runtime_error{"Database::executeQuery(): Unknown query command."};
  }

  QueryResult executeFile(const FilePath& file_path) {
    std::ifstream file{file_path};
    if (!file.is_open()) {
      throw std::runtime_error{
        "Database::executeFile(): Unable to open file."
      };
    }

    std::string line;
    std::string result;
    while (std::getline(file, line)) {
      if (line.empty() || line[0] == '#') {
        continue;
      }
      auto query_result{executeQuery(line)};
      if (!query_result.empty()) {
        result += query_result + '\n';
      }
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

  static Tag parse_tag(const std::string& tag_str) {
    auto sep{tag_str.find('=')};
    if (sep == std::string::npos) {
      throw std::runtime_error{
        "Database::parse_tag(): Invalid tag equality."
      };
    }

    auto tag_key{tag_str.substr(0, sep)};
    auto tag_value{tag_str.substr(sep + 1)};
    if (!tag_value.empty() && tag_value.back() == ';') {
      tag_value.pop_back();
    }

    check_not_keyword(tag_key);
    check_not_keyword(tag_value);

    return {tag_key, tag_value};
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

  void parse_where_clause(std::istringstream& stream, auto& query) {
    std::string where_str;
    stream >> where_str;
    if (where_str != "WHERE" && !where_str.empty()) {
      throw std::runtime_error{
        "Database::parse_where_clause(): Missing WHERE keyword."
      };
    }
    if (where_str != "WHERE") {
      throw std::runtime_error{
        "Database::parse_where_clause(): Invalid WHERE syntax."
      };
    }

    auto tag_table{parse_tag_list_clause(stream, query)};
    for (const auto& [tag_key, tag_value] : tag_table) {
      std::ignore = query.whereTagsContain({tag_key, tag_value});
    }
  }

  static TagTable parse_tag_list_clause(
    std::istringstream& stream,
    auto& query,
    bool is_put = false
  ) {
    TagTable tag_table;
    std::string tag;
    while (stream >> tag) {
      auto [tag_key, tag_value] = parse_tag(tag);
      if (is_put) {
        check_valid_identifier(tag_key);
        check_valid_identifier(tag_value);
      }
      tag_table.emplace(tag_key, tag_value);
    }
    return tag_table;
  }

  static TagColumns parse_tag_columns_clause(std::istringstream& stream) {
    std::string tags_str;
    stream >> tags_str;
    check_match_or_empty(tags_str, "TAGS");

    TagColumns tags;
    std::string tag_key;
    while (stream >> tag_key) {
      if (tag_key.back() == ';') {
        tag_key.pop_back();
      }
      check_not_keyword(tag_key);
      tags.insert(tag_key);
    }

    return tags;
  }

  std::tuple<std::string, std::string, Table&> get_select_header(
    std::istringstream& stream
  ) {
    std::string select_type, metric, from_str, table_name;
    stream >> select_type >> metric >> from_str >> table_name;

    if (from_str != "FROM") {
      throw std::runtime_error{
        "Database::get_select_header(): Missing FROM keyword."
      };
    }

    auto& table{getTable(table_name)};
    return {select_type, metric, table};
  }

  [[nodiscard]] QueryResult parse_all_clause(
    std::istringstream& stream,
    auto& query,
    const std::string& select_type
  ) {
    parse_where_clause(stream, query);
    if (select_type == "DATA") {
      auto results{query.execute()};
      return datapointsToString<double>(results);
    }
    return handle_aggregate(query, select_type);
  }

  [[nodiscard]] QueryResult parse_between_clause(
    std::istringstream& stream,
    auto& query,
    const std::string& select_type
  ) {
    Timestamp start, end;
    std::string and_str;

    stream >> start >> and_str >> end;

    if (and_str != "AND") {
      throw std::runtime_error{
        "Database::parse_between_clause(): Invalid BETWEEN syntax."
      };
    }

    std::ignore = query.between(start, end);
    parse_where_clause(stream, query);

    if (select_type == "DATA") {
      auto results{query.execute()};
      return datapointsToString<double>(results);
    }

    return handle_aggregate(query, select_type);
  }

  [[nodiscard]] QueryResult parse_at_clause(
    std::istringstream& stream,
    auto& query,
    const std::string& select_type
  ) {
    std::string time_str;
    stream >> time_str;
    auto time{try_convert_to_timestamp(time_str)};
    std::ignore = query.whereTimestampIs(time);

    parse_where_clause(stream, query);

    if (select_type == "DATA") {
      auto results{query.execute()};
      return datapointsToString<double>(results);
    }

    return handle_aggregate(query, select_type);
  }

  [[nodiscard]] QueryResult parse_select_clause(std::istringstream& stream) {
    auto [select_type, metric, table] = get_select_header(stream);

    if (!SELECT_TYPES.contains(select_type)) {
      throw std::runtime_error{
        "Database::parse_select_clause(): Unknown SELECT type."
      };
    }

    auto query{table.query().whereMetricIs(metric)};

    std::string clause;
    stream >> clause;
    
    QueryResult result{};

    if (clause == "ALL" || clause == "ALL;") {
      return parse_all_clause(stream, query, select_type);
    }

    if (clause == "BETWEEN") {
      return parse_between_clause(stream, query, select_type);
    }

    if (clause == "AT") {
      return parse_at_clause(stream, query, select_type);
    }

    throw std::runtime_error{
      "Database::parse_select_clause(): Invalid SELECT syntax."
    };
  }

  [[nodiscard]] QueryResult parse_put_clause(std::istringstream& stream) {
    std::string metric, timestamp_str, value_str, into_str, table_name;

    stream >> metric >> timestamp_str >> value_str >> into_str >> table_name;
    if (into_str != "INTO") {
      throw std::runtime_error{
        "Database::parse_put_clause(): Invalid PUT syntax."
      };
    }

    check_valid_identifier(metric);
    auto timestamp{try_convert_to_timestamp(timestamp_str)};
    auto value{try_convert_to_double(value_str)};
    auto tag_table{parse_tag_list_clause(stream, table_name, true)};

    auto& table{getTable(table_name)};
    for (const auto& tag_column : table.tagColumns()) {
      if (!tag_table.contains(tag_column)) {
        throw std::runtime_error{
          "Database::parse_put_clause(): Missing tag column."
        };
      }
    }
    table.query().put(timestamp, metric, tag_table, value).execute();
    return format_put_output(timestamp, metric, tag_table, table_name, value);
  }

  [[nodiscard]] QueryResult parse_delete_clause(std::istringstream& stream) {
    std::string metric, timestamp_str, from_str, table_name;

    stream >> metric >> timestamp_str >> from_str >> table_name;
    if (from_str != "FROM") {
      throw std::runtime_error{
        "Database::parse_delete_clause(): Invalid DELETE syntax."
      };
    }

    check_valid_identifier(metric);
    auto timestamp{try_convert_to_timestamp(timestamp_str)};
    auto tag_table{parse_tag_list_clause(stream, table_name)};

    auto& table{getTable(table_name)};
    for (const auto& tag_column : table.tagColumns()) {
      if (!tag_table.contains(tag_column)) {
        throw std::runtime_error{
          "Database::parse_delete_clause(): Missing tag column."
        };
      }
    }

    table.query().remove(timestamp, metric, tag_table).execute();

    return format_delete_output(timestamp, metric, tag_table, table_name);
  }

  QueryResult parse_create_clause(std::istringstream& stream) {
    std::string table_name, tags_str;
    stream >> table_name;
    if (table_name.empty() || table_name == "TAGS") {
      throw std::runtime_error{
        "Database::parse_create_clause(): Missing table name."
      };
    }
    
    if (table_name.back() == ';') {
      table_name.pop_back();
    }
    check_not_keyword(table_name);

    createTable(table_name);
    auto tag_columns{parse_tag_columns_clause(stream)};
    if (!tag_columns.empty()) {
      getTable(table_name).setTagColumns(tag_columns);
    }
    
    return format_create_output(table_name, tag_columns);
  }

  QueryResult parse_drop_clause(std::istringstream& stream) {
    std::string table_name;
    stream >> table_name;
    if (table_name.empty()) {
      throw std::runtime_error{
        "Database::parse_drop_clause(): Missing table name."
      };
    }

    table_name.pop_back();
    check_not_keyword(table_name);

    dropTable(table_name);
    return "Dropped table " + table_name;
  }

  QueryResult parse_add_clause(std::istringstream& stream) {
    std::string table_name, to_str;
    std::vector<std::string> new_tags;

    std::string tag_key;
    while (stream >> tag_key && tag_key != "TO") {
      check_not_keyword(tag_key);
      check_valid_identifier(tag_key);
      new_tags.push_back(tag_key);
    }

    if (new_tags.empty()) {
      throw std::runtime_error{
        "Database::parse_add_clause(): Missing tags to add."
      };
    }

    to_str = tag_key;
    if (to_str.empty() || to_str != "TO") {
      throw std::runtime_error{
        "Database::parse_add_clause(): Invalid ADD TAGS syntax."
      };
    }

    stream >> table_name;
    if (table_name.empty()) {
      throw std::runtime_error{
        "Database::parse_add_clause(): Invalid ADD TAGS syntax."
      };
    }

    table_name.pop_back();
    check_not_keyword(table_name);

    auto& table{getTable(table_name)};
    for (const auto& tag : new_tags) {
      table.addTagColumn(tag);
    }

    return format_add_output(table_name, new_tags);
  }

  static QueryResult format_create_output(
    const TableName& table_name,
    const TagColumns& tag_columns
  ) {
    QueryResult result{"Created table " + table_name};
    if (tag_columns.empty()) {
      return result;
    }
    result += " with tag columns (";
    for (const auto& tag : tag_columns) {
      result += tag + ", ";
    }
    result.pop_back();
    result.pop_back();

    return result;
  }

  QueryResult parse_remove_clause(std::istringstream& stream) {
    std::string table_name, from_str;
    std::vector<TagKey> tags_to_remove;

    std::string tag_key;
    while (stream >> tag_key && tag_key != "FROM") {
      tags_to_remove.push_back(tag_key);
    }

    if (tags_to_remove.empty()) {
      throw std::runtime_error{
        "Database::parse_remove_clause(): Missing tags to remove."
      };
    }

    stream >> table_name;
    if (table_name.empty()) {
      throw std::runtime_error{
        "Database::parse_remove_clause(): Invalid REMOVE TAGS syntax."
      };
    }
    table_name.pop_back();
    
    auto& table{getTable(table_name)};
    for (const auto& tag : tags_to_remove) {
      table.removeTagColumn(tag);
    }

    return format_remove_output(table_name, tags_to_remove);
  }

  static QueryResult format_put_output(
    Timestamp timestamp,
    const Metric& metric,
    const TagTable& tag_table,
    const TableName& table_name,
    double value
  ) {
    QueryResult result{"Put key "};
    TimeSeriesKey key{timestamp, metric, tag_table};
    result += key.toString() + " into table " + table_name;
    result += " with value " + std::to_string(value);
    return result;
  }

  static QueryResult format_delete_output(
    Timestamp timestamp,
    const Metric& metric,
    const TagTable& tag_table,
    const TableName& table_name
  ) {
    QueryResult result{"Deleted key "};
    TimeSeriesKey key{timestamp, metric, tag_table};
    result += key.toString() + " from table: " + table_name;
    return result;
  }

  static QueryResult format_add_output(
    const TableName& table_name,
    const std::vector<TagKey>& new_tags
  ) {
    QueryResult result{"Added tags ("};
    for (const auto& tag : new_tags) {
      result += tag + ", ";
    }
    result.pop_back();
    result.pop_back();
    result += ") ";

    return result += "to table " + table_name;
  }

  static QueryResult format_remove_output(
    const TableName& table_name,
    const std::vector<TagKey>& tags_to_remove
  ) {
    QueryResult result{"Removed tags ("};
    for (const auto& tag : tags_to_remove) {
      result += tag + ", ";
    }
    result.pop_back();
    result.pop_back();
    result += ") ";

    return result += "from table " + table_name;
  }

  static void check_not_keyword(const std::string& str) {
    if (KEYWORDS.contains(str)) {
      throw std::runtime_error{
        "Database::check_not_keyword(): Invalid keyword."
      };
    }
  }

  static void check_match_or_empty(
    const std::string& str,
    const std::string& to_match
  ) {
    if (!str.empty() && str != to_match) {
      throw std::runtime_error{
        "Database::check_match_or_empty(): Invalid syntax."
      };
    }
  }

  static double try_convert_to_double(const std::string& str) {
    char *end;
    auto number{std::strtod(str.c_str(), &end)};
    if (*end != '\0') {
      throw std::runtime_error{
        "Database::try_convert_to_number(): Not a number."
      };
    }
    return number;
  }

  static Timestamp try_convert_to_timestamp(const std::string& str) {
    char *end;
    auto number{std::strtoull(str.c_str(), &end, 10)};
    if (*end != '\0') {
      throw std::runtime_error{
        "Database::try_convert_to_number(): Not a number."
      };
    }
    return number;
  }

  static void check_valid_identifier(const std::string& str) {
    if (str.empty() || std::isdigit(str[0])) {
      throw std::runtime_error{
        "Database::check_valid_identifier(): Invalid name."
      };
    }
    for (const auto& c : str) {
      if (!std::isalnum(c)) {
        throw std::runtime_error{
          "Database::check_valid_identifier(): Invalid name."
        };
      }
    }
  }                          

  std::unordered_map<TableName, Table> tables_;
  DatabaseName name_;
};
}  // namespace vkdb

#endif // DATABASE_DATABASE_H