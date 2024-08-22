#ifndef VQL_PARSER_HPP
#define VQL_PARSER_HPP

#include <string>
#include <unordered_map>
#include <vector>

/**
 * @brief A type of VQL query.
 *
 */
enum class VQLQueryType {
    SELECT,
    INSERT,
    UPDATE,
    DELETE
};

/**
 * @brief A VQL query mapped to a struct.
 *
 */
struct VQLQuery {
    VQLQueryType type;                                   // The type of the query.
    std::string table;                                   // The table name.
    std::vector<std::string> columns;                    // The columns to select.
    std::unordered_map<std::string, std::string> values; // The values to insert or update.
    std::string condition;                               // The condition to filter rows.
};

/**
 * @brief A VQL parser.
 * 
 */
class VQLParser {
public:
    /**
     * @brief Parse a VQL query.
     * 
     * @param query The VQL query.
     * @return `VQLQuery` The parsed VQL query.
     */
    static VQLQuery parse(const std::string &query);

private:
    /**
     * @brief Parse a SELECT query.
     * 
     * @param tokens The tokens of the query.
     * @return `VQLQuery` The parsed VQL query.
     */
    static VQLQuery parseSelect(const std::vector<std::string> &tokens);

    /**
     * @brief Parse an INSERT query.
     * 
     * @param tokens The tokens of the query.
     * @return `VQLQuery` The parsed VQL query.
     */
    static VQLQuery parseInsert(const std::vector<std::string> &tokens);
    
    /**
     * @brief Parse an UPDATE query.
     * 
     * @param tokens The tokens of the query.
     * @return `VQLQuery` The parsed VQL query.
     */
    static VQLQuery parseUpdate(const std::vector<std::string> &tokens);
    
    /**
     * @brief Parse a DELETE query.
     * 
     * @param tokens The tokens of the query.
     * @return `VQLQuery` The parsed VQL query.
     */
    static VQLQuery parseDelete(const std::vector<std::string> &tokens);
    
    /**
     * @brief Tokenize a query.
     * 
     * @param query The query.
     * @return `std::vector<std::string>` The tokens of the query.
     */
    static std::vector<std::string> tokenize(const std::string &query);
};

#endif // VQL_PARSER_HPP