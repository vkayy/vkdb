#include "query/vql_parser.hpp"

#include <algorithm>
#include <sstream>
#include <stdexcept>

VQLQuery VQLParser::parse(const std::string &query) {
    std::vector<std::string> tokens = tokenize(query);

    if (tokens.empty()) {
        throw std::runtime_error("VQLParser::parse: Empty query");
    }

    if (tokens[0] == "SELECT") {
        return parseSelect(tokens);
    } else if (tokens[0] == "INSERT") {
        return parseInsert(tokens);
    } else if (tokens[0] == "UPDATE") {
        return parseUpdate(tokens);
    } else if (tokens[0] == "DELETE") {
        return parseDelete(tokens);
    } else {
        throw std::runtime_error("VQLParser::parse: Invalid query");
    }
}

std::vector<std::string> VQLParser::tokenize(const std::string &query) {
    std::istringstream iss(query);
    std::vector<std::string> tokens;
    std::string token;

    while (iss >> token) {
        tokens.push_back(token);
    }

    return tokens;
}