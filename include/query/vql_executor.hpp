#ifndef VQL_EXECUTOR_HPP
#define VQL_EXECUTOR_HPP

#include "query/vql_parser.hpp"
#include "storage/lsm_tree.hpp"
#include <string>
#include <vector>

/**
 * @brief A VQL executor.
 *
 * @tparam TKey The type of the key.
 * @tparam TValue The type of the value.
 */
template <typename TKey, typename TValue>
class VQLExecutor {
public:
    /**
     * @brief Construct a new VQLExecutor object.
     *
     * @param lsm_tree The LSM tree.
     */
    VQLExecutor(LSMTree<TKey, TValue> &lsm_tree)
        : lsm_tree(lsm_tree) {}

    /**
     * @brief Execute a VQL query.
     *
     * @param query The VQL query.
     * @return `std::string` The result of the query.
     */
    std::string execute(const std::string &query);

private:
    LSMTree<TKey, TValue> &lsm_tree; // The LSM tree.

    /**
     * @brief Execute a SELECT query.
     *
     * @param query The parsed VQL query.
     * @return `std::string` The result of the query.
     */
    std::string executeSelect(const VQLQuery &query);

    /**
     * @brief Execute an INSERT query.
     *
     * @param query The parsed VQL query.
     * @return `std::string` The result of the query.
     */
    std::string executeInsert(const VQLQuery &query);

    /**
     * @brief Execute an UPDATE query.
     *
     * @param query The parsed VQL query.
     * @return `std::string` The result of the query.
     */
    std::string executeUpdate(const VQLQuery &query);

    /**
     * @brief Execute a DELETE query.
     *
     * @param query The parsed VQL query.
     * @return `std::string` The result of the query.
     */
    std::string executeDelete(const VQLQuery &query);
};

#endif // VQL_EXECUTOR_HPP