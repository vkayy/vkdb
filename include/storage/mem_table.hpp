#ifndef MEM_TABLE_HPP
#define MEM_TABLE_HPP

#include "skip_list.hpp"
#include <cstdint>
#include <fstream>
#include <map>

/**
 * @brief A memory table (MemTable) implementation with a skip list as the underlying data structure.
 *
 * This class provides key-value storage with logarithmic insert and lookup operations, leveraging
 * the lock-free skip list for concurrent access.
 *
 * @tparam TKey The type of the key.
 * @tparam TValue The type of the value.
 */
template <typename TKey, typename TValue>
class MemTable {
private:
    SkipList<TKey, TValue> table; // The skip list used as the underlying data structure.

public:
    /**
     * @brief Insert a key-value pair into the MemTable.
     *
     * @param key The key associated with the value to insert.
     * @param value The value associated with the key to insert.
     */
    void put(const TKey &key, const TValue &value) {
        table.insert(key, value);
    }

    /**
     * @brief Retrieves the value associated with the given key.
     *
     * @param key The key to search for.
     * @return `TValue` The value associated with the key.
     * @throws `std::runtime_error` if the key is not found.
     */
    TValue get(const TKey &key) const {
        TValue *value = table.findWaitFree(key);
        if (!value) {
            throw std::runtime_error("key not found in memtable");
        }

        return *value;
    }

    /**
     * @brief Serialize the current state of the MemTable to a file.
     *
     * The MemTable's contents are saved to a file in binary format, providing
     * later restoration via deserialization.
     *
     * @param filename The name of the file to serialize into.
     * @throws `std::runtime_error` if the file cannot be opened for writing.
     */
    void serialize(const std::string &filename) const {
        table.serialize(filename);
    }

    /**
     * @brief Deserialize a file into current MemTable state.
     *
     * The MemTable is populated with data from the specified file, restoring
     * the serialized state.
     *
     * @param filename The name of the file to deserialize from.
     * @throws `std::runtime_error` if the file cannot be opened for reading.
     */
    void deserialize(const std::string &filename) {
        table.deserialize(filename);
    }
};

#endif // MEM_TABLE_HPP
