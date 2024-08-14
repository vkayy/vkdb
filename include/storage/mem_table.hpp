#ifndef MEM_TABLE_HPP
#define MEM_TABLE_HPP

#include "skip_list.hpp"
#include <cstdint>
#include <fstream>
#include <map>
#include <optional>

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
     * @brief Add a key-value pair to the MemTable.
     *
     * @param key The key associated with the value to add.
     * @param value The value associated with the key to add.
     *
     * @throws `std::runtime_error` if the value is a tmobstone.
     */
    void put(const TKey &key, const std::optional<TValue> &value) {
        if (!value.has_value()) {
            throw std::runtime_error("cannot insert a tombstone value");
        }
        table.insert(key, value);
    }

    /**
     * @brief Retrieves the value associated with the given key.
     *
     * @param key The key to search for.
     * @return `TValue` The value associated with the key.
     * @throws `std::runtime_error` if the key is not found or is marked deleted.
     */
    TValue get(const TKey &key) const {
        std::optional<TValue> *value = table.findWaitFree(key);
        if (!value || !value->has_value()) {
            throw std::runtime_error("key not found in memtable");
        }

        return value->value();
    }

    /**
     * @brief Marks a key as deleted by inserting a tombstone.
     *
     * @param key The key to delete.
     * @throws `std::runtime_error` if the key is not found or is marked deleted.
     */
    void remove(const TKey &key) {
        std::optional<TValue> *value = table.findWaitFree(key);
        if (!value || !value->has_value()) {
            throw std::runtime_error("key not found in memtable");
        }

        table.insert(key, std::nullopt);
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
