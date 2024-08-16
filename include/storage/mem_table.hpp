#ifndef MEM_TABLE_HPP
#define MEM_TABLE_HPP

#include "skip_list.hpp"
#include <atomic>
#include <cstdint>
#include <fstream>
#include <limits>
#include <optional>

#include <atomic>
#include <mutex>
#include <string>

/**
 * @brief A non-atomic key range class for managing the minimum and maximum keys.
 *
 * This class assumes external synchronization (e.g., through a lock on the MemTable).
 *
 * @tparam TKey The type of the key.
 */
template <typename TKey>
class KeyRange {
private:
    TKey min_key;  // The minimum key.
    TKey max_key;  // The maximum key.
    bool keys_set; // Flag to indicate if the keys are set.

public:
    /**
     * @brief Construct a new KeyRange object.
     */
    KeyRange()
        : keys_set(false) {}

    /**
     * @brief Update the key range with the given key.
     *
     * @param key The key to update the range with.
     */
    void updateKeyRange(const TKey &key) {
        if (!keys_set) {
            min_key = max_key = key;
            keys_set = true;
        } else {
            if (key < min_key) {
                min_key = key;
            }
            if (key > max_key) {
                max_key = key;
            }
        }
    }

    /**
     * @brief Get the minimum key.
     *
     * @return TKey The minimum key.
     * @throws `std::runtime_error` if the keys are not set.
     */
    TKey getMinKey() const {
        if (!keys_set) {
            throw std::runtime_error("keys not set");
        }
        return min_key;
    }

    /**
     * @brief Get the maximum key.
     *
     * @return TKey The maximum key.
     * @throws `std::runtime_error` if the keys are not set.
     */
    TKey getMaxKey() const {
        if (!keys_set) {
            throw std::runtime_error("keys not set");
        }
        return max_key;
    }
};

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
    SkipList<TKey, TValue> table;       // The skip list used as the underlying data structure.
    KeyRange<TKey> key_range;           // The atomic key range for the MemTable.
    mutable std::mutex key_range_mutex; // Mutex to protect the key range.
    size_t size;                        // The size of the MemTable.

public:
    /**
     * @brief Iterate over all key-value pairs in the MemTable.
     *
     * @param callback A function to call for each key-value pair.
     */
    void forEach(std::function<void(const TKey &, const std::optional<TValue> &)> callback) {
        for (auto it = table.begin(); it != table.end(); ++it) {
            callback(it->first, it->second);
        }
    }

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
        {
            std::lock_guard<std::mutex> lock(key_range_mutex);
            key_range.updateKeyRange(key);
        }
        size += sizeof(key) + sizeof(value);
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
        size += sizeof(key) + sizeof(std::nullopt);
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

    /**
     * @brief Get the minimum key.
     *
     * @return TKey The minimum key.
     * @throws `std::runtime_error` if the keys are not set.
     */
    TKey getMinKey() const {
        return key_range.getMinKey();
    }

    /**
     * @brief Get the maximum key.
     *
     * @return TKey The maximum key.
     * @throws `std::runtime_error` if the keys are not set.
     */
    TKey getMaxKey() const {
        return key_range.getMaxKey();
    }

    /**
     * @brief Print the contents of the memtable for testing purposes.
     *
     * This function traverses the memtable's underlying skip list and prints
     * out all the key-value pairs.
     */
    void print() {
        table.print();
    }

    /**
     * @brief Get the size of the MemTable.
     *
     * @return `size_t` The size of the MemTable.
     */
    size_t getSize() const {
        return size;
    }
};

#endif // MEM_TABLE_HPP
