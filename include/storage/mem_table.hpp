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
    std::atomic<TKey> min_key;  // The minimum key.
    std::atomic<TKey> max_key;  // The maximum key.
    std::atomic<bool> keys_set; // Flag to indicate if the keys are set.

public:
    /**
     * @brief Construct a new KeyRange object.
     */
    KeyRange() {
        keys_set.store(false);
    }

    /**
     * @brief Copy assignment operator.
     *
     * @param other The KeyRange to copy from.
     */
    void operator=(const KeyRange &other) {
        min_key.store(other.min_key.load());
        max_key.store(other.max_key.load());
        keys_set.store(other.keys_set.load());
    }

    /**
     * @brief Update the key range with the given key.
     *
     * @param key The key to update the range with.
     */
    void updateKeyRange(const TKey &key) {
        if (!keys_set.load()) {
            min_key.store(key);
            max_key.store(key);
            keys_set.store(true);
            return;
        }
        TKey current_min_key = min_key.load();
        TKey current_max_key = max_key.load();
        while (key < current_min_key && !min_key.compare_exchange_weak(current_min_key, key)) {
        }
        while (key > current_max_key && !max_key.compare_exchange_weak(current_max_key, key)) {
        }
    }

    /**
     * @brief Get the minimum key.
     *
     * @return TKey The minimum key.
     * @throws `std::runtime_error` if the keys are not set.
     */
    TKey getMinKey() const {
        if (!keys_set.load()) {
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
        if (!keys_set.load()) {
            throw std::runtime_error("keys not set");
        }
        return max_key;
    }

    /**
     * @brief Check if the keys are set.
     *
     */
    bool areKeysSet() const {
        return keys_set.load();
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
    SkipList<TKey, TValue> table; // The skip list used as the underlying data structure.
    KeyRange<TKey> key_range;     // The atomic key range for the MemTable.
    size_t item_count = 0;        // The number of items in the MemTable.

public:
    /**
     * @brief Iterate over all key-value pairs in the MemTable.
     *
     * @param callback A function to call for each key-value pair.
     */
    void forEach(std::function<void(const TKey &, const TimestampedValue<TValue> &)> callback) {
        for (auto it = table.begin(); it != table.end(); ++it) {
            callback(it->first, it->second);
        }
    }

    /**
     * @brief Add a key-value pair to the MemTable.
     *
     * @param key The key associated with the value to add.
     * @param timestamped_value The timestamped value associated with the key to add.
     *
     * @throws `std::runtime_error` if the value is a tmobstone.
     */
    void put(const TKey &key, const TimestampedValue<TValue> &timestamped_value) {
        if (!timestamped_value.value.has_value()) {
            throw std::runtime_error("cannot insert a tombstone value");
        }
        table.insert(key, timestamped_value);
        key_range.updateKeyRange(key);
        ++item_count;
    }

    /**
     * @brief Retrieves the timestamped value associated with the given key.
     *
     * @param key The key to search for.
     * @return `TimestampedValue<TValue>` The timestamped value associated with the key.
     * @throws `std::runtime_error` if the key is not found or is marked deleted.
     */
    TimestampedValue<TValue> get(const TKey &key) const {
        if (key_range.areKeysSet() && (key < key_range.getMinKey() || key > key_range.getMaxKey())) {
            throw std::runtime_error("MemTable::get - key not found in memtable by key range");
        }
        TimestampedValue<TValue> *timestamped_value = table.findWaitFree(key);
        if (!timestamped_value) {
            throw std::runtime_error("MemTable::get - key not found in memtable by non-entry");
        }
        if (!timestamped_value || !timestamped_value->value.has_value()) {
            throw std::runtime_error("MemTable::get - key not found in memtable by tombstone");
        }

        return *timestamped_value;
    }

    /**
     * @brief Marks a key as deleted by inserting a tombstone.
     *
     * Does not check if the key exists in the MemTable, as tombstones are idempotent.
     *
     * @param key The key to delete.
     */
    void remove(const TKey &key) {
        TimestampedValue<TValue> tombstone = {std::nullopt, std::time_t(nullptr)};
        table.insert(key, tombstone);
        ++item_count;
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
     * @brief Get the number of items in the MemTable.
     *
     * @return `size_t` The number of items in the MemTable.
     */
    size_t getItemCount() const {
        return item_count;
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
};

#endif // MEM_TABLE_HPP
