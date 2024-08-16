#ifndef LSM_TREE_HPP
#define LSM_TREE_HPP

#include "mem_table.hpp"
#include "ss_table.hpp"
#include "write_ahead_log.hpp"
#include <memory>
#include <mutex>

/**
 * @brief A Log-Structured Merge-Tree (LSM Tree) for key-value storage.
 *
 * @tparam TKey The type of the key.
 * @tparam TValue The type of the value.
 */
template <typename TKey, typename TValue>
class LSMTree {
private:
    mutable std::mutex lsm_tree_mutex;                            // Mutex for thread safety.
    WriteAheadLog<TKey, TValue> wal;                              // The write-ahead log.
    std::unique_ptr<MemTable<TKey, TValue>> memtable;             // The in-memory table.
    std::vector<std::unique_ptr<SSTable<TKey, TValue>>> sstables; // The disk-based SSTables.

    constexpr static size_t MEMTABLE_SIZE_THRESHOLD = 128 * 1000 * 1000 * 8; // 128MB memtable size threshold.

    /**
     * @brief Flushes the memtable to a new SSTable if the size threshold is exceeded.
     *
     */
    void flushMemTableIfExceeded() {
        if (memtable->getSize() < MEMTABLE_SIZE_THRESHOLD) {
            return;
        }
        std::string sstable_filename = "sstable_" + std::to_string(sstables.size()) + ".db";
        auto new_sstable = std::make_unique<SSTable<TKey, TValue>>(sstable_filename);
        new_sstable->flushFromMemTable(*memtable);
        sstables.push_back(std::move(new_sstable));

        memtable = std::make_unique<MemTable<TKey, TValue>>();
    }

public:
    /**
     * @brief Construct a new LSMTree object.
     *
     * @param wal_path The path to the write-ahead log.
     */
    LSMTree(const std::string &wal_path)
        : memtable(std::make_unique<MemTable<TKey, TValue>>()),
          wal(wal_path) {}

    /**
     * @brief Put a key-value pair into the LSM tree.
     *
     * @param key The key.
     * @param value The value.
     */
    void put(const TKey &key, const TValue &value) {
        std::lock_guard<std::mutex> lock(lsm_tree_mutex);

        wal.appendLog(key, value);
        memtable->put(key, value);

        flushMemTableIfExceeded();
    }

    /**
     * @brief Get the value associated with a key.
     *
     * @param key The key.
     * @return std::optional<TValue>
     */
    std::optional<TValue> get(const TKey &key) {
        std::lock_guard<std::mutex> lock(lsm_tree_mutex);

        try {
            return memtable->get(key);
        } catch (const std::runtime_error &) {
            for (auto it = sstables.rbegin(); it != sstables.rend(); ++it) {
                auto result = (*it)->get(key);
                if (result.has_value()) {
                    return result;
                }
            }
        }

        return std::nullopt;
    }

    /**
     * @brief Remove a key from the LSM tree.
     *
     * @param key The key to remove.
     */
    void remove(const TKey &key) {
        std::lock_guard<std::mutex> lock(lsm_tree_mutex);

        wal.appendLog(key, std::nullopt);
        memtable->remove(key);

        flushMemTableIfExceeded();
    }

    /**
     * @brief Recover the LSM tree from the write-ahead log.
     *
     */
    void recover() {
        std::lock_guard<std::mutex> lock(lsm_tree_mutex);

        auto temp_memtable = std::make_unique<MemTable<TKey, TValue>>();

        wal.recoverFromLog(temp_memtable);
        if (temp_memtable->getSize() >= MEMTABLE_SIZE_THRESHOLD) {
            std::string sstable_filename = "sstable_" + std::to_string(sstables.size()) + "_recovered.db";
            auto new_sstable = std::make_unique<SSTable<TKey, TValue>>(sstable_filename);
            new_sstable->flushFromMemTable(*temp_memtable);
            sstables.push_back(std::move(new_sstable));

            temp_memtable = std::make_unique<MemTable<TKey, TValue>>();
        }

        memtable = std::move(temp_memtable);
    }
};

#endif // LSM_TREE_HPP
