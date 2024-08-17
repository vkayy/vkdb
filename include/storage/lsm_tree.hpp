#ifndef LSM_TREE_HPP
#define LSM_TREE_HPP

#include "mem_table.hpp"
#include "ss_table.hpp"
#include "write_ahead_log.hpp"
#include <memory>
#include <mutex>
#include <queue>

/**
 * @brief A Log-Structured Merge-Tree (LSM Tree) for key-value storage.
 *
 * @tparam TKey The type of the key.
 * @tparam TValue The type of the value.
 */
template <typename TKey, typename TValue>
class LSMTree {
private:
    /**
     * @brief A level in the LSM tree.
     *
     */
    struct LSMTreeLevel {
        size_t max_size;                                              // The maximum size of the level.
        std::vector<std::unique_ptr<SSTable<TKey, TValue>>> sstables; // The SSTables in the level.
    };

    std::unique_ptr<MemTable<TKey, TValue>> memtable; // The in-memory table.
    std::vector<LSMTreeLevel> levels;                 // The levels of the LSM tree.
    WriteAheadLog<TKey, TValue> wal;                  // The write-ahead log.
    mutable std::mutex lsm_tree_mutex;                // Mutex for thread safety.

    constexpr static size_t MEMTABLE_SIZE_THRESHOLD = 128 * 100 * 1000 * 8; // 128MB memtable size threshold.
    constexpr static std::time_t COMPACTION_TIME_WINDOW = 60 * 60 * 24;     // Compaction time window.
    constexpr static size_t LEVEL_SIZE_MULTIPLIER = 10;                     // SSTable size multiplier for each level.
    constexpr static size_t NUM_LEVELS = 4;                                 // Number of levels in the LSM tree.

    /**
     * @brief Initializes the levels of the LSM tree.
     *
     * @param num_levels The number of levels.
     */
    void initializeLevels(size_t num_levels) {
        levels.resize(num_levels);
        for (size_t i = 0; i < num_levels; ++i) {
            levels[i].max_size = std::pow(LEVEL_SIZE_MULTIPLIER, i + 1);
        }
    }

    /**
     * @brief Flushes the memtable to a new SSTable if the size threshold is exceeded.
     *
     */
    void flushMemTableIfExceeded() {
        if (memtable->getByteSize() < MEMTABLE_SIZE_THRESHOLD) {
            return;
        }
        std::string sstable_filename = "sstable_L1_" + std::to_string(levels[0].sstables.size()) + ".db";
        auto new_sstable = std::make_unique<SSTable<TKey, TValue>>(sstable_filename);
        new_sstable->flushFromMemTable(*memtable);

        levels[0].sstables.push_back(std::move(new_sstable));
        memtable = std::make_unique<MemTable<TKey, TValue>>();

        balanceLevels();
    }

    /**
     * @brief Balances the levels of the LSM tree by moving SSTables between levels.
     *
     */
    void balanceLevels() {
        for (size_t i = 0; i < levels.size() - 1; ++i) {
            auto &current_level = levels[i];
            auto &next_level = levels[i + 1];

            size_t current_size = current_level.sstables.size();
            size_t max_size = current_level.max_size;

            if (current_size > max_size) {
                size_t excess_size = current_size - max_size;

                std::vector<std::unique_ptr<SSTable<TKey, TValue>>> to_move(excess_size);

                to_move.insert(to_move.end(),
                               std::make_move_iterator(current_level.sstables.begin()),
                               std::make_move_iterator(current_level.sstables.begin() + excess_size));

                current_level.sstables.erase(current_level.sstables.begin(),
                                             current_level.sstables.begin() + excess_size);

                next_level.sstables.insert(next_level.sstables.end(),
                                           std::make_move_iterator(to_move.begin()),
                                           std::make_move_iterator(to_move.end()));
            }
        }
    }

    /**
     * @brief Compacts the SSTables in a level.
     *
     * This method compacts the SSTables in a level by merging them into a new SSTable. First,
     * it filters out the SSTables that are not within the compaction time window. Then, it merges
     * the remaining SSTables into a new SSTable and replaces the old SSTables with the new one.
     *
     * @param level_index The index of the level to compact.
     */
    void compactLevel(size_t level_index) {
        if (level_index >= levels.size()) {
            std::invalid_argument("level index is outside range");
        }
        auto &level = levels[level_index];
        auto now = std::time(nullptr);
        std::vector<std::unique_ptr<SSTable<TKey, TValue>>> to_compact;

        auto it = std::partition(level.sstables.begin(), level.sstables.end(),
                                 [&](const std::unique_ptr<SSTable<TKey, TValue>> &sstable) {
                                     return now - sstable->getCreationTime() <= COMPACTION_TIME_WINDOW;
                                 });

        to_compact.insert(to_compact.end(),
                          std::make_move_iterator(it),
                          std::make_move_iterator(level.sstables.end()));
        level.sstables.erase(it, level.sstables.end());

        if (to_compact.size() > 1) {
            std::string new_sstable_filename = "sstable_L" + std::to_string(level_index) + "_compacted_" + std::to_string(level.sstables.size()) + ".db";
            auto merged_sstable = mergeSSTables(to_compact, new_sstable_filename);
            level.sstables.push_back(std::move(merged_sstable));
        } else if (to_compact.size() == 1) {
            level.sstables.push_back(std::move(to_compact[0]));
        }
    }

    /**
     * @brief Merges a vector of SSTables into a new SSTable.
     *
     * @param sstables The SSTables to merge.
     * @param new_filename The filename of the new SSTable.
     * @return `std::unique_ptr<SSTable<TKey, TValue>>` The new SSTable.
     */
    std::unique_ptr<SSTable<TKey, TValue>> mergeSSTables(
        const std::vector<std::unique_ptr<SSTable<TKey, TValue>>> &sstables,
        const std::string &new_filename) {

        auto merged_sstable = std::make_unique<SSTable<TKey, TValue>>(new_filename);

        for (const auto &sstable : sstables) {
            merged_sstable->merge(*sstable);
        }

        return merged_sstable;
    }

    /**
     * @brief Compact the levels of the LSM tree.
     *
     * This method runs in the background and compacts the levels of the LSM tree.
     *
     */
    void backgroundCompaction() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::hours(1));

            std::lock_guard<std::mutex> lock(lsm_tree_mutex);
            for (size_t i = 0; i < levels.size(); ++i) {
                compactLevel(i);
            }
        }
    }

public:
    /**
     * @brief Construct a new LSMTree object.
     *
     * This initializes the LSM tree with the given write-ahead log path and
     * number of levels. Then, it starts a background thread for compaction.
     *
     * @param wal_path The path to the write-ahead log.
     * @param num_levels The number of levels in the LSM tree.
     */
    LSMTree(const std::string &wal_path, size_t num_levels = NUM_LEVELS)
        : memtable(std::make_unique<MemTable<TKey, TValue>>()),
          wal(wal_path) {
        initializeLevels(num_levels);
        std::thread compaction_thread(&LSMTree::backgroundCompaction, this);
        compaction_thread.detach();
    }

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
            for (const auto &level : levels) {
                for (auto it = level.sstables.rbegin(); it != level.sstables.rend(); ++it) {
                    auto result = (*it)->get(key);
                    if (!result) {
                        continue;
                    }
                    if (result.has_value()) {
                        return result;
                    }
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
        if (temp_memtable->getByteSize() >= MEMTABLE_SIZE_THRESHOLD) {
            std::string sstable_filename = "sstable_" + std::to_string(levels[0].sstables.size()) + "_recovered.db";
            auto new_sstable = std::make_unique<SSTable<TKey, TValue>>(sstable_filename);
            new_sstable->flushFromMemTable(*temp_memtable);
            levels[0].sstables.push_back(std::move(new_sstable));

            temp_memtable = std::make_unique<MemTable<TKey, TValue>>();

            balanceLevels();
        }

        memtable = std::move(temp_memtable);
    }
};

#endif // LSM_TREE_HPP
