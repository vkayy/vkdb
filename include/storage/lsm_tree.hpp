#ifndef LSM_TREE_HPP
#define LSM_TREE_HPP

#include "lru_cache.hpp"
#include "mem_table.hpp"
#include "ss_table.hpp"
#include "write_ahead_log.hpp"
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>

/**
 * @brief A log-structured merge tree (LSM tree) implementation.
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
        size_t max_size;                            // The maximum size of the level.
        std::vector<std::string> sstable_filenames; // The SSTable filenames.
    };

    std::string data_directory;        // The directory to store the SSTables.
    mutable std::mutex lsm_tree_mutex; // The mutex for the LSM tree.

    WriteAheadLog<TKey, TValue> wal;                                  // The write-ahead log.
    std::unique_ptr<MemTable<TKey, TValue>> memtable;                 // The in-memory table.
    std::unordered_map<std::string, BloomFilter<TKey>> bloom_filters; // The Bloom filters for each SSTable.
    std::unordered_map<std::string, KeyRange<TKey>> key_ranges;       // The key ranges for each SSTable.
    std::vector<LSMTreeLevel> levels;                                 // The levels of the LSM tree.

    std::atomic<size_t> next_sstable_id; // The next SSTable ID.

    LSMTreeCache<TKey, TValue> cache; // The block cache.

    constexpr static std::time_t COMPACTION_TIME_WINDOW = 60 * 60 * 24; // Compaction time window.
    constexpr static size_t LEVEL_SIZE_MULTIPLIER = 10;                 // SSTable size multiplier for each level.
    constexpr static size_t CACHE_BLOCK_SIZE = 1000;                    // Cache size for SSTable blocks.
    constexpr static size_t NUM_LEVELS = 10;                            // Number of levels in the LSM tree.

    constexpr static size_t BLOCK_CACHE_CAPACITY = CACHE_BLOCK_SIZE * 1000; // Block cache capacity.
    constexpr static size_t KEY_VALUE_CACHE_CAPACITY = 1000;                // Key-value cache capacity.

    /**
     * @brief Initialise the levels of the LSM tree.
     *
     * @param num_levels The number of levels.
     */
    void initialise_levels(size_t num_levels) {
        levels.resize(num_levels);
        for (size_t i = 0; i < num_levels; ++i) {
            levels[i].max_size = std::pow(LEVEL_SIZE_MULTIPLIER, i + 1);
        }
    }

    /**
     * @brief Get the next SSTable filename.
     *
     */
    std::string get_next_sstable_filename() {
        return data_directory + "sstable_" + std::to_string(++next_sstable_id) + ".db";
    }

    /**
     * @brief Flush the memtable to a new SSTable if the size threshold is exceeded.
     *
     */
    void flush_memtable_if_exceeded() {
        if (sizeof(memtable) < MEMTABLE_SIZE_THRESHOLD) {
            return;
        }
        std::string sstable_filename = get_next_sstable_filename();
        auto new_sstable = std::make_unique<SSTable<TKey, TValue>>(sstable_filename);

        bloom_filters[sstable_filename] = BloomFilter<TKey>(memtable->getItemCount(), BLOOM_FILTER_FALSE_POSITIVE_RATE);
        new_sstable->flushFromMemTable(*memtable, bloom_filters[sstable_filename], key_ranges[sstable_filename]);

        levels[0].sstable_filenames.push_back(sstable_filename);

        memtable = std::make_unique<MemTable<TKey, TValue>>();

        wal.clearLog();
        balance_levels();
    }

    /**
     * @brief Balance the levels of the LSM tree by moving SSTables between levels.
     *
     */
    void balance_levels() {
        for (size_t i = 0; i < levels.size() - 1; ++i) {
            auto &current_level = levels[i];
            auto &next_level = levels[i + 1];

            while (current_level.sstable_filenames.size() > current_level.max_size) {
                next_level.sstable_filenames.push_back(current_level.sstable_filenames.back());
                current_level.sstable_filenames.pop_back();
            }
        }
    }

    /**
     * @brief Compact the SSTables in a level.
     *
     * This method compacts the SSTables in a level by merging them into a new SSTable. First,
     * it filters out the SSTables that are not within the compaction time window. Then, it merges
     * the remaining SSTables into a new SSTable and replaces the old SSTables with the new one.
     *
     * @param level_index The index of the level to compact.
     */
    void compact_level(size_t level_index) {
        if (level_index >= levels.size()) {
            throw std::invalid_argument("LSMTree::compact_level - level index is outside range");
        }
        auto &level = levels[level_index];
        std::time_t now = std::time(nullptr);
        std::vector<std::string> to_compact;

        auto it = std::partition(level.sstable_filenames.begin(), level.sstable_filenames.end(),
                                 [&](const std::string &filename) {
                                     SSTable<TKey, TValue> sstable(filename);
                                     return now - sstable.getCreationTime() <= COMPACTION_TIME_WINDOW;
                                 });

        to_compact.insert(to_compact.end(), it, level.sstable_filenames.end());
        level.sstable_filenames.erase(it, level.sstable_filenames.end());

        if (to_compact.size() > 1) {
            std::string new_sstable_filename = get_next_sstable_filename();
            auto merged_sstable = mergeSSTables(to_compact, new_sstable_filename);
            level.sstable_filenames.push_back(new_sstable_filename);

            for (const auto &filename : to_compact) {
                bloom_filters.erase(filename);
                key_ranges.erase(filename);
                std::filesystem::remove(filename);
            }
        } else if (to_compact.size() == 1) {
            level.sstable_filenames.push_back(to_compact[0]);
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
        const std::vector<std::string> &sstable_filenames,
        const std::string &new_filename) {

        auto merged_sstable = std::make_unique<SSTable<TKey, TValue>>(new_filename);

        for (const auto &filename : sstable_filenames) {
            SSTable<TKey, TValue> sstable(filename);
            merged_sstable->merge(sstable, bloom_filters[new_filename], key_ranges[new_filename]);
        }

        return merged_sstable;
    }

    /**
     * @brief Compact the levels of the LSM tree.
     *
     * This method runs in the background and compacts the levels of the LSM tree.
     *
     */
    void compact_periodically() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::hours(1));

            std::lock_guard<std::mutex> lock(lsm_tree_mutex);
            for (size_t i = 0; i < levels.size(); ++i) {
                compact_level(i);
            }
        }
    }

public:
    /**
     * @brief Construct a new LSMTree object.
     *
     * @param wal_path The path to the write-ahead log.
     * @param data_dir The directory to store the SSTables.
     */
    LSMTree(const std::string &wal_path, const std::string &data_dir = "./test_dbs/")
        : memtable(std::make_unique<MemTable<TKey, TValue>>()),
          wal(wal_path),
          data_directory(data_dir),
          next_sstable_id(0),
          cache{{BLOCK_CACHE_CAPACITY}, 0, CACHE_BLOCK_SIZE, {KEY_VALUE_CACHE_CAPACITY}} {
        initialise_levels(NUM_LEVELS);
        std::filesystem::create_directories(data_directory);
        std::thread compaction_thread(&LSMTree::compact_periodically, this);
        compaction_thread.detach();
    }

    ~LSMTree() {
        std::lock_guard<std::mutex> lock(lsm_tree_mutex);
        wal.clearLog();
        for (size_t i = 0; i < levels.size(); ++i) {
            for (const auto &filename : levels[i].sstable_filenames) {
                bloom_filters.erase(filename);
                key_ranges.erase(filename);
                std::filesystem::remove(filename);
            }
        }
    }

    /**
     * @brief Put a key-value pair into the LSM tree.
     *
     * @param key The key.
     * @param value The value.
     * @param timestamp The timestamp of the key-value pair.
     */
    void put(const TKey &key, const TValue &value, std::time_t timestamp) {
        std::lock_guard<std::mutex> lock(lsm_tree_mutex);
        TimestampedValue<TValue> timestamped_value(value, timestamp);

        wal.appendLog(key, timestamped_value);
        memtable->put(key, timestamped_value);

        cache.kv_cache_version.fetch_add(1);
        cache.kv_cache.put(key, {timestamped_value, cache.kv_cache_version.load()});

        flush_memtable_if_exceeded();
    }

    /**
     * @brief Get the timestamped value associated with a key.
     *
     * @param key The key.
     *
     * @return `TimestampedValue<TValue>` The timestamped value associated with the key.
     */
    TimestampedValue<TValue> get(const TKey &key) {
        std::lock_guard<std::mutex> lock(lsm_tree_mutex);

        try {
            auto result = memtable->get(key);
            return result;
        } catch (const std::runtime_error &) {
        }

        for (size_t i = 0; i < levels.size(); ++i) {
            const auto &level = levels[i];

            for (auto it = level.sstable_filenames.rbegin(); it != level.sstable_filenames.rend(); ++it) {
                const auto &filename = *it;

                if (key_ranges[filename].areKeysSet() && (key < key_ranges[filename].getMinKey() || key > key_ranges[filename].getMaxKey())) {
                    continue;
                }

                if (!bloom_filters[filename].mightContain(key)) {
                    continue;
                }

                auto cached_value_entry = cache.kv_cache.get(key);
                if (cached_value_entry && cached_value_entry->second == cache.kv_cache_version.load()) {
                    std::cout << "KV cache hit on " << key << std::endl;
                    return cached_value_entry->first;
                }

                try {
                    SSTable<TKey, TValue> sstable(filename);
                    TimestampedValue<TValue> result = sstable.get(key, cache);
                    cache.kv_cache_version.fetch_add(1);
                    cache.kv_cache.put(key, {result, cache.kv_cache_version.load()});
                    return result;
                } catch (const std::runtime_error &) {
                    continue;
                }
            }
        }

        return {std::nullopt, std::time_t(nullptr)};
    }

    /**
     * @brief Remove a key from the LSM tree.
     *
     *
     *
     * @param key The key to remove.
     */
    void remove(const TKey &key) {
        std::lock_guard<std::mutex> lock(lsm_tree_mutex);
        TimestampedValue<TValue> tombstone = {std::nullopt, std::time_t(nullptr)};

        wal.appendLog(key, tombstone);
        memtable->remove(key);

        cache.kv_cache_version.fetch_add(1);
        cache.kv_cache.put(key, {tombstone, cache.kv_cache_version.load()});

        flush_memtable_if_exceeded();
    }

    /**
     * @brief Recover the LSM tree from the write-ahead log.
     *
     */
    void recover() {
        std::lock_guard<std::mutex> lock(lsm_tree_mutex);

        auto temp_memtable = std::make_unique<MemTable<TKey, TValue>>();

        wal.recoverFromLog(temp_memtable);
        if (sizeof(temp_memtable) >= MEMTABLE_SIZE_THRESHOLD) {
            std::string sstable_filename = get_next_sstable_filename();
            auto new_sstable = std::make_unique<SSTable<TKey, TValue>>(sstable_filename);

            bloom_filters[sstable_filename] = BloomFilter<TKey>(temp_memtable->getItemCount(), BLOOM_FILTER_FALSE_POSITIVE_RATE);
            new_sstable->flushFromMemTable(*temp_memtable, bloom_filters[sstable_filename], key_ranges[sstable_filename]);

            levels[0].sstable_filenames.push_back(sstable_filename);
            temp_memtable = std::make_unique<MemTable<TKey, TValue>>();

            balance_levels();
        }

        memtable = std::move(temp_memtable);
    }
};

#endif // LSM_TREE_HPP
