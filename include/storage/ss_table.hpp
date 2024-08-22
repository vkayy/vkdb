#ifndef SS_TABLE_HPP
#define SS_TABLE_HPP

#include "bloom_filter.hpp"
#include "lru_cache.hpp"
#include "mem_table.hpp"
#include "utils/serialize.hpp"
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

constexpr double BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.01; // The false positive rate for the Bloom filter.

/**
 * @brief A block in an SSTable.
 *
 * @tparam TKey The type of the key.
 * @tparam TValue The type of the value.
 */
template <typename TKey, typename TValue>
struct SSTableBlock {
private:
    std::map<TKey, TimestampedValue<TValue>> index; // The key to value mapping.

public:
    /**
     * @brief Get the value associated with the key.
     *
     * @param key The key.
     * @return `TimestampedValue<TValue> *` The value associated with the key or `nullptr` if not found.
     */
    TimestampedValue<TValue> *get(const TKey &key) {
        auto it = index.find(key);
        if (it == index.end()) {
            return nullptr;
        }
        return &it->second;
    }

    /**
     * @brief Put the key-value pair into the block index.
     *
     * @param key The key.
     * @param timestamped_value The timestamped value.
     */
    void put(const TKey &key, const TimestampedValue<TValue> &timestamped_value) {
        index[key] = timestamped_value;
    }
};

/**
 * @brief A cache for the LSM tree.
 *
 * @tparam TKey The type of the key.
 * @tparam TValue The type of the value.
 */
template <typename TKey, typename TValue>
struct LSMTreeCache {
    LRUCache<size_t, std::pair<SSTableBlock<TKey, TValue>, size_t>> block_cache; // The block cache.
    std::atomic<int32_t> block_cache_version;                                    // The version of the block cache.
    int32_t block_size;                                                          // The block size for the block cache.
    LRUCache<TKey, std::pair<TimestampedValue<TValue>, size_t>> kv_cache;        // The key-value cache.
    std::atomic<int32_t> kv_cache_version;                                       // The version of the key-value cache.
};

/**
 * @brief A structure to hold the metadata of an SSTable.
 */
template <typename TKey>
struct SSTableMetadata {
    std::map<TKey, std::streampos> index; // The key to offset mapping.
    std::time_t creation_time;            // The creation time.

    /**
     * @brief Serialize the SSTable metadata to a file stream.
     *
     * @param ofs The output file stream.
     */
    void serialize(std::ofstream &ofs) const {
        serializeValue(ofs, creation_time);
        serializeValue(ofs, index.size());
        for (const auto &entry : index) {
            serializeValue(ofs, entry.first);
            serializeValue(ofs, entry.second);
        }
    }

    /**
     * @brief Deserialize the SSTable metadata from a file stream.
     *
     * @param ifs The input file stream.
     */
    void deserialize(std::ifstream &ifs) {
        deserializeValue(ifs, creation_time);
        size_t index_size;
        deserializeValue(ifs, index_size);
        for (size_t i = 0; i < index_size; ++i) {
            TKey key;
            std::streampos offset;
            deserializeValue(ifs, key);
            deserializeValue(ifs, offset);
            index[key] = offset;
        }
    }
};

/**
 * @brief An immutable disk-based SSTable for key-value storage.
 *
 * @tparam TKey The type of the key.
 * @tparam TValue The type of the value.
 */
template <typename TKey, typename TValue>
class SSTable {
private:
    /**
     * @brief Read a block from the SSTable file.
     *
     * @param block_offset The offset of the block.
     * @param block_size The size of the block.
     *
     * @return `SSTableBlock<TKey, TValue>` The block read from the file.
     * @throws `std::runtime_error` if the SSTable file fails to open or seek to the block offset.
     */
    SSTableBlock<TKey, TValue> read_block(size_t block_index, size_t block_size) const {
        std::ifstream ifs(filename, std::ios::binary);
        if (!ifs.is_open()) {
            throw std::runtime_error("SSTable::read_block - unable to open SSTable file for reading");
        }

        ifs.seekg(0, std::ios::end);
        std::streampos file_size = ifs.tellg();
        ifs.seekg(block_index * block_size);

        if (ifs.fail()) {
            throw std::runtime_error("SSTable::read_block - unable to seek to block offset");
        }

        SSTableBlock<TKey, TValue> block;
        std::streampos remaining = file_size - ifs.tellg();
        size_t bytes_to_read = std::min(static_cast<size_t>(remaining), block_size);

        std::vector<char> buffer(bytes_to_read);
        ifs.read(buffer.data(), bytes_to_read);
        size_t bytes_read = ifs.gcount();

        std::istringstream block_stream(std::string(buffer.data(), bytes_read));
        while (block_stream.good() && block_stream.tellg() < static_cast<std::streampos>(bytes_read)) {
            TKey key;
            TimestampedValue<TValue> timestamped_value;
            try {
                deserializeValue(block_stream, key);
                deserializeValue(block_stream, timestamped_value);
                block.put(key, timestamped_value);
            } catch (const std::exception &e) {
                std::cerr << "Error deserializing entry: " << e.what() << std::endl;
                break;
            }
        }

        return block;
    }

    std::string filename; // The name of the SSTable file.

public:
    /**
     * @brief Construct a new SSTable object.
     *
     * @param filename The name of the SSTable file.
     */
    SSTable(const std::string &filename)
        : filename(filename) {}

    /**
     * @brief Serialize the SSTable metadata to a separate file.
     *
     * @param metadata The SSTable metadata to serialize.
     * @param sstable_filename The name of the SSTable file.
     *
     * @throws `std::runtime_error` if the metadata file cannot be opened for writing.
     */
    static void serializeMetadata(const SSTableMetadata<TKey> &metadata, const std::string &sstable_filename) {
        std::string metadata_filename = sstable_filename + ".meta";
        std::ofstream ofs(metadata_filename, std::ios::binary);
        if (!ofs.is_open()) {
            throw std::runtime_error("unable to open metadata file for writing");
        }
        metadata.serialize(ofs);
        ofs.close();
    }

    /**
     * @brief Deserialize the SSTable metadata from a file.
     *
     * @param metadata The SSTable metadata to deserialize into.
     * @param sstable_filename The name of the SSTable file.
     *
     * @throws `std::runtime_error` if the metadata file cannot be opened for reading.
     */
    static void deserializeMetadata(SSTableMetadata<TKey> &metadata, const std::string &sstable_filename) {
        std::string metadata_filename = sstable_filename + ".meta";
        std::ifstream ifs(metadata_filename, std::ios::binary);
        if (!ifs.is_open()) {
            throw std::runtime_error("unable to open metadata file for reading");
        }
        metadata.deserialize(ifs);
        ifs.close();
    }

    /**
     * @brief Flush a MemTable to disk as an SSTable.
     *
     * The contents of the MemTable are written to the SSTable file in binary format,
     * and an index is created to map keys to their respective offsets in the file.
     * This prevents the need to scan the entire file to find a key later.
     *
     * @param mem_table The MemTable to flush.
     * @param bloom_filter The Bloom filter to update.
     * @param key_range The key range to update.
     *
     * @throws `std::runtime_error` if the SSTable file cannot be opened for writing.
     */
    void flushFromMemTable(
        MemTable<TKey, TValue> &mem_table,
        BloomFilter<TKey> &bloom_filter,
        KeyRange<TKey> &key_range) {
        std::ofstream ofs(filename, std::ios::binary);
        if (!ofs.is_open()) {
            throw std::runtime_error("unable to open SSTable file for writing");
        }

        SSTableMetadata<TKey> metadata;
        metadata.creation_time = std::time(nullptr);

        mem_table.forEach([&](const TKey &key, const TimestampedValue<TValue> &timestamped_value) {
            std::streampos pos = ofs.tellp();
            metadata.index[key] = pos;
            serializeValue(ofs, key);
            serializeValue(ofs, timestamped_value);
            bloom_filter.insert(key);
            key_range.updateKeyRange(key);
        });

        ofs.close();

        serializeMetadata(metadata, filename);
    }

    /**
     * @brief Retrieve a value from the SSTable by its key.
     *
     * @param key The key to search for.
     * @param cache The cache to use.
     *
     * @return `TimestampedValue<TValue>` The value associated with the key.
     * @throws `std::runtime_error` if the SSTable file cannot be opened for reading or the key is not found.
     */
    TimestampedValue<TValue> get(const TKey &key, LSMTreeCache<TKey, TValue> &cache) const {
        SSTableMetadata<TKey> metadata;
        deserializeMetadata(metadata, filename);

        auto it = metadata.index.find(key);
        if (it == metadata.index.end()) {
            throw std::runtime_error("SSTable::get - key not found in SSTable index");
        }
        std::streampos block_index = it->second / cache.block_size;
        size_t cache_key = std::hash<std::string>{}(filename + std::to_string(block_index));

        SSTableBlock<TKey, TValue> key_block;
        auto cached_block_entry = cache.block_cache.get(cache_key);

        if (cached_block_entry && cached_block_entry->second == cache.block_cache_version.load()) {
            key_block = cached_block_entry->first;
        } else {
            key_block = read_block(block_index, cache.block_size);
            cache.block_cache_version.fetch_add(1);
            cache.block_cache.put(cache_key, {key_block, cache.block_cache_version.load()});
        }

        TimestampedValue<TValue> *timestamped_value = key_block.get(key);
        if (!timestamped_value || !timestamped_value->value.has_value()) {
            throw std::runtime_error("SSTable::get - key not found in SSTable block");
        }

        return *timestamped_value;
    }

    /**
     * @brief Get the creation time.
     *
     * This function reads the creation time from the metadata file.
     *
     * @return `std::time_t` The creation time.
     * @throws `std::runtime_error` if the metadata file cannot be opened for reading.
     *
     */
    std::time_t getCreationTime() const {
        std::ifstream ifs(filename + ".meta", std::ios::binary);
        if (!ifs.is_open()) {
            throw std::runtime_error("unable to open metadata file for reading");
        }
        std::time_t creation_time;
        deserializeValue(ifs, creation_time);
        return creation_time;
    }

    /**
     * @brief Merge this SSTable with another SSTable.
     *
     * Assumes the second table is newer than the current one.
     *
     * @param other The other SSTable to merge with.
     * @param bloom_filter The Bloom filter to update.
     * @param key_range The key range to update.
     *
     * @return `std::unique_ptr<SSTable<TKey, TValue>>` The new SSTable resulting from the merge.
     * @throws `std::runtime_error` if files are unable to be opened.
     */
    std::unique_ptr<SSTable<TKey, TValue>> merge(
        const SSTable<TKey, TValue> &other,
        BloomFilter<TKey> &bloom_filter,
        KeyRange<TKey> &key_range) {

        std::unordered_map<TKey, bool> seen;
        std::map<TKey, TimestampedValue<TValue>> merged_entries;

        auto process_sstable = [&](const std::string &sstable_filename) {
            SSTableMetadata<TKey> metadata;
            deserializeMetadata(metadata, sstable_filename);

            for (const auto &entry : metadata.index) {
                TKey key = entry.first;
                std::ifstream ifs(sstable_filename, std::ios::binary);
                ifs.seekg(entry.second);

                TKey read_key;
                deserializeValue(ifs, key);
                if (seen[key]) {
                    continue;
                }

                TimestampedValue<TValue> timestamp_value;
                deserializeValue(ifs, timestamp_value);
                if (!timestamp_value.value.has_value()) {
                    continue;
                }

                seen[key] = true;
                merged_entries[key] = timestamp_value;
                key_range.updateKeyRange(key);
            }
        };

        process_sstable(other.filename);
        process_sstable(filename);

        std::string new_filename = filename + "_merged";
        std::ofstream ofs(new_filename, std::ios::binary);
        if (!ofs.is_open()) {
            throw std::runtime_error("unable to open merged SSTable file for writing");
        }

        SSTableMetadata<TKey> new_metadata;
        new_metadata.creation_time = std::time(nullptr);
        bloom_filter = BloomFilter<TKey>(merged_entries.size(), BLOOM_FILTER_FALSE_POSITIVE_RATE);

        for (const auto &[key, timestamped_value] : merged_entries) {
            if (timestamped_value.value.has_value()) {
                std::streampos pos = ofs.tellp();
                new_metadata.index[key] = pos;

                serializeValue(ofs, key);
                serializeValue(ofs, timestamped_value);

                bloom_filter.insert(key);
            } else {
                throw std::runtime_error("cannot merge tombstone values");
            }
        }

        ofs.close();

        auto new_sstable = std::make_unique<SSTable<TKey, TValue>>(new_filename);
        new_sstable->serializeMetadata(new_metadata, new_filename);

        return new_sstable;
    }
};

#endif // SS_TABLE_HPP
