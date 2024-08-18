#ifndef SS_TABLE_HPP
#define SS_TABLE_HPP

#include "bloom_filter.hpp"
#include "mem_table.hpp"
#include "utils/serialize.hpp"
#include <fstream>
#include <map>
#include <string>
#include <vector>

constexpr double BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.01; // The false positive rate for the Bloom filter.

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
     * @brief A structure to hold the metadata of an SSTable.
     */
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
    void serializeMetadata(const SSTableMetadata &metadata, const std::string &sstable_filename) const {
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
    void deserializeMetadata(SSTableMetadata &metadata, const std::string &sstable_filename) {
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

        SSTableMetadata metadata;

        mem_table.forEach([&](const TKey &key, const std::optional<TValue> &value) {
            std::streampos pos = ofs.tellp();
            metadata.index[key] = pos;
            serializeValue(ofs, key);
            serializeValue(ofs, value.has_value());

            if (value.has_value()) {
                serializeValue(ofs, value.value());
            }

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
     *
     * @return `std::optional<TValue>` The value associated with the key, or `std::nullopt` if not present.
     * @throws `std::runtime_error` if the SSTable file cannot be opened for reading.
     */
    std::optional<TValue>
    get(const TKey &key) const {
        std::ifstream meta_ifs(filename + ".meta", std::ios::binary);
        if (!meta_ifs.is_open()) {
            throw std::runtime_error("unable to open metadata file for reading");
        }

        SSTableMetadata new_metadata;
        new_metadata.deserialize(meta_ifs);

        auto it = new_metadata.index.find(key);
        if (it == new_metadata.index.end()) {
            return std::nullopt;
        }

        std::ifstream data_ifs(filename, std::ios::binary);
        if (!data_ifs.is_open()) {
            throw std::runtime_error("unable to open SSTable file for reading");
        }

        data_ifs.seekg(it->second);
        if (data_ifs.fail()) {
            throw std::runtime_error("unable to seek to key offset");
        }

        TKey found_key;
        deserializeValue(data_ifs, found_key);

        if (found_key != key) {
            throw std::runtime_error("key mismatch during deserialization");
        }

        bool has_value;
        deserializeValue(data_ifs, has_value);

        if (has_value) {
            TValue value;
            deserializeValue(data_ifs, value);
            return value;
        }

        return std::nullopt;
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
        std::map<TKey, std::optional<TValue>> merged_entries;

        SSTableMetadata metadata;
        deserializeMetadata(metadata, filename);

        SSTableMetadata other_metadata;
        deserializeMetadata(other_metadata, other.filename);

        for (const auto &entry : other_metadata.index) {
            TKey key = entry.first;
            std::ifstream ifs(other.filename, std::ios::binary);
            ifs.seekg(entry.second);

            TKey read_key;
            bool has_value;
            deserializeValue(ifs, read_key);
            deserializeValue(ifs, has_value);

            if (!has_value || seen[key]) {
                continue;
            }

            TValue actual_value;
            deserializeValue(ifs, actual_value);

            std::optional<TValue> value;
            value = actual_value;

            seen[key] = true;
            merged_entries[key] = value;
            key_range.updateKeyRange(key);
        }

        for (const auto &entry : metadata.index) {
            TKey key = entry.first;
            std::ifstream ifs(filename, std::ios::binary);
            ifs.seekg(entry.second);

            TKey read_key;
            bool has_value;
            deserializeValue(ifs, read_key);
            deserializeValue(ifs, has_value);

            if (!has_value || seen[key]) {
                continue;
            }

            TValue actual_value;
            deserializeValue(ifs, actual_value);

            std::optional<TValue> value;
            value = actual_value;

            seen[key] = true;
            merged_entries[key] = value;
            key_range.updateKeyRange(key);
        }

        std::string new_filename = filename + "_merged";
        std::ofstream ofs(new_filename, std::ios::binary);
        if (!ofs.is_open()) {
            throw std::runtime_error("unable to open merged SSTable file for writing");
        }

        SSTableMetadata new_metadata;
        new_metadata.creation_time = std::time(nullptr);
        bloom_filter = BloomFilter<TKey>(merged_entries.size(), BLOOM_FILTER_FALSE_POSITIVE_RATE);

        for (const auto &[key, value] : merged_entries) {
            if (value.has_value()) {
                std::streampos pos = ofs.tellp();
                new_metadata.index[key] = pos;
                bloom_filter.insert(key);

                serializeValue(ofs, key);
                serializeValue(ofs, true);
                serializeValue(ofs, value.value());
            } else {
                std::cout << "tried merging tombstone value in key " << key << std::endl;
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
