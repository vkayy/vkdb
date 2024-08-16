#ifndef SS_TABLE_HPP
#define SS_TABLE_HPP

#include "mem_table.hpp"
#include "utils/serialize.hpp"
#include <fstream>
#include <map>
#include <string>
#include <vector>

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
        std::time_t creation_time;            // The creation time.
        TKey min_key;                         // The minimum key.
        TKey max_key;                         // The maximum key.
        std::map<TKey, std::streampos> index; // The key to offset mapping.

        /**
         * @brief Serialize the SSTable metadata to a file stream.
         *
         * @param ofs The output file stream.
         */
        void serialize(std::ofstream &ofs) const {
            serializeValue(ofs, creation_time);
            serializeValue(ofs, min_key);
            serializeValue(ofs, max_key);
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
            deserializeValue(ifs, min_key);
            deserializeValue(ifs, max_key);
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

    std::string filename;     // The name of the SSTable file.
    SSTableMetadata metadata; // The metadata of the SSTable.

public:
    /**
     * @brief Construct a new SSTable object.
     *
     * @param filename The name of the SSTable file.
     */
    SSTable(const std::string &filename)
        : filename(filename) {
        metadata.creation_time = std::time(nullptr);
    }

    /**
     * @brief Serialize the SSTable metadata to a separate file.
     *
     * @throws `std::runtime_error` if the metadata file cannot be opened for writing.
     */
    void serializeMetadata() const {
        std::string metadata_filename = filename + ".idx";
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
     * @throws `std::runtime_error` if the metadata file cannot be opened for reading.
     */
    void deserializeMetadata() {
        std::string metadata_filename = filename + ".idx";
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
     *
     * @throws `std::runtime_error` if the SSTable file cannot be opened for writing.
     */
    void flushFromMemTable(MemTable<TKey, TValue> &mem_table) {
        std::ofstream ofs(filename, std::ios::binary);
        if (!ofs.is_open()) {
            throw std::runtime_error("unable to open SSTable file for writing");
        }

        bool keys_set = false;

        mem_table.forEach([&](const TKey &key, const std::optional<TValue> &value) {
            std::streampos pos = ofs.tellp();
            metadata.index[key] = pos;
            serializeValue(ofs, key);
            serializeValue(ofs, value.has_value());

            if (value.has_value()) {
                serializeValue(ofs, value.value());
            }

            if (!keys_set) {
                metadata.min_key = key;
                metadata.max_key = key;
                keys_set = true;
                return;
            }

            if (key < metadata.min_key) {
                metadata.min_key = key;
            }

            if (key > metadata.max_key) {
                metadata.max_key = key;
            }
        });

        ofs.close();
        serializeMetadata();
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
        std::ifstream ifs(filename, std::ios::binary);
        if (!ifs.is_open()) {
            throw std::runtime_error("unable to open SSTable file for reading");
        }

        if (key > metadata.max_key || key < metadata.min_key) {
            return std::nullopt;
        }

        auto it = metadata.index.find(key);
        if (it == metadata.index.end()) {
            return std::nullopt;
        }

        ifs.seekg(it->second);
        if (ifs.fail()) {
            throw std::runtime_error("unable to seek to key offset");
        }

        TKey found_key;
        deserializeValue(ifs, found_key);

        if (found_key != key) {
            throw std::runtime_error("key mismatch during deserialization");
        }

        bool has_value;
        deserializeValue(ifs, has_value);

        if (has_value) {
            TValue value;
            deserializeValue(ifs, value);
            return value;
        }

        return std::nullopt;
    }

    /**
     * @brief Get the minimum key in the SSTable.
     *
     * @return `TKey` The minimum key.
     */
    TKey getMinKey() const {
        return metadata.min_key;
    }

    /**
     * @brief Get the maximum key in the SSTable.
     *
     * @return `TKey` The maximum key.
     */
    TKey getMaxKey() const {
        return metadata.max_key;
    }

    /**
     * @brief Get the creation time.
     *
     */
    std::time_t getCreationTime() const {
        return metadata.creation_time;
    }

    /**
     * @brief Merge this SSTable with another SSTable.
     *
     * @param other The other SSTable to merge with.
     *
     * @return `std::unique_ptr<SSTable<TKey, TValue>>` The new SSTable resulting from the merge.
     */
    std::unique_ptr<SSTable<TKey, TValue>> merge(const SSTable<TKey, TValue> &other) {
        std::map<TKey, std::optional<TValue>> merged_entries;
        TKey new_min_key, new_max_key;
        bool first_entry = true;

        for (const auto &entry : metadata.index) {
            TKey key = entry.first;
            std::ifstream ifs(filename, std::ios::binary);
            ifs.seekg(entry.second);

            TKey read_key;
            bool has_value;
            deserializeValue(ifs, read_key);
            deserializeValue(ifs, has_value);

            std::optional<TValue> value;
            if (has_value) {
                TValue actual_value;
                deserializeValue(ifs, actual_value);
                value = actual_value;
            }

            merged_entries[key] = value;
            if (first_entry) {
                new_min_key = new_max_key = key;
                first_entry = false;
            } else {
                if (key < new_min_key) {
                    new_min_key = key;
                }
                if (key > new_max_key) {
                    new_max_key = key;
                }
            }
        }

        for (const auto &entry : other.metadata.index) {
            TKey key = entry.first;
            std::ifstream ifs(other.filename, std::ios::binary);
            ifs.seekg(entry.second);

            TKey read_key;
            bool has_value;
            deserializeValue(ifs, read_key);
            deserializeValue(ifs, has_value);

            std::optional<TValue> value;
            if (has_value) {
                TValue actual_value;
                deserializeValue(ifs, actual_value);
                value = actual_value;
            }

            merged_entries[key] = value;
            if (first_entry) {
                new_min_key = new_max_key = key;
                first_entry = false;
            } else {
                if (key < new_min_key)
                    new_min_key = key;
                if (key > new_max_key)
                    new_max_key = key;
            }
        }

        // Create the new SSTable using the merged entries
        std::string new_filename = filename + "_merged";
        std::ofstream ofs(new_filename, std::ios::binary);
        if (!ofs.is_open()) {
            throw std::runtime_error("unable to open merged SSTable file for writing");
        }

        SSTableMetadata new_metadata;
        new_metadata.min_key = new_min_key;
        new_metadata.max_key = new_max_key;

        for (const auto &entry : merged_entries) {
            if (entry.second.has_value()) {
                std::streampos pos = ofs.tellp();
                new_metadata.index[entry.first] = pos;

                serializeValue(ofs, entry.first);
                serializeValue(ofs, true);
                serializeValue(ofs, entry.second.value());
            }
        }

        ofs.close();

        auto new_sstable = std::make_unique<SSTable<TKey, TValue>>(new_filename);
        new_sstable->metadata = new_metadata;
        new_sstable->serializeMetadata();

        return new_sstable;
    }
};

#endif // SS_TABLE_HPP
