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
     * @brief A structure to hold the index of an SSTable.
     */
    struct SSTableIndex {
        std::map<TKey, std::streampos> key_offset_map;
    };

    std::string filename; // The name of the SSTable file.
    SSTableIndex index;   // The index of the SSTable.

public:
    /**
     * @brief Construct a new SSTable object.
     *
     * @param filename The name of the SSTable file.
     */
    SSTable(const std::string &filename)
        : filename(filename) {}

    /**
     * @brief Serialize the SSTable index to a separate file.
     *
     * @throws `std::runtime_error` if the index file cannot be opened for writing.
     */
    void serializeIndex() const {
        std::string index_filename = filename + ".idx";
        std::ofstream ofs(index_filename, std::ios::binary);
        if (!ofs.is_open()) {
            throw std::runtime_error("unable to open index file for writing");
        }

        for (const auto &entry : index.key_offset_map) {
            serializeValue(ofs, entry.first);
            serializeValue(ofs, entry.second);
        }

        ofs.close();
    }

    /**
     * @brief Deserialize the SSTable index from a file.
     *
     * @throws `std::runtime_error` if the index file cannot be opened for reading.
     */
    void deserializeIndex() {
        std::string index_filename = filename + ".idx";
        std::ifstream ifs(index_filename, std::ios::binary);
        if (!ifs.is_open()) {
            throw std::runtime_error("unable to open index file for reading");
        }

        while (ifs.peek() != EOF) {
            TKey key;
            std::streampos offset;
            deserializeValue(ifs, key);
            deserializeValue(ifs, offset);
            index.key_offset_map[key] = offset;
        }

        ifs.close();
    }

    /**
     * @brief Flush a MemTable to disk as an SSTable.
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

        mem_table.forEach([&](const TKey &key, const std::optional<TValue> &value) {
            std::streampos pos = ofs.tellp();
            index.key_offset_map[key] = pos;
            serializeValue(ofs, key);
            serializeValue(ofs, value.has_value());
            if (value.has_value()) {
                serializeValue(ofs, value.value());
            }
        });

        ofs.close();

        serializeIndex();
    }

    /**
     * @brief Retrieve a value from the SSTable by its key.
     *
     * @param key The key to search for.
     *
     * @return `std::optional<TValue>` The value associated with the key, or `std::nullopt` if not present.
     * @throws `std::runtime_error` if the SSTable file cannot be opened for reading.
     */
    std::optional<TValue> get(const TKey &key) const {
        std::ifstream ifs(filename, std::ios::binary);
        if (!ifs.is_open()) {
            throw std::runtime_error("unable to open SSTable file for reading");
        }

        auto it = index.key_offset_map.find(key);
        if (it == index.key_offset_map.end()) {
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
};

#endif // SS_TABLE_HPP
