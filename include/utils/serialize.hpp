#ifndef SERIALIZE_HPP
#define SERIALIZE_HPP

#include <fstream>
#include <string>

/**
 * @brief Serialize data into a file.
 *
 * A utility function to serialize any data type to a binary file.
 *
 * @tparam T The type of the data to serialize.
 * @param ofs The `ofstream` to serialize into.
 * @param value The value to serialize.
 */
template <typename T>
void serializeValue(std::ofstream &ofs, const T &value) {
    size_t size = sizeof(value);
    ofs.write(reinterpret_cast<const char *>(&size), sizeof(size));
    ofs.write(reinterpret_cast<const char *>(&value), size);
};

/**
 * @brief Serialize a string into a file.
 *
 * A utility function to serialize strings to a binary file.
 *
 * @param ofs The `ofstream` to serialize into.
 * @param value The string to serialize.
 */
void serializeValue(std::ofstream &ofs, const std::string &value);

/**
 * @brief Deserialize data from a file.
 *
 * A utility function to deserialize any data type to a binary file.
 *
 * @tparam T The type of the data to deserialize.
 * @param ifs The `ifstream` to deserialize from.
 * @param value The value to deserialize into.
 */
template <typename T>
void deserializeValue(std::ifstream &ifs, T &value) {
    size_t size;
    ifs.read(reinterpret_cast<char *>(&size), sizeof(size));
    ifs.read(reinterpret_cast<char *>(&value), size);
};

/**
 * @brief Deserialize a string from a file.
 *
 * A utility function to deserialize strings to a binary file.
 *
 * @param ifs The `ifstream` to deserialize from.
 * @param value The string to deserialize into.
 */
void deserializeValue(std::ifstream &ifs, std::string &value);

#endif // SERIALIZE_HPP
