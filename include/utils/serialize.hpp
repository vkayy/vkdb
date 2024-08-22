#ifndef SERIALIZE_HPP
#define SERIALIZE_HPP

#include <fstream>
#include <string>

/**
 * @brief Serialize a value to an output stream.
 * 
 * @tparam TOutputStream The output stream type.
 * @tparam TValue The value type.
 * @param ofs The output stream.
 * @param value The value to serialize.
 */
template <typename TOutputStream, typename TValue>
void serializeValue(TOutputStream &ofs, const TValue &value) {
    size_t size = sizeof(value);
    ofs.write(reinterpret_cast<const char *>(&size), sizeof(size));
    ofs.write(reinterpret_cast<const char *>(&value), size);
};

/**
 * @brief Serialize a string to an output stream.
 * 
 * @tparam TOutputStream The output stream type.
 * @param ofs The output stream.
 * @param value The string to serialize.
 */
template <typename TOutputStream>
void serializeValue(TOutputStream &ofs, const std::string &value) {
    size_t size = value.size();
    ofs.write(reinterpret_cast<const char *>(&size), sizeof(size));
    ofs.write(value.data(), size);
};

/**
 * @brief Deserialize a value from an input stream.
 * 
 * @tparam TInputStream The input stream type.
 * @tparam TValue The value type.
 * @param ifs The input stream.
 * @param value The value to deserialize.
 */
template <typename TInputStream, typename TValue>
void deserializeValue(TInputStream &ifs, TValue &value) {
    size_t size;
    ifs.read(reinterpret_cast<char *>(&size), sizeof(size));
    ifs.read(reinterpret_cast<char *>(&value), size);
};

/**
 * @brief Deserialize a string from an input stream.
 * 
 * @tparam TInputStream The input stream type.
 * @param ifs The input stream.
 * @param value The string to deserialize.
 */
template <typename TInputStream>
void deserializeValue(TInputStream &ifs, std::string &value) {
    size_t size;
    ifs.read(reinterpret_cast<char *>(&size), sizeof(size));
    value.resize(size);
    ifs.read(&value[0], size);
};

#endif // SERIALIZE_HPP
