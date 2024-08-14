#include "utils/serialize.hpp"

void serializeValue(std::ofstream &ofs, const std::string &value) {
    size_t size = value.size();
    ofs.write(reinterpret_cast<const char *>(&size), sizeof(size));
    ofs.write(value.data(), size);
};

void deserializeValue(std::ifstream &ifs, std::string &value) {
    size_t size;
    ifs.read(reinterpret_cast<char *>(&size), sizeof(size));
    value.resize(size);
    ifs.read(&value[0], size);
};
