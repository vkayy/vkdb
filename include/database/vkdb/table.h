#ifndef DATABASE_TABLE_H
#define DATABASE_TABLE_H

#include <vkdb/concepts.h>
#include <vkdb/lsm_tree.h>
#include <vkdb/friendly_builder.h>
#include <fstream>

namespace vkdb {
static const FilePath TAG_COLUMNS_FILENAME{"tag_columns.metadata"};

using DatabaseName = std::string;
using TableName = std::string;

class Table {
public:
    Table() = delete;
    
    explicit Table(const FilePath& db_path, const TableName& name);
    
    Table(Table&&) noexcept = default;
    Table& operator=(Table&&) noexcept = default;
    
    Table(const Table&) = delete;
    Table& operator=(const Table&) = delete;
    
    ~Table() = default;
    
    Table& setTagColumns(const TagColumns& tag_columns);
    Table& addTagColumn(const TagKey& tag_column);
    Table& removeTagColumn(const TagKey& tag_column);
    void clear() const noexcept;
    
    [[nodiscard]] FriendlyQueryBuilder<double> query();
    [[nodiscard]] TableName name() const noexcept;
    [[nodiscard]] TagColumns tagColumns() const noexcept;
    [[nodiscard]] FilePath path() const noexcept;
    [[nodiscard]] bool beenPopulated() const noexcept;

private:
    using StorageEngine = LSMTree<double>;
    
    void save_tag_columns() const;
    void load_tag_columns();
    [[nodiscard]] FilePath tag_columns_path() const noexcept;
    
    DatabaseName db_path_;
    TableName name_;
    TagColumns tag_columns_;
    StorageEngine storage_engine_;
};

} // namespace vkdb

#endif // DATABASE_TABLE_H