#ifndef DATABASE_TABLE_H
#define DATABASE_TABLE_H

#include <vkdb/concepts.h>
#include <vkdb/lsm_tree.h>
#include <vkdb/friendly_builder.h>
#include <fstream>

namespace vkdb {
/**
 * @brief The name of the file that stores the tag columns for a table.
 * 
 */
static const FilePath TAG_COLUMNS_FILENAME{"tag_columns.metadata"};

/**
 * @brief Type alias for a string.
 * 
 */
using DatabaseName = std::string;

/**
 * @brief Type alias for a string.
 * 
 */
using TableName = std::string;

/**
 * @brief Represents a table in vkdb.
 * 
 */
class Table {
public:
    /**
     * @brief Deleted default constructor.
     * 
     */
    Table() = delete;
    
    /**
     * @brief Construct a new Table object.
     * @details Loads the table if it exists.
     * 
     * @param db_path Path to the database directory.
     * @param name Name of the table.
     * 
     * @throw std::runtime_error If loading the table fails.
     */
    explicit Table(const FilePath& db_path, const TableName& name);
    
    /**
     * @brief Move-construct a new Table object.
     * 
     */
    Table(Table&&) noexcept = default;

    /**
     * @brief Move-assign a new Table object.
     * 
     */
    Table& operator=(Table&&) noexcept = default;
    
    /**
     * @brief Deleted copy constructor.
     * 
     */
    Table(const Table&) = delete;

    /**
     * @brief Deleted copy assignment operator.
     * 
     */
    Table& operator=(const Table&) = delete;
    
    /**
     * @brief Destroy the Table object.
     * 
     */
    ~Table() noexcept = default;
    
    /**
     * @brief Set the TagColumns object.
     * 
     * @param tag_columns Tag columns.
     * @return Table& Reference to the table.
     */
    Table& setTagColumns(const TagColumns& tag_columns) noexcept;

    /**
     * @brief Add a tag column.
     * 
     * @param tag_column Tag column.
     * @return Table& Reference to the table.
     * 
     * @throw std::runtime_error If the table has previously been populated
     * with data or the tag column already exists.
     */
    Table& addTagColumn(const TagKey& tag_column);

    /**
     * @brief Remove a tag column.
     * 
     * @param tag_column Tag column.
     * @return Table& Reference to the table.
     * 
     * @throw std::runtime_error If the table has previously been populated
     * with data or the tag column does not exist.
     */
    Table& removeTagColumn(const TagKey& tag_column);

    /**
     * @brief Clear the table.
     * @details Clears the table directory.
     * 
     */
    void clear() const noexcept;
    
    /**
     * @brief Get a FriendlyQueryBuilder object.
     * 
     * @return FriendlyQueryBuilder<double> Friendly query builder.
     */
    [[nodiscard]] FriendlyQueryBuilder<double> query() noexcept;

    /**
     * @brief Get the name of the table.
     * 
     * @return TableName Name of the table.
     */
    [[nodiscard]] TableName name() const noexcept;

    /**
     * @brief Get the TagColumns object.
     * 
     * @return TagColumns Tag columns.
     */
    [[nodiscard]] TagColumns tagColumns() const noexcept;

    /**
     * @brief Get the path to the table directory.
     * 
     * @return FilePath Path to the directory.
     */
    [[nodiscard]] FilePath path() const noexcept;

private:
    /**
     * @brief Type alias for the storage engine.
     * 
     */
    using StorageEngine = LSMTree<double>;

    /**
     * @brief Check if the table has been populated.
     * @details Checks if the storage engine is not empty.
     * 
     * @return true If the table has been populated.
     * @return false If the table has not been populated.
     */
    [[nodiscard]] bool been_populated() const noexcept;
    
    /**
     * @brief Save the tag columns to a file.
     * @details Writes the tag columns to a file.
     * 
     * @throw std::runtime_error If the file cannot be opened.
     */
    void save_tag_columns() const;

    /**
     * @brief Load the tag columns from a file.
     * @details Reads the tag columns from a file.
     * 
     * @throw std::runtime_error If the file cannot be opened.
     */
    void load_tag_columns();

    /**
     * @brief Get the path to the file that stores the tag columns.
     * 
     * @return FilePath Path to the file.
     */
    [[nodiscard]] FilePath tag_columns_path() const noexcept;
    
    /**
     * @brief Load the table.
     * @details Creates the table directory if it does not exist, then
     * loads the tag columns and replays the storage engine's WAL.
     * 
     * @throw std::runtime_error If loading the tag columns or replaying the
     * WAL fails.
     */
    void load();
    
    /**
     * @brief Path to the database directory.
     * 
     */
    DatabaseName db_path_;

    /**
     * @brief Name of the table.
     * 
     */
    TableName name_;

    /**
     * @brief Tag columns.
     * 
     */
    TagColumns tag_columns_;

    /**
     * @brief Storage engine.
     * 
     */
    StorageEngine storage_engine_;
};

} // namespace vkdb

#endif // DATABASE_TABLE_H