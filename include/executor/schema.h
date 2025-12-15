#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/types.h"

namespace dbms {

// Column metadata for execution
struct ColumnInfo {
    std::string name;           // Column name (may include table prefix: "users.id")
    ColumnType type;            // INTEGER, DOUBLE, STRING
    std::size_t sourceIndex;    // Position in source record
    std::string tableName;      // Source table name

    ColumnInfo() = default;
    ColumnInfo(std::string n, ColumnType t, std::size_t idx, std::string tbl = "")
        : name(std::move(n)), type(t), sourceIndex(idx), tableName(std::move(tbl)) {}
};

// Runtime schema - describes tuple structure
class Schema {
public:
    Schema() = default;

    // Add a column to the schema
    void addColumn(const ColumnInfo& col);
    // Add an alternate name that maps to an existing column index
    void addAlias(const std::string& alias, std::size_t index);

    // Get column count
    std::size_t columnCount() const { return columns_.size(); }

    // Get column by index
    const ColumnInfo& getColumn(std::size_t index) const;

    // Find column index by name (returns nullopt if not found)
    std::optional<std::size_t> findColumn(const std::string& name) const;

    // Check if column exists
    bool hasColumn(const std::string& name) const;

    // Get all columns
    const std::vector<ColumnInfo>& columns() const { return columns_; }

private:
    std::vector<ColumnInfo> columns_;
    std::unordered_map<std::string, std::size_t> columnIndex_;
};

// Runtime tuple - extends Record with schema awareness
struct Tuple {
    std::vector<std::string> values;  // Reuse existing Record values
    std::shared_ptr<Schema> schema;   // Schema for this tuple

    Tuple() = default;

    Tuple(std::vector<std::string> vals, std::shared_ptr<Schema> sch)
        : values(std::move(vals)), schema(std::move(sch)) {}

    // Get value by index
    const std::string& getValue(std::size_t index) const;

    // Get value by column name
    const std::string& getValue(const std::string& columnName) const;

    // Get number of values
    std::size_t size() const { return values.size(); }

    // Check if empty
    bool empty() const { return values.empty(); }
};

} // namespace dbms
