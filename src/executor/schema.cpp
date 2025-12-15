#include "executor/schema.h"

#include <stdexcept>

namespace dbms {

void Schema::addColumn(const ColumnInfo& col) {
    const std::size_t idx = columns_.size();
    columnIndex_[col.name] = idx;
    columns_.push_back(col);
    if (!col.tableName.empty()) {
        const std::string qualified = col.tableName + "." + col.name;
        columnIndex_[qualified] = idx;
    }
}

void Schema::addAlias(const std::string& alias, std::size_t index) {
    if (index >= columns_.size()) {
        throw std::out_of_range("alias refers to invalid column index");
    }
    columnIndex_[alias] = index;
}

const ColumnInfo& Schema::getColumn(std::size_t index) const {
    if (index >= columns_.size()) {
        throw std::out_of_range("column index out of range");
    }
    return columns_[index];
}

std::optional<std::size_t> Schema::findColumn(const std::string& name) const {
    auto it = columnIndex_.find(name);
    if (it == columnIndex_.end()) {
        return std::nullopt;
    }
    return it->second;
}

bool Schema::hasColumn(const std::string& name) const {
    return columnIndex_.find(name) != columnIndex_.end();
}

const std::string& Tuple::getValue(std::size_t index) const {
    if (index >= values.size()) {
        throw std::out_of_range("tuple value index out of range");
    }
    return values[index];
}

const std::string& Tuple::getValue(const std::string& columnName) const {
    if (!schema) {
        throw std::logic_error("tuple has no schema");
    }
    auto idx = schema->findColumn(columnName);
    if (!idx) {
        throw std::invalid_argument("column not found: " + columnName);
    }
    return getValue(*idx);
}

} // namespace dbms
