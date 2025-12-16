#include "executor/alias.h"

#include <stdexcept>

namespace dbms {

AliasOperator::AliasOperator(std::unique_ptr<Operator> child, std::string alias)
    : child_(std::move(child)), alias_(std::move(alias)) {}

void AliasOperator::init() {
    if (initialized_) {
        return;
    }
    if (!child_) {
        throw std::runtime_error("alias operator missing child");
    }
    child_->init();

    const Schema& childSchema = child_->getSchema();
    Schema renamed;
    for (std::size_t i = 0; i < childSchema.columnCount(); ++i) {
        ColumnInfo col = childSchema.getColumn(i);
        const std::string originalTable = col.tableName;
        if (!alias_.empty()) {
            col.tableName = alias_;
        }
        renamed.addColumn(col);
        if (!alias_.empty() && !originalTable.empty()) {
            renamed.addAlias(originalTable + "." + col.name, i);
        }
    }
    schema_ = std::make_shared<Schema>(renamed);
    initialized_ = true;
}

std::optional<Tuple> AliasOperator::next() {
    if (!initialized_) {
        throw std::logic_error("operator not initialized");
    }
    auto tuple = child_->next();
    if (!tuple) {
        return std::nullopt;
    }
    tuple->schema = schema_;
    return tuple;
}

void AliasOperator::close() {
    if (child_) {
        child_->close();
    }
    initialized_ = false;
}

void AliasOperator::reset() {
    if (child_) {
        child_->reset();
    }
    initialized_ = false;
}

} // namespace dbms

