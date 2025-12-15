#include "executor/index_scan.h"

#include <stdexcept>

namespace dbms {

IndexScanOperator::IndexScanOperator(DatabaseSystem& db,
                                     std::string table,
                                     std::string index,
                                     std::string key)
    : db_(db),
      tableName_(std::move(table)),
      indexName_(std::move(index)),
      searchKey_(std::move(key)) {}

void IndexScanOperator::init() {
    if (initialized_) {
        return;
    }
    const auto& table = db_.getTable(tableName_);
    schema_ = buildSchemaFromTable(table);
    done_ = false;
    initialized_ = true;
}

std::optional<Tuple> IndexScanOperator::next() {
    if (!initialized_) {
        throw std::logic_error("operator not initialized");
    }
    if (done_) {
        return std::nullopt;
    }

    auto ptr = db_.searchIndex(indexName_, searchKey_);
    done_ = true;
    if (!ptr.has_value()) {
        return std::nullopt;
    }
    auto record = db_.readRecord(ptr->address, ptr->slot);
    if (!record.has_value()) {
        return std::nullopt;
    }

    Tuple tuple;
    tuple.values = std::move(record->values);
    tuple.schema = std::make_shared<Schema>(schema_);
    return tuple;
}

void IndexScanOperator::close() {
    initialized_ = false;
    done_ = true;
}

void IndexScanOperator::reset() {
    done_ = false;
    initialized_ = false;
}

Schema IndexScanOperator::buildSchemaFromTable(const Table& table) {
    Schema schema;
    const auto& tableName = table.schema().name();
    const auto& columns = table.schema().columns();
    for (std::size_t i = 0; i < columns.size(); ++i) {
        ColumnInfo col;
        col.name = columns[i].name;
        col.type = columns[i].type;
        col.sourceIndex = i;
        col.tableName = tableName;
        schema.addColumn(col);
    }
    return schema;
}

} // namespace dbms
