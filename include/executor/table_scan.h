#pragma once

#include <memory>
#include <string>
#include <vector>

#include "executor/operator.h"
#include "system/database.h"

namespace dbms {

// Forward declarations
class DatabaseSystem;

// Table scan operator - sequential scan of all records in a table
class TableScanOperator : public Operator {
public:
    TableScanOperator(DatabaseSystem& db, const std::string& tableName);

    void init() override;
    std::optional<Tuple> next() override;
    void close() override;
    const Schema& getSchema() const override { return schema_; }
    void reset() override;

private:
    DatabaseSystem& db_;
    std::string tableName_;
    Schema schema_;

    // Iteration state
    const Table* table_;
    std::vector<BlockAddress> blocks_;
    std::size_t currentBlockIdx_;
    std::size_t currentSlotIdx_;
    std::size_t currentSlotCount_;
    bool initialized_;
    bool exhausted_;

    // Current block data (copied from buffer pool)
    std::vector<Record> currentBlockRecords_;

    // Helper methods
    void fetchNextBlock();
    Schema buildSchemaFromTable(const Table& table);
};

} // namespace dbms
