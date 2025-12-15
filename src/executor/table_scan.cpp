#include "executor/table_scan.h"

namespace dbms {

TableScanOperator::TableScanOperator(DatabaseSystem& db, const std::string& tableName)
    : db_(db),
      tableName_(tableName),
      table_(nullptr),
      currentBlockIdx_(0),
      currentSlotIdx_(0),
      currentSlotCount_(0),
      initialized_(false),
      exhausted_(false) {}

void TableScanOperator::init() {
    if (initialized_) {
        return;
    }

    table_ = &db_.getTable(tableName_);
    schema_ = buildSchemaFromTable(*table_);
    blocks_ = table_->blocks();
    currentBlockIdx_ = 0;
    currentSlotIdx_ = 0;
    currentSlotCount_ = 0;
    exhausted_ = false;
    currentBlockRecords_.clear();

    initialized_ = true;
}

std::optional<Tuple> TableScanOperator::next() {
    if (!initialized_) {
        throw std::logic_error("operator not initialized");
    }

    if (exhausted_) {
        return std::nullopt;
    }

    // If we've exhausted the current block, fetch the next one
    while (currentSlotIdx_ >= currentSlotCount_) {
        if (currentBlockIdx_ >= blocks_.size()) {
            exhausted_ = true;
            return std::nullopt;
        }
        fetchNextBlock();
    }

    // Get current record
    if (currentSlotIdx_ < currentBlockRecords_.size()) {
        const Record& record = currentBlockRecords_[currentSlotIdx_];
        ++currentSlotIdx_;

        // Convert Record to Tuple
        Tuple tuple;
        tuple.values = record.values;
        tuple.schema = std::make_shared<Schema>(schema_);
        return tuple;
    }

    exhausted_ = true;
    return std::nullopt;
}

void TableScanOperator::close() {
    // Nothing to clean up (buffer pool manages blocks)
    initialized_ = false;
}

void TableScanOperator::reset() {
    currentBlockIdx_ = 0;
    currentSlotIdx_ = 0;
    currentSlotCount_ = 0;
    exhausted_ = false;
    currentBlockRecords_.clear();
    initialized_ = false;
}

void TableScanOperator::fetchNextBlock() {
    if (currentBlockIdx_ >= blocks_.size()) {
        currentSlotCount_ = 0;
        currentBlockRecords_.clear();
        return;
    }

    const BlockAddress& addr = blocks_[currentBlockIdx_];
    auto fetchResult = db_.buffer().fetch(addr, false);  // Read-only
    fetchResult.block.ensureInitialized(db_.blockSize());

    // Extract all records from the block
    currentBlockRecords_.clear();
    fetchResult.block.page.forEachRecord(
        [this](std::size_t slotIdx, const Record& record) {
            (void)slotIdx;  // Unused
            currentBlockRecords_.push_back(record);
        });

    currentSlotCount_ = currentBlockRecords_.size();
    currentSlotIdx_ = 0;
    ++currentBlockIdx_;
}

Schema TableScanOperator::buildSchemaFromTable(const Table& table) {
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
