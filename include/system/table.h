#pragma once

#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/types.h"

namespace dbms {

class TableSchema {
public:
    TableSchema() = default;

    TableSchema(std::string name, std::vector<ColumnDefinition> columns)
        : name_(std::move(name)), columns_(std::move(columns)) {
        if (columns_.empty()) {
            throw std::invalid_argument("schema must contain at least one column");
        }
        recordSize_ = 0;
        for (const auto &col : columns_) {
            if (col.length == 0) {
                throw std::invalid_argument("column length must be positive");
            }
            recordSize_ += col.length;
        }
    }

    const std::string &name() const {
        return name_;
    }

    const std::vector<ColumnDefinition> &columns() const {
        return columns_;
    }

    std::size_t recordSize() const {
        return recordSize_;
    }

    std::string describe() const {
        std::ostringstream oss;
        oss << "Table " << name_ << " (record size: " << recordSize_ << " bytes)\n";
        for (const auto &col : columns_) {
            oss << "  - " << col.name << " [";
            switch (col.type) {
            case ColumnType::Integer:
                oss << "INT";
                break;
            case ColumnType::Double:
                oss << "DOUBLE";
                break;
            case ColumnType::String:
                oss << "STRING";
                break;
            }
            oss << ", " << col.length << " bytes]\n";
        }
        return oss.str();
    }

private:
    std::string name_;
    std::vector<ColumnDefinition> columns_;
    std::size_t recordSize_{0};
};

class Table {
public:
    Table() = default;

    Table(TableSchema schema, std::size_t pageSizeBytes)
        : schema_(std::move(schema)),
          pageSizeBytes_(pageSizeBytes) {
        if (pageSizeBytes_ == 0) {
            throw std::invalid_argument("page size must be positive");
        }
    }

    const TableSchema &schema() const {
        return schema_;
    }

    std::size_t pageSizeBytes() const {
        return pageSizeBytes_;
    }

    std::size_t totalRecords() const {
        return totalRecords_;
    }

    std::size_t blockCount() const {
        return blocks_.size();
    }

    const std::vector<BlockAddress> &blocks() const {
        return blocks_;
    }

    void addBlock(const BlockAddress &addr) {
        blocks_.push_back(addr);
    }

    void addExistingBlock(const BlockAddress &addr, std::size_t recordCount) {
        blocks_.push_back(addr);
        totalRecords_ += recordCount;
    }

    BlockAddress lastBlock() const {
        if (blocks_.empty()) {
            throw std::logic_error("table has no blocks");
        }
        return blocks_.back();
    }

    void incrementRecords() {
        ++totalRecords_;
    }

    void decrementRecords() {
        if (totalRecords_ == 0) {
            throw std::logic_error("table record count underflow");
        }
        --totalRecords_;
    }

private:
    TableSchema schema_;
    std::size_t pageSizeBytes_{0};
    std::vector<BlockAddress> blocks_;
    std::size_t totalRecords_{0};
};

} // namespace dbms
