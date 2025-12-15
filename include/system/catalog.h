#pragma once

#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/types.h"
#include "index/index_manager.h"
#include "system/table.h"

namespace dbms {

class DataDictionary {
public:
    explicit DataDictionary(std::size_t capacityBytes)
        : capacityBytes_(capacityBytes) {}

    void registerTable(const TableSchema &schema) {
        TableInfo info;
        info.schema = schema;
        tables_[schema.name()] = info;
        recalcBytes();
    }

    void registerIndex(const IndexDefinition &definition,
                       std::size_t entriesPerPage) {
        IndexInfo info;
        info.definition = definition;
        info.entriesPerPage = entriesPerPage;
        indexes_[definition.name] = info;
        recalcBytes();
    }

    void dropIndex(const std::string &indexName) {
        indexes_.erase(indexName);
        recalcBytes();
    }

    void updateTableStats(const std::string &tableName,
                          std::size_t records,
                          std::size_t blocks) {
        auto it = tables_.find(tableName);
        if (it != tables_.end()) {
            it->second.recordCount = records;
            it->second.blockCount = blocks;
        }
    }

    std::size_t capacityBytes() const {
        return capacityBytes_;
    }

    std::size_t usedBytes() const {
        return usedBytes_;
    }

    std::string describe() const {
        std::ostringstream oss;
        oss << "Data dictionary usage: " << usedBytes_ << " / "
            << capacityBytes_ << " bytes\n";
        for (const auto &entry : tables_) {
            const auto &info = entry.second;
            oss << "  * " << info.schema.name() << " -> "
                << info.recordCount << " records in "
                << info.blockCount << " blocks\n";
        }
        if (!indexes_.empty()) {
            oss << "Index catalog (" << indexes_.size() << " index(es)):\n";
            for (const auto &entry : indexes_) {
                const auto &info = entry.second;
                oss << "  * " << info.definition.name << " ON "
                    << info.definition.tableName << "("
                    << info.definition.columnName << ") -> "
                    << info.entriesPerPage << " entry/entries per page\n";
            }
        }
        return oss.str();
    }

    std::vector<std::string> describeTables() const {
        std::vector<std::string> rows;
        for (const auto &entry : tables_) {
            const auto &info = entry.second;
            std::ostringstream oss;
            oss << "SYS_TABLES | " << info.schema.name()
                << " | columns=" << info.schema.columns().size()
                << " | recordSize=" << info.schema.recordSize();
            rows.push_back(oss.str());
        }
        return rows;
    }

    std::vector<std::string> describeIndexCatalog() const {
        std::vector<std::string> rows;
        for (const auto &entry : indexes_) {
            const auto &info = entry.second;
            std::ostringstream oss;
            oss << "SYS_INDEXES | " << info.definition.name
                << " | table=" << info.definition.tableName
                << " | column=" << info.definition.columnName
                << " | entries/page=" << info.entriesPerPage;
            rows.push_back(oss.str());
        }
        if (rows.empty()) {
            rows.push_back("SYS_INDEXES | [empty]");
        }
        return rows;
    }

private:
    struct TableInfo {
        TableSchema schema;
        std::size_t recordCount{0};
        std::size_t blockCount{0};
    };

    struct IndexInfo {
        IndexDefinition definition;
        std::size_t entriesPerPage{0};
    };

    void recalcBytes() {
        usedBytes_ = 0;
        for (const auto &entry : tables_) {
            const auto &schema = entry.second.schema;
            usedBytes_ += 128;
            usedBytes_ += schema.columns().size() * 64;
        }
        usedBytes_ += indexes_.size() * 96;
        if (usedBytes_ > capacityBytes_) {
            overflow_ = true;
        }
    }

    std::size_t capacityBytes_;
    std::size_t usedBytes_{0};
    bool overflow_{false};
    std::unordered_map<std::string, TableInfo> tables_;
    std::unordered_map<std::string, IndexInfo> indexes_;
};

} // namespace dbms
