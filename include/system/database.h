#pragma once

#include <algorithm>
#include <cstddef>
#include <deque>
#include <fstream>
#include <iostream>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/types.h"
#include "common/utils.h"
#include "index/index_manager.h"
#include "storage/buffer_pool.h"
#include "storage/disk_manager.h"
#include "storage/write_ahead_log.h"
#include "system/catalog.h"
#include "system/table.h"
#include "parser/query_processor.h"

namespace dbms {

    class DatabaseSystem {
        class ScopedFlagGuard {
        public:
            ScopedFlagGuard(bool &flag, bool value) : flag_(flag), previous_(flag) {
                flag_ = value;
            }

            ~ScopedFlagGuard() {
                flag_ = previous_;
            }

        private:
            bool &flag_;
            bool previous_;
        };

        enum class UndoType { Insert, Delete, Update };

        struct UndoEntry {
            UndoType type;
            BlockAddress address;
            std::size_t slot{0};
            std::optional<Record> before;
            std::optional<Record> after;
        };

        struct WalContext {
            std::size_t txnId{0};
            bool implicit{false};
            bool active{false};
        };

    public:
        struct TableDumpRow {
            std::size_t blockIndex{0};
            std::size_t slotIndex{0};
            std::vector<std::string> values;
        };

        struct TableDumpResult {
            std::vector<TableDumpRow> rows;
            std::size_t totalRecords{0};
            std::size_t blocksAccessed{0};
            std::size_t recordsSkipped{0};
            bool truncated{false};
        };
        DatabaseSystem(std::size_t blockSizeBytes,
                       std::size_t mainMemoryBytes,
                       std::size_t diskBytes)
            : blockSize_(blockSizeBytes),
              mainMemoryBytes_(mainMemoryBytes),
              diskBytes_(diskBytes),
              storagePath_("storage"),
              disk_(computeDiskBlocks(diskBytes, blockSizeBytes),
                    storagePath_,
                    blockSizeBytes),
              buffer_(computeBufferCapacity(mainMemoryBytes, blockSizeBytes), disk_),
              dictionary_(static_cast<std::size_t>(mainMemoryBytes * 0.15)),
          planCache_(static_cast<std::size_t>(mainMemoryBytes * 0.15),
                     planCacheFilePath(storagePath_)),
          logBuffer_(static_cast<std::size_t>(mainMemoryBytes * 0.10),
                     logFilePath(storagePath_)),
          wal_(walFilePath(storagePath_)),
          indexCatalogFile_(indexCatalogFilePath(storagePath_)),
          rng_(std::random_device{}()) {
        if (blockSize_ == 0) {
            throw std::invalid_argument("block size must be positive");
        }
        if (mainMemoryBytes_ < blockSize_) {
            throw std::invalid_argument("main memory must be at least one block");
        }
        computePartitions();
        loadIndexCatalogFromDisk();
        pendingWalEntries_ = wal_.load();
        std::size_t maxWalTxn = 0;
        for (const auto &entry : pendingWalEntries_) {
            if (entry.type == WriteAheadLog::EntryType::Insert ||
                entry.type == WriteAheadLog::EntryType::Delete ||
                entry.type == WriteAheadLog::EntryType::Update) {
                walTables_.insert(entry.address.table);
            }
            if (entry.txnId > maxWalTxn) {
                maxWalTxn = entry.txnId;
            }
        }
        if (maxWalTxn >= nextTxnId_) {
            nextTxnId_ = maxWalTxn + 1;
        }
        if (pendingWalEntries_.empty()) {
            recoveryPerformed_ = true;
        }
    }

        void executeSQL(const std::string &sql) {
            QueryProcessor processor(*this);
            processor.processQuery(sql);
        }


        void registerTable(const TableSchema &schema) {
            const std::size_t minimalPayload =
                VariableLengthPage::kRecordHeaderBytes +
                schema.columns().size() * sizeof(std::uint32_t);
            const std::size_t minimalFootprint =
                minimalPayload + VariableLengthPage::kSlotOverheadBytes;
            if (blockSize_ < minimalFootprint) {
                std::ostringstream oss;
                oss << "block size " << blockSize_
                    << " bytes is insufficient for table " << schema.name()
                    << " (requires at least " << minimalFootprint << " bytes)";
                throw std::runtime_error(oss.str());
            }
            Table table(schema, blockSize_);
            dictionary_.registerTable(schema);
            auto existingBlocks = disk_.loadExistingBlocks(schema.name());
            for (const auto &block : existingBlocks) {
                table.addExistingBlock(block.address, block.recordCount());
            }
        auto [it, inserted] = tables_.emplace(schema.name(), std::move(table));
        (void)inserted;
        dictionary_.updateTableStats(schema.name(),
                                     it->second.totalRecords(),
                                     it->second.blockCount());
        restoreIndexesForTable(schema.name());
        recoverFromWalIfNeeded();
    }


        const Table &getTable(const std::string &name) const {
            auto it = tables_.find(name);
            if (it == tables_.end()) {
                throw std::out_of_range("unknown table: " + name);
            }
            return it->second;
        }

        Table &getTable(const std::string &name) {
            auto it = tables_.find(name);
            if (it == tables_.end()) {
                throw std::out_of_range("unknown table: " + name);
            }
            return it->second;
        }

        std::size_t blockSize() const {
            return blockSize_;
        }

        std::size_t diskBlocks() const {
            return disk_.totalBlocks();
        }

        std::size_t freeDiskBlocks() const {
            return disk_.freeBlocks();
        }

        BufferPool &buffer() {
            return buffer_;
        }

        bool inTransaction() const {
            return transactionActive_;
        }

        void beginTransaction() {
            if (transactionActive_) {
                throw std::runtime_error("transaction already in progress");
            }
            currentTxnId_ = nextTxnId_++;
            transactionActive_ = true;
            undoLog_.clear();
            if (!suppressWal_) {
                wal_.logBegin(*currentTxnId_);
            }
            logBuffer_.append("begin");
        }

        void commitTransaction() {
            if (!transactionActive_) {
                throw std::runtime_error("no active transaction to commit");
            }
            undoLog_.clear();
            if (!suppressWal_ && currentTxnId_.has_value()) {
                wal_.logCommit(*currentTxnId_);
            }
            transactionActive_ = false;
            currentTxnId_.reset();
            logBuffer_.append("commit");
            logBuffer_.flushToDisk();
            buffer_.flush();
        }

        void rollbackTransaction() {
            if (!transactionActive_) {
                throw std::runtime_error("no active transaction to rollback");
            }
            if (!suppressWal_ && currentTxnId_.has_value()) {
                wal_.logRollback(*currentTxnId_);
            }
            {
                ScopedFlagGuard undoGuard(suppressUndo_, true);
                ScopedFlagGuard applyingGuard(applyingUndo_, true);
                ScopedFlagGuard walGuard(suppressWal_, true);
                for (auto it = undoLog_.rbegin(); it != undoLog_.rend(); ++it) {
                    applyUndo(*it);
                }
            }
            undoLog_.clear();
            transactionActive_ = false;
            currentTxnId_.reset();
            logBuffer_.append("rollback");
            logBuffer_.flushToDisk();
            buffer_.flush();
        }


    void insertRecord(const std::string &tableName, Record record) {
        auto walCtx = startWalContext();
        bool walSuccess = false;
        try {
            auto &table = getTable(tableName);
            ensureRecordFits(table.schema(), record);
            enforceUniqueKeys(tableName, record, nullptr, std::nullopt);
            const std::size_t footprint =
                VariableLengthPage::estimatePayload(record) +
                VariableLengthPage::kSlotOverheadBytes;
            if (footprint > blockSize_) {
                std::ostringstream oss;
                oss << "record does not fit into a single block (requires "
                    << footprint << " bytes, block size is " << blockSize_ << ")";
                throw std::runtime_error(oss.str());
            }
            if (table.blocks().empty()) {
                auto addr = disk_.allocateBlock(tableName);
                table.addBlock(addr);
            }

            auto fetchResult = buffer_.fetch(table.lastBlock(), true);
            fetchResult.block.ensureInitialized(blockSize_);
            Block *targetBlock = &fetchResult.block;
            if (!targetBlock->hasSpaceFor(record)) {
                auto addr = disk_.allocateBlock(tableName);
                table.addBlock(addr);
                auto newFetch = buffer_.fetch(addr, true);
                newFetch.block.ensureInitialized(blockSize_);
                targetBlock = &newFetch.block;
                if (!targetBlock->hasSpaceFor(record)) {
                    std::ostringstream oss;
                    oss << "record cannot be placed even in an empty block for "
                        << tableName;
                    throw std::runtime_error(oss.str());
                }
            }
            auto slotId = targetBlock->insertRecord(std::move(record));
            if (!slotId.has_value()) {
                std::ostringstream oss;
                oss << "failed to insert record into block " << targetBlock->address.table
                    << "#" << targetBlock->address.index;
                throw std::runtime_error(oss.str());
            }
            const Record *stored = targetBlock->getRecord(*slotId);
            if (stored) {
                try {
                    applyIndexInsert(tableName, *stored, targetBlock->address, *slotId);
                } catch (...) {
                    targetBlock->eraseRecord(*slotId);
                    throw;
                }
            }
            table.incrementRecords();
            dictionary_.updateTableStats(tableName,
                                         table.totalRecords(),
                                         table.blockCount());
            if (transactionActive_ && !suppressUndo_) {
                UndoEntry entry;
                entry.type = UndoType::Insert;
                entry.address = targetBlock->address;
                entry.slot = *slotId;
                if (stored) {
                    entry.after = *stored;
                }
                undoLog_.push_back(std::move(entry));
            }
            if (!applyingUndo_) {
                planCache_.recordPlan("INSERT INTO " + tableName);
                logBuffer_.append("insert into " + tableName);
            }
            if (walCtx.active && !suppressWal_ && stored) {
                wal_.logInsert(walCtx.txnId, targetBlock->address, *slotId, *stored);
            }
            persistIndexesForTable(tableName);
            walSuccess = true;
        } catch (...) {
            finishWalContext(walCtx, false);
            throw;
        }
        finishWalContext(walCtx, walSuccess);
    }

        std::optional<Record> readRecord(const BlockAddress &addr,
                                         std::size_t slotIndex) {
            (void)getTable(addr.table);
            auto fetchResult = buffer_.fetch(addr, false);
            fetchResult.block.ensureInitialized(blockSize_);
            const Record *recordPtr = fetchResult.block.getRecord(slotIndex);
            if (!recordPtr) {
                return std::nullopt;
            }
            planCache_.recordPlan("SELECT FROM " + addr.table);
            logBuffer_.append("select from " + addr.table);
            return *recordPtr;
        }

    bool updateRecord(const BlockAddress &addr,
                      std::size_t slotIndex,
                      Record record) {
        auto walCtx = startWalContext();
        bool walSuccess = false;
        bool success = false;
        try {
            auto &table = getTable(addr.table);
            ensureRecordFits(table.schema(), record);
            enforceUniqueKeys(addr.table, record, &addr, slotIndex);
            const std::size_t footprint =
                VariableLengthPage::estimatePayload(record) +
                VariableLengthPage::kSlotOverheadBytes;
            if (footprint > blockSize_) {
                std::ostringstream oss;
                oss << "updated record exceeds block capacity (requires "
                    << footprint << " bytes, block size " << blockSize_ << ")";
                throw std::runtime_error(oss.str());
            }
            auto fetchResult = buffer_.fetch(addr, true);
            fetchResult.block.ensureInitialized(blockSize_);
            const Record *beforePtr = fetchResult.block.getRecord(slotIndex);
            if (!beforePtr) {
                success = false;
            } else {
                Record before = *beforePtr;
                Record newRecordCopy = record;
                success = fetchResult.block.updateRecord(slotIndex, std::move(record));
                if (success) {
                    applyIndexUpdate(addr.table, before, newRecordCopy, addr, slotIndex);
                    if (transactionActive_ && !suppressUndo_) {
                        UndoEntry entry;
                        entry.type = UndoType::Update;
                        entry.address = addr;
                        entry.slot = slotIndex;
                        entry.before = before;
                        undoLog_.push_back(std::move(entry));
                    }
                    if (!applyingUndo_) {
                        planCache_.recordPlan("UPDATE " + addr.table);
                        logBuffer_.append("update " + addr.table);
                    }
                    if (walCtx.active && !suppressWal_) {
                        wal_.logUpdate(walCtx.txnId, addr, slotIndex, before, newRecordCopy);
                    }
                    persistIndexesForTable(addr.table);
                    walSuccess = true;
                }
            }
        } catch (...) {
            finishWalContext(walCtx, false);
            throw;
        }
        finishWalContext(walCtx, walSuccess);
        return success;
    }

    bool deleteRecord(const BlockAddress &addr, std::size_t slotIndex) {
        auto walCtx = startWalContext();
        bool walSuccess = false;
        bool success = false;
        try {
            auto &table = getTable(addr.table);
            auto fetchResult = buffer_.fetch(addr, true);
            fetchResult.block.ensureInitialized(blockSize_);
            std::optional<Record> before;
            if (const Record *recordPtr = fetchResult.block.getRecord(slotIndex)) {
                before = *recordPtr;
            }
            success = fetchResult.block.eraseRecord(slotIndex);
            if (success) {
                if (before.has_value()) {
                    applyIndexDelete(addr.table, *before);
                    if (transactionActive_ && !suppressUndo_) {
                        UndoEntry entry;
                        entry.type = UndoType::Delete;
                        entry.address = addr;
                        entry.slot = slotIndex;
                        entry.before = *before;
                        undoLog_.push_back(std::move(entry));
                    }
                    if (walCtx.active && !suppressWal_) {
                        wal_.logDelete(walCtx.txnId, addr, slotIndex, *before);
                    }
                }
                table.decrementRecords();
                dictionary_.updateTableStats(addr.table,
                                             table.totalRecords(),
                                             table.blockCount());
                if (!applyingUndo_) {
                    planCache_.recordPlan("DELETE FROM " + addr.table);
                    logBuffer_.append("delete from " + addr.table);
                }
                persistIndexesForTable(addr.table);
                walSuccess = true;
            }
        } catch (...) {
            finishWalContext(walCtx, false);
            throw;
        }
        finishWalContext(walCtx, walSuccess);
        return success;
    }


        struct VacuumReport {
            std::string tableName;
            std::size_t blocksVisited{0};
            std::size_t blocksModified{0};
            std::size_t slotsCleared{0};
            std::size_t bytesReclaimed{0};
            std::size_t blocksNowEmpty{0};
        };

        VacuumReport vacuumTable(const std::string &tableName) {
            VacuumReport report;
            report.tableName = tableName;
            auto &table = getTable(tableName);
            for (const auto &addr : table.blocks()) {
                auto fetchResult = buffer_.fetch(addr, true);
                fetchResult.block.ensureInitialized(blockSize_);
                ++report.blocksVisited;
                const bool hadGarbageOnly =
                    (fetchResult.block.recordCount() == 0 && fetchResult.block.deletedCount() > 0);
                const auto stats = fetchResult.block.vacuumDeletedSlots();
                if (stats.clearedSlots > 0) {
                    ++report.blocksModified;
                    report.slotsCleared += stats.clearedSlots;
                    report.bytesReclaimed += stats.reclaimedBytes;
                    if (hadGarbageOnly && fetchResult.block.recordCount() == 0) {
                        ++report.blocksNowEmpty;
                    }
                }
            }
            if (report.blocksModified > 0) {
                dictionary_.updateTableStats(tableName,
                                             table.totalRecords(),
                                             table.blockCount());
                planCache_.recordPlan("VACUUM " + tableName);
                logBuffer_.append("vacuum " + tableName);
            }
            return report;
        }

        std::vector<VacuumReport> vacuumAllTables() {
            std::vector<VacuumReport> reports;
            reports.reserve(tables_.size());
            for (const auto &entry : tables_) {
                reports.push_back(vacuumTable(entry.first));
            }
            return reports;
        }


        BufferPool::FetchResult accessBlock(const BlockAddress &addr,
                                            bool forWrite,
                                            const std::string &planText) {
            planCache_.recordPlan(planText);
            auto result = buffer_.fetch(addr, forWrite);
            logBuffer_.append("access block " + addr.table + "#" + std::to_string(addr.index));
            return result;
        }


        void flushAll() {
            buffer_.flush();
            logBuffer_.flushToDisk();
        }


        std::string memoryLayoutDescription() const {
            std::ostringstream oss;
            oss << "Memory layout (bytes):\n";
            oss << "  - Access plans: " << accessPlanBytes_ << "\n";
            oss << "  - Data dictionary: " << dictionaryBytes_ << "\n";
            oss << "  - Data buffer: " << bufferBytes_ << " ("
                << buffer_.capacity() << " frame(s))\n";
            oss << "  - Log buffer: " << logBufferBytes_ << "\n";
            oss << dictionary_.describe();
            oss << planCache_.describe();
            oss << logBuffer_.describe() << "\n";
            return oss.str();
        }


        std::vector<std::string> tableSummaries() const {
            std::vector<std::string> summaries;
            for (const auto &entry : tables_) {
                const auto &table = entry.second;
                std::ostringstream oss;
                oss << table.schema().describe();
                oss << "  Records: " << table.totalRecords()
                    << " spanning " << table.blockCount() << " blocks\n";
                summaries.push_back(oss.str());
            }
            return summaries;
        }

        std::vector<std::string> systemCatalogRows() const {
            auto rows = dictionary_.describeTables();
            auto indexRows = dictionary_.describeIndexCatalog();
            rows.insert(rows.end(), indexRows.begin(), indexRows.end());
            return rows;
        }

    std::vector<std::string> indexSummaries() const {
        std::vector<std::string> rows;
        for (const auto &entry : indexes_) {
            const auto &def = entry.second.definition();
            std::ostringstream oss;
            oss << def.name << " ON " << def.tableName << "("
                << def.columnName << ") | entries/page="
                << entry.second.entriesPerPage();
            rows.push_back(oss.str());
        }
        return rows;
    }

        std::vector<std::string> describeIndexFile(const std::string &indexName) const {
            auto it = indexes_.find(indexName);
            if (it == indexes_.end()) {
                throw std::out_of_range("unknown index: " + indexName);
            }
            return it->second.describePages();
        }

        std::vector<std::string> createIndex(const std::string &indexName,
                                             const std::string &tableName,
                                             const std::string &columnName) {
            if (indexes_.find(indexName) != indexes_.end()) {
                throw std::runtime_error("index already exists: " + indexName);
            }
            auto tableIt = tables_.find(tableName);
            if (tableIt == tables_.end()) {
                throw std::out_of_range("unknown table: " + tableName);
            }
            const auto &schema = tableIt->second.schema();
            const auto &columns = schema.columns();
            auto colIt = std::find_if(
                columns.begin(), columns.end(),
                [&](const ColumnDefinition &col) {
                    return col.name == columnName;
                });
            if (colIt == columns.end()) {
                throw std::runtime_error("unknown column '" + columnName +
                                         "' on table " + tableName);
            }
            const std::size_t columnIndex =
                static_cast<std::size_t>(std::distance(columns.begin(), colIt));
            IndexDefinition definition;
            definition.name = indexName;
            definition.tableName = tableName;
            definition.columnName = columnName;
            definition.columnIndex = columnIndex;
            definition.keyLength = colIt->length;
            definition.unique = false;
            BPlusTreeIndex index(definition, blockSize_);
            auto entries =
                collectIndexEntries(tableName, columnIndex, definition.keyLength);
            std::sort(entries.begin(), entries.end(),
                      [](const auto &a, const auto &b) { return a.first < b.first; });
            entries.erase(std::unique(entries.begin(), entries.end(),
                                      [](const auto &a, const auto &b) {
                                          return a.first == b.first;
                                      }),
                          entries.end());
            index.rebuild(entries);
            auto insertResult = indexes_.emplace(indexName, std::move(index));
            auto &perTable = indexesByTable_[tableName];
            if (std::find(perTable.begin(), perTable.end(), indexName) == perTable.end()) {
                perTable.push_back(indexName);
            }
            dictionary_.registerIndex(definition,
                                      insertResult.first->second.entriesPerPage());
            indexDefinitions_[definition.name] = definition;
            removePendingIndex(tableName, definition.name);
            persistIndexCatalog();
            persistIndex(definition.name);
            planCache_.recordPlan("CREATE INDEX " + indexName + " ON " + tableName);
            logBuffer_.append("create index " + indexName + " on " + tableName);
            return insertResult.first->second.describePages();
        }

        std::optional<std::string> findIndexForColumn(const std::string &tableName,
                                                      const std::string &columnName) const {
            auto binding = indexesByTable_.find(tableName);
            if (binding == indexesByTable_.end()) {
                return std::nullopt;
            }
            for (const auto &indexName : binding->second) {
                auto defIt = indexDefinitions_.find(indexName);
                if (defIt == indexDefinitions_.end()) {
                    continue;
                }
                if (defIt->second.columnName == columnName) {
                    return indexName;
                }
            }
            return std::nullopt;
        }

        std::optional<IndexPointer> searchIndex(const std::string &indexName,
                                                const std::string &key) const {
            auto it = indexes_.find(indexName);
            if (it == indexes_.end()) {
                throw std::out_of_range("unknown index: " + indexName);
            }
            return it->second.find(key);
        }

        TableDumpResult dumpTable(const std::string &tableName,
                                  std::size_t limit = 0,
                                  std::size_t offset = 0) {
            auto &table = getTable(tableName);
            TableDumpResult result;
            result.totalRecords = table.totalRecords();
            planCache_.recordPlan("SCAN " + tableName);
            logBuffer_.append("scan " + tableName);
            std::size_t skipped = 0;
            std::size_t accessed = 0;
            for (const auto &addr : table.blocks()) {
                auto fetchResult = buffer_.fetch(addr, false);
                fetchResult.block.ensureInitialized(blockSize_);
                ++accessed;
                fetchResult.block.page.forEachRecord(
                    [&](std::size_t slotIdx, const Record &record) {
                        if (offset > 0 && skipped < offset) {
                            ++skipped;
                            return;
                        }
                        if (limit != 0 && result.rows.size() >= limit) {
                            return;
                        }
                        TableDumpRow row;
                        row.blockIndex = addr.index;
                        row.slotIndex = slotIdx;
                        row.values = record.values;
                        result.rows.push_back(std::move(row));
                    });
                if (limit != 0 && result.rows.size() >= limit) {
                    break;
                }
            }
            result.blocksAccessed = accessed;
            result.recordsSkipped = skipped;
            if (limit != 0 && (offset + result.rows.size()) < result.totalRecords) {
                result.truncated = true;
            }
            return result;
        }
        std::vector<std::string> cachedAccessPlans(std::size_t limit = 0) const {
            return planCache_.recentPlans(limit);
        }

        std::vector<std::string> persistedAccessPlans(std::size_t limit) const {
            return planCache_.persistedPlans(limit);
        }

        std::size_t totalPersistedAccessPlans() const {
            return planCache_.persistedCount();
        }

        std::vector<std::string> bufferedLogs() const {
            return logBuffer_.bufferedEntries();
        }

        std::vector<std::string> persistedLogs(std::size_t limit) const {
            return logBuffer_.persistedEntries(limit);
        }

        std::size_t totalPersistedLogs() const {
            return logBuffer_.persistedCount();
        }

        const DiskStorage &disk() const {
            return disk_;
        }

        std::mt19937 &rng() {
            return rng_;
        }

    private:
        WalContext startWalContext() {
            WalContext ctx;
            if (suppressWal_) {
                return ctx;
            }
            if (transactionActive_) {
                if (!currentTxnId_.has_value()) {
                    currentTxnId_ = nextTxnId_++;
                    wal_.logBegin(*currentTxnId_);
                }
                ctx.txnId = *currentTxnId_;
                ctx.implicit = false;
                ctx.active = true;
            } else {
                ctx.txnId = nextTxnId_++;
                ctx.implicit = true;
                ctx.active = true;
                wal_.logBegin(ctx.txnId);
            }
            return ctx;
        }

        void finishWalContext(const WalContext &ctx, bool success) {
            if (suppressWal_ || !ctx.active || !ctx.implicit) {
                return;
            }
            if (success) {
                wal_.logCommit(ctx.txnId);
            } else {
                wal_.logRollback(ctx.txnId);
            }
        }

        bool isWalDataEntry(const WriteAheadLog::Entry &entry) const {
            return entry.type == WriteAheadLog::EntryType::Insert ||
                   entry.type == WriteAheadLog::EntryType::Delete ||
                   entry.type == WriteAheadLog::EntryType::Update;
        }

        bool allWalTablesRegistered() const {
            for (const auto &name : walTables_) {
                if (tables_.find(name) == tables_.end()) {
                    return false;
                }
            }
            return true;
        }

        void recoverFromWalIfNeeded() {
            if (recoveryPerformed_) {
                return;
            }
            if (pendingWalEntries_.empty()) {
                wal_.clear();
                recoveryPerformed_ = true;
                return;
            }
            if (!allWalTablesRegistered()) {
                return;
            }
            performCrashRecovery();
        }

        void performCrashRecovery() {
            std::unordered_map<std::size_t, bool> committed;
            for (const auto &entry : pendingWalEntries_) {
                if (entry.type == WriteAheadLog::EntryType::Begin) {
                    committed.emplace(entry.txnId, false);
                } else if (entry.type == WriteAheadLog::EntryType::Commit) {
                    committed[entry.txnId] = true;
                } else if (entry.type == WriteAheadLog::EntryType::Rollback) {
                    committed[entry.txnId] = false;
                }
            }

            ScopedFlagGuard undoGuard(suppressUndo_, true);
            ScopedFlagGuard applyingGuard(applyingUndo_, true);
            ScopedFlagGuard walGuard(suppressWal_, true);

            for (const auto &entry : pendingWalEntries_) {
                if (isWalDataEntry(entry) && committed[entry.txnId]) {
                    applyWalRedo(entry);
                }
            }
            for (auto it = pendingWalEntries_.rbegin(); it != pendingWalEntries_.rend(); ++it) {
                if (isWalDataEntry(*it) && !committed[it->txnId]) {
                    applyWalUndo(*it);
                }
            }
            buffer_.flush();
            for (const auto &binding : indexesByTable_) {
                for (const auto &indexName : binding.second) {
                    persistIndex(indexName);
                }
            }
            wal_.clear();
            pendingWalEntries_.clear();
            walTables_.clear();
            recoveryPerformed_ = true;
        }

        void applyWalRedo(const WriteAheadLog::Entry &entry) {
            try {
                switch (entry.type) {
                case WriteAheadLog::EntryType::Insert: {
                    if (!entry.after.has_value()) {
                        return;
                    }
                    BlockAddress addrFound;
                    std::size_t slotFound{0};
                    if (findRecord(entry.address.table, *entry.after, addrFound, slotFound)) {
                        return;
                    }
                    insertRecord(entry.address.table, *entry.after);
                    break;
                }
                case WriteAheadLog::EntryType::Delete: {
                    if (!entry.before.has_value()) {
                        return;
                    }
                    BlockAddress addrFound;
                    std::size_t slotFound{0};
                    if (findRecord(entry.address.table, *entry.before, addrFound, slotFound)) {
                        deleteRecord(addrFound, slotFound);
                    }
                    break;
                }
                case WriteAheadLog::EntryType::Update: {
                    if (!entry.after.has_value()) {
                        return;
                    }
                    BlockAddress addrFound = entry.address;
                    std::size_t slotFound = entry.slot;
                    bool located = false;
                    if (entry.before.has_value()) {
                        located = findRecord(entry.address.table, *entry.before, addrFound, slotFound);
                    }
                    if (!located) {
                        if (disk_.contains(entry.address)) {
                            auto fetch = buffer_.fetch(entry.address, false);
                            fetch.block.ensureInitialized(blockSize_);
                            if (fetch.block.getRecord(entry.slot)) {
                                located = true;
                                addrFound = entry.address;
                                slotFound = entry.slot;
                            }
                        }
                    }
                    if (located) {
                        if (!updateRecord(addrFound, slotFound, *entry.after)) {
                            insertRecord(entry.address.table, *entry.after);
                        }
                    } else {
                        insertRecord(entry.address.table, *entry.after);
                    }
                    break;
                }
                default:
                    break;
                }
            } catch (const std::exception &ex) {
                std::cerr << "WAL redo skipped entry: " << ex.what() << "\n";
            }
        }

        void applyWalUndo(const WriteAheadLog::Entry &entry) {
            UndoEntry undo;
            switch (entry.type) {
            case WriteAheadLog::EntryType::Insert:
                undo.type = UndoType::Insert;
                undo.address = entry.address;
                undo.slot = entry.slot;
                undo.after = entry.after;
                break;
            case WriteAheadLog::EntryType::Delete:
                undo.type = UndoType::Delete;
                undo.address = entry.address;
                undo.slot = entry.slot;
                undo.before = entry.before;
                break;
            case WriteAheadLog::EntryType::Update:
                undo.type = UndoType::Update;
                undo.address = entry.address;
                undo.slot = entry.slot;
                undo.before = entry.before;
                break;
            default:
                return;
            }
            applyUndo(undo);
        }

        bool findRecord(const std::string &tableName,
                        const Record &record,
                        BlockAddress &addrOut,
                        std::size_t &slotOut) {
            auto &table = getTable(tableName);
            for (const auto &addr : table.blocks()) {
                auto fetchResult = buffer_.fetch(addr, false);
                fetchResult.block.ensureInitialized(blockSize_);
                const auto slots = fetchResult.block.slotCount();
                for (std::size_t i = 0; i < slots; ++i) {
                    const Record *candidate = fetchResult.block.getRecord(i);
                    if (candidate && candidate->values == record.values) {
                        addrOut = addr;
                        slotOut = i;
                        return true;
                    }
                }
            }
            return false;
        }

        void applyUndo(const UndoEntry &entry) {
            switch (entry.type) {
            case UndoType::Insert: {
                bool removed = deleteRecord(entry.address, entry.slot);
                if (!removed && entry.after.has_value()) {
                    removeMatchingRecord(entry.address.table, *entry.after);
                }
                break;
            }
            case UndoType::Delete: {
                if (entry.before.has_value()) {
                    if (!restoreDeletedRecord(entry.address, entry.slot, *entry.before)) {
                        insertRecord(entry.address.table, *entry.before);
                    }
                }
                break;
            }
            case UndoType::Update: {
                if (entry.before.has_value()) {
                    updateRecord(entry.address, entry.slot, *entry.before);
                }
                break;
            }
            }
        }

        bool restoreDeletedRecord(const BlockAddress &addr,
                                  std::size_t slotIndex,
                                  const Record &record) {
            auto &table = getTable(addr.table);
            auto fetchResult = buffer_.fetch(addr, true);
            fetchResult.block.ensureInitialized(blockSize_);
            if (!fetchResult.block.restoreDeletedRecord(slotIndex)) {
                return false;
            }
            applyIndexInsert(addr.table, record, addr, slotIndex);
            table.incrementRecords();
            dictionary_.updateTableStats(addr.table,
                                         table.totalRecords(),
                                         table.blockCount());
            persistIndexesForTable(addr.table);
            return true;
        }

        bool removeMatchingRecord(const std::string &tableName,
                                  const Record &target) {
            auto &table = getTable(tableName);
            for (const auto &addr : table.blocks()) {
                auto fetchResult = buffer_.fetch(addr, false);
                fetchResult.block.ensureInitialized(blockSize_);
                const auto slots = fetchResult.block.slotCount();
                for (std::size_t i = 0; i < slots; ++i) {
                    const Record *candidate = fetchResult.block.getRecord(i);
                    if (candidate && candidate->values == target.values) {
                        return deleteRecord(addr, i);
                    }
                }
            }
            return false;
        }


    std::vector<std::pair<std::string, IndexPointer>>
    collectIndexEntries(const std::string &tableName,
                        std::size_t columnIndex,
                        std::size_t keyLength) {
        std::vector<std::pair<std::string, IndexPointer>> entries;
        const auto &table = getTable(tableName);
        entries.reserve(table.totalRecords());
        for (const auto &addr : table.blocks()) {
            auto fetchResult = buffer_.fetch(addr, false);
            fetchResult.block.ensureInitialized(blockSize_);
            fetchResult.block.page.forEachRecord(
                [&](std::size_t slotIdx, const Record &record) {
                    std::string key =
                        sliceIndexKey(record, columnIndex, keyLength);
                    if (!key.empty()) {
                        entries.emplace_back(key, IndexPointer{addr, slotIdx});
                    }
                });
        }
        return entries;
    }

    void applyIndexInsert(const std::string &tableName,
                          const Record &record,
                          const BlockAddress &addr,
                          std::size_t slotIndex) {
        auto binding = indexesByTable_.find(tableName);
        if (binding == indexesByTable_.end()) {
            return;
        }
        for (const auto &indexName : binding->second) {
            auto it = indexes_.find(indexName);
            if (it == indexes_.end()) {
                continue;
            }
            it->second.insertRecord(record, addr, slotIndex);
        }
    }

    void applyIndexUpdate(const std::string &tableName,
                          const Record &before,
                          const Record &after,
                          const BlockAddress &addr,
                          std::size_t slotIndex) {
        auto binding = indexesByTable_.find(tableName);
        if (binding == indexesByTable_.end()) {
            return;
        }
        for (const auto &indexName : binding->second) {
            auto it = indexes_.find(indexName);
            if (it == indexes_.end()) {
                continue;
            }
            it->second.updateRecord(before, after, addr, slotIndex);
        }
    }

    void applyIndexDelete(const std::string &tableName,
                          const Record &record) {
        auto binding = indexesByTable_.find(tableName);
        if (binding == indexesByTable_.end()) {
            return;
        }
        for (const auto &indexName : binding->second) {
            auto it = indexes_.find(indexName);
            if (it == indexes_.end()) {
                continue;
            }
            it->second.deleteRecord(record);
        }
    }

    void enforceUniqueKeys(const std::string &tableName,
                           const Record &record,
                           const BlockAddress *selfAddr,
                           std::optional<std::size_t> slotIndex) const {
        auto binding = indexesByTable_.find(tableName);
        if (binding == indexesByTable_.end()) {
            return;
        }
        for (const auto &indexName : binding->second) {
            auto it = indexes_.find(indexName);
            if (it == indexes_.end()) {
                continue;
            }
            auto defIt = indexDefinitions_.find(indexName);
            if (defIt != indexDefinitions_.end() && !defIt->second.unique) {
                continue;
            }
            const std::string key = it->second.projectKey(record);
            if (key.empty()) {
                continue;
            }
            auto existing = it->second.find(key);
            if (!existing.has_value()) {
                continue;
            }
            if (selfAddr && slotIndex.has_value() &&
                existing->address == *selfAddr &&
                existing->slot == *slotIndex) {
                continue;
            }
            std::ostringstream oss;
            oss << "duplicate key '" << key << "' for index " << indexName;
            throw std::runtime_error(oss.str());
        }
    }

    void persistIndexesForTable(const std::string &tableName) const {
        auto binding = indexesByTable_.find(tableName);
        if (binding == indexesByTable_.end()) {
            return;
        }
        for (const auto &indexName : binding->second) {
            persistIndex(indexName);
        }
    }

    void persistIndex(const std::string &indexName) const {
        auto it = indexes_.find(indexName);
        if (it == indexes_.end()) {
            return;
        }
        const std::string path = indexDataFilePath(storagePath_, indexName);
        it->second.saveToFile(path);
    }

    void loadIndexFromDisk(const IndexDefinition &definition) {
        BPlusTreeIndex index(definition, blockSize_);
        const std::string dataPath = indexDataFilePath(storagePath_, definition.name);
        bool loadedFromDisk = false;
        if (pathutil::fileExists(dataPath)) {
            try {
                index.loadFromFile(dataPath);
                loadedFromDisk = true;
            } catch (const std::exception &ex) {
                std::cerr << "Warning: unable to load index '" << definition.name
                          << "' (" << ex.what() << "); rebuilding.\n";
            }
        }
        if (!loadedFromDisk) {
            auto entries = collectIndexEntries(definition.tableName,
                                               definition.columnIndex,
                                               definition.keyLength);
            index.rebuild(entries);
        }
        auto &perTable = indexesByTable_[definition.tableName];
        if (std::find(perTable.begin(), perTable.end(), definition.name) == perTable.end()) {
            perTable.push_back(definition.name);
        }
        auto emplaced = indexes_.emplace(definition.name, std::move(index));
        dictionary_.registerIndex(definition,
                                  emplaced.first->second.entriesPerPage());
    }

    void restoreIndexesForTable(const std::string &tableName) {
        auto pendingIt = pendingIndexLoadsByTable_.find(tableName);
        if (pendingIt == pendingIndexLoadsByTable_.end()) {
            return;
        }
        for (const auto &indexName : pendingIt->second) {
            auto defIt = indexDefinitions_.find(indexName);
            if (defIt == indexDefinitions_.end()) {
                continue;
            }
            if (indexes_.find(indexName) != indexes_.end()) {
                continue;
            }
            loadIndexFromDisk(defIt->second);
        }
        pendingIndexLoadsByTable_.erase(pendingIt);
    }

    void loadIndexCatalogFromDisk() {
        pendingIndexLoadsByTable_.clear();
        std::ifstream in(indexCatalogFile_);
        if (!in) {
            return;
        }
        std::string line;
        while (std::getline(in, line)) {
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            line = trimCopy(line);
            if (line.empty()) {
                continue;
            }
            std::vector<std::string> parts;
            std::stringstream ss(line);
            std::string part;
            while (std::getline(ss, part, '|')) {
                parts.push_back(part);
            }
            if (parts.size() < 6) {
                continue;
            }
            IndexDefinition def;
            def.name = parts[0];
            def.tableName = parts[1];
            def.columnName = parts[2];
            def.columnIndex = static_cast<std::size_t>(std::stoull(parts[3]));
            def.keyLength = static_cast<std::size_t>(std::stoull(parts[4]));
            def.unique = (parts[5] == "1");
            indexDefinitions_[def.name] = def;
            pendingIndexLoadsByTable_[def.tableName].push_back(def.name);
        }
    }

    void persistIndexCatalog() const {
        pathutil::ensureParentDirectory(indexCatalogFile_);
        std::ofstream out(indexCatalogFile_, std::ios::binary);
        if (!out) {
            std::ostringstream oss;
            oss << "failed to persist index catalog: " << indexCatalogFile_;
            throw std::runtime_error(oss.str());
        }
        for (const auto &entry : indexDefinitions_) {
            const auto &def = entry.second;
            out << def.name << "|" << def.tableName << "|" << def.columnName << "|"
                << def.columnIndex << "|" << def.keyLength << "|"
                << (def.unique ? 1 : 0) << "\n";
        }
    }

    void removePendingIndex(const std::string &tableName,
                            const std::string &indexName) {
        auto it = pendingIndexLoadsByTable_.find(tableName);
        if (it == pendingIndexLoadsByTable_.end()) {
            return;
        }
        auto &vec = it->second;
        vec.erase(std::remove(vec.begin(), vec.end(), indexName), vec.end());
        if (vec.empty()) {
            pendingIndexLoadsByTable_.erase(it);
        }
    }

    static std::string indexDirectory(const std::string &root) {
        return pathutil::join(root, "indexes");
    }

    static std::string indexDataFilePath(const std::string &root,
                                         const std::string &indexName) {
        return pathutil::join(indexDirectory(root), indexName + ".tree");
    }

    static std::string indexCatalogFilePath(const std::string &root) {
        return pathutil::join(metadataDirectory(root), "indexes.meta");
    }

    static std::string trimCopy(const std::string &input) {
        const auto first = input.find_first_not_of(" \t\r\n");
        if (first == std::string::npos) {
            return "";
        }
        const auto last = input.find_last_not_of(" \t\r\n");
        return input.substr(first, last - first + 1);
    }


    static std::size_t computeDiskBlocks(std::size_t diskBytes,
                                         std::size_t blockSize) {
        if (diskBytes == 0 || blockSize == 0) {
            throw std::invalid_argument("disk capacity and block size must be positive");
        }
        if (diskBytes < blockSize) {
            return 1;
        }
        return diskBytes / blockSize;
    }


    static std::size_t computeBufferCapacity(std::size_t memoryBytes,
                                             std::size_t blockSize) {
        std::size_t bufferBytes = static_cast<std::size_t>(memoryBytes * 0.60);
        std::size_t blocks = bufferBytes / blockSize;
        return blocks > 0 ? blocks : 1;
    }

    static std::string metadataDirectory(const std::string &root) {
        return pathutil::join(root, "meta");
    }

    static std::string planCacheFilePath(const std::string &root) {
        return pathutil::join(metadataDirectory(root), "access_plans.log");
    }

    static std::string logFilePath(const std::string &root) {
        return pathutil::join(pathutil::join(root, "logs"), "operations.log");
    }

    static std::string walFilePath(const std::string &root) {
        return pathutil::join(pathutil::join(root, "logs"), "wal.log");
    }


    void ensureRecordFits(const TableSchema &schema,
                          const Record &record) const {
        if (record.values.size() != schema.columns().size()) {
            throw std::invalid_argument("record column count mismatch");
        }
        const auto &columns = schema.columns();
        for (std::size_t i = 0; i < columns.size(); ++i) {
            if (record.values[i].size() > columns[i].length) {
                std::ostringstream oss;
                oss << "value '" << record.values[i] << "' in column "
                    << columns[i].name << " exceeds length " << columns[i].length;
                throw std::runtime_error(oss.str());
            }
        }
    }


    void computePartitions() {
        accessPlanBytes_ = static_cast<std::size_t>(mainMemoryBytes_ * 0.15);
        dictionaryBytes_ = static_cast<std::size_t>(mainMemoryBytes_ * 0.15);
        logBufferBytes_ = static_cast<std::size_t>(mainMemoryBytes_ * 0.10);
        bufferBytes_ = mainMemoryBytes_ - (accessPlanBytes_ + dictionaryBytes_ + logBufferBytes_);
        if (bufferBytes_ < blockSize_) {
            bufferBytes_ = blockSize_;
        }
    }

    std::size_t blockSize_;
    std::size_t mainMemoryBytes_;
    std::size_t diskBytes_;
    std::string storagePath_;
    DiskStorage disk_;
    BufferPool buffer_;
    DataDictionary dictionary_;
    AccessPlanCache planCache_;
    LogBuffer logBuffer_;
    WriteAheadLog wal_;
    std::unordered_map<std::string, Table> tables_;
    std::unordered_map<std::string, BPlusTreeIndex> indexes_;
    std::unordered_map<std::string, std::vector<std::string>> indexesByTable_;
    std::string indexCatalogFile_;
    std::unordered_map<std::string, IndexDefinition> indexDefinitions_;
    std::unordered_map<std::string, std::vector<std::string>> pendingIndexLoadsByTable_;
    bool transactionActive_{false};
    bool suppressUndo_{false};
    bool applyingUndo_{false};
    bool suppressWal_{false};
    std::optional<std::size_t> currentTxnId_;
    std::size_t nextTxnId_{1};
    std::vector<UndoEntry> undoLog_;
    std::vector<WriteAheadLog::Entry> pendingWalEntries_;
    std::unordered_set<std::string> walTables_;
    bool recoveryPerformed_{false};

    std::size_t accessPlanBytes_{0};
    std::size_t dictionaryBytes_{0};
    std::size_t bufferBytes_{0};
    std::size_t logBufferBytes_{0};

    std::mt19937 rng_;
};

} // namespace dbms
