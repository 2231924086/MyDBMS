#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

#include "executor/executor.h"
#include "executor/result_set.h"
#include "index/index_manager.h"
#include "storage/buffer_pool.h"
#include "storage/page.h"
#include "system/database.h"

using namespace dbms;
namespace fs = std::filesystem;

namespace {

void require(bool condition, const std::string &message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

void removeIfExists(const fs::path &path) {
    std::error_code ec;
    fs::remove_all(path, ec);
}

class WorkingDirGuard {
public:
    explicit WorkingDirGuard(fs::path next) : previous_(fs::current_path()) {
        fs::create_directories(next);
        fs::current_path(next);
    }

    ~WorkingDirGuard() {
        std::error_code ec;
        fs::current_path(previous_, ec);
    }

private:
    fs::path previous_;
};

struct TestRunner {
    void run(const std::string &name, const std::function<void()> &fn) {
        try {
            fn();
            std::cout << "[PASS] " << name << '\n';
            ++passed_;
        } catch (const std::exception &ex) {
            std::cout << "[FAIL] " << name << " -> " << ex.what() << '\n';
            ++failed_;
        } catch (...) {
            std::cout << "[FAIL] " << name << " -> unknown exception\n";
            ++failed_;
        }
    }

    int summary() const {
        std::cout << "\nTests passed: " << passed_ << ", failed: " << failed_ << '\n';
        return failed_;
    }

private:
    int passed_{0};
    int failed_{0};
};

void testVariableLengthPage() {
    VariableLengthPage page(256);

    auto first = page.insert(Record{"1", "Alice"});
    auto second = page.insert(Record{"2", "Bob"});
    require(first.has_value() && second.has_value(), "initial inserts should succeed");
    require(page.activeCount() == 2, "page should report two active records");

    require(page.get(*second)->values[1] == "Bob", "second record should be Bob");
    const auto usedBeforeUpdate = page.usedBytes();
    require(page.update(*second, Record{"2", "Bobby"}), "update should succeed");
    require(page.get(*second)->values[1] == "Bobby", "updated record should reflect change");
    require(page.usedBytes() >= usedBeforeUpdate, "used bytes should not shrink after growing record");

    const auto usedBeforeErase = page.usedBytes();
    require(page.erase(*first), "erase should succeed");
    require(page.deletedCount() == 1, "deleted count should track erased slot");
    auto vac = page.vacuumDeletedSlots();
    require(vac.clearedSlots == 1, "vacuum should clear one slot");
    require(page.usedBytes() < usedBeforeErase, "vacuum should reclaim space");

    auto third = page.insert(Record{"3", "Carol"});
    require(third.has_value(), "insert after vacuum should succeed");
    require(page.activeCount() == 2, "page should have two active records after reinsertion");
}

void testBufferPoolLRU() {
    const fs::path path = fs::current_path() / "tmp_dbms_tests" / "buffer_pool";
    removeIfExists(path);

    DiskStorage disk(3, path.string(), 256);
    BufferPool pool(2, disk);

    auto a1 = disk.allocateBlock("t");
    auto a2 = disk.allocateBlock("t");
    auto a3 = disk.allocateBlock("t");

    auto r1 = pool.fetch(a1, false);
    auto r2 = pool.fetch(a2, false);
    auto r3 = pool.fetch(a1, false);
    auto r4 = pool.fetch(a3, true); // should evict a2 (LRU) and mark a3 dirty

    require(!r1.wasHit && !r2.wasHit, "first two fetches should be misses");
    require(r3.wasHit, "re-fetching a1 should be a hit");
    require(r4.evicted.has_value(), "fetching a3 should evict one block");
    require(r4.evicted->table == a2.table && r4.evicted->index == a2.index,
            "LRU should evict the oldest (a2)");
    require(pool.hits() == 1 && pool.misses() == 3, "hit/miss counters should match access pattern");

    pool.flush(); // ensure dirty page path is covered
    removeIfExists(path);
}

void testBPlusTreeIndexOps() {
    IndexDefinition def{"idx_test", "t", "k", 0, 8, false};
    BPlusTreeIndex index(def, 256);

    const BlockAddress addr{"t", 0};
    Record r1{"k1", "v1"};
    index.insertRecord(r1, addr, 0);

    auto found = index.find("k1");
    require(found.has_value() && found->slot == 0, "inserted key should be found");

    Record r1Updated{"k2", "v1"};
    index.updateRecord(r1, r1Updated, addr, 0);
    require(!index.find("k1").has_value(), "old key should be removed after update");
    auto foundNew = index.find("k2");
    require(foundNew.has_value(), "new key should exist after update");

    index.deleteRecord(r1Updated);
    require(!index.find("k2").has_value(), "key should be removed after delete");
}

DatabaseSystem buildSampleDatabase() {
    const std::size_t blockSizeBytes = 512;
    const std::size_t mainMemoryBytes = 2 * 1024 * 1024; // 2 MiB
    const std::size_t diskBytes = 8 * 1024 * 1024;       // 8 MiB

    DatabaseSystem db(blockSizeBytes, mainMemoryBytes, diskBytes);

    TableSchema users(
        "users",
        {
            {"id", ColumnType::Integer, 16},
            {"name", ColumnType::String, 64},
            {"age", ColumnType::Integer, 8},
        });
    db.registerTable(users);

    TableSchema orders(
        "orders",
        {
            {"id", ColumnType::Integer, 16},
            {"user_id", ColumnType::Integer, 16},
            {"amount", ColumnType::Integer, 16},
        });
    db.registerTable(orders);

    db.insertRecord("users", Record{"1", "Alice", "30"});
    db.insertRecord("users", Record{"2", "Bob", "42"});
    db.insertRecord("users", Record{"3", "Carol", "28"});
    db.insertRecord("users", Record{"4", "Dave", "55"});

    db.insertRecord("orders", Record{"100", "1", "200"});
    db.insertRecord("orders", Record{"101", "2", "300"});
    db.insertRecord("orders", Record{"102", "3", "150"});
    db.insertRecord("orders", Record{"103", "4", "500"});

    db.createIndex("idx_users_id", "users", "id");
    return db;
}

std::optional<std::pair<BlockAddress, std::size_t>> findRecordById(DatabaseSystem& db,
                                                                   const std::string& table,
                                                                   const std::string& id) {
    const Table& t = db.getTable(table);
    for (const auto& addr : t.blocks()) {
        auto fetch = db.buffer().fetch(addr, false);
        fetch.block.ensureInitialized(db.blockSize());
        const auto slots = fetch.block.slotCount();
        for (std::size_t i = 0; i < slots; ++i) {
            const Record* rec = fetch.block.getRecord(i);
            if (rec && !rec->values.empty() && rec->values[0] == id) {
                return std::make_pair(addr, i);
            }
        }
    }
    return std::nullopt;
}

void testIndexScanAndJoinPipeline() {
    const fs::path tempRoot = fs::current_path() / "tmp_dbms_tests" / "integration";
    removeIfExists(tempRoot);

    {
        WorkingDirGuard guard(tempRoot);
        removeIfExists("storage");

        DatabaseSystem db = buildSampleDatabase();
        QueryExecutor executor(db);

        // Index scan equality lookup
        auto indexScan = std::make_shared<PhysicalPlanNode>(PhysicalOpType::kIndexScan,
                                                            "Index scan users by id");
        indexScan->parameters["table"] = "users";
        indexScan->parameters["index"] = "idx_users_id";
        indexScan->parameters["key"] = "2";

        auto indexResult = executor.execute(indexScan);
        require(indexResult.size() == 1, "index scan should return exactly one tuple");
        const auto &user = indexResult.getTuple(0);
        require(user.getValue("name") == "Bob", "index scan should return Bob for id=2");

        // Hash join with projection
        auto scanUsers = std::make_shared<PhysicalPlanNode>(PhysicalOpType::kTableScan, "scan users");
        scanUsers->parameters["table"] = "users";

        auto scanOrders = std::make_shared<PhysicalPlanNode>(PhysicalOpType::kTableScan, "scan orders");
        scanOrders->parameters["table"] = "orders";

        auto join = std::make_shared<PhysicalPlanNode>(PhysicalOpType::kHashJoin,
                                                       "users.id = orders.user_id");
        join->parameters["condition"] = "users.id = orders.user_id";
        join->parameters["left_key"] = "users.id";
        join->parameters["right_key"] = "orders.user_id";
        join->addChild(scanUsers);
        join->addChild(scanOrders);

        auto project = std::make_shared<PhysicalPlanNode>(PhysicalOpType::kProjection,
                                                          "project joined columns");
        project->outputColumns = {"users.name", "orders.amount"};
        project->addChild(join);

        auto joinResult = executor.execute(project);
        require(joinResult.size() == 4, "join should produce one row per order");

        std::unordered_set<std::string> actual;
        for (const auto &tuple : joinResult) {
            actual.insert(tuple.getValue("name") + "|" + tuple.getValue("amount"));
        }

        std::unordered_set<std::string> expected = {
            "Alice|200", "Bob|300", "Carol|150", "Dave|500"};
        require(actual == expected, "join output should match expected name-amount pairs");

        db.flushAll();
    }

    removeIfExists(tempRoot);
}

void testPersistenceAcrossRestart() {
    const fs::path tempRoot = fs::current_path() / "tmp_dbms_tests" / "persistence_restart";
    removeIfExists(tempRoot);

    TableSchema users(
        "users",
        {
            {"id", ColumnType::Integer, 16},
            {"name", ColumnType::String, 64},
            {"age", ColumnType::Integer, 8},
        });
    TableSchema orders(
        "orders",
        {
            {"id", ColumnType::Integer, 16},
            {"user_id", ColumnType::Integer, 16},
            {"amount", ColumnType::Integer, 16},
        });

    const std::size_t blockSizeBytes = 512;
    const std::size_t mainMemoryBytes = 2 * 1024 * 1024; // 2 MiB
    const std::size_t diskBytes = 8 * 1024 * 1024;       // 8 MiB

    {
        WorkingDirGuard guard(tempRoot);
        removeIfExists("storage");
        DatabaseSystem db(blockSizeBytes, mainMemoryBytes, diskBytes);
        db.registerTable(users);
        db.registerTable(orders);

        db.insertRecord("users", Record{"1", "Alice", "30"});
        db.insertRecord("users", Record{"2", "Bob", "42"});
        db.insertRecord("users", Record{"3", "Carol", "28"});
        db.insertRecord("users", Record{"4", "Dave", "55"});
        db.insertRecord("orders", Record{"100", "1", "200"});
        db.insertRecord("orders", Record{"101", "2", "300"});
        db.insertRecord("orders", Record{"102", "3", "150"});
        db.insertRecord("orders", Record{"103", "4", "500"});

        db.createIndex("idx_users_id", "users", "id");
        db.flushAll(); // persist data and index
    }

    {
        WorkingDirGuard guard(tempRoot);
        DatabaseSystem db(blockSizeBytes, mainMemoryBytes, diskBytes);
        db.registerTable(users);
        db.registerTable(orders);

        require(db.getTable("users").totalRecords() == 4,
                "users table should restore 4 records after restart");
        require(db.getTable("orders").totalRecords() == 4,
                "orders table should restore 4 records after restart");

        auto ptr = db.searchIndex("idx_users_id", "2");
        require(ptr.has_value(), "index lookup should succeed after restart");
        auto record = db.readRecord(ptr->address, ptr->slot);
        require(record.has_value() && record->values[1] == "Bob",
                "restored index should point to Bob's record");
    }

    removeIfExists(tempRoot);
}

void testIndexRebuildWithoutDataFile() {
    const fs::path tempRoot = fs::current_path() / "tmp_dbms_tests" / "index_rebuild";
    removeIfExists(tempRoot);

    TableSchema users(
        "users",
        {
            {"id", ColumnType::Integer, 16},
            {"name", ColumnType::String, 64},
        });

    const std::size_t blockSizeBytes = 512;
    const std::size_t mainMemoryBytes = 2 * 1024 * 1024; // 2 MiB
    const std::size_t diskBytes = 8 * 1024 * 1024;       // 8 MiB

    {
        WorkingDirGuard guard(tempRoot);
        removeIfExists("storage");
        DatabaseSystem db(blockSizeBytes, mainMemoryBytes, diskBytes);
        db.registerTable(users);
        db.insertRecord("users", Record{"1", "Alice"});
        db.insertRecord("users", Record{"2", "Bob"});
        db.insertRecord("users", Record{"3", "Carol"});
        db.createIndex("idx_users_id", "users", "id");
        db.flushAll();

        // Simulate lost index data file while keeping catalog
        const fs::path indexFile = tempRoot / "storage" / "indexes" / "idx_users_id.tree";
        std::error_code ec;
        fs::remove(indexFile, ec);
    }

    {
        WorkingDirGuard guard(tempRoot);
        DatabaseSystem db(blockSizeBytes, mainMemoryBytes, diskBytes);
        db.registerTable(users); // should rebuild/load index

        auto ptr = db.searchIndex("idx_users_id", "2");
        require(ptr.has_value(), "index should be rebuilt from data when file is missing");
        auto record = db.readRecord(ptr->address, ptr->slot);
        require(record.has_value() && record->values[0] == "2",
                "rebuilt index should point to correct record");
    }

    removeIfExists(tempRoot);
}

void testInsertRecordTooLarge() {
    const fs::path tempRoot = fs::current_path() / "tmp_dbms_tests" / "record_size";
    removeIfExists(tempRoot);
    WorkingDirGuard guard(tempRoot);
    removeIfExists("storage");

    const std::size_t blockSizeBytes = 64;               // Intentionally tiny
    const std::size_t mainMemoryBytes = 1 * 1024 * 1024; // 1 MiB
    const std::size_t diskBytes = 1 * 1024 * 1024;       // 1 MiB
    DatabaseSystem db(blockSizeBytes, mainMemoryBytes, diskBytes);

    TableSchema bigTable("big_values", {{"payload", ColumnType::String, 200}});
    db.registerTable(bigTable);

    bool threw = false;
    try {
        db.insertRecord("big_values", Record{std::string(80, 'x')});
    } catch (const std::exception &) {
        threw = true;
    }

    require(threw, "insert should throw when record footprint exceeds block size");
}

void testComplexPredicateFilterExecution() {
    const fs::path tempRoot = fs::current_path() / "tmp_dbms_tests" / "complex_predicate";
    removeIfExists(tempRoot);
    WorkingDirGuard guard(tempRoot);
    removeIfExists("storage");

    const std::size_t blockSizeBytes = 512;
    const std::size_t mainMemoryBytes = 2 * 1024 * 1024; // 2 MiB
    const std::size_t diskBytes = 8 * 1024 * 1024;       // 8 MiB
    DatabaseSystem db(blockSizeBytes, mainMemoryBytes, diskBytes);

    TableSchema items(
        "items",
        {
            {"id", ColumnType::Integer, 16},
            {"category", ColumnType::String, 16},
            {"price", ColumnType::Integer, 8},
            {"qty", ColumnType::Integer, 8},
        });
    db.registerTable(items);

    db.insertRecord("items", Record{"1", "A", "15", "2"});
    db.insertRecord("items", Record{"2", "B", "5", "5"});
    db.insertRecord("items", Record{"3", "A", "8", "3"});
    db.insertRecord("items", Record{"4", "B", "20", "1"});

    QueryExecutor executor(db);
    auto scan = std::make_shared<PhysicalPlanNode>(PhysicalOpType::kTableScan, "scan items");
    scan->parameters["table"] = "items";

    auto filter = std::make_shared<PhysicalPlanNode>(PhysicalOpType::kFilter, "complex predicate");
    filter->parameters["condition"] =
        "(category = 'A' AND price > 10) OR (category = 'B' AND qty = 5)";
    filter->addChild(scan);

    auto result = executor.execute(filter);
    require(result.size() == 2, "complex predicate should return two rows");
    std::unordered_set<std::string> ids;
    for (const auto &tuple : result) {
        ids.insert(tuple.getValue("id"));
    }
    require(ids.count("1") == 1 && ids.count("2") == 1,
            "predicate should match id=1 and id=2");
}

void testPlanCacheEvictionUnderCapacity() {
    const fs::path tempRoot = fs::current_path() / "tmp_dbms_tests" / "plan_cache";
    removeIfExists(tempRoot);
    WorkingDirGuard guard(tempRoot);
    removeIfExists("storage");

    const std::size_t blockSizeBytes = 64;
    const std::size_t mainMemoryBytes = 128; // keeps access plan cache tiny
    const std::size_t diskBytes = 1024;
    DatabaseSystem db(blockSizeBytes, mainMemoryBytes, diskBytes);

    TableSchema t1("t1", {{"v", ColumnType::Integer, 8}});
    TableSchema t2("t2", {{"v", ColumnType::Integer, 8}});
    db.registerTable(t1);
    db.registerTable(t2);

    db.insertRecord("t1", Record{"1"});
    auto firstPlans = db.cachedAccessPlans();
    require(!firstPlans.empty(), "plan cache should record first insert");

    db.insertRecord("t2", Record{"2"});
    auto afterEvict = db.cachedAccessPlans();
    require(afterEvict.size() == 1, "plan cache should evict oldest plan when full");
    require(afterEvict[0].find("INSERT INTO t2") != std::string::npos,
            "plan cache should retain most recent plan");
}

void testTransactionRollback() {
    const fs::path tempRoot = fs::current_path() / "tmp_dbms_tests" / "tx_rollback";
    removeIfExists(tempRoot);
    WorkingDirGuard guard(tempRoot);
    removeIfExists("storage");

    DatabaseSystem db = buildSampleDatabase();
    const auto baseline = db.getTable("users").totalRecords();

    db.beginTransaction();
    db.insertRecord("users", Record{"99", "Temp", "99"});

    auto deletePtr = db.searchIndex("idx_users_id", "1");
    require(deletePtr.has_value(), "expected idx_users_id to be present");
    db.deleteRecord(deletePtr->address, deletePtr->slot);

    auto updatePtr = findRecordById(db, "users", "2");
    require(updatePtr.has_value(), "id=2 should exist before update");
    db.updateRecord(updatePtr->first, updatePtr->second, Record{"2", "Bobby", "43"});

    db.rollbackTransaction();

    auto dump = db.dumpTable("users");
    require(dump.totalRecords == baseline, "rollback should restore record count");
    bool hasId1 = false;
    bool hasId99 = false;
    std::string id2Name;
    for (const auto &row : dump.rows) {
        if (!row.values.empty()) {
            if (row.values[0] == "1") {
                hasId1 = true;
            } else if (row.values[0] == "99") {
                hasId99 = true;
            } else if (row.values[0] == "2") {
                id2Name = row.values[1];
            }
        }
    }
    require(hasId1, "rollback should restore deleted row");
    require(!hasId99, "rolled back insert should not persist");
    require(id2Name == "Bob", "rollback should undo updates inside the transaction");
}

void testTransactionCommit() {
    const fs::path tempRoot = fs::current_path() / "tmp_dbms_tests" / "tx_commit";
    removeIfExists(tempRoot);
    WorkingDirGuard guard(tempRoot);
    removeIfExists("storage");

    DatabaseSystem db = buildSampleDatabase();
    const auto baseOrders = db.getTable("orders").totalRecords();

    db.beginTransaction();
    db.insertRecord("orders", Record{"2000", "1", "777"});
    auto updatePtr = db.searchIndex("idx_users_id", "3");
    require(updatePtr.has_value(), "users index should be available for updates");
    db.updateRecord(updatePtr->address, updatePtr->slot, Record{"3", "Carolyn", "28"});
    db.commitTransaction();

    auto ordersDump = db.dumpTable("orders");
    require(ordersDump.totalRecords == baseOrders + 1,
            "committed insert should increase row count");
    bool foundOrder = false;
    for (const auto &row : ordersDump.rows) {
        if (!row.values.empty() && row.values[0] == "2000") {
            foundOrder = true;
            break;
        }
    }
    require(foundOrder, "committed order insert must persist");

    auto usersDump = db.dumpTable("users");
    std::string nameFor3;
    for (const auto &row : usersDump.rows) {
        if (!row.values.empty() && row.values[0] == "3") {
            nameFor3 = row.values[1];
            break;
        }
    }
    require(nameFor3 == "Carolyn", "committed update should persist after commit");
}

void testBufferEvictionFlushesDirtyPage() {
    const fs::path tempRoot = fs::current_path() / "tmp_dbms_tests" / "buffer_pressure";
    removeIfExists(tempRoot);
    WorkingDirGuard guard(tempRoot);
    removeIfExists("storage");

    const std::size_t blockSizeBytes = 128;
    const std::size_t mainMemoryBytes = 256; // buffer capacity becomes 1 frame
    const std::size_t diskBytes = blockSizeBytes * 2;
    DatabaseSystem db(blockSizeBytes, mainMemoryBytes, diskBytes);

    TableSchema big("big", {{"payload", ColumnType::String, 120}});
    db.registerTable(big);

    const std::string payloadA(100, 'a');
    const std::string payloadB(100, 'b');
    db.insertRecord("big", Record{payloadA}); // occupies first block
    db.insertRecord("big", Record{payloadB}); // forces eviction/writeback of first block
    db.flushAll();

    DatabaseSystem reopened(blockSizeBytes, mainMemoryBytes, diskBytes);
    reopened.registerTable(big);
    require(reopened.getTable("big").totalRecords() == 2,
            "records should persist even when buffer evicts dirty blocks");
    auto dump = reopened.dumpTable("big");
    std::unordered_set<std::string> payloads;
    for (const auto &row : dump.rows) {
        payloads.insert(row.values[0]);
    }
    require(payloads.count(payloadA) == 1 && payloads.count(payloadB) == 1,
            "evicted pages must be written back correctly");
}

void testDiskFullStopsInsertion() {
    const fs::path tempRoot = fs::current_path() / "tmp_dbms_tests" / "disk_full";
    removeIfExists(tempRoot);
    WorkingDirGuard guard(tempRoot);
    removeIfExists("storage");

    const std::size_t blockSizeBytes = 64;
    const std::size_t mainMemoryBytes = 128;
    const std::size_t diskBytes = blockSizeBytes; // single block disk
    DatabaseSystem db(blockSizeBytes, mainMemoryBytes, diskBytes);

    TableSchema bulky("bulky", {{"payload", ColumnType::String, 64}});
    db.registerTable(bulky);

    const std::string largeValue(40, 'x');
    db.insertRecord("bulky", Record{largeValue}); // fills the only block

    bool threw = false;
    try {
        db.insertRecord("bulky", Record{largeValue}); // requires a second block
    } catch (const std::exception &) {
        threw = true;
    }
    require(threw, "second insert should fail when disk runs out of blocks");
}

void testCorruptedDataFileDetection() {
    const fs::path tempRoot = fs::current_path() / "tmp_dbms_tests" / "corrupt_data";
    removeIfExists(tempRoot);

    TableSchema table("corrupt", {{"id", ColumnType::Integer, 8}});
    const std::size_t blockSizeBytes = 128;
    const std::size_t mainMemoryBytes = 512;
    const std::size_t diskBytes = blockSizeBytes * 2;

    {
        WorkingDirGuard guard(tempRoot);
        removeIfExists("storage");
        DatabaseSystem db(blockSizeBytes, mainMemoryBytes, diskBytes);
        db.registerTable(table);
        db.insertRecord("corrupt", Record{"1"});
        db.flushAll();

        const fs::path blockFile = tempRoot / "storage" / "corrupt" / "block_0.blk";
        std::fstream out(blockFile, std::ios::in | std::ios::out | std::ios::binary);
        require(out.good(), "should open block file to corrupt");
        std::uint32_t badSig = 0u;
        out.seekp(0);
        out.write(reinterpret_cast<const char *>(&badSig), sizeof(badSig));
    }

    bool detected = false;
    try {
        WorkingDirGuard guard(tempRoot);
        DatabaseSystem db(blockSizeBytes, mainMemoryBytes, diskBytes);
        db.registerTable(table);
    } catch (const std::exception &) {
        detected = true;
    }
    require(detected, "corrupted data block should be rejected during load");

    removeIfExists(tempRoot);
}

void testCorruptedIndexFileRebuild() {
    const fs::path tempRoot = fs::current_path() / "tmp_dbms_tests" / "corrupt_index";
    removeIfExists(tempRoot);

    TableSchema users(
        "users",
        {
            {"id", ColumnType::Integer, 16},
            {"name", ColumnType::String, 32},
        });

    const std::size_t blockSizeBytes = 256;
    const std::size_t mainMemoryBytes = 1 * 1024 * 1024;
    const std::size_t diskBytes = 4 * 1024 * 1024;

    {
        WorkingDirGuard guard(tempRoot);
        removeIfExists("storage");
        DatabaseSystem db(blockSizeBytes, mainMemoryBytes, diskBytes);
        db.registerTable(users);
        db.insertRecord("users", Record{"1", "Alice"});
        db.insertRecord("users", Record{"2", "Bob"});
        db.insertRecord("users", Record{"3", "Carol"});
        db.createIndex("idx_users_id", "users", "id");
        db.flushAll();

        const fs::path indexFile = tempRoot / "storage" / "indexes" / "idx_users_id.tree";
        std::fstream out(indexFile, std::ios::in | std::ios::out | std::ios::binary);
        require(out.good(), "should open index file to corrupt");
        out.seekp(0);
        out << "BAD\n";
    }

    {
        WorkingDirGuard guard(tempRoot);
        DatabaseSystem db(blockSizeBytes, mainMemoryBytes, diskBytes);
        db.registerTable(users);

        auto ptr = db.searchIndex("idx_users_id", "2");
        require(ptr.has_value(), "index should be rebuilt when on-disk format is invalid");
        auto rec = db.readRecord(ptr->address, ptr->slot);
        require(rec.has_value() && rec->values[0] == "2",
                "rebuilt index should still point to the correct record");
    }

    removeIfExists(tempRoot);
}


ResultSet runSql(DatabaseSystem &db, const std::string &sql) {
    Lexer lexer(sql);
    Parser parser(lexer.tokenize());
    auto ast = parser.parse();
    SemanticAnalyzer analyzer(db);
    analyzer.analyze(ast);

    if (ast->nodeType == ASTNodeType::UPDATE_STATEMENT) {
        executeUpdateStatement(db, ast);
        return ResultSet{};
    }
    if (ast->nodeType == ASTNodeType::DELETE_STATEMENT) {
        executeDeleteStatement(db, ast);
        return ResultSet{};
    }

    if (ast->nodeType != ASTNodeType::SELECT_STATEMENT) {
        throw std::runtime_error("runSql only supports SELECT/UPDATE/DELETE");
    }

    LogicalPlanGenerator logicalGen;
    auto logicalPlan = logicalGen.generateLogicalPlan(ast);
    LogicalOptimizer optimizer;
    auto optimizedPlan = optimizer.optimize(logicalPlan);
    PhysicalPlanGenerator physGen(db);
    auto physicalPlan = physGen.generatePhysicalPlan(optimizedPlan);
    QueryExecutor executor(db);
    return executor.execute(physicalPlan);
}

void testSqlDistinctAndOrderBy() {
    const fs::path tempRoot = fs::current_path() / "tmp_dbms_tests" / "sql_distinct_order";
    removeIfExists(tempRoot);
    WorkingDirGuard guard(tempRoot);
    removeIfExists("storage");

    const std::size_t blockSizeBytes = 512;
    const std::size_t mainMemoryBytes = 2 * 1024 * 1024; // 2 MiB
    const std::size_t diskBytes = 8 * 1024 * 1024;       // 8 MiB
    DatabaseSystem db(blockSizeBytes, mainMemoryBytes, diskBytes);

    TableSchema nums("numbers", {{"value", ColumnType::Integer, 8}});
    db.registerTable(nums);
    db.insertRecord("numbers", Record{"1"});
    db.insertRecord("numbers", Record{"2"});
    db.insertRecord("numbers", Record{"2"});
    db.insertRecord("numbers", Record{"3"});
    db.insertRecord("numbers", Record{"3"});

    auto result = runSql(db, "SELECT DISTINCT value FROM numbers ORDER BY value DESC");
    require(result.size() == 3, "distinct should remove duplicate values");
    std::vector<std::string> values;
    for (const auto &row : result) {
        values.push_back(row.getValue("value"));
    }
    require((values == std::vector<std::string>{"3", "2", "1"}),
            "distinct + order by should return 3,2,1");
}

void testLeftAndRightJoinSupport() {
    const fs::path tempRoot = fs::current_path() / "tmp_dbms_tests" / "sql_join_types";
    removeIfExists(tempRoot);
    WorkingDirGuard guard(tempRoot);
    removeIfExists("storage");

    const std::size_t blockSizeBytes = 512;
    const std::size_t mainMemoryBytes = 2 * 1024 * 1024; // 2 MiB
    const std::size_t diskBytes = 8 * 1024 * 1024;       // 8 MiB
    DatabaseSystem db(blockSizeBytes, mainMemoryBytes, diskBytes);

    TableSchema users(
        "users",
        {
            {"id", ColumnType::Integer, 16},
            {"name", ColumnType::String, 32},
        });
    TableSchema purchases(
        "purchases",
        {
            {"user_id", ColumnType::Integer, 16},
            {"amount", ColumnType::Integer, 16},
        });
    db.registerTable(users);
    db.registerTable(purchases);

    db.insertRecord("users", Record{"1", "Alice"});
    db.insertRecord("users", Record{"2", "Bob"});
    db.insertRecord("users", Record{"3", "Carol"});

    db.insertRecord("purchases", Record{"1", "100"});
    db.insertRecord("purchases", Record{"2", "200"});
    db.insertRecord("purchases", Record{"4", "400"}); // No matching user

    auto leftResult = runSql(
        db,
        "SELECT users.id, purchases.amount "
        "FROM users LEFT JOIN purchases ON users.id = purchases.user_id "
        "ORDER BY users.id");
    require(leftResult.size() == 3, "left join should keep all users");
    std::vector<std::string> leftIds;
    std::vector<std::string> leftAmounts;
    for (const auto &row : leftResult) {
        leftIds.push_back(row.getValue("id"));
        leftAmounts.push_back(row.getValue("amount"));
    }
    require((leftIds == std::vector<std::string>{"1", "2", "3"}),
            "left join should output ids 1,2,3 in order");
    require((leftAmounts == std::vector<std::string>{"100", "200", "NULL"}),
            "left join should fill NULL for missing purchases");

    auto rightResult = runSql(
        db,
        "SELECT purchases.user_id, users.name "
        "FROM users RIGHT JOIN purchases ON users.id = purchases.user_id "
        "ORDER BY purchases.user_id");
    require(rightResult.size() == 3, "right join should keep all purchases");
    std::vector<std::string> rightUserIds;
    std::vector<std::string> purchaserNames;
    for (const auto &row : rightResult) {
        rightUserIds.push_back(row.getValue("user_id"));
        purchaserNames.push_back(row.getValue("name"));
    }
    require((rightUserIds == std::vector<std::string>{"1", "2", "4"}),
            "right join should output all purchase user_ids");
    require((purchaserNames == std::vector<std::string>{"Alice", "Bob", "NULL"}),
            "right join should set NULL when no matching user exists");
}

void testSqlUpdateExecution() {
    const fs::path tempRoot = fs::current_path() / "tmp_dbms_tests" / "sql_update";
    removeIfExists(tempRoot);
    WorkingDirGuard guard(tempRoot);
    removeIfExists("storage");

    const std::size_t blockSizeBytes = 512;
    const std::size_t mainMemoryBytes = 2 * 1024 * 1024; // 2 MiB
    const std::size_t diskBytes = 8 * 1024 * 1024;       // 8 MiB
    DatabaseSystem db(blockSizeBytes, mainMemoryBytes, diskBytes);

    TableSchema users(
        "users",
        {
            {"id", ColumnType::Integer, 16},
            {"name", ColumnType::String, 32},
            {"age", ColumnType::Integer, 8},
        });
    db.registerTable(users);

    db.insertRecord("users", Record{"1", "Alice", "30"});
    db.insertRecord("users", Record{"2", "Bob", "42"});
    db.insertRecord("users", Record{"3", "Carol", "28"});

    auto before = runSql(db, "SELECT name, age FROM users WHERE id = 1");
    require(before.size() == 1, "baseline row should exist before update");

    runSql(db, "UPDATE users SET name = 'Alicia', age = age + 1 WHERE id = 1");

    auto after = runSql(db, "SELECT name, age FROM users WHERE id = 1");
    require(after.size() == 1, "update should keep the matching row present");
    require(after.getTuple(0).getValue("name") == "Alicia", "name should be updated");
    require(after.getTuple(0).getValue("age") == "31", "age should be incremented");

    auto untouched = runSql(db, "SELECT name FROM users WHERE id = 2");
    require(untouched.size() == 1 && untouched.getTuple(0).getValue("name") == "Bob",
            "non-matching rows should not be modified");
}

void testSqlDeleteExecution() {
    const fs::path tempRoot = fs::current_path() / "tmp_dbms_tests" / "sql_delete";
    removeIfExists(tempRoot);
    WorkingDirGuard guard(tempRoot);
    removeIfExists("storage");

    const std::size_t blockSizeBytes = 512;
    const std::size_t mainMemoryBytes = 2 * 1024 * 1024; // 2 MiB
    const std::size_t diskBytes = 8 * 1024 * 1024;       // 8 MiB
    DatabaseSystem db(blockSizeBytes, mainMemoryBytes, diskBytes);

    TableSchema users(
        "users",
        {
            {"id", ColumnType::Integer, 16},
            {"name", ColumnType::String, 32},
        });
    db.registerTable(users);

    db.insertRecord("users", Record{"1", "Alice"});
    db.insertRecord("users", Record{"2", "Bob"});
    db.insertRecord("users", Record{"3", "Carol"});

    runSql(db, "DELETE FROM users WHERE id = 2");
    auto remaining = runSql(db, "SELECT id FROM users ORDER BY id");
    require(remaining.size() == 2, "one row should be removed by delete");
    std::vector<std::string> ids;
    for (const auto &row : remaining) {
        ids.push_back(row.getValue("id"));
    }
    require((ids == std::vector<std::string>{"1", "3"}), "deleted id should be missing");

    runSql(db, "DELETE FROM users");
    auto empty = runSql(db, "SELECT id FROM users");
    require(empty.size() == 0, "delete without where should clear all rows");
}

void testSortOperatorOrdersResults() {
    const fs::path tempRoot = fs::current_path() / "tmp_dbms_tests" / "sort_operator";
    removeIfExists(tempRoot);
    WorkingDirGuard guard(tempRoot);
    removeIfExists("storage");

    const std::size_t blockSizeBytes = 512;
    const std::size_t mainMemoryBytes = 2 * 1024 * 1024;
    const std::size_t diskBytes = 8 * 1024 * 1024;
    DatabaseSystem db(blockSizeBytes, mainMemoryBytes, diskBytes);

    TableSchema users(
        "users",
        {
            {"id", ColumnType::Integer, 8},
            {"name", ColumnType::String, 32},
            {"age", ColumnType::Integer, 8},
        });
    db.registerTable(users);

    db.insertRecord("users", Record{"1", "Alice", "30"});
    db.insertRecord("users", Record{"2", "Bob", "42"});
    db.insertRecord("users", Record{"3", "Carol", "28"});

    QueryExecutor executor(db);
    auto scan = std::make_shared<PhysicalPlanNode>(PhysicalOpType::kTableScan, "scan users");
    scan->parameters["table"] = "users";

    auto sort = std::make_shared<PhysicalPlanNode>(PhysicalOpType::kSort, "sort by age desc");
    sort->parameters["order_by"] = "age:DESC";
    sort->addChild(scan);

    auto result = executor.execute(sort);
    require(result.size() == 3, "sort should return all rows");
    std::vector<std::string> ages;
    for (const auto &row : result) {
        ages.push_back(row.getValue("age"));
    }
    require((ages == std::vector<std::string>{"42", "30", "28"}),
            "ages should be ordered descending");
}

void testAggregateGroupByHaving() {
    const fs::path tempRoot = fs::current_path() / "tmp_dbms_tests" / "aggregate_operator";
    removeIfExists(tempRoot);
    WorkingDirGuard guard(tempRoot);
    removeIfExists("storage");

    const std::size_t blockSizeBytes = 512;
    const std::size_t mainMemoryBytes = 2 * 1024 * 1024;
    const std::size_t diskBytes = 8 * 1024 * 1024;
    DatabaseSystem db(blockSizeBytes, mainMemoryBytes, diskBytes);

    TableSchema sales(
        "sales",
        {
            {"region", ColumnType::String, 16},
            {"amount", ColumnType::Integer, 8},
        });
    db.registerTable(sales);

    db.insertRecord("sales", Record{"north", "10"});
    db.insertRecord("sales", Record{"north", "15"});
    db.insertRecord("sales", Record{"south", "20"});
    db.insertRecord("sales", Record{"south", "5"});
    db.insertRecord("sales", Record{"south", "8"});

    QueryExecutor executor(db);
    auto scan = std::make_shared<PhysicalPlanNode>(PhysicalOpType::kTableScan, "scan sales");
    scan->parameters["table"] = "sales";

    auto agg = std::make_shared<PhysicalPlanNode>(PhysicalOpType::kAggregate, "group sales");
    agg->parameters["group_by"] = "region";
    agg->parameters["aggregates"] = "COUNT(*) AS cnt,SUM(amount) AS total";
    agg->parameters["having"] = "cnt > 2";
    agg->addChild(scan);

    auto result = executor.execute(agg);
    require(result.size() == 1, "only regions with more than two sales should remain");
    const auto &row = result.getTuple(0);
    require(row.getValue("region") == "south", "south should be the only group");
    require(row.getValue("cnt") == "3", "south should have three rows");
    require(row.getValue("total") == "33", "sum should be 33 for south");
}

} // namespace

int main() {
    TestRunner runner;
    runner.run("VariableLengthPage insert/update/delete/vacuum", testVariableLengthPage);
    runner.run("BufferPool LRU eviction and flush", testBufferPoolLRU);
    runner.run("BPlusTree index CRUD", testBPlusTreeIndexOps);
    runner.run("Index scan and hash join pipeline", testIndexScanAndJoinPipeline);
    runner.run("Persistence across restart (data + index)", testPersistenceAcrossRestart);
    runner.run("Index rebuild when data file is missing", testIndexRebuildWithoutDataFile);
    runner.run("Insert exceeding block capacity is rejected", testInsertRecordTooLarge);
    runner.run("Complex predicate filter evaluation", testComplexPredicateFilterExecution);
    runner.run("Access plan cache evicts when over capacity", testPlanCacheEvictionUnderCapacity);
    runner.run("Transaction rollback restores state", testTransactionRollback);
    runner.run("Transaction commit persists changes", testTransactionCommit);
    runner.run("Buffer eviction flushes dirty pages", testBufferEvictionFlushesDirtyPage);
    runner.run("Disk full prevents further inserts", testDiskFullStopsInsertion);
    runner.run("Corrupted data block is detected", testCorruptedDataFileDetection);
    runner.run("Corrupted index file triggers rebuild", testCorruptedIndexFileRebuild);
    runner.run("SQL DISTINCT with ORDER BY", testSqlDistinctAndOrderBy);
    runner.run("LEFT/RIGHT join execution", testLeftAndRightJoinSupport);
    runner.run("SQL UPDATE applies SET with WHERE", testSqlUpdateExecution);
    runner.run("SQL DELETE removes matching rows", testSqlDeleteExecution);
    runner.run("Sort operator orders tuples", testSortOperatorOrdersResults);
    runner.run("Aggregate operator group by + having", testAggregateGroupByHaving);
    return runner.summary() == 0 ? 0 : 1;
}
