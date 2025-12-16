# Mini DBMS 开发者文档

本文档面向希望理解、修改或扩展Mini DBMS的开发者。

---

## 目录

1. [开发环境设置](#1-开发环境设置)
2. [项目结构](#2-项目结构)
3. [核心API参考](#3-核心api参考)
4. [扩展开发指南](#4-扩展开发指南)
5. [测试与调试](#5-测试与调试)
6. [贡献指南](#6-贡献指南)

---

## 1. 开发环境设置

### 1.1 依赖项

**必需**:
- C++17兼容编译器
- CMake 3.16+

**可选**:
- GDB/LLDB (调试)
- Valgrind (内存检查)
- Clang-Format (代码格式化)

### 1.2 编译选项

**Debug模式** (用于开发):
```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Debug ..
cmake --build .
```

**Release模式** (用于性能测试):
```bash
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build .
```

### 1.3 IDE配置

**VS Code**:
```json
// .vscode/settings.json
{
    "C_Cpp.default.configurationProvider": "ms-vscode.cmake-tools",
    "cmake.buildDirectory": "${workspaceFolder}/build"
}
```

**CLion**: 直接打开项目根目录，自动识别CMake配置。

---

## 2. 项目结构

```
work/
├── CMakeLists.txt           # CMake配置
├── main.cpp                 # 主程序入口
├── include/                 # 公共头文件
│   ├── common/
│   │   ├── types.h          # 基础类型定义
│   │   └── utils.h          # 工具函数
│   ├── storage/
│   │   ├── buffer_pool.h    # 缓冲池
│   │   ├── disk_manager.h   # 磁盘管理
│   │   ├── page.h           # 页面管理
│   │   └── write_ahead_log.h # WAL日志
│   ├── index/
│   │   ├── b_plus_tree.h    # B+树索引
│   │   └── index_manager.h  # 索引管理
│   ├── executor/
│   │   ├── executor.h       # 执行器基类
│   │   ├── table_scan.h     # 表扫描
│   │   ├── index_scan.h     # 索引扫描
│   │   ├── filter.h         # 过滤器
│   │   ├── projection.h     # 投影
│   │   ├── join.h           # 连接
│   │   ├── sort.h           # 排序
│   │   ├── aggregate.h      # 聚合
│   │   ├── distinct.h       # 去重
│   │   ├── limit.h          # 限制
│   │   ├── alias.h          # 别名
│   │   ├── expression.h     # 表达式
│   │   ├── schema.h         # Schema定义
│   │   └── result_set.h     # 结果集
│   ├── parser/
│   │   └── query_processor.h # SQL解析器
│   └── system/
│       ├── database.h       # 数据库系统
│       ├── table.h          # 表管理
│       └── catalog.h        # 数据字典
└── src/                     # 实现文件 (结构同include/)
```

---

## 3. 核心API参考

### 3.1 DatabaseSystem (数据库系统核心类)

**头文件**: `include/system/database.h`

#### 构造函数
```cpp
DatabaseSystem(
    std::size_t blockSizeBytes,    // 数据块大小
    std::size_t mainMemoryBytes,   // 主内存大小
    std::size_t diskBytes          // 磁盘容量
);
```

**示例**:
```cpp
DatabaseSystem db(4096, 32*1024*1024, 256*1024*1024);
```

#### 表管理

```cpp
// 注册表
void registerTable(const TableSchema& schema);

// 获取表对象
Table& getTable(const std::string& name);
const Table& getTable(const std::string& name) const;
```

**示例**:
```cpp
TableSchema schema("users", {
    {"id", ColumnType::Integer, 16},
    {"name", ColumnType::String, 64}
});
db.registerTable(schema);
```

#### 记录操作

```cpp
// 插入记录
void insertRecord(const std::string& tableName, Record record);

// 读取记录
std::optional<Record> readRecord(
    const BlockAddress& addr,
    std::size_t slotIndex
);

// 更新记录
bool updateRecord(
    const BlockAddress& addr,
    std::size_t slotIndex,
    Record record
);

// 删除记录
bool deleteRecord(
    const BlockAddress& addr,
    std::size_t slotIndex
);
```

**示例**:
```cpp
// 插入
Record record{"1", "Alice", "30"};
db.insertRecord("users", record);

// 更新
BlockAddress addr{"users", 0};
Record newRecord{"1", "Alicia", "31"};
db.updateRecord(addr, 0, newRecord);

// 删除
db.deleteRecord(addr, 0);
```

#### 事务管理

```cpp
// 开始事务
void beginTransaction();

// 提交事务
void commitTransaction();

// 回滚事务
void rollbackTransaction();

// 检查是否在事务中
bool inTransaction() const;
```

**示例**:
```cpp
db.beginTransaction();
try {
    db.insertRecord("users", record1);
    db.insertRecord("users", record2);
    db.commitTransaction();
} catch (...) {
    db.rollbackTransaction();
    throw;
}
```

#### 索引管理

```cpp
// 创建索引
std::vector<std::string> createIndex(
    const std::string& indexName,
    const std::string& tableName,
    const std::string& columnName
);

// 查找列的索引
std::optional<std::string> findIndexForColumn(
    const std::string& tableName,
    const std::string& columnName
) const;

// 索引查询
std::optional<IndexPointer> searchIndex(
    const std::string& indexName,
    const std::string& key
) const;
```

**示例**:
```cpp
// 创建索引
db.createIndex("idx_users_id", "users", "id");

// 使用索引查找
auto ptr = db.searchIndex("idx_users_id", "42");
if (ptr.has_value()) {
    auto record = db.readRecord(ptr->address, ptr->slot);
}
```

#### SQL执行

```cpp
// 执行SQL语句
void executeSQL(const std::string& sql);
```

**示例**:
```cpp
db.executeSQL("SELECT * FROM users WHERE age > 30");
```

---

### 3.2 BufferPool (缓冲池)

**头文件**: `include/storage/buffer_pool.h`

#### 主要接口

```cpp
class BufferPool {
public:
    // 构造函数
    BufferPool(std::size_t capacity, DiskStorage& disk);

    // 获取页面
    FetchResult fetch(const BlockAddress& addr, bool forWrite);

    // 刷新脏页
    void flush();

    // 获取统计信息
    std::size_t hits() const;
    std::size_t misses() const;
    std::size_t capacity() const;
};

// 返回结果
struct FetchResult {
    Block& block;              // 页面引用
    bool wasHit;               // 是否缓存命中
    std::optional<BlockAddress> evicted;  // 被驱逐的页面
};
```

**示例**:
```cpp
BufferPool pool(1000, disk);

// 读取页面
auto result = pool.fetch(BlockAddress{"users", 0}, false);
if (result.wasHit) {
    std::cout << "Cache hit\n";
}

// 修改页面
auto writeResult = pool.fetch(addr, true);  // forWrite=true
writeResult.block.insertRecord(record);

// 刷新所有脏页
pool.flush();
```

---

### 3.3 DiskStorage (磁盘管理)

**头文件**: `include/storage/disk_manager.h`

#### 主要接口

```cpp
class DiskStorage {
public:
    // 构造函数
    DiskStorage(
        std::size_t totalBlocks,
        const std::string& basePath,
        std::size_t blockSizeBytes
    );

    // 分配新数据块
    BlockAddress allocateBlock(const std::string& tableName);

    // 读取块
    std::vector<std::uint8_t> readBlock(const BlockAddress& addr);

    // 写入块
    void writeBlock(
        const BlockAddress& addr,
        const std::vector<std::uint8_t>& data
    );

    // 检查块是否存在
    bool contains(const BlockAddress& addr) const;
};
```

---

### 3.4 VariableLengthPage (变长页面)

**头文件**: `include/storage/page.h`

#### 主要接口

```cpp
class VariableLengthPage {
public:
    // 构造函数
    explicit VariableLengthPage(std::size_t capacity);

    // 插入记录
    std::optional<std::size_t> insert(const Record& record);

    // 获取记录
    const Record* get(std::size_t slotIndex) const;

    // 更新记录
    bool update(std::size_t slotIndex, const Record& record);

    // 删除记录
    bool erase(std::size_t slotIndex);

    // 空间回收
    struct VacuumStats {
        std::size_t clearedSlots;
        std::size_t reclaimedBytes;
    };
    VacuumStats vacuumDeletedSlots();

    // 统计信息
    std::size_t activeCount() const;
    std::size_t deletedCount() const;
    std::size_t usedBytes() const;
    std::size_t freeBytes() const;
};
```

**示例**:
```cpp
VariableLengthPage page(4096);

// 插入
Record rec{"1", "Alice"};
auto slot = page.insert(rec);

// 读取
if (slot.has_value()) {
    const Record* stored = page.get(*slot);
}

// 更新
page.update(*slot, Record{"1", "Bob"});

// 删除
page.erase(*slot);

// 回收空间
auto stats = page.vacuumDeletedSlots();
std::cout << "Reclaimed " << stats.reclaimedBytes << " bytes\n";
```

---

### 3.5 BPlusTreeIndex (B+树索引)

**头文件**: `include/index/b_plus_tree.h`

#### 主要接口

```cpp
class BPlusTreeIndex {
public:
    // 构造函数
    BPlusTreeIndex(
        const IndexDefinition& definition,
        std::size_t blockSizeBytes
    );

    // 查找
    std::optional<IndexPointer> find(const std::string& key) const;

    // 插入
    void insertRecord(
        const Record& record,
        const BlockAddress& addr,
        std::size_t slot
    );

    // 删除
    void deleteRecord(const Record& record);

    // 更新
    void updateRecord(
        const Record& oldRecord,
        const Record& newRecord,
        const BlockAddress& addr,
        std::size_t slot
    );

    // 批量构建
    void rebuild(
        const std::vector<std::pair<std::string, IndexPointer>>& entries
    );

    // 持久化
    void saveToFile(const std::string& path) const;
    void loadFromFile(const std::string& path);
};
```

---

### 3.6 QueryExecutor (查询执行器)

**头文件**: `include/executor/executor.h`

#### 主要接口

```cpp
class QueryExecutor {
public:
    explicit QueryExecutor(DatabaseSystem& db);

    // 执行物理计划
    ResultSet execute(std::shared_ptr<PhysicalPlanNode> plan);

private:
    // 各种操作符的执行
    ResultSet executeTableScan(std::shared_ptr<PhysicalPlanNode> node);
    ResultSet executeIndexScan(std::shared_ptr<PhysicalPlanNode> node);
    ResultSet executeFilter(std::shared_ptr<PhysicalPlanNode> node);
    ResultSet executeProjection(std::shared_ptr<PhysicalPlanNode> node);
    ResultSet executeJoin(std::shared_ptr<PhysicalPlanNode> node);
    // ... 更多操作符
};
```

**示例**:
```cpp
QueryExecutor executor(db);

// 构建物理计划
auto scan = std::make_shared<PhysicalPlanNode>(
    PhysicalOpType::kTableScan, "scan users"
);
scan->parameters["table"] = "users";

auto filter = std::make_shared<PhysicalPlanNode>(
    PhysicalOpType::kFilter, "age > 30"
);
filter->parameters["condition"] = "age > 30";
filter->addChild(scan);

// 执行
ResultSet result = executor.execute(filter);

// 遍历结果
for (const auto& tuple : result) {
    std::cout << tuple.getValue("name") << "\n";
}
```

---

### 3.7 SQL解析器

**头文件**: `include/parser/query_processor.h`

#### 完整解析流程

```cpp
// 1. 词法分析
Lexer lexer("SELECT * FROM users WHERE age > 30");
std::vector<Token> tokens = lexer.tokenize();

// 2. 语法分析
Parser parser(tokens);
std::shared_ptr<ASTNode> ast = parser.parse();

// 3. 语义分析
SemanticAnalyzer analyzer(db);
analyzer.analyze(ast);

// 4. 逻辑计划生成
LogicalPlanGenerator logicalGen;
auto logicalPlan = logicalGen.generateLogicalPlan(ast);

// 5. 逻辑优化
LogicalOptimizer optimizer;
auto optimizedPlan = optimizer.optimize(logicalPlan);

// 6. 物理计划生成
PhysicalPlanGenerator physGen(db);
auto physicalPlan = physGen.generatePhysicalPlan(optimizedPlan);

// 7. 执行
QueryExecutor executor(db);
ResultSet result = executor.execute(physicalPlan);
```

**或使用简化接口**:
```cpp
QueryProcessor processor(db);
processor.processQuery("SELECT * FROM users WHERE age > 30");

// 获取各阶段结果
std::string astStr = processor.getLastAST();
std::string logicalPlanStr = processor.getLastLogicalPlan();
```

---

## 4. 扩展开发指南

### 4.1 添加新的数据类型

**步骤1**: 修改 `include/common/types.h`

```cpp
enum class ColumnType {
    Integer,
    Double,
    String,
    DateTime,  // 新增
    Boolean    // 新增
};
```

**步骤2**: 实现类型转换和比较

```cpp
// include/common/utils.h
namespace dbms {
    std::string serializeDateTime(const DateTime& dt);
    DateTime deserializeDateTime(const std::string& str);
}
```

**步骤3**: 更新表达式求值器 `src/executor/expression.cpp`

```cpp
Value evaluateExpression(const Expression& expr) {
    if (expr.type == ExprType::DateTime) {
        // 处理日期时间表达式
    }
}
```

**步骤4**: 更新SQL解析器

```cpp
// src/parser/query_processor.cpp
ColumnType parseColumnType(const std::string& typeStr) {
    if (typeStr == "datetime") return ColumnType::DateTime;
    // ...
}
```

### 4.2 添加新的聚合函数

**步骤1**: 扩展函数枚举 `include/executor/aggregate.h`

```cpp
enum class AggregateFunction {
    COUNT, SUM, AVG, MIN, MAX, VARIANCE, STDDEV,
    MEDIAN,     // 新增
    MODE        // 新增
};
```

**步骤2**: 实现聚合逻辑 `src/executor/aggregate.cpp`

```cpp
class MedianAggregator : public Aggregator {
    std::vector<double> values;

    void accumulate(const std::string& value) override {
        values.push_back(std::stod(value));
    }

    std::string finalize() override {
        std::sort(values.begin(), values.end());
        size_t n = values.size();
        if (n % 2 == 0) {
            return std::to_string((values[n/2-1] + values[n/2]) / 2.0);
        } else {
            return std::to_string(values[n/2]);
        }
    }
};
```

**步骤3**: 注册到执行器

```cpp
std::unique_ptr<Aggregator> createAggregator(AggregateFunction func) {
    switch (func) {
        case AggregateFunction::MEDIAN:
            return std::make_unique<MedianAggregator>();
        // ...
    }
}
```

### 4.3 添加新的JOIN算法

**步骤1**: 定义新算法类型

```cpp
enum class PhysicalOpType {
    // ...
    kSortMergeJoin,  // 新增
};
```

**步骤2**: 实现算法 `src/executor/join.cpp`

```cpp
ResultSet executeSortMergeJoin(
    std::shared_ptr<PhysicalPlanNode> node,
    DatabaseSystem& db
) {
    // 1. 对两个输入排序
    auto leftSorted = executeSort(node->children[0], sortKey);
    auto rightSorted = executeSort(node->children[1], sortKey);

    // 2. 归并
    ResultSet result;
    size_t i = 0, j = 0;
    while (i < leftSorted.size() && j < rightSorted.size()) {
        if (leftSorted[i].key == rightSorted[j].key) {
            result.addTuple(merge(leftSorted[i], rightSorted[j]));
            i++; j++;
        } else if (leftSorted[i].key < rightSorted[j].key) {
            i++;
        } else {
            j++;
        }
    }
    return result;
}
```

**步骤3**: 在物理计划生成器中选择

```cpp
std::shared_ptr<PhysicalPlanNode> chooseJoinMethod(
    std::shared_ptr<RelAlgNode> node
) {
    if (bothInputsSorted(node)) {
        return generateSortMergeJoin(node);
    } else if (hasEquiJoinCondition(node)) {
        return generateHashJoin(node);
    } else {
        return generateNestedLoopJoin(node);
    }
}
```

### 4.4 实现并发控制

**步骤1**: 添加锁管理器 `include/concurrency/lock_manager.h`

```cpp
class LockManager {
public:
    // 获取共享锁
    bool acquireSharedLock(
        TransactionId txnId,
        const ResourceId& rid
    );

    // 获取排他锁
    bool acquireExclusiveLock(
        TransactionId txnId,
        const ResourceId& rid
    );

    // 释放锁
    void releaseLock(
        TransactionId txnId,
        const ResourceId& rid
    );

    // 检测死锁
    bool detectDeadlock();

private:
    std::unordered_map<ResourceId, LockEntry> lockTable_;
    std::mutex mutex_;
};
```

**步骤2**: 在记录操作中加锁

```cpp
void DatabaseSystem::insertRecord(
    const std::string& tableName,
    Record record
) {
    auto txnId = getCurrentTransactionId();

    // 获取表级意向锁
    lockManager_.acquireIntentionExclusiveLock(txnId, tableName);

    // 执行插入...

    // 事务结束时自动释放锁
}
```

### 4.5 实现查询缓存

**步骤1**: 定义缓存结构

```cpp
class QueryCache {
public:
    struct CacheEntry {
        std::string sql;
        ResultSet result;
        std::chrono::system_clock::time_point timestamp;
    };

    std::optional<ResultSet> get(const std::string& sql);
    void put(const std::string& sql, const ResultSet& result);
    void invalidate(const std::string& tableName);

private:
    std::unordered_map<std::string, CacheEntry> cache_;
    std::size_t maxSize_{1000};
};
```

**步骤2**: 集成到执行流程

```cpp
void DatabaseSystem::executeSQL(const std::string& sql) {
    // 1. 检查缓存
    if (auto cached = queryCache_.get(sql)) {
        return *cached;
    }

    // 2. 执行查询
    ResultSet result = executeQuery(sql);

    // 3. 写入缓存
    queryCache_.put(sql, result);

    return result;
}
```

---

## 5. 测试与调试

### 5.1 单元测试

**添加新测试** (`tests/dbms_tests.cpp`):

```cpp
void testMyNewFeature() {
    const fs::path tempRoot = fs::current_path() / "tmp_test_feature";
    removeIfExists(tempRoot);
    WorkingDirGuard guard(tempRoot);

    DatabaseSystem db(512, 2*1024*1024, 8*1024*1024);

    // 测试逻辑
    TableSchema schema("test", {{"id", ColumnType::Integer, 16}});
    db.registerTable(schema);

    db.insertRecord("test", Record{"1"});

    auto dump = db.dumpTable("test");
    require(dump.totalRecords == 1, "Should have 1 record");

    removeIfExists(tempRoot);
}

int main() {
    TestRunner runner;
    runner.run("My new feature", testMyNewFeature);
    // ... 其他测试
    return runner.summary();
}
```

**运行测试**:
```bash
./dbms_tests
```

### 5.2 调试技巧

**使用GDB**:
```bash
gdb ./dbms
(gdb) break DatabaseSystem::insertRecord
(gdb) run
(gdb) print record.values
```

**日志输出**:
```cpp
#ifdef DEBUG
    std::cerr << "Inserting record: " << record.toString() << "\n";
#endif
```

**内存检查** (Valgrind):
```bash
valgrind --leak-check=full ./dbms
```

### 5.3 性能测试

**示例基准测试**:
```cpp
#include <chrono>

void benchmarkInsert() {
    DatabaseSystem db(4096, 64*1024*1024, 512*1024*1024);
    TableSchema schema("bench", {{"id", ColumnType::Integer, 16}});
    db.registerTable(schema);

    auto start = std::chrono::high_resolution_clock::now();

    db.beginTransaction();
    for (int i = 0; i < 100000; ++i) {
        db.insertRecord("bench", Record{std::to_string(i)});
    }
    db.commitTransaction();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end - start
    );

    std::cout << "100K inserts: " << duration.count() << " ms\n";
    std::cout << "Throughput: " << (100000.0 / duration.count() * 1000)
              << " ops/sec\n";
}
```

---

## 6. 贡献指南

### 6.1 代码风格

**命名约定**:
- 类名: `PascalCase` (如 `DatabaseSystem`)
- 函数/方法: `camelCase` (如 `insertRecord`)
- 变量: `camelCase` (如 `blockSize`)
- 常量: `kPascalCase` (如 `kMaxRecordSize`)
- 成员变量: `camelCase_` (如 `buffer_`)

**格式化**:
使用clang-format (配置见`.clang-format`):
```bash
clang-format -i src/**/*.cpp include/**/*.h
```

### 6.2 提交Pull Request

1. Fork项目
2. 创建功能分支: `git checkout -b feature/my-feature`
3. 提交更改: `git commit -am 'Add some feature'`
4. 推送到分支: `git push origin feature/my-feature`
5. 提交Pull Request

**PR检查清单**:
- [ ] 代码编译无警告
- [ ] 所有测试通过
- [ ] 添加了相应的测试
- [ ] 更新了文档
- [ ] 遵循代码风格

### 6.3 报告Bug

**Issue模板**:
```markdown
**描述**: 简要描述问题

**重现步骤**:
1. 执行 ...
2. 输入 ...
3. 观察到 ...

**期望行为**: 应该发生什么

**实际行为**: 实际发生了什么

**环境**:
- OS: Windows 10
- 编译器: GCC 11.2
- 版本: commit abc123

**额外信息**: 其他相关信息
```

---

## 7. API使用示例汇总

### 7.1 完整示例：从零创建数据库应用

```cpp
#include "system/database.h"
#include "common/types.h"
#include <iostream>

using namespace dbms;

int main() {
    // 1. 创建数据库系统
    DatabaseSystem db(
        4096,           // 4KB块
        32*1024*1024,   // 32MB内存
        256*1024*1024   // 256MB磁盘
    );

    // 2. 定义schema
    TableSchema users(
        "users",
        {
            {"id", ColumnType::Integer, 16},
            {"name", ColumnType::String, 64},
            {"email", ColumnType::String, 128},
            {"age", ColumnType::Integer, 8}
        }
    );

    TableSchema orders(
        "orders",
        {
            {"id", ColumnType::Integer, 16},
            {"user_id", ColumnType::Integer, 16},
            {"amount", ColumnType::Integer, 16}
        }
    );

    // 3. 注册表
    db.registerTable(users);
    db.registerTable(orders);

    // 4. 创建索引
    db.createIndex("idx_users_id", "users", "id");
    db.createIndex("idx_orders_user_id", "orders", "user_id");

    // 5. 插入数据（带事务）
    db.beginTransaction();
    try {
        db.insertRecord("users", Record{"1", "Alice", "alice@example.com", "30"});
        db.insertRecord("users", Record{"2", "Bob", "bob@example.com", "42"});
        db.insertRecord("orders", Record{"100", "1", "1200"});
        db.insertRecord("orders", Record{"101", "2", "500"});
        db.commitTransaction();
    } catch (const std::exception& e) {
        db.rollbackTransaction();
        std::cerr << "Transaction failed: " << e.what() << "\n";
        return 1;
    }

    // 6. 执行查询
    db.executeSQL(
        "SELECT u.name, o.amount "
        "FROM users u "
        "JOIN orders o ON u.id = o.user_id "
        "WHERE u.age > 30 "
        "ORDER BY o.amount DESC"
    );

    // 7. 清理
    db.flushAll();

    return 0;
}
```

### 7.2 程序化查询构建

```cpp
#include "executor/executor.h"
#include "parser/query_processor.h"

// 不使用SQL字符串，直接构建物理计划
std::shared_ptr<PhysicalPlanNode> buildPlan(DatabaseSystem& db) {
    // 表扫描
    auto scan = std::make_shared<PhysicalPlanNode>(
        PhysicalOpType::kTableScan, "scan users"
    );
    scan->parameters["table"] = "users";

    // 过滤
    auto filter = std::make_shared<PhysicalPlanNode>(
        PhysicalOpType::kFilter, "age > 30"
    );
    filter->parameters["condition"] = "age > 30";
    filter->addChild(scan);

    // 投影
    auto project = std::make_shared<PhysicalPlanNode>(
        PhysicalOpType::kProjection, "select columns"
    );
    project->outputColumns = {"name", "age"};
    project->addChild(filter);

    // 排序
    auto sort = std::make_shared<PhysicalPlanNode>(
        PhysicalOpType::kSort, "order by age"
    );
    sort->parameters["order_by"] = "age:DESC";
    sort->addChild(project);

    return sort;
}

// 执行
QueryExecutor executor(db);
ResultSet result = executor.execute(buildPlan(db));

// 输出结果
for (const auto& tuple : result) {
    std::cout << tuple.getValue("name") << ", "
              << tuple.getValue("age") << "\n";
}
```

---

## 8. 常见陷阱与最佳实践

### 8.1 内存管理

❌ **错误**:
```cpp
auto& block = buffer_.fetch(addr, false).block;
// block引用可能在下次fetch时失效！
```

✅ **正确**:
```cpp
auto fetchResult = buffer_.fetch(addr, false);
// 在作用域内使用fetchResult.block
const Record* rec = fetchResult.block.getRecord(0);
```

### 8.2 事务边界

❌ **错误**:
```cpp
db.insertRecord("users", rec1);
db.beginTransaction();  // 太晚了
db.insertRecord("users", rec2);
db.commitTransaction();
```

✅ **正确**:
```cpp
db.beginTransaction();
db.insertRecord("users", rec1);
db.insertRecord("users", rec2);
db.commitTransaction();
```

### 8.3 索引失效

❌ **不会使用索引**:
```cpp
db.executeSQL("SELECT * FROM users WHERE age + 1 > 30");
```

✅ **会使用索引**:
```cpp
db.executeSQL("SELECT * FROM users WHERE age > 29");
```

---

## 总结

本文档提供了Mini DBMS开发所需的核心API和扩展指南。更多细节请查看源代码注释和[架构设计文档](ARCHITECTURE.md)。

欢迎贡献！
