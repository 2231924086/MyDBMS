# Mini DBMS 架构设计文档

## 概述

本文档详细描述Mini DBMS的整体架构、各模块设计和数据流。本系统采用经典的分层架构，从上到下包括SQL解析层、查询优化层、执行引擎层、事务管理层和存储引擎层。

## 系统架构

### 整体架构图

```
┌──────────────────────────────────────────────────────────┐
│                     Client Interface                      │
│                   (命令行交互 main.cpp)                   │
└───────────────────────────────┬──────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────┐
│                    SQL Parser Layer                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐               │
│  │  Lexer   │→ │  Parser  │→ │ Semantic │               │
│  │ (词法)   │  │ (语法)   │  │ Analyzer │               │
│  └──────────┘  └──────────┘  └──────────┘               │
└───────────────────────────────┬──────────────────────────┘
                                │ AST (抽象语法树)
┌───────────────────────────────▼──────────────────────────┐
│                 Query Optimizer Layer                     │
│  ┌─────────────────┐  ┌─────────────────┐               │
│  │ Logical Plan    │→ │ Logical         │               │
│  │ Generator       │  │ Optimizer       │               │
│  └─────────────────┘  └─────────────────┘               │
│           │                     │                         │
│           └─────────┬───────────┘                         │
│                     ▼                                     │
│           ┌─────────────────┐                            │
│           │ Physical Plan   │                            │
│           │ Generator       │                            │
│           └─────────────────┘                            │
└───────────────────────────────┬──────────────────────────┘
                                │ Physical Plan
┌───────────────────────────────▼──────────────────────────┐
│                   Executor Engine Layer                   │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐   │
│  │TableScan │ │IndexScan │ │  Filter  │ │  Join    │   │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘   │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐   │
│  │  Sort    │ │Aggregate │ │ Distinct │ │  Limit   │   │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘   │
│  ┌──────────┐ ┌──────────┐                              │
│  │Projection│ │  Alias   │                              │
│  └──────────┘ └──────────┘                              │
└───────────────────────────────┬──────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────┐
│              Transaction & Index Manager                  │
│  ┌─────────────────┐  ┌─────────────────┐               │
│  │ Transaction Mgr │  │  Index Manager  │               │
│  │ (BEGIN/COMMIT)  │  │  (B+Tree)       │               │
│  └─────────────────┘  └─────────────────┘               │
│  ┌─────────────────┐  ┌─────────────────┐               │
│  │   Undo Log      │  │   WAL Logger    │               │
│  └─────────────────┘  └─────────────────┘               │
└───────────────────────────────┬──────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────┐
│                   Storage Engine Layer                    │
│  ┌─────────────────┐  ┌─────────────────┐               │
│  │  Buffer Pool    │  │  Disk Manager   │               │
│  │  (LRU缓存)      │  │  (块分配)       │               │
│  └─────────────────┘  └─────────────────┘               │
│  ┌─────────────────┐  ┌─────────────────┐               │
│  │ Variable-Length │  │ Data Dictionary │               │
│  │     Page        │  │  & Catalog      │               │
│  └─────────────────┘  └─────────────────┘               │
└──────────────────────────────────────────────────────────┘
                                │
                                ▼
                        ┌───────────────┐
                        │  Disk Files   │
                        │  (存储目录)   │
                        └───────────────┘
```

---

## 各层详细设计

### 1. SQL Parser Layer (SQL解析层)

#### 1.1 Lexer (词法分析器)
**文件**: `include/parser/query_processor.h`, `src/parser/query_processor.cpp`

**功能**:
- 将SQL字符串转换为Token流
- 识别关键字、标识符、字面量、运算符

**Token类型**:
```cpp
enum class TokenType {
    // Keywords
    SELECT, FROM, WHERE, AND, OR, JOIN, ORDER, BY, ...
    // Operators
    EQUAL, NOT_EQUAL, LESS, GREATER, ...
    // Literals
    IDENTIFIER, STRING_LITERAL, NUMBER_LITERAL
};
```

#### 1.2 Parser (语法分析器)
**功能**:
- 根据SQL语法规则解析Token流
- 构建抽象语法树 (AST)
- 递归下降解析

**AST节点类型**:
```cpp
enum class ASTNodeType {
    SELECT_STATEMENT, INSERT_STATEMENT, UPDATE_STATEMENT, DELETE_STATEMENT,
    WHERE_CLAUSE, JOIN_CLAUSE, ORDER_BY, GROUP_BY, ...
};
```

**解析流程**:
```
SQL字符串 → tokenize() → Token流 → parse() → AST
```

#### 1.3 Semantic Analyzer (语义分析器)
**功能**:
- 验证表和列是否存在
- 类型检查
- 作用域检查

---

### 2. Query Optimizer Layer (查询优化层)

#### 2.1 Logical Plan Generator
**文件**: `include/parser/query_processor.h`

**功能**: 将AST转换为关系代数逻辑计划

**关系代数操作**:
```cpp
enum class RelAlgOpType {
    kScan,        // 表扫描
    kSelect,      // 选择 (σ)
    kProject,     // 投影 (π)
    kJoin,        // 连接 (⋈)
    kSort,        // 排序
    kGroup,       // 分组
    kDistinct,    // 去重
    kLimit        // 限制
};
```

**示例转换**:
```sql
SELECT name, age FROM users WHERE age > 30 ORDER BY age
```
转换为:
```
Sort(age)
  └─ Project(name, age)
      └─ Select(age > 30)
          └─ Scan(users)
```

#### 2.2 Logical Optimizer
**功能**: 对逻辑计划进行等价变换优化

**优化规则**:
1. **谓词下推 (Predicate Pushdown)**
   - 将过滤条件尽可能推到数据源附近
   - 减少中间结果集大小

2. **投影下推 (Projection Pushdown)**
   - 尽早消除不需要的列
   - 减少数据传输量

3. **选择合并 (Selection Combining)**
   - 合并多个过滤条件
   - 减少操作符数量

4. **JOIN重排序 (Join Reordering)**
   - 优化多表连接顺序
   - 先连接结果较小的表

#### 2.3 Physical Plan Generator
**功能**: 将逻辑计划转换为物理执行计划

**物理操作符**:
```cpp
enum class PhysicalOpType {
    kTableScan,       // 全表扫描
    kIndexScan,       // 索引扫描
    kFilter,          // 过滤
    kProjection,      // 投影
    kHashJoin,        // Hash连接
    kNestedLoopJoin,  // 嵌套循环连接
    kMergeJoin,       // 归并连接
    kSort,            // 排序
    kAggregate,       // 聚合
    ...
};
```

**索引选择逻辑**:
- 检查WHERE条件中的等值比较
- 查找对应列上的索引
- 评估索引扫描 vs 全表扫描的代价

---

### 3. Executor Engine Layer (执行引擎层)

#### 3.1 Iterator Model (迭代器模型)
**设计模式**: Volcano/Iterator Model

每个物理操作符实现以下接口:
```cpp
class Operator {
    virtual void init() = 0;       // 初始化
    virtual Tuple* next() = 0;     // 获取下一条记录
    virtual void close() = 0;      // 清理资源
};
```

#### 3.2 主要操作符实现

**TableScan** (`src/executor/table_scan.cpp`)
- 顺序扫描表的所有数据块
- 通过Buffer Pool获取页面
- 返回每条记录

**IndexScan** (`src/executor/index_scan.cpp`)
- 使用B+树索引查找
- 支持等值查询和范围查询
- 直接定位到目标记录

**Filter** (`src/executor/filter.cpp`)
- 评估WHERE条件
- 支持复杂表达式 (AND/OR/NOT)
- 支持比较运算符 (=, !=, <, >, <=, >=)

**Projection** (`src/executor/projection.cpp`)
- 选择输出列
- 列重命名和别名

**Join** (`src/executor/join.cpp`)
- **Hash Join**: 构建哈希表，适合等值连接
- **Nested Loop Join**: 双重循环，适合小表
- **Merge Join**: 归并连接，适合已排序数据
- 支持 INNER/LEFT/RIGHT JOIN

**Sort** (`src/executor/sort.cpp`)
- 内存排序
- 支持多列排序
- 支持ASC/DESC

**Aggregate** (`src/executor/aggregate.cpp`)
- GROUP BY分组
- 聚合函数: COUNT, SUM, AVG, MIN, MAX, VARIANCE, STDDEV
- HAVING过滤

**Distinct** (`src/executor/distinct.cpp`)
- 使用哈希集合去重
- 支持多列去重

**Limit** (`src/executor/limit.cpp`)
- LIMIT限制结果数量
- OFFSET跳过记录

#### 3.3 执行流程示例

```sql
SELECT u.name, COUNT(*)
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE u.age > 30
GROUP BY u.name
```

执行计划:
```
Aggregate(GROUP BY u.name, COUNT(*))
  └─ HashJoin(u.id = o.user_id)
      ├─ Filter(u.age > 30)
      │   └─ TableScan(users)
      └─ TableScan(orders)
```

---

### 4. Transaction & Index Manager (事务与索引管理层)

#### 4.1 Transaction Manager

**组件**:
- **Transaction Context**: 事务上下文
- **Undo Log**: 回滚日志
- **WAL (Write-Ahead Log)**: 写前日志

**事务操作流程**:

**BEGIN事务**:
```
1. 分配事务ID
2. 初始化Undo Log
3. 写WAL Begin记录
4. 设置事务标志
```

**操作执行** (INSERT/UPDATE/DELETE):
```
1. 执行操作
2. 记录到Undo Log
3. 写WAL数据记录
4. 标记Buffer Pool页面为脏
```

**COMMIT提交**:
```
1. 写WAL Commit记录
2. 刷新日志缓冲到磁盘
3. 刷新脏页到磁盘
4. 清空Undo Log
5. 释放事务资源
```

**ROLLBACK回滚**:
```
1. 反向遍历Undo Log
2. 应用逆操作 (undo)
   - INSERT → DELETE
   - DELETE → INSERT
   - UPDATE → 恢复旧值
3. 写WAL Rollback记录
4. 清空Undo Log
```

#### 4.2 Crash Recovery (崩溃恢复)

**WAL结构**:
```cpp
struct WALEntry {
    size_t txnId;              // 事务ID
    EntryType type;            // BEGIN/COMMIT/ROLLBACK/INSERT/UPDATE/DELETE
    BlockAddress address;      // 数据块地址
    size_t slot;               // 槽位索引
    optional<Record> before;   // 旧值
    optional<Record> after;    // 新值
};
```

**恢复流程**:
```
1. 读取WAL日志文件
2. 分析阶段: 确定已提交/未提交事务
3. Redo阶段: 重做已提交事务的操作
4. Undo阶段: 回滚未提交事务的操作
5. 清空WAL日志
```

#### 4.3 Index Manager

**B+树索引结构**:
```
                    [Root Node]
                   /     |     \
          [Internal]  [Internal]  [Internal]
           /    \        |          /    \
      [Leaf] [Leaf]   [Leaf]    [Leaf] [Leaf]
         ↓      ↓        ↓         ↓      ↓
      Records Records Records  Records Records
```

**索引操作**:
- **Insert**: 插入键值对，可能触发节点分裂
- **Delete**: 删除键，可能触发节点合并
- **Update**: 先删除后插入
- **Search**: 从根到叶查找

**索引持久化**:
```
storage/indexes/<index_name>.tree
```

**索引元数据**:
```
storage/meta/indexes.meta
格式: name|table|column|column_idx|key_length|unique
```

---

### 5. Storage Engine Layer (存储引擎层)

#### 5.1 Buffer Pool (缓冲池)

**文件**: `include/storage/buffer_pool.h`, `src/storage/buffer_pool.cpp`

**设计**:
- **Frame**: 内存中的页面槽
- **Page Table**: 维护 BlockAddress → Frame 映射
- **LRU队列**: 页面置换策略

**LRU替换算法**:
```
每次访问:
1. 如果页面在缓冲池 → 移到队列头部 (最近使用)
2. 如果页面不在 → 淘汰队列尾部 (最久未使用)
3. 如果被淘汰页面是脏页 → 先写回磁盘
```

**Fetch流程**:
```cpp
FetchResult fetch(BlockAddress addr, bool forWrite) {
    if (页面在缓冲池) {
        更新LRU位置;
        if (forWrite) 标记为脏页;
        return {block, hit=true};
    } else {
        if (缓冲池满) {
            evict = 选择LRU页面;
            if (evict是脏页) 写回磁盘;
        }
        从磁盘读取页面;
        加入缓冲池;
        return {block, hit=false, evicted};
    }
}
```

#### 5.2 Disk Manager (磁盘管理器)

**文件**: `include/storage/disk_manager.h`, `src/storage/disk_manager.cpp`

**职责**:
- 分配和释放数据块
- 读写物理页面
- 管理磁盘空间

**存储布局**:
```
storage/
├── meta/
│   ├── schemas.meta       # 表schema
│   ├── indexes.meta       # 索引定义
│   └── access_plans.log   # 执行计划缓存
├── logs/
│   ├── operations.log     # 操作日志
│   └── wal.log            # WAL日志
├── indexes/
│   └── <index_name>.tree  # 索引文件
└── <table_name>/
    ├── block_0.blk        # 数据块
    ├── block_1.blk
    └── ...
```

**Block文件格式**:
```
┌─────────────────────────────────────────┐
│  Signature (4 bytes)  0xDEADBEEF        │
├─────────────────────────────────────────┤
│  Table Name Length (4 bytes)            │
├─────────────────────────────────────────┤
│  Table Name (variable)                  │
├─────────────────────────────────────────┤
│  Block Index (4 bytes)                  │
├─────────────────────────────────────────┤
│  Page Data (variable length)            │
│  - Record Count                         │
│  - Slot Directory                       │
│  - Record Data                          │
└─────────────────────────────────────────┘
```

#### 5.3 Variable-Length Page (变长页面)

**文件**: `include/storage/page.h`, `src/storage/page.cpp`

**页面结构**:
```
┌───────────────────────────────────────────────────┐
│               Page Header (16 bytes)              │
│  - Record Count (4 bytes)                         │
│  - Deleted Count (4 bytes)                        │
│  - Used Bytes (4 bytes)                           │
│  - Free Space Offset (4 bytes)                    │
├───────────────────────────────────────────────────┤
│                Slot Directory                      │
│  [Slot 0: Offset(4) + Length(4) + Flag(1)]       │
│  [Slot 1: ...]                                    │
│  [Slot 2: ...]                                    │
│  ...                                              │
├───────────────────────────────────────────────────┤
│               Free Space (grows down)             │
│                                                   │
├───────────────────────────────────────────────────┤
│               Record Data (grows up)              │
│  [Record N]                                       │
│  [Record 2]                                       │
│  [Record 1]                                       │
│  [Record 0]                                       │
└───────────────────────────────────────────────────┘
```

**Record格式**:
```
┌─────────────────────────────────────────┐
│  Field Count (4 bytes)                  │
├─────────────────────────────────────────┤
│  Field 0 Offset (4 bytes)               │
│  Field 1 Offset (4 bytes)               │
│  ...                                    │
├─────────────────────────────────────────┤
│  Field 0 Data (variable)                │
│  Field 1 Data (variable)                │
│  ...                                    │
└─────────────────────────────────────────┘
```

**操作**:
- **Insert**: 在空闲空间插入，添加Slot条目
- **Update**: 如果空间足够就地更新，否则删除后重插
- **Delete**: 标记Slot为已删除
- **Vacuum**: 清理已删除记录，回收空间

#### 5.4 Data Dictionary & Catalog

**文件**: `include/system/catalog.h`, `src/system/catalog.cpp`

**维护信息**:
- 表schema定义
- 列信息 (名称、类型、长度)
- 表统计信息 (记录数、块数)
- 索引元数据

**内存结构**:
```cpp
struct DataDictionary {
    map<string, TableSchema> tables;
    map<string, TableStats> stats;
    map<string, vector<IndexDefinition>> indexes;
};
```

---

## 数据流示例

### SELECT查询完整流程

```sql
SELECT u.name, o.amount
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE u.age > 30
ORDER BY o.amount DESC
LIMIT 10
```

**1. SQL解析**:
```
Input: SQL字符串
↓ Lexer
Token流: [SELECT, IDENTIFIER(u.name), COMMA, ...]
↓ Parser
AST: SELECT_STATEMENT
      ├─ SELECT_LIST [u.name, o.amount]
      ├─ FROM_CLAUSE
      │   └─ JOIN_CLAUSE (u.id = o.user_id)
      ├─ WHERE_CLAUSE (u.age > 30)
      ├─ ORDER_BY [o.amount DESC]
      └─ LIMIT_CLAUSE [10]
↓ Semantic Analyzer
验证: users表存在, orders表存在, 列存在
```

**2. 逻辑计划生成**:
```
Limit(10)
  └─ Sort(o.amount DESC)
      └─ Project(u.name, o.amount)
          └─ Join(u.id = o.user_id)
              ├─ Select(u.age > 30)
              │   └─ Scan(users as u)
              └─ Scan(orders as o)
```

**3. 逻辑优化**:
```
应用谓词下推:
Limit(10)
  └─ Sort(o.amount DESC)
      └─ Project(u.name, o.amount)
          └─ Join(u.id = o.user_id)
              ├─ Scan(users) + Filter(age > 30)  ← 下推到扫描
              └─ Scan(orders)
```

**4. 物理计划生成**:
```
检测到索引 idx_users_id 存在
选择 Hash Join

Limit(n=10)
  └─ Sort(key=o.amount, order=DESC)
      └─ Projection(columns=[u.name, o.amount])
          └─ HashJoin(left_key=u.id, right_key=o.user_id)
              ├─ Filter(condition=u.age > 30)
              │   └─ TableScan(table=users)
              └─ TableScan(table=orders)
```

**5. 执行**:
```
1. TableScan(users) → 通过Buffer Pool读取所有users块
2. Filter(age > 30) → 只保留age > 30的记录
3. HashJoin构建: 对users的id建立哈希表
4. TableScan(orders) → 读取orders块
5. HashJoin探测: 用orders.user_id查哈希表，匹配连接
6. Projection → 选出 name, amount 列
7. Sort → 按amount降序排序
8. Limit → 取前10条
9. 返回结果集
```

---

## 内存管理

### 内存分配策略

假设 `--memory=32M`:

```
总内存: 32MB
├─ Buffer Pool (60%):    19.2MB  ← 数据页缓存
├─ Data Dictionary (15%): 4.8MB  ← schema、统计信息
├─ Access Plan Cache (15%): 4.8MB ← 执行计划缓存
└─ Log Buffer (10%):     3.2MB  ← 日志缓冲
```

### Buffer Pool容量计算

```cpp
blockSize = 4096 bytes
bufferMemory = 32MB * 0.60 = 19.2MB
capacity = 19.2MB / 4KB = 4800 frames
```

---

## 关键算法

### 1. LRU页面置换
```cpp
双向链表 + 哈希表:
- 链表: 维护访问顺序 (头=最近, 尾=最久)
- 哈希: 快速查找页面是否在缓冲池

访问页面(addr):
    if (在哈希表中):
        移到链表头部
    else:
        if (缓冲池满):
            evict = 链表尾部页面
            if (evict.dirty):
                写回磁盘
        从磁盘读取页面
        插入链表头部
```

### 2. B+树查找
```cpp
search(key):
    node = root
    while (node不是叶子):
        找到第一个 key[i] > search_key
        node = node.children[i]

    在叶子节点中线性查找key
    return 对应的 (BlockAddress, slot)
```

### 3. Hash Join
```cpp
hashJoin(left, right, leftKey, rightKey):
    // 构建阶段
    hashTable = {}
    for row in left:
        key = row[leftKey]
        hashTable[key].append(row)

    // 探测阶段
    result = []
    for row in right:
        key = row[rightKey]
        if key in hashTable:
            for leftRow in hashTable[key]:
                result.append(leftRow + row)

    return result
```

### 4. VACUUM空间回收
```cpp
vacuum(page):
    活动记录 = []
    for slot in slots:
        if (slot.active):
            活动记录.append(slot.record)

    清空页面
    for record in 活动记录:
        page.insert(record)

    返回回收的空间大小
```

---

## 性能考虑

### 1. 索引选择策略
```
if (WHERE条件包含等值比较 column = value):
    if (存在column的索引):
        if (选择性高 > 0.1):  # 过滤掉超过90%的数据
            使用索引扫描
        else:
            使用全表扫描
    else:
        使用全表扫描
```

### 2. JOIN算法选择
```
if (等值连接 AND 左表较小):
    使用Hash Join  # O(M+N)
else if (两表都已排序):
    使用Merge Join  # O(M+N)
else:
    使用Nested Loop Join  # O(M*N)
```

### 3. Buffer Pool优化
- **预读**: 扫描时预读下一个块
- **Pin机制**: 正在使用的页面不被置换
- **Dirty Flag**: 减少不必要的磁盘写入

---

## 扩展方向

### 1. 并发控制
```
添加MVCC (Multi-Version Concurrency Control):
- 每条记录维护多个版本
- 事务ID和时间戳
- 读不阻塞写，写不阻塞读
```

### 2. 查询优化增强
```
- 收集列的统计信息 (最小值、最大值、分布)
- 构建直方图
- 基于代价的优化 (CBO)
- 动态规划JOIN重排序
```

### 3. 分区表
```
按范围/哈希分区:
- 提高查询性能 (分区裁剪)
- 便于数据管理
```

### 4. 并行执行
```
- 多线程扫描
- 并行JOIN
- 并行聚合
```

---

## 参考实现

本架构参考以下系统设计:
- **PostgreSQL**: 查询优化和执行器
- **MySQL InnoDB**: Buffer Pool和事务
- **SQLite**: 简洁的架构设计
- **CMU 15-445**: 教学实现

---

## 总结

Mini DBMS实现了一个完整的关系数据库管理系统的核心组件，包括:
- 完整的SQL解析和优化流程
- 多种查询执行算法
- 事务管理和崩溃恢复
- B+树索引和LRU缓冲池

虽然简化了许多细节（如并发控制、复杂的查询优化），但展示了数据库系统的基本原理和实现技术。
