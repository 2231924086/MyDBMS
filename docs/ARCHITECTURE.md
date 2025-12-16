# Mini DBMS 架构设计文档

## 目录
- [1. 系统概览](#1-系统概览)
- [2. 分层架构](#2-分层架构)
- [3. 核心组件详解](#3-核心组件详解)
- [4. 数据流与交互](#4-数据流与交互)
- [5. 关键设计决策](#5-关键设计决策)
- [6. 性能优化](#6-性能优化)
- [7. 扩展性考虑](#7-扩展性考虑)

---

## 1. 系统概览

### 1.1 设计目标

Mini DBMS是一个教学型关系型数据库管理系统，设计目标包括：

- **完整性**: 实现从SQL解析到物理存储的完整数据流
- **正确性**: 保证ACID特性（部分实现）
- **可读性**: 代码清晰，便于理解数据库原理
- **可扩展**: 模块化设计，便于添加新功能

### 1.2 技术栈

- **编程语言**: C++17
- **构建系统**: CMake
- **数据结构**: B+树、哈希表、LRU链表
- **设计模式**: 迭代器模式（火山模型）、工厂模式、策略模式

### 1.3 关键指标

```
代码规模:    ~11,000行
模块数量:    25个核心模块
测试覆盖:    20个测试用例
支持SQL:     DDL、DML、事务控制
索引类型:    B+树（单列）
事务隔离:    无（单线程）
```

---

## 2. 分层架构

### 2.1 架构图

```
┌──────────────────────────────────────────────────────────┐
│                                                          │
│              交互式命令行 (main.cpp)                      │
│                                                          │
│  - 命令解析 (CREATE, SELECT, INSERT, ...)               │
│  - 结果展示 (表格格式化输出)                              │
│  - Schema持久化管理                                      │
│                                                          │
└────────────────────────┬─────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────┐
│                                                          │
│               SQL解析层 (parser/)                        │
│                                                          │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐        │
│  │   Lexer    │→ │   Parser   │→ │ Semantic   │        │
│  │ (词法分析)  │  │ (语法分析)  │  │  Analyzer  │        │
│  └────────────┘  └────────────┘  └────────────┘        │
│                                                          │
│  - Token识别 (关键字、标识符、字面量)                     │
│  - AST构建 (抽象语法树)                                  │
│  - 语义检查 (表/列存在性、类型检查)                       │
│                                                          │
└────────────────────────┬─────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────┐
│                                                          │
│              查询优化层 (parser/)                         │
│                                                          │
│  ┌──────────────────┐       ┌──────────────────┐        │
│  │ Logical Plan     │   →   │ Logical          │        │
│  │ Generator        │       │ Optimizer        │        │
│  └──────────────────┘       └──────────────────┘        │
│           │                           │                 │
│           ▼                           ▼                 │
│  (关系代数节点)              (优化后的逻辑计划)           │
│   - Selection                 - 谓词下推                │
│   - Projection                - 投影下推                │
│   - Join                      - 连接重排序              │
│   - Sort/Group                - 选择合并                │
│                                                          │
│                         │                               │
│                         ▼                               │
│               ┌──────────────────┐                      │
│               │ Physical Plan    │                      │
│               │ Generator        │                      │
│               └──────────────────┘                      │
│                         │                               │
│                         ▼                               │
│                  (物理执行计划)                           │
│                   - 扫描方法选择                          │
│                   - 连接算法选择                          │
│                   - 代价估算                             │
│                                                          │
└────────────────────────┬─────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────┐
│                                                          │
│               执行引擎层 (executor/)                      │
│                                                          │
│  ┌──────────────────────────────────────────────┐       │
│  │          Executor (查询执行器)                │       │
│  │                                              │       │
│  │  火山模型: init() → next() → close()          │       │
│  └──────────────────────────────────────────────┘       │
│                                                          │
│  算子类型:                                               │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐          │
│  │ TableScan  │ │ IndexScan  │ │   Filter   │          │
│  └────────────┘ └────────────┘ └────────────┘          │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐          │
│  │ Projection │ │  HashJoin  │ │    Sort    │          │
│  └────────────┘ └────────────┘ └────────────┘          │
│  ┌────────────┐ ┌────────────┐                         │
│  │  Distinct  │ │ Aggregate  │                         │
│  └────────────┘ └────────────┘                         │
│                                                          │
│  表达式求值:                                             │
│  - 算术运算 (+, -, *, /, %)                             │
│  - 比较运算 (=, !=, <, <=, >, >=)                       │
│  - 逻辑运算 (AND, OR, NOT)                              │
│  - 聚合函数 (COUNT, SUM, AVG, MIN, MAX)                │
│                                                          │
└────────────────────────┬─────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────┐
│                                                          │
│               系统管理层 (system/)                        │
│                                                          │
│  ┌──────────────────────────────────────────────┐       │
│  │          Database (数据库核心)                │       │
│  │                                              │       │
│  │  - 表注册与管理                               │       │
│  │  - 事务管理 (BEGIN/COMMIT/ROLLBACK)          │       │
│  │  - Undo日志记录                              │       │
│  │  - 索引自动维护                               │       │
│  │  - 内存分区管理                               │       │
│  │  - 记录CRUD操作                              │       │
│  └──────────────────────────────────────────────┘       │
│                                                          │
│  ┌────────────┐       ┌────────────┐                    │
│  │  Catalog   │       │   Table    │                    │
│  │ (数据字典)  │       │  (表管理)   │                    │
│  └────────────┘       └────────────┘                    │
│                                                          │
│  - 表统计信息            - Schema定义                     │
│  - 索引注册表            - 列定义                         │
│  - 元数据持久化          - 块地址列表                     │
│                                                          │
└────────────────────────┬─────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────┐
│                                                          │
│               索引管理层 (index/)                         │
│                                                          │
│  ┌──────────────────────────────────────────────┐       │
│  │         B+ Tree (B+树索引)                    │       │
│  │                                              │       │
│  │  - 插入 (insert)                             │       │
│  │  - 查找 (search, range_search)               │       │
│  │  - 更新 (update)                             │       │
│  │  - 删除 (remove)                             │       │
│  │  - 节点分裂/合并                              │       │
│  │  - 节点借用 (rebalance)                       │       │
│  │  - 持久化 (save/load)                        │       │
│  └──────────────────────────────────────────────┘       │
│                                                          │
│  ┌──────────────────────────────────────────────┐       │
│  │      Index Manager (索引管理器)               │       │
│  │                                              │       │
│  │  - 索引定义管理                               │       │
│  │  - 批量重建索引                               │       │
│  │  - 唯一性约束检查                             │       │
│  │  - 索引目录持久化                             │       │
│  └──────────────────────────────────────────────┘       │
│                                                          │
└────────────────────────┬─────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────┐
│                                                          │
│               存储引擎层 (storage/)                       │
│                                                          │
│  ┌──────────────────────────────────────────────┐       │
│  │       Buffer Pool (缓冲池管理)                │       │
│  │                                              │       │
│  │  - LRU替换策略                               │       │
│  │  - 脏页检测                                  │       │
│  │  - 刷盘控制                                  │       │
│  │  - 命中率统计                                │       │
│  └──────────────────────────────────────────────┘       │
│                                                          │
│  ┌────────────┐       ┌────────────┐                    │
│  │   Disk     │       │    Page    │                    │
│  │  Manager   │       │  (页管理)   │                    │
│  └────────────┘       └────────────┘                    │
│                                                          │
│  - 块分配/回收          - 变长页实现                      │
│  - 块文件读写           - 槽位管理                        │
│  - 容量限制             - 记录CRUD                        │
│  - 按表组织存储         - Vacuum (空间回收)               │
│                                                          │
└──────────────────────────────────────────────────────────┘
                         │
                         ▼
                 ┌───────────────┐
                 │   磁盘文件     │
                 │               │
                 │ *.blk (数据)  │
                 │ *.tree (索引) │
                 │ *.meta (元数据)│
                 └───────────────┘
```

### 2.2 层次职责

| 层次 | 职责 | 主要组件 |
|------|------|---------|
| **交互层** | 用户界面、命令解析、结果展示 | main.cpp |
| **解析层** | SQL解析、语义分析 | Lexer, Parser, SemanticAnalyzer |
| **优化层** | 查询优化、计划生成 | LogicalOptimizer, PhysicalPlanGenerator |
| **执行层** | 查询执行、表达式求值 | Executor, Operators, Expression |
| **系统层** | 数据库管理、事务控制 | Database, Catalog, Table |
| **索引层** | 索引管理、B+树实现 | BPlusTree, IndexManager |
| **存储层** | 页管理、缓冲池、磁盘I/O | Page, BufferPool, DiskManager |

---

## 3. 核心组件详解

### 3.1 存储引擎层

#### 3.1.1 变长页管理 (VariableLengthPage)

**设计目标**: 支持变长记录的高效存储和管理

**数据结构**:
```
┌─────────────────────────────────────────────┐ ← 0
│ Page Header                                 │
│ - magic (4 bytes)                           │
│ - version (4 bytes)                         │
│ - record_count (4 bytes)                    │
│ - free_space_start (4 bytes)                │
│ - free_space_end (4 bytes)                  │
├─────────────────────────────────────────────┤ ← 20
│ Slot Array (向下增长)                        │
│                                             │
│ Slot[0]: offset=3800, length=100, del=0    │
│ Slot[1]: offset=3700, length=80, del=0     │
│ Slot[2]: offset=3620, length=60, del=1     │ ← 已删除
│ ...                                         │
├─────────────────────────────────────────────┤
│                                             │
│            Free Space                       │
│                                             │
├─────────────────────────────────────────────┤ ← free_space_end
│ Records (向上增长)                           │
│                                             │
│ [Record 1 - 100 bytes]                      │
│ [Record 2 - 80 bytes]                       │
│ [Record 3 - 60 bytes] (deleted)             │
│ ...                                         │
└─────────────────────────────────────────────┘ ← 4096
```

**核心操作**:

1. **插入记录** (`insertRecord`)
   - 检查空间是否足够: `free_space_end - free_space_start >= needed_size`
   - 在Slot Array中添加新槽位
   - 在Records区域写入数据
   - 更新指针

2. **删除记录** (`deleteRecord`)
   - 标记槽位的deleted标志为true
   - **不立即回收空间**（软删除）
   - 等待Vacuum操作

3. **Vacuum操作** (`vacuum`)
   - 遍历所有槽位
   - 压缩有效记录（去除deleted记录）
   - 重建Slot Array
   - 回收空闲空间

4. **估算记录大小** (`estimateRecordSize`)
   - 用于判断记录是否能插入
   - 考虑对齐和元数据开销

**优点**:
- 支持变长记录
- 空间利用率高
- 软删除支持事务回滚

**缺点**:
- 需要定期Vacuum
- 随机访问需遍历槽位

#### 3.1.2 缓冲池管理 (BufferPool)

**设计目标**: 减少磁盘I/O，提高数据访问速度

**LRU实现**:
```cpp
class BufferPool {
    struct Frame {
        Block block;              // 数据块
        BlockAddress address;     // 块地址
        bool dirty;               // 脏页标志
        bool pinned;              // 是否被锁定
        int lru_position;         // LRU位置
    };

    std::unordered_map<BlockAddress, Frame*> page_table;  // 页表
    std::list<Frame*> lru_list;                           // LRU链表
    size_t capacity;                                      // 容量
};
```

**核心流程**:

1. **读取块** (`getBlock`)
   ```
   查找页表
     │
     ├─ 命中 → 移到LRU头部 → 返回
     │
     └─ 未命中
          │
          ├─ 缓冲池未满 → 从磁盘加载 → 插入LRU头部 → 返回
          │
          └─ 缓冲池已满
               │
               └─ 驱逐LRU尾部
                    │
                    ├─ 脏页? → 刷盘
                    │
                    └─ 加载新页 → 插入LRU头部 → 返回
   ```

2. **标记脏页** (`markDirty`)
   - 设置Frame的dirty标志
   - 延迟写入（Write-back策略）

3. **刷盘** (`flush`)
   - 遍历所有脏页
   - 调用DiskManager写入磁盘
   - 清除dirty标志

**性能指标**:
- 命中率 = hits / (hits + misses)
- 目标命中率 > 90%（充足内存）

#### 3.1.3 磁盘管理 (DiskManager)

**文件组织**:
```
storage/
├── meta/
│   └── schemas.meta          # 表模式定义
├── users/                    # 表目录
│   ├── block_0.blk          # 数据块0
│   ├── block_1.blk          # 数据块1
│   └── ...
├── orders/
│   ├── block_0.blk
│   └── ...
└── indexes/
    ├── idx_users_id.tree    # B+树索引文件
    └── ...
```

**块地址格式**:
```cpp
struct BlockAddress {
    std::string table;    // 表名
    int block_id;         // 块ID
};
```

**核心操作**:
- `allocateBlock(table)`: 分配新块
- `writeBlock(addr, data)`: 写入块
- `readBlock(addr)`: 读取块
- `freeBlock(addr)`: 释放块（删除文件）

---

### 3.2 索引管理层

#### 3.2.1 B+树实现 (BPlusTree)

**设计参数**:
```cpp
template<typename KeyType, typename ValueType>
class BPlusTree {
    int order;                    // 树的阶（默认4）
    Node* root;                   // 根节点
    std::string index_file;       // 持久化文件路径
};
```

**节点结构**:
```cpp
struct Node {
    bool is_leaf;                         // 叶子节点?
    int key_count;                        // 当前键数量
    std::vector<KeyType> keys;            // 键数组
    std::vector<ValueType> values;        // 值数组（叶子节点）
    std::vector<Node*> children;          // 子节点指针（内部节点）
    Node* next;                           // 右兄弟指针（叶子节点链表）
};
```

**B+树性质**:
- 内部节点: ⌈order/2⌉ ≤ key_count ≤ order
- 叶子节点: ⌈order/2⌉ ≤ key_count ≤ order
- 所有叶子节点在同一层
- 叶子节点通过next指针形成链表

**核心算法**:

1. **插入** (`insert`)
   ```
   查找插入位置
     │
     └─ 到达叶子节点
          │
          ├─ 节点未满 → 直接插入 → 完成
          │
          └─ 节点已满
               │
               └─ 分裂节点
                    │
                    ├─ 中间键上升到父节点
                    │
                    └─ 递归处理父节点（可能继续分裂）
   ```

2. **删除** (`remove`)
   ```
   查找删除键
     │
     └─ 到达叶子节点
          │
          └─ 删除键
               │
               ├─ 节点键数 ≥ ⌈order/2⌉ → 完成
               │
               └─ 节点键数 < ⌈order/2⌉ (下溢)
                    │
                    ├─ 尝试从左兄弟借键
                    │
                    ├─ 尝试从右兄弟借键
                    │
                    └─ 无法借键 → 合并节点
                         │
                         └─ 递归处理父节点
   ```

3. **节点借用** (`borrowFromLeft/borrowFromRight`)
   - 条件: 兄弟节点有多余的键
   - 过程: 移动兄弟的键 → 更新父节点分隔符

4. **节点合并** (`mergeNodes`)
   - 条件: 兄弟节点都只有最少键数
   - 过程: 合并两个节点 → 删除父节点的分隔符

**持久化格式**:
```
IDXTREE V1 4             # Magic + Version + Order
NODE 0 0 2 50 100 1 2    # 内部节点: id, is_leaf, key_count, keys, children
NODE 1 1 2 10 20 v1 v2   # 叶子节点: id, is_leaf, key_count, keys, values
NODE 2 1 3 60 70 80 v3 v4 v5
```

**优化技术**:
- 叶子节点链表: 加速范围查询
- 延迟合并: 避免频繁合并操作
- 批量加载: 支持从已排序数据构建

---

### 3.3 系统管理层

#### 3.3.1 数据库核心 (Database)

**职责**: 系统的中央控制器

**主要功能**:

1. **表管理**
   ```cpp
   void registerTable(const TableSchema& schema);
   Table& getTable(const std::string& name);
   std::vector<std::string> getAllTableNames();
   ```

2. **记录操作**
   ```cpp
   BlockAddress insertRecord(const std::string& table, const std::vector<uint8_t>& data);
   void updateRecord(BlockAddress addr, const std::vector<uint8_t>& new_data);
   void deleteRecord(BlockAddress addr);
   std::vector<uint8_t> readRecord(BlockAddress addr);
   ```

3. **事务管理**
   ```cpp
   struct UndoRecord {
       enum UndoType { Insert, Update, Delete } type;
       std::string table;
       BlockAddress address;
       std::vector<uint8_t> old_data;  // 用于UPDATE和DELETE
   };

   void beginTransaction();
   void commitTransaction();
   void rollbackTransaction();  // 回放undo日志
   ```

4. **索引自动维护**
   - INSERT时自动插入索引
   - UPDATE时更新索引
   - DELETE时删除索引
   - 唯一性约束检查

**内存分区策略**:
```cpp
total_memory = user_specified_memory;

buffer_pool_size = total_memory * 0.60;      // 60% 数据缓冲
plan_cache_size = total_memory * 0.15;       // 15% 计划缓存
catalog_size = total_memory * 0.15;          // 15% 数据字典
log_buffer_size = total_memory * 0.10;       // 10% 日志缓冲
```

**事务回滚流程**:
```
ROLLBACK
  │
  └─ 逆序遍历undo日志
       │
       ├─ UndoType::Insert
       │    └─ deleteRecord(addr)  (不记录新undo)
       │
       ├─ UndoType::Update
       │    └─ updateRecord(addr, old_data)
       │
       └─ UndoType::Delete
            └─ undeleteRecord(addr)  (恢复已删除记录)
```

---

### 3.4 查询优化层

#### 3.4.1 逻辑优化器 (LogicalOptimizer)

**优化规则**:

1. **谓词下推 (Selection Push-down)**
   ```
   优化前:
       π (Projection)
         │
         σ (Selection: age > 18)
         │
       Scan(students)

   优化后:
       π (Projection)
         │
       Scan(students)  ← σ (age > 18) 融合到Scan
   ```
   - 减少中间结果集大小
   - 尽早过滤无关数据

2. **投影下推 (Projection Push-down)**
   ```
   优化前:
       π (name, age)
         │
         σ (gpa > 3.5)
         │
       Scan(students) → 读取所有列

   优化后:
       Scan(students)
         ↓
       只读取 {name, age, gpa} 三列
   ```
   - 减少I/O量
   - 减少内存占用

3. **连接重排序 (Join Reordering)**
   ```
   优化前:
       students (10000 rows) ⨝ courses (100 rows)

   优化后:
       courses (100 rows) ⨝ students (10000 rows)
       (小表作为build table)
   ```
   - 基于表大小启发式排序
   - 未来可扩展为基于代价的优化

4. **选择条件合并 (Selection Merging)**
   ```
   优化前:
       σ (age > 18)
         │
       σ (gpa > 3.0)
         │
       Scan

   优化后:
       σ (age > 18 AND gpa > 3.0)
         │
       Scan
   ```

#### 3.4.2 物理计划生成器 (PhysicalPlanGenerator)

**扫描方法选择**:
```cpp
if (有等值谓词 && 列上有索引) {
    return IndexScan(index, key);
} else {
    return TableScan(table);
}
```

**连接算法选择**:
```cpp
if (等值连接 && 右表较小) {
    return HashJoin(left, right);  // Build hash table on right
} else if (两表都已排序) {
    return MergeJoin(left, right);
} else {
    return NestedLoopJoin(left, right);
}
```

**代价估算**（简化版）:
```cpp
Cost估算公式:
- TableScan: table_blocks * IO_COST
- IndexScan: (log(index_entries) + selectivity * table_blocks) * IO_COST
- HashJoin: (left_size + right_size) * CPU_COST + build_hash_cost
```

---

### 3.5 执行引擎层

#### 3.5.1 火山模型 (Volcano Model)

**算子接口**:
```cpp
class Operator {
public:
    virtual void init() = 0;                      // 初始化
    virtual std::optional<Tuple> next() = 0;      // 获取下一个元组
    virtual void close() = 0;                     // 清理资源
    virtual void reset() = 0;                     // 重置迭代器
    virtual const Schema& getSchema() const = 0;  // 输出模式
};
```

**执行流程**:
```
Executor::execute(root_operator)
  │
  └─ root->init()
       │
       └─ while (tuple = root->next()) {
            result_set.add(tuple);
          }
            │
            └─ root->close()
```

**算子树示例**:
```sql
SELECT name, AVG(gpa)
FROM students
WHERE age > 18
GROUP BY name
HAVING AVG(gpa) > 3.5
ORDER BY AVG(gpa) DESC

算子树:
            Sort (AVG(gpa) DESC)
              │
            Filter (HAVING AVG(gpa) > 3.5)
              │
            Aggregate (GROUP BY name, AVG(gpa))
              │
            Filter (age > 18)
              │
            TableScan (students)
```

**算子详解**:

1. **TableScan**
   ```cpp
   void init() {
       current_block = 0;
       current_slot = 0;
       loadNextBlock();
   }

   std::optional<Tuple> next() {
       while (true) {
           if (current_slot < page.record_count) {
               if (!page.slots[current_slot].deleted) {
                   return deserializeTuple(page.records[current_slot++]);
               }
               current_slot++;
           } else if (current_block < table.blocks.size()) {
               loadNextBlock();
           } else {
               return std::nullopt;  // 扫描完成
           }
       }
   }
   ```

2. **HashJoin**
   ```cpp
   void init() {
       // Phase 1: Build
       right->init();
       while (auto tuple = right->next()) {
           auto key = tuple.get(right_join_column);
           hash_table[key].push_back(tuple);
       }
       right->close();

       // Phase 2: Probe
       left->init();
   }

   std::optional<Tuple> next() {
       while (true) {
           if (current_matches 非空) {
               return joinTuples(left_tuple, current_matches[index++]);
           }

           if (auto left_tuple = left->next()) {
               auto key = left_tuple.get(left_join_column);
               current_matches = hash_table[key];
               index = 0;
           } else {
               return std::nullopt;
           }
       }
   }
   ```

3. **Aggregate**
   ```cpp
   void init() {
       child->init();
       while (auto tuple = child->next()) {
           auto group_key = extractGroupKey(tuple);
           aggregators[group_key].accumulate(tuple);
       }
       child->close();
       iterator = aggregators.begin();
   }

   std::optional<Tuple> next() {
       if (iterator != aggregators.end()) {
           auto result = iterator->second.finalize();
           iterator++;
           return result;
       }
       return std::nullopt;
   }
   ```

---

## 4. 数据流与交互

### 4.1 查询执行完整流程

以 `SELECT name, AVG(gpa) FROM students WHERE age > 18 GROUP BY name` 为例：

```
┌─────────────────────────────────────────────────────────────┐
│ 1. SQL输入                                                  │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. 词法分析 (Lexer)                                         │
│                                                             │
│ Token流: SELECT, name, COMMA, AVG, LPAREN, gpa, ...         │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. 语法分析 (Parser)                                        │
│                                                             │
│ AST:                                                        │
│   SelectStatement                                           │
│   ├─ columns: [name, AVG(gpa)]                             │
│   ├─ from: students                                         │
│   ├─ where: age > 18                                        │
│   └─ groupBy: [name]                                        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. 语义分析 (SemanticAnalyzer)                              │
│                                                             │
│ - 检查表 students 存在 ✓                                    │
│ - 检查列 name, age, gpa 存在 ✓                              │
│ - 类型检查: age (INT), gpa (FLOAT) ✓                        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. 逻辑计划生成 (LogicalPlanGenerator)                      │
│                                                             │
│   Projection[name, AVG(gpa)]                                │
│     │                                                       │
│   Aggregate[GROUP BY name, AVG(gpa)]                        │
│     │                                                       │
│   Selection[age > 18]                                       │
│     │                                                       │
│   TableScan[students]                                       │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 6. 逻辑优化 (LogicalOptimizer)                              │
│                                                             │
│ - 谓词下推: age > 18 推到TableScan                          │
│ - 投影下推: 只读取 {name, age, gpa} 列                      │
│                                                             │
│   优化后:                                                   │
│   Projection[name, AVG(gpa)]                                │
│     │                                                       │
│   Aggregate[GROUP BY name, AVG(gpa)]                        │
│     │                                                       │
│   TableScan[students, filter: age>18, cols: {name,age,gpa}]│
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 7. 物理计划生成 (PhysicalPlanGenerator)                     │
│                                                             │
│ - 检查索引: students(age) 无索引 → TableScan                │
│ - 选择算子实现                                               │
│                                                             │
│   物理计划:                                                 │
│   ProjectionOp[name, AVG(gpa)]                              │
│     │                                                       │
│   AggregateOp[GROUP BY name, AVG(gpa)]                      │
│     │                                                       │
│   TableScanOp[students, filter: age>18]                     │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 8. 查询执行 (Executor)                                      │
│                                                             │
│ root_op = ProjectionOp(...)                                 │
│                                                             │
│ root_op->init()                                             │
│   ├─ AggregateOp->init()                                    │
│   │   ├─ TableScanOp->init()                                │
│   │   │   └─ 从BufferPool获取第一个块                       │
│   │   └─ 构建聚合状态 (HashMap<name, AvgAggregator>)        │
│   │                                                         │
│   └─ 准备投影逻辑                                            │
│                                                             │
│ while (tuple = root_op->next()) {                           │
│   // TableScanOp: 逐块扫描，过滤 age > 18                   │
│   // AggregateOp: 累积到聚合器                              │
│   // ProjectionOp: 选择输出列                               │
│   result_set.add(tuple);                                    │
│ }                                                           │
│                                                             │
│ root_op->close()                                            │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 9. 结果展示                                                 │
│                                                             │
│ ┌──────────┬──────────┐                                    │
│ │   name   │ AVG(gpa) │                                    │
│ ├──────────┼──────────┤                                    │
│ │  Alice   │   3.8    │                                    │
│ │   Bob    │   3.5    │                                    │
│ └──────────┴──────────┘                                    │
│                                                             │
│ 2 rows, 执行时间: 15ms                                      │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 事务执行流程

```sql
BEGIN;
INSERT INTO students VALUES (10, 'Eve', 20, 3.9);
UPDATE students SET gpa = 4.0 WHERE id = 10;
DELETE FROM students WHERE id = 1;
ROLLBACK;
```

**执行过程**:

```
1. BEGIN
   ├─ 清空 undo_log
   └─ 设置 in_transaction = true

2. INSERT INTO students VALUES (10, 'Eve', 20, 3.9)
   ├─ Database::insertRecord(...)
   │    ├─ 分配块并插入记录 → addr = (students, block_5, slot_3)
   │    ├─ 插入索引 (如果存在)
   │    └─ 记录Undo: {type: Insert, table: students, addr: (students,5,3)}
   └─ 返回成功

3. UPDATE students SET gpa = 4.0 WHERE id = 10
   ├─ 查询: WHERE id = 10 → 找到 addr = (students, block_5, slot_3)
   ├─ 读取旧记录: old_data = serialize(10, 'Eve', 20, 3.9)
   ├─ 写入新记录: new_data = serialize(10, 'Eve', 20, 4.0)
   ├─ 更新索引
   └─ 记录Undo: {type: Update, table: students, addr: ..., old_data: ...}

4. DELETE FROM students WHERE id = 1
   ├─ 查询: WHERE id = 1 → 找到 addr = (students, block_0, slot_0)
   ├─ 读取旧记录: old_data = serialize(1, 'Alice', 19, 3.8)
   ├─ 标记删除: page.slots[0].deleted = true
   ├─ 删除索引
   └─ 记录Undo: {type: Delete, table: students, addr: ..., old_data: ...}

5. ROLLBACK
   ├─ 逆序遍历 undo_log (从后往前):
   │
   │   [3] UndoType::Delete
   │        ├─ undeleteRecord(addr) → 恢复 id=1 的记录
   │        └─ 重新插入索引
   │
   │   [2] UndoType::Update
   │        ├─ updateRecord(addr, old_data) → 恢复 gpa=3.9
   │        └─ 更新索引
   │
   │   [1] UndoType::Insert
   │        ├─ deleteRecord(addr) → 删除 id=10 的记录
   │        └─ 删除索引
   │
   ├─ 清空 undo_log
   └─ 设置 in_transaction = false

结果: 数据库状态恢复到BEGIN之前
```

### 4.3 索引使用流程

```sql
-- 创建索引
CREATE INDEX idx_students_id ON students(id);

-- 等值查询
SELECT * FROM students WHERE id = 10;
```

**索引创建**:
```
1. IndexManager::createIndex("idx_students_id", "students", "id")
   │
   ├─ 创建 BPlusTree<int, BlockAddress>(order=4)
   │
   ├─ 全表扫描 students
   │    └─ for each record:
   │         ├─ 提取 id 值
   │         ├─ tree.insert(id, block_address)
   │         └─ 检查唯一性约束 (如果定义)
   │
   ├─ 持久化树到 storage/indexes/idx_students_id.tree
   │
   └─ 注册到 Catalog
```

**索引查询**:
```
2. SELECT * FROM students WHERE id = 10
   │
   ├─ Parser: 识别 WHERE id = 10 是等值谓词
   │
   ├─ PhysicalPlanGenerator:
   │    ├─ 检测到 id 列有索引 idx_students_id
   │    └─ 选择 IndexScan 而非 TableScan
   │
   └─ IndexScanOp::next()
        │
        ├─ tree.search(10) → BlockAddress(students, block_5, slot_3)
        │
        ├─ Database::readRecord(addr)
        │    └─ BufferPool::getBlock(students, block_5)
        │         ├─ 命中缓存? → 直接返回
        │         └─ 未命中 → DiskManager::readBlock() → 加载到缓冲池
        │
        └─ 返回 Tuple{id:10, name:'Eve', age:20, gpa:3.9}

性能对比:
- TableScan: O(N) - 扫描所有记录
- IndexScan: O(log N) - B+树查找
```

---

## 5. 关键设计决策

### 5.1 为什么选择B+树而非B树？

| 特性 | B+树 | B树 |
|------|------|-----|
| 数据存储位置 | 仅叶子节点 | 所有节点 |
| 范围查询 | 高效（叶子链表） | 需要树遍历 |
| 磁盘I/O | 更少（内部节点只存键） | 更多 |
| 缓存友好性 | 更好 | 一般 |

**选择理由**: 数据库查询中范围查询（`WHERE age BETWEEN 18 AND 25`）非常常见，B+树的叶子节点链表可以高效支持。

### 5.2 为什么使用火山模型？

**优点**:
- ✅ 算子可组合（类似Unix管道）
- ✅ 内存占用小（流式处理）
- ✅ 代码清晰易维护
- ✅ 支持流水线执行

**缺点**:
- ❌ 虚函数调用开销
- ❌ 不利于向量化（SIMD）
- ❌ CPU缓存利用率低

**选择理由**: 对于教学项目，代码清晰度 > 极致性能。现代数据库（如DuckDB）正在转向向量化执行，但实现复杂度高。

### 5.3 为什么使用Undo日志而非Redo日志？

| 特性 | Undo日志 | Redo日志 |
|------|---------|---------|
| 记录内容 | 旧值 | 新值 |
| 用途 | 回滚事务 | 崩溃恢复 |
| 持久化要求 | 无需持久化 | 必须持久化 |
| 实现复杂度 | 低 | 高 |

**当前实现**: 仅Undo日志（不持久化）
- ✅ 支持 ROLLBACK
- ❌ 不支持崩溃恢复

**未来改进**: 增加WAL（Write-Ahead Logging）
- 结合Undo + Redo
- 支持完整的ACID

### 5.4 为什么使用变长页而非定长页？

**定长页**:
- ✅ 访问速度快（直接计算偏移）
- ❌ 空间浪费大（VARCHAR需要预留最大空间）

**变长页**:
- ✅ 空间利用率高
- ✅ 支持灵活的记录大小
- ❌ 访问需遍历槽位数组

**选择理由**: 对于支持VARCHAR的系统，变长页是更合理的选择。性能损失可通过缓存和索引弥补。

### 5.5 为什么使用LRU而非LRU-K或Clock？

**LRU**:
- ✅ 实现简单（哈希表 + 双向链表）
- ✅ 性能稳定
- ❌ 不能应对序列扫描（Sequential Flooding）

**LRU-K**:
- ✅ 更智能的驱逐策略
- ❌ 实现复杂，需维护K次访问历史

**Clock**:
- ✅ 实现简单，性能接近LRU
- ❌ 不如LRU直观

**选择理由**: 教学项目优先选择经典算法。未来可扩展为LRU-K或ARC（Adaptive Replacement Cache）。

---

## 6. 性能优化

### 6.1 已实现的优化

1. **缓冲池管理**
   - LRU替换策略
   - 延迟刷盘（批量写入）
   - 脏页检测

2. **查询优化**
   - 谓词下推
   - 投影下推
   - 索引自动选择

3. **索引加速**
   - B+树索引（O(log N)查找）
   - 叶子节点链表（范围查询）

4. **访问计划缓存**
   - 缓存已解析的SQL
   - LRU淘汰策略
   - 15%内存分配

5. **内存池设计**
   - 精细的内存分区
   - 避免频繁malloc/free

### 6.2 未来优化方向

1. **并行执行**
   - 多线程扫描
   - 并行Hash Join
   - 流水线并行

2. **向量化执行**
   - 批量处理（1000行/batch）
   - SIMD指令
   - 减少虚函数调用

3. **统计信息**
   - 列直方图
   - 基数估计（NDV）
   - 代价模型改进

4. **物理优化**
   - 列存储（OLAP场景）
   - 数据压缩
   - 预取（Prefetching）

5. **高级索引**
   - 多列索引
   - 覆盖索引（Index-Only Scan）
   - 部分索引

---

## 7. 扩展性考虑

### 7.1 易于扩展的设计

1. **插件化算子**
   - 继承 Operator 基类
   - 实现 init/next/close
   - 无需修改执行器

2. **可扩展的索引**
   - IndexManager 管理多种索引类型
   - 当前: B+树
   - 未来: Hash索引、位图索引

3. **灵活的存储格式**
   - DiskManager 抽象磁盘I/O
   - 可替换为远程存储（S3）

4. **模块化的优化器**
   - 优化规则独立
   - 可添加新规则

### 7.2 潜在扩展功能

| 功能 | 难度 | 价值 |
|------|------|------|
| **WAL日志** | 中 | 高（崩溃恢复）|
| **MVCC** | 高 | 高（并发控制）|
| **子查询** | 中 | 高（SQL完整性）|
| **视图** | 低 | 中 |
| **外键** | 中 | 中 |
| **存储过程** | 高 | 低（教学项目）|
| **网络协议** | 中 | 高（客户端-服务器）|
| **复制** | 高 | 中 |

---

## 8. 总结

Mini DBMS 是一个精心设计的教学型数据库系统，展示了现代数据库的核心架构和关键技术。通过清晰的分层设计、标准的算法实现和完善的测试覆盖，为学习数据库原理提供了宝贵的实践平台。

**核心价值**:
- 📚 理解数据库内部工作原理
- 🛠️ 实践系统编程技能
- 🎯 掌握关键数据结构和算法
- 🚀 为进一步研究奠定基础

**后续发展**:
- 短期: 完善文档和测试
- 中期: 增加WAL、子查询等功能
- 长期: 探索并发控制和分布式

---

*最后更新: 2025-12-16*
