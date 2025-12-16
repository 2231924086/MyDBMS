# Mini DBMS - 关系型数据库管理系统

一个使用C++17实现的教学型关系型数据库管理系统，包含完整的存储引擎、B+树索引、SQL解析器、查询优化器和执行引擎。

## 特性概览

### 核心功能
- ✅ **完整的SQL支持** - SELECT、INSERT、UPDATE、DELETE
- ✅ **B+树索引** - 支持插入、删除、范围查询，自动重平衡
- ✅ **事务管理** - BEGIN/COMMIT/ROLLBACK，Undo日志
- ✅ **查询优化** - 谓词下推、投影下推、连接重排序
- ✅ **多种连接算法** - Hash Join、Nested Loop Join、Merge Join
- ✅ **聚合查询** - GROUP BY、HAVING、COUNT/SUM/AVG/MIN/MAX
- ✅ **缓冲池管理** - LRU替换策略，脏页检测
- ✅ **持久化存储** - 变长页管理，数据和索引持久化

### 技术亮点
- **火山模型执行引擎** - 标准的迭代器模式，算子可组合
- **三段式查询优化** - 逻辑计划 → 优化 → 物理计划
- **精细的内存管理** - 四区分配（Buffer/Plan/Dict/Log）
- **完整的测试覆盖** - 20个测试用例，覆盖基础、集成、边界、事务、容错

## 快速开始

### 编译要求
- CMake 3.16+
- C++17 编译器
  - Windows: MSVC 2019+ 或 MinGW
  - Linux/macOS: GCC 8+ 或 Clang 7+

### 编译步骤

```bash
# 1. 创建构建目录
mkdir build && cd build

# 2. 配置项目
cmake ..

# 3. 编译
cmake --build . --config Debug

# 4. 运行主程序
./Debug/dbms.exe

# 5. 运行测试
./Debug/dbms_tests.exe
```

### 命令行参数

```bash
dbms.exe [选项]
  --block-size=4096    # 页大小（字节，默认4096）
  --memory=32M         # 总内存限制（支持K/M/G，默认32M）
  --disk=256M          # 磁盘容量限制（支持K/M/G，默认256M）
```

示例：
```bash
dbms.exe --block-size=8192 --memory=64M --disk=1G
```

## 使用指南

### 启动交互式命令行

运行 `dbms.exe` 后进入交互式Shell：

```
======================================
  Mini DBMS - 交互式命令行
======================================
内存: 32.00 MB | 磁盘: 256.00 MB | 块大小: 4096 字节

输入 HELP 查看帮助，输入 EXIT 退出。
======================================

dbms>
```

### SQL命令示例

#### 1. 创建表

```sql
dbms> CREATE TABLE students (
        id INT(10),
        name VARCHAR(50),
        age INT(3),
        gpa FLOAT(5)
      )
```

#### 2. 创建索引

```sql
dbms> CREATE INDEX idx_students_id ON students(id)
dbms> CREATE INDEX idx_students_name ON students(name)
```

#### 3. 插入数据

```sql
dbms> INSERT INTO students VALUES (1, 'Alice', 20, 3.8)
dbms> INSERT INTO students VALUES (2, 'Bob', 21, 3.5)
dbms> INSERT INTO students VALUES (3, 'Charlie', 19, 3.9)
```

#### 4. 查询数据

```sql
-- 简单查询
dbms> SELECT * FROM students WHERE age > 19

-- 排序
dbms> SELECT name, gpa FROM students ORDER BY gpa DESC

-- 去重
dbms> SELECT DISTINCT age FROM students

-- 连接查询
dbms> SELECT s.name, c.course_name
      FROM students s
      JOIN courses c ON s.id = c.student_id

-- 聚合查询
dbms> SELECT age, AVG(gpa) as avg_gpa
      FROM students
      GROUP BY age
      HAVING AVG(gpa) > 3.5
```

#### 5. 更新和删除

```sql
-- 更新
dbms> UPDATE students SET gpa = 4.0 WHERE name = 'Alice'

-- 删除
dbms> DELETE FROM students WHERE age < 18
```

#### 6. 事务管理

```sql
dbms> BEGIN
dbms> INSERT INTO students VALUES (4, 'David', 22, 3.7)
dbms> UPDATE students SET age = 23 WHERE name = 'David'
dbms> COMMIT

-- 回滚示例
dbms> BEGIN
dbms> DELETE FROM students WHERE id = 1
dbms> ROLLBACK  -- 撤销删除
```

### 管理命令

```sql
TABLES              -- 列出所有表
INDEXES             -- 列出所有索引
DUMP students       -- 导出表数据
VACUUM students     -- 回收表空间
PLANS [n]           -- 查看最近n条访问计划（默认10）
LOGS [n]            -- 查看最近n条日志（默认20）
MEM                 -- 显示内存布局
HELP                -- 显示帮助信息
EXIT                -- 退出程序
```

## SQL语法支持

### 支持的语句

#### DDL（数据定义语言）
```sql
CREATE TABLE table_name (
  column_name TYPE(length),
  ...
)

CREATE INDEX index_name ON table_name(column_name)
```

支持的数据类型：
- `INT(length)` - 整数
- `VARCHAR(length)` - 可变长字符串
- `FLOAT(length)` - 浮点数

#### DML（数据操作语言）
```sql
-- 插入
INSERT INTO table_name VALUES (value1, value2, ...)

-- 查询
SELECT [DISTINCT] columns
FROM table1 [JOIN table2 ON condition]
WHERE condition
GROUP BY columns
HAVING condition
ORDER BY columns [ASC|DESC]

-- 更新
UPDATE table_name SET column = value WHERE condition

-- 删除
DELETE FROM table_name WHERE condition
```

#### 事务控制
```sql
BEGIN       -- 开始事务
COMMIT      -- 提交事务
ROLLBACK    -- 回滚事务
```

### 支持的运算符

**比较运算符**: `=`, `!=`, `<`, `<=`, `>`, `>=`

**逻辑运算符**: `AND`, `OR`, `NOT`

**算术运算符**: `+`, `-`, `*`, `/`, `%`

**聚合函数**: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`

### 连接类型
- `INNER JOIN` - 内连接
- `LEFT JOIN` - 左外连接
- `RIGHT JOIN` - 右外连接

## 项目架构

### 分层设计

```
┌─────────────────────────────────────┐
│   交互式命令行 (main.cpp)           │
├─────────────────────────────────────┤
│   SQL解析器 (parser/)               │
│   - Lexer (词法分析)                │
│   - Parser (语法分析)               │
│   - Semantic Analyzer (语义分析)   │
├─────────────────────────────────────┤
│   查询优化器 (parser/)              │
│   - Logical Optimizer (逻辑优化)   │
│   - Physical Planner (物理计划)    │
├─────────────────────────────────────┤
│   执行引擎 (executor/)              │
│   - Operators (算子)                │
│   - Expression Evaluator (表达式)  │
├─────────────────────────────────────┤
│   系统管理 (system/)                │
│   - Database (数据库核心)          │
│   - Catalog (数据字典)              │
│   - Table (表管理)                  │
├─────────────────────────────────────┤
│   索引管理 (index/)                 │
│   - B+ Tree (B+树)                  │
│   - Index Manager (索引管理器)     │
├─────────────────────────────────────┤
│   存储引擎 (storage/)               │
│   - Buffer Pool (缓冲池)            │
│   - Disk Manager (磁盘管理)         │
│   - Page (页管理)                   │
└─────────────────────────────────────┘
```

### 执行引擎算子

采用**火山模型 (Volcano Model)**，支持的算子包括：

- **TableScan** - 全表扫描
- **IndexScan** - 索引扫描（等值查询）
- **Filter** - 谓词过滤
- **Projection** - 列投影
- **Distinct** - 去重
- **HashJoin** - 哈希连接（支持INNER/LEFT/RIGHT）
- **Sort** - 排序（支持多列、ASC/DESC）
- **Aggregate** - 聚合（GROUP BY, HAVING）

### 内存布局

系统将内存分为四个区域：

```
总内存 (默认32MB)
├─ 60% - 数据缓冲池 (Buffer Pool)
│   └─ LRU替换策略
├─ 15% - 访问计划缓存 (Access Plan Cache)
├─ 15% - 数据字典 (Catalog)
└─ 10% - 日志缓冲 (Log Buffer)
```

## 项目结构

```
tmp/
├── include/              # 头文件
│   ├── common/          # 通用组件
│   ├── storage/         # 存储层
│   ├── index/           # 索引层
│   ├── system/          # 系统层
│   ├── executor/        # 执行层
│   └── parser/          # 解析层
├── src/                 # 源文件（与include对应）
├── tests/               # 测试文件
│   └── dbms_tests.cpp
├── storage/             # 运行时数据目录
│   ├── meta/           # 元数据（schemas.meta）
│   ├── indexes/        # 索引文件（.tree）
│   └── */              # 表数据目录（.blk文件）
├── build/               # 编译输出
├── main.cpp             # 主程序
└── CMakeLists.txt       # 构建配置
```

## 测试

项目包含20个综合测试用例，覆盖：

### 基础功能
- 变长页插入/更新/删除/vacuum
- 缓冲池LRU淘汰与刷盘
- B+树索引CRUD操作

### 集成测试
- 索引扫描与Hash Join流水线
- 跨重启持久化（数据+索引）
- 索引文件丢失自动重建

### 边界条件
- 超大记录插入拒绝
- 复杂谓词过滤求值
- 计划缓存容量淘汰

### 事务测试
- 事务回滚恢复状态
- 事务提交持久化

### 容错测试
- 损坏数据块检测
- 损坏索引文件重建

### 运行测试

```bash
cd build/Debug
./dbms_tests.exe
```

预期输出：
```
========================================
  DBMS 测试套件
========================================

[1/20] VariableLengthPage基础操作... PASS
[2/20] BufferPool LRU淘汰测试... PASS
...
[20/20] 聚合算子测试... PASS

========================================
测试完成: 20/20 通过
========================================
```

## 性能特性

### 查询优化

1. **谓词下推 (Predicate Pushdown)**
   - 将过滤条件尽早应用，减少数据量

2. **投影下推 (Projection Pushdown)**
   - 尽早裁剪不需要的列

3. **索引选择**
   - 自动检测可用索引
   - 等值查询优先使用索引扫描

4. **连接算法选择**
   - 根据数据量选择Hash Join或Nested Loop Join

### 缓存机制

- **数据缓冲池**: LRU策略，减少磁盘I/O
- **访问计划缓存**: 缓存查询计划，避免重复解析
- **脏页延迟刷盘**: 批量写入，提升性能

## 限制与已知问题

### 当前限制
- ❌ 无并发控制（单线程）
- ❌ 无WAL日志（崩溃后可能丢数据）
- ❌ 不支持子查询
- ❌ 不支持视图
- ❌ 不支持外键约束
- ❌ 索引仅支持单列

### 适用场景
- ✅ 数据库系统教学
- ✅ 算法原理演示
- ✅ 研究实验平台
- ❌ 生产环境使用
- ❌ 高并发场景

## 技术细节

### B+树索引

实现了完整的B+树算法（781行代码），包括：
- 节点分裂与合并
- 节点间键值借用
- 自动重平衡
- 叶子节点链表（支持范围查询）
- 持久化与损坏检测

索引文件格式：
```
IDXTREE V1 <order>
NODE <id> <is_leaf> <key_count> [keys...] [children/values...]
...
```

### 事务机制

采用**Undo日志**实现事务回滚：

```cpp
struct UndoRecord {
    UndoType type;      // Insert/Update/Delete
    std::string table;
    BlockAddress addr;
    std::vector<uint8_t> old_data;  // 旧值
};
```

支持的操作：
- INSERT回滚 → 删除插入的记录
- UPDATE回滚 → 恢复旧值
- DELETE回滚 → 恢复删除的记录

### 变长页管理

采用槽位（Slot）数组管理记录：

```
┌─────────────────────────────────────┐
│ Page Header                         │
├─────────────────────────────────────┤
│ Slot Array (grows downward)         │
│ [offset, length, deleted_flag] ...  │
├─────────────────────────────────────┤
│          Free Space                 │
├─────────────────────────────────────┤
│ Records (grow upward)               │
│ [record1] [record2] ...             │
└─────────────────────────────────────┘
```

支持：
- 变长记录存储
- 记录软删除（标记删除）
- Vacuum操作（空间回收）

## 代码统计

```
Language       Files    Lines    Code    Comment
─────────────────────────────────────────────────
C++ Header        25     3,676   3,200      150
C++ Source        25     5,210   4,500      300
C++ Tests          1     1,018     900       50
CMake              1        29      25        2
─────────────────────────────────────────────────
Total             52    11,561  10,100      520
```

## 许可证

本项目为教学目的开发，仅供学习和研究使用。

## 作者

DBMS项目组

## 参考资料

- [CMU 15-445: Database Systems](https://15445.courses.cs.cmu.edu/)
- [Database System Concepts (Silberschatz)](https://www.db-book.com/)
- [PostgreSQL源码](https://github.com/postgres/postgres)
- [SQLite源码](https://www.sqlite.org/src/doc/trunk/README.md)

## 常见问题

### Q: 如何清空所有数据？
```bash
# 删除storage目录
rm -rf storage/
```

### Q: 如何查看查询执行计划？
使用 `PLANS` 命令查看最近的访问计划缓存。

### Q: 为什么CREATE TABLE后数据丢失？
确保执行了 `COMMIT` 或者程序正常退出（会自动保存Schema）。

### Q: 如何调试性能问题？
1. 使用 `MEM` 查看内存使用
2. 使用 `PLANS` 查看查询计划
3. 检查是否创建了合适的索引

### Q: 支持并发吗？
不支持。这是单线程设计，不适合多用户并发访问。

---

**Happy Querying! 🚀**
