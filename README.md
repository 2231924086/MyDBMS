# Mini DBMS - 教学型关系数据库管理系统

一个功能完整的教学型关系数据库管理系统，使用C++17实现，包含完整的SQL解析、查询优化、事务管理和索引系统。

## 特性

### 核心功能
- ✅ **完整的SQL支持**: SELECT, INSERT, UPDATE, DELETE
- ✅ **查询功能**: WHERE, JOIN (INNER/LEFT/RIGHT), ORDER BY, GROUP BY, HAVING, DISTINCT, LIMIT/OFFSET
- ✅ **事务管理**: BEGIN, COMMIT, ROLLBACK with ACID保证
- ✅ **索引系统**: B+树索引，支持自动索引选择
- ✅ **查询优化**: 谓词下推、投影下推、JOIN重排序
- ✅ **崩溃恢复**: Write-Ahead Logging (WAL)
- ✅ **聚合函数**: COUNT, SUM, AVG, MIN, MAX, VARIANCE, STDDEV
- ✅ **子查询**: 支持FROM子句中的子查询

### 存储引擎
- 变长页面管理
- LRU缓冲池
- 磁盘块管理
- VACUUM操作（空间回收）

### 查询执行
- 表扫描 / 索引扫描
- Hash Join / Nested Loop Join / Merge Join
- 管道式查询执行
- 基于统计的执行计划缓存

## 快速开始

### 编译

```bash
# 创建构建目录
mkdir build && cd build

# 配置CMake
cmake ..

# 编译
cmake --build . --config Release

# 运行测试
./dbms_tests
```

### 运行数据库

```bash
# 使用默认配置运行
./dbms

# 自定义配置
./dbms --block-size=4096 --memory=64M --disk=512M
```

### 基本使用示例

```sql
db> -- 创建表
db> CREATE TABLE users (id INT(16), name STRING(64), age INT(8))
Table 'users' created.

db> -- 插入数据
db> INSERT INTO users VALUES (1, 'Alice', 30)
Inserted into users.

db> INSERT INTO users VALUES (2, 'Bob', 42)
Inserted into users.

db> -- 创建索引
db> CREATE INDEX idx_users_id ON users(id)
Index 'idx_users_id' created (2 page(s)).

db> -- 查询数据
db> SELECT * FROM users WHERE age > 35
+----+------+-----+
| id | name | age |
+----+------+-----+
| 2  | Bob  | 42  |
+----+------+-----+
1 row(s) returned

db> -- 事务示例
db> BEGIN
Transaction started.

db> UPDATE users SET age = 31 WHERE id = 1
1 row(s) updated

db> COMMIT
Transaction committed.

db> -- 聚合查询
db> SELECT COUNT(*), AVG(age) FROM users
+----------+----------+
| COUNT(*) | AVG(age) |
+----------+----------+
| 2        | 36.0     |
+----------+----------+

db> EXIT
```

## 命令参考

### DDL (数据定义语言)

```sql
-- 创建表
CREATE TABLE table_name (col1 TYPE(len), col2 TYPE(len), ...)
-- 支持的类型: INT/INTEGER, DOUBLE, STRING

-- 创建索引
CREATE INDEX index_name ON table_name(column_name)
```

### DML (数据操作语言)

```sql
-- 查询
SELECT [DISTINCT] columns
FROM table1
[JOIN table2 ON condition]
[WHERE condition]
[GROUP BY columns]
[HAVING condition]
[ORDER BY columns [ASC|DESC]]
[LIMIT n [OFFSET m]]

-- 插入
INSERT INTO table_name VALUES (val1, val2, ...)

-- 更新
UPDATE table_name SET col1 = val1, col2 = val2 WHERE condition

-- 删除
DELETE FROM table_name WHERE condition
```

### 事务控制

```sql
BEGIN       -- 开始事务
COMMIT      -- 提交事务
ROLLBACK    -- 回滚事务
```

### 管理命令

```sql
TABLES              -- 列出所有表
INDEXES             -- 列出所有索引
DUMP <table>        -- 查看表的物理存储
VACUUM <table|all>  -- 回收空间
PLANS [n]           -- 显示执行计划缓存
LOGS [n]            -- 显示操作日志
MEM                 -- 显示内存布局
HELP                -- 显示帮助
EXIT                -- 退出
```

## 架构设计

```
┌─────────────────────────────────────────┐
│         SQL Parser & Optimizer          │
│  (Lexer → Parser → Optimizer → Plan)    │
└─────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────┐
│        Query Executor Engine            │
│  (TableScan, IndexScan, Join, Filter)   │
└─────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────┐
│     Transaction & Index Manager         │
│    (MVCC, WAL, B+Tree Index)            │
└─────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────┐
│         Storage Engine                  │
│   (Buffer Pool, Disk Manager, Page)     │
└─────────────────────────────────────────┘
```

详细架构请参阅 [ARCHITECTURE.md](docs/ARCHITECTURE.md)

## 项目结构

```
work/
├── CMakeLists.txt           # 构建配置
├── main.cpp                 # 主程序入口
├── include/                 # 头文件
│   ├── common/              # 公共类型和工具
│   ├── storage/             # 存储引擎
│   ├── index/               # 索引系统
│   ├── executor/            # 查询执行器
│   ├── parser/              # SQL解析器
│   └── system/              # 系统管理
├── src/                     # 源文件
│   ├── storage/
│   ├── index/
│   ├── executor/
│   ├── parser/
│   └── system/
├── tests/                   # 测试套件
│   └── dbms_tests.cpp
├── storage/                 # 数据存储目录（运行时创建）
│   ├── meta/                # 元数据
│   ├── logs/                # 日志文件
│   ├── indexes/             # 索引文件
│   └── <table>/             # 表数据文件
└── docs/                    # 文档（可选）
    ├── ARCHITECTURE.md      # 架构设计
    ├── USER_GUIDE.md        # 用户指南
    └── DEVELOPER.md         # 开发者文档
```

## 配置选项

### 命令行参数

```bash
--block-size=<bytes>    # 数据块大小 (默认: 4096)
--memory=<size>         # 内存大小 (默认: 32M)
--disk=<size>           # 磁盘大小 (默认: 256M)

# 示例
./dbms --block-size=8192 --memory=128M --disk=1G
```

大小单位支持: K (KB), M (MB), G (GB)

### 内存分配策略

- 60% - 数据缓冲池 (Buffer Pool)
- 15% - 数据字典 (Data Dictionary)
- 15% - 执行计划缓存 (Access Plan Cache)
- 10% - 日志缓冲 (Log Buffer)

## 性能特性

### 查询优化
- **谓词下推**: 将过滤条件推到数据源附近
- **投影下推**: 尽早减少列数
- **JOIN重排序**: 优化连接顺序
- **索引选择**: 自动选择最优索引

### 存储优化
- **LRU缓冲池**: 智能页面置换
- **变长页面**: 节省存储空间
- **VACUUM**: 回收已删除记录空间

### 事务特性
- **ACID保证**: 原子性、一致性、隔离性、持久性
- **WAL**: 写前日志保证崩溃恢复
- **Undo Log**: 支持事务回滚

## 测试

项目包含23个综合测试用例，覆盖所有核心功能：

```bash
# 运行所有测试
./dbms_tests

# 测试覆盖
✓ 页面管理
✓ 缓冲池LRU
✓ B+树索引
✓ 索引扫描和JOIN
✓ 持久化和恢复
✓ 事务COMMIT/ROLLBACK
✓ SQL查询执行
✓ 聚合和分组
✓ 约束检查
```

## 限制和已知问题

1. **单线程**: 当前版本不支持并发访问
2. **无并发控制**: 未实现MVCC或锁机制
3. **无约束**: 不支持PRIMARY KEY, FOREIGN KEY等约束
4. **固定类型**: 仅支持INT, DOUBLE, STRING三种数据类型
5. **内存限制**: 所有索引必须能装入内存

## 下一步计划

- [ ] 实现并发控制 (MVCC)
- [ ] 添加约束系统 (PRIMARY KEY, FOREIGN KEY)
- [ ] 扩展数据类型 (DATE, TIMESTAMP, BLOB)
- [ ] 窗口函数支持
- [ ] 查询缓存
- [ ] 主从复制

## 参考资料

- [数据库系统概念](https://www.db-book.com/) - Silberschatz et al.
- [CMU 15-445: Database Systems](https://15445.courses.cs.cmu.edu/)
- [SQLite Architecture](https://www.sqlite.org/arch.html)

## 许可证

本项目仅用于教学目的。

## 贡献

欢迎提交Issue和Pull Request。

## 联系方式

如有问题，请提交Issue。
