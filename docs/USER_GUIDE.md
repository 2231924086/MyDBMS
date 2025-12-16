# Mini DBMS 用户使用手册

本手册将帮助你快速上手Mini DBMS，从安装到高级查询，涵盖所有常用功能。

---

## 目录

1. [安装与配置](#1-安装与配置)
2. [启动数据库](#2-启动数据库)
3. [基本操作](#3-基本操作)
4. [数据定义 (DDL)](#4-数据定义-ddl)
5. [数据操作 (DML)](#5-数据操作-dml)
6. [查询语言](#6-查询语言)
7. [事务管理](#7-事务管理)
8. [索引管理](#8-索引管理)
9. [系统管理](#9-系统管理)
10. [高级特性](#10-高级特性)
11. [性能优化建议](#11-性能优化建议)
12. [常见问题](#12-常见问题)

---

## 1. 安装与配置

### 1.1 系统要求

- **操作系统**: Windows, Linux, macOS
- **编译器**: 支持C++17的编译器
  - GCC 7.0+
  - Clang 5.0+
  - MSVC 2017+
- **CMake**: 3.16或更高版本

### 1.2 编译安装

```bash
# 克隆或解压项目
cd work

# 创建构建目录
mkdir build && cd build

# 配置项目
cmake ..

# 编译 (Release模式以获得最佳性能)
cmake --build . --config Release

# 验证安装
./dbms_tests  # 运行测试确保一切正常
```

### 1.3 目录结构

编译完成后，你会看到:
```
build/
├── Release/
│   ├── dbms.exe         # 主程序
│   └── dbms_tests.exe   # 测试程序
└── Debug/
    └── ...
```

---

## 2. 启动数据库

### 2.1 基本启动

```bash
# 使用默认配置启动
./dbms

# 输出示例:
Mini DBMS ready. Storage directory: storage
Block size: 4096 bytes, buffer: 4800 frame(s), disk blocks: 65536
Schema catalog: storage/meta/schemas.meta
```

### 2.2 自定义配置

```bash
# 设置块大小为8KB，内存64MB，磁盘512MB
./dbms --block-size=8192 --memory=64M --disk=512M
```

**配置参数说明**:
- `--block-size`: 数据块大小（字节），默认4096
- `--memory`: 主内存大小，默认32M
- `--disk`: 磁盘容量，默认256M

**大小单位**:
- 不带单位: 字节 (bytes)
- `K` 或 `k`: KB (1024 bytes)
- `M` 或 `m`: MB (1024 KB)
- `G` 或 `g`: GB (1024 MB)

### 2.3 交互式命令行

启动后会看到提示符:
```
db> _
```

在这里输入SQL命令或管理命令。

---

## 3. 基本操作

### 3.1 查看帮助

```sql
db> HELP
Commands:
  CREATE TABLE ...
  CREATE INDEX ...
  INSERT INTO ...
  SELECT ...
  ...
```

### 3.2 查看现有表

```sql
db> TABLES
Table: users
  Columns:
    - id: int(16)
    - name: string(64)
    - age: int(8)
  Records: 4 spanning 1 blocks
```

### 3.3 退出数据库

```sql
db> EXIT
```
或者按 `Ctrl+C`

---

## 4. 数据定义 (DDL)

### 4.1 创建表

**基本语法**:
```sql
CREATE TABLE table_name (
    column1 TYPE(length),
    column2 TYPE(length),
    ...
)
```

**支持的数据类型**:
- `INT` 或 `INTEGER`: 整数
- `DOUBLE`: 浮点数
- `STRING`: 字符串

**示例**:
```sql
-- 创建用户表
db> CREATE TABLE users (
    id INT(16),
    name STRING(64),
    age INT(8)
)
Table 'users' created.

-- 创建订单表
db> CREATE TABLE orders (
    id INT(16),
    user_id INT(16),
    amount INT(16),
    status STRING(32)
)
```

**简写语法**:
```sql
-- 使用冒号分隔
db> CREATE TABLE products (id:int:16, name:string:128, price:double:16)
```

### 4.2 查看表结构

```sql
db> TABLES
```

### 4.3 删除表

⚠️ 当前版本不支持 `DROP TABLE`。如需删除表，请手动删除 `storage/<table_name>/` 目录。

---

## 5. 数据操作 (DML)

### 5.1 插入数据 (INSERT)

**语法**:
```sql
INSERT INTO table_name VALUES (value1, value2, ...)
```

**示例**:
```sql
-- 插入单条记录
db> INSERT INTO users VALUES (1, 'Alice', 30)
Inserted into users.

db> INSERT INTO users VALUES (2, 'Bob', 42)
Inserted into users.

db> INSERT INTO users VALUES (3, 'Carol', 28)
Inserted into users.

-- 字符串可以用单引号或双引号
db> INSERT INTO users VALUES (4, "Dave", 55)
Inserted into users.
```

**注意事项**:
- 值的数量必须与表的列数匹配
- 字符串不能超过定义的长度
- 整数和浮点数会自动类型转换

### 5.2 更新数据 (UPDATE)

**语法**:
```sql
UPDATE table_name
SET column1 = value1, column2 = value2, ...
WHERE condition
```

**示例**:
```sql
-- 更新单个字段
db> UPDATE users SET age = 31 WHERE id = 1
1 row(s) updated

-- 更新多个字段
db> UPDATE users SET name = 'Bobby', age = 43 WHERE id = 2
1 row(s) updated

-- 基于表达式更新
db> UPDATE users SET age = age + 1 WHERE age < 40
2 row(s) updated

-- 不带WHERE则更新所有记录
db> UPDATE users SET status = 'active'
```

### 5.3 删除数据 (DELETE)

**语法**:
```sql
DELETE FROM table_name WHERE condition
```

**示例**:
```sql
-- 删除特定记录
db> DELETE FROM users WHERE id = 4
1 row(s) deleted

-- 删除符合条件的记录
db> DELETE FROM users WHERE age < 25
0 row(s) deleted

-- 删除所有记录
db> DELETE FROM users
4 row(s) deleted
```

---

## 6. 查询语言

### 6.1 简单查询

**基本SELECT**:
```sql
-- 查询所有列
db> SELECT * FROM users
+----+-------+-----+
| id | name  | age |
+----+-------+-----+
| 1  | Alice | 30  |
| 2  | Bob   | 42  |
| 3  | Carol | 28  |
+----+-------+-----+
3 row(s) returned

-- 查询特定列
db> SELECT name, age FROM users
+-------+-----+
| name  | age |
+-------+-----+
| Alice | 30  |
| Bob   | 42  |
| Carol | 28  |
+-------+-----+
```

### 6.2 条件过滤 (WHERE)

**比较运算符**:
- `=`: 等于
- `!=` 或 `<>`: 不等于
- `<`: 小于
- `>`: 大于
- `<=`: 小于等于
- `>=`: 大于等于

**逻辑运算符**:
- `AND`: 与
- `OR`: 或
- `NOT`: 非

**示例**:
```sql
-- 单条件
db> SELECT * FROM users WHERE age > 30
+----+------+-----+
| id | name | age |
+----+------+-----+
| 2  | Bob  | 42  |
+----+------+-----+

-- 多条件 (AND)
db> SELECT * FROM users WHERE age > 25 AND age < 40
+----+-------+-----+
| id | name  | age |
+----+-------+-----+
| 1  | Alice | 30  |
| 3  | Carol | 28  |
+----+-------+-----+

-- 多条件 (OR)
db> SELECT * FROM users WHERE age < 30 OR age > 40
+----+-------+-----+
| id | name  | age |
+----+-------+-----+
| 2  | Bob   | 42  |
| 3  | Carol | 28  |
+----+-------+-----+

-- 复杂条件
db> SELECT * FROM users WHERE (age > 25 AND age < 35) OR name = 'Bob'
```

### 6.3 排序 (ORDER BY)

**语法**:
```sql
SELECT ... ORDER BY column [ASC|DESC]
```

**示例**:
```sql
-- 升序（默认）
db> SELECT * FROM users ORDER BY age
+----+-------+-----+
| id | name  | age |
+----+-------+-----+
| 3  | Carol | 28  |
| 1  | Alice | 30  |
| 2  | Bob   | 42  |
+----+-------+-----+

-- 降序
db> SELECT * FROM users ORDER BY age DESC

-- 多列排序
db> SELECT * FROM users ORDER BY age DESC, name ASC
```

### 6.4 去重 (DISTINCT)

```sql
-- 去除重复值
db> SELECT DISTINCT age FROM users

-- 多列去重
db> SELECT DISTINCT department, position FROM employees
```

### 6.5 限制结果 (LIMIT & OFFSET)

```sql
-- 只返回前N条
db> SELECT * FROM users ORDER BY age LIMIT 5

-- 跳过M条，返回N条（分页）
db> SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 20

-- 实用场景：第3页，每页10条
db> SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 20
```

### 6.6 聚合函数

**支持的函数**:
- `COUNT(*)` 或 `COUNT(column)`: 计数
- `SUM(column)`: 求和
- `AVG(column)`: 平均值
- `MIN(column)`: 最小值
- `MAX(column)`: 最大值
- `VARIANCE(column)`: 方差
- `STDDEV(column)`: 标准差

**示例**:
```sql
-- 统计记录数
db> SELECT COUNT(*) FROM users
+----------+
| COUNT(*) |
+----------+
| 4        |
+----------+

-- 求平均年龄
db> SELECT AVG(age) FROM users
+----------+
| AVG(age) |
+----------+
| 37.5     |
+----------+

-- 多个聚合
db> SELECT COUNT(*), AVG(age), MIN(age), MAX(age) FROM users
+----------+----------+----------+----------+
| COUNT(*) | AVG(age) | MIN(age) | MAX(age) |
+----------+----------+----------+----------+
| 4        | 37.5     | 28       | 55       |
+----------+----------+----------+----------+
```

### 6.7 分组 (GROUP BY)

**语法**:
```sql
SELECT column, AGG_FUNC(column)
FROM table
GROUP BY column
[HAVING condition]
```

**示例**:
```sql
-- 按部门统计人数
db> SELECT department, COUNT(*) FROM employees GROUP BY department

-- 按城市统计平均年龄
db> SELECT city, AVG(age) FROM users GROUP BY city

-- HAVING过滤分组结果
db> SELECT department, COUNT(*) AS cnt
    FROM employees
    GROUP BY department
    HAVING cnt > 5
```

### 6.8 表连接 (JOIN)

#### 6.8.1 内连接 (INNER JOIN)

```sql
-- 语法
SELECT columns
FROM table1
[INNER] JOIN table2 ON condition

-- 示例
db> SELECT u.name, o.amount
    FROM users u
    JOIN orders o ON u.id = o.user_id
+-------+--------+
| name  | amount |
+-------+--------+
| Alice | 200    |
| Bob   | 300    |
| Carol | 150    |
+-------+--------+
```

#### 6.8.2 左连接 (LEFT JOIN)

```sql
-- 保留左表所有记录，右表不匹配则填充NULL
db> SELECT u.name, o.amount
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
+-------+--------+
| name  | amount |
+-------+--------+
| Alice | 200    |
| Bob   | 300    |
| Carol | NULL   |
| Dave  | NULL   |
+-------+--------+
```

#### 6.8.3 右连接 (RIGHT JOIN)

```sql
-- 保留右表所有记录，左表不匹配则填充NULL
db> SELECT u.name, o.amount
    FROM users u
    RIGHT JOIN orders o ON u.id = o.user_id
```

#### 6.8.4 多表连接

```sql
db> SELECT u.name, o.amount, p.product_name
    FROM users u
    JOIN orders o ON u.id = o.user_id
    JOIN products p ON o.product_id = p.id
```

### 6.9 子查询

**FROM子句中的子查询**:
```sql
db> SELECT sub.name
    FROM (SELECT name, age FROM users WHERE age > 30) AS sub
    ORDER BY sub.name
+------+
| name |
+------+
| Bob  |
| Dave |
+------+
```

---

## 7. 事务管理

### 7.1 事务基础

事务保证ACID特性:
- **A**tomicity (原子性): 全部成功或全部失败
- **C**onsistency (一致性): 数据保持一致状态
- **I**solation (隔离性): 事务之间互不干扰
- **D**urability (持久性): 提交后永久保存

### 7.2 开启事务

```sql
db> BEGIN
Transaction started.
```

### 7.3 提交事务

```sql
db> COMMIT
Transaction committed.
```

所有操作永久保存到磁盘。

### 7.4 回滚事务

```sql
db> ROLLBACK
Transaction rolled back.
```

撤销事务中的所有操作。

### 7.5 完整事务示例

```sql
db> BEGIN
Transaction started.

db> INSERT INTO accounts VALUES (1, 'Alice', 1000)
Inserted into accounts.

db> INSERT INTO accounts VALUES (2, 'Bob', 500)
Inserted into accounts.

db> UPDATE accounts SET balance = balance - 100 WHERE id = 1
1 row(s) updated

db> UPDATE accounts SET balance = balance + 100 WHERE id = 2
1 row(s) updated

-- 检查无误后提交
db> COMMIT
Transaction committed.
```

**回滚示例**:
```sql
db> BEGIN
db> DELETE FROM users WHERE age < 30
2 row(s) deleted

-- 发现误操作，立即回滚
db> ROLLBACK
Transaction rolled back.

-- 数据已恢复
db> SELECT COUNT(*) FROM users
+----------+
| COUNT(*) |
+----------+
| 4        |
+----------+
```

### 7.6 自动提交模式

⚠️ 如果不显式使用 `BEGIN`，每条SQL自动包装在事务中（自动提交）。

---

## 8. 索引管理

### 8.1 创建索引

**语法**:
```sql
CREATE INDEX index_name ON table_name(column_name)
```

**示例**:
```sql
db> CREATE INDEX idx_users_id ON users(id)
Index 'idx_users_id' created (2 page(s)).

db> CREATE INDEX idx_users_age ON users(age)
Index 'idx_users_age' created (1 page(s)).
```

### 8.2 查看索引

```sql
db> INDEXES
idx_users_id ON users(id) | entries/page=100
idx_users_age ON users(age) | entries/page=120
```

### 8.3 索引自动使用

查询优化器会自动选择索引:
```sql
-- 会使用 idx_users_id 索引
db> SELECT * FROM users WHERE id = 2

-- 不会使用索引（范围查询）
db> SELECT * FROM users WHERE age > 30
```

### 8.4 索引使用建议

**适合建索引的场景**:
- 经常作为WHERE条件的列
- JOIN连接键
- ORDER BY排序的列
- 外键列

**不适合建索引的场景**:
- 频繁更新的列
- 低选择性的列（如性别、布尔值）
- 小表（全表扫描更快）

---

## 9. 系统管理

### 9.1 查看表数据 (DUMP)

```sql
-- 查看表的物理存储
db> DUMP users
Table 'users' rows (limit=0, offset=0):
  #1 [block 0, slot 0]: 1 | Alice | 30
  #2 [block 0, slot 1]: 2 | Bob | 42
  #3 [block 0, slot 2]: 3 | Carol | 28
Total records: 3 (blocks scanned: 1)

-- 限制输出
db> DUMP users 10 0     -- 前10条
db> DUMP users 10 20    -- 跳过20条，显示10条
```

### 9.2 空间回收 (VACUUM)

删除记录后，空间不会立即释放。使用VACUUM回收:

```sql
-- 回收单个表
db> VACUUM users
Vacuumed users: 1 blocks visited, 5 slots cleared

-- 回收所有表
db> VACUUM all
Vacuumed users: 1 blocks visited, 5 slots cleared
Vacuumed orders: 2 blocks visited, 3 slots cleared
```

**建议**: 定期执行VACUUM，尤其在大量删除后。

### 9.3 查看执行计划

```sql
db> PLANS 10
[1] SELECT FROM users
[2] INSERT INTO orders
[3] JOIN users orders
...
```

### 9.4 查看操作日志

```sql
db> LOGS 20
[2024-01-15 10:30:45] insert into users
[2024-01-15 10:31:02] select from users
[2024-01-15 10:31:15] create index idx_users_id on users
...
```

### 9.5 查看内存布局

```sql
db> MEM
Memory layout (bytes):
  - Access plans: 4980736
  - Data dictionary: 4980736
  - Data buffer: 19922944 (4800 frame(s))
  - Log buffer: 3321856
...
```

---

## 10. 高级特性

### 10.1 复杂查询组合

```sql
-- 组合多种子句
db> SELECT u.name, COUNT(o.id) AS order_count, SUM(o.amount) AS total
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    WHERE u.age > 25
    GROUP BY u.name
    HAVING order_count > 0
    ORDER BY total DESC
    LIMIT 5
```

### 10.2 表达式计算

```sql
-- 算术表达式
db> SELECT name, age, age + 10 AS future_age FROM users

db> UPDATE products SET price = price * 1.1  -- 涨价10%
```

### 10.3 字符串匹配

⚠️ 当前版本不支持 `LIKE` 运算符，使用 `=` 进行精确匹配。

### 10.4 NULL值处理

```sql
-- LEFT JOIN产生的NULL
db> SELECT u.name, o.amount
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
-- 无订单的用户，amount显示为 NULL
```

---

## 11. 性能优化建议

### 11.1 索引优化

```sql
-- ✅ 好：为高选择性列创建索引
CREATE INDEX idx_users_email ON users(email)

-- ❌ 差：低选择性列不适合索引
CREATE INDEX idx_users_gender ON users(gender)  -- 只有男/女
```

### 11.2 查询优化

```sql
-- ✅ 好：使用列名而非 *
SELECT id, name FROM users

-- ❌ 差：返回不需要的列
SELECT * FROM users

-- ✅ 好：WHERE条件使用索引列
SELECT * FROM users WHERE id = 100

-- ❌ 差：对索引列使用函数
SELECT * FROM users WHERE age + 1 > 30  -- 无法使用索引
```

### 11.3 JOIN优化

```sql
-- ✅ 好：小表在左，大表在右（Hash Join）
SELECT * FROM small_table JOIN large_table ON ...

-- ✅ 好：JOIN键有索引
CREATE INDEX idx_orders_user_id ON orders(user_id)
SELECT * FROM users JOIN orders ON users.id = orders.user_id
```

### 11.4 事务优化

```sql
-- ✅ 好：批量操作使用事务
BEGIN
INSERT INTO users VALUES (...)  -- 1000次插入
COMMIT

-- ❌ 差：每次插入一个事务（慢）
INSERT INTO users VALUES (...)
INSERT INTO users VALUES (...)
...
```

### 11.5 定期维护

```sql
-- 定期执行VACUUM
VACUUM all

-- 重建索引（如果数据大量变动）
-- 当前需手动删除索引文件后重新CREATE INDEX
```

---

## 12. 常见问题

### Q1: 插入失败 "record does not fit into a single block"

**原因**: 记录太大，超过块大小。

**解决**:
```bash
# 增大块大小
./dbms --block-size=8192
```
或减小列长度:
```sql
CREATE TABLE users (name STRING(32))  -- 而非 STRING(1024)
```

### Q2: "disk full" 错误

**原因**: 磁盘空间不足。

**解决**:
```bash
# 增大磁盘容量
./dbms --disk=1G
```

### Q3: 查询很慢

**诊断**:
1. 检查是否有索引
2. 查看是否全表扫描
3. 使用 `PLANS` 查看执行计划

**解决**:
```sql
-- 为常用查询条件创建索引
CREATE INDEX idx_column ON table(column)
```

### Q4: 事务回滚后数据未恢复

**原因**: 可能在事务外执行了操作（自动提交）。

**解决**: 确保先执行 `BEGIN` 再操作。

### Q5: 如何删除表？

**当前方法**: 手动删除 `storage/<table_name>/` 目录，然后重启数据库。

### Q6: 如何备份数据？

**方法**:
1. 停止数据库
2. 复制整个 `storage/` 目录
3. 重启数据库

**恢复**:
1. 停止数据库
2. 用备份替换 `storage/` 目录
3. 重启数据库

### Q7: 支持多用户并发吗？

**不支持**。当前版本是单线程，只能单用户访问。

### Q8: 如何导入大量数据？

**建议**:
```sql
BEGIN  -- 使用事务批量插入
INSERT INTO table VALUES (...)
INSERT INTO table VALUES (...)
-- ... 多次插入
COMMIT
```

### Q9: 数据存储在哪里？

**位置**: 运行目录下的 `storage/` 目录
- `storage/<table>/`: 表数据
- `storage/indexes/`: 索引文件
- `storage/meta/`: 元数据
- `storage/logs/`: 日志文件

### Q10: 如何清空所有数据重新开始？

```bash
# 停止数据库
# 删除storage目录
rm -rf storage/
# 重新启动数据库
./dbms
```

---

## 13. 命令速查表

| 命令 | 说明 | 示例 |
|------|------|------|
| `CREATE TABLE` | 创建表 | `CREATE TABLE t (id INT(16))` |
| `CREATE INDEX` | 创建索引 | `CREATE INDEX idx ON t(id)` |
| `INSERT INTO` | 插入数据 | `INSERT INTO t VALUES (1)` |
| `SELECT` | 查询 | `SELECT * FROM t WHERE ...` |
| `UPDATE` | 更新 | `UPDATE t SET col = val` |
| `DELETE` | 删除 | `DELETE FROM t WHERE ...` |
| `BEGIN` | 开始事务 | `BEGIN` |
| `COMMIT` | 提交事务 | `COMMIT` |
| `ROLLBACK` | 回滚事务 | `ROLLBACK` |
| `TABLES` | 列出所有表 | `TABLES` |
| `INDEXES` | 列出索引 | `INDEXES` |
| `DUMP` | 查看表存储 | `DUMP t 10 0` |
| `VACUUM` | 回收空间 | `VACUUM t` 或 `VACUUM all` |
| `PLANS` | 执行计划 | `PLANS 10` |
| `LOGS` | 操作日志 | `LOGS 20` |
| `MEM` | 内存布局 | `MEM` |
| `HELP` | 帮助 | `HELP` |
| `EXIT` | 退出 | `EXIT` |

---

## 14. 示例数据库

完整示例：创建一个简单的电商数据库

```sql
-- 创建表
db> CREATE TABLE customers (id INT(16), name STRING(64), email STRING(128))
db> CREATE TABLE products (id INT(16), name STRING(128), price INT(16))
db> CREATE TABLE orders (id INT(16), customer_id INT(16), product_id INT(16), quantity INT(8))

-- 插入数据
db> BEGIN
db> INSERT INTO customers VALUES (1, 'Alice', 'alice@example.com')
db> INSERT INTO customers VALUES (2, 'Bob', 'bob@example.com')
db> INSERT INTO products VALUES (100, 'Laptop', 1200)
db> INSERT INTO products VALUES (101, 'Mouse', 25)
db> INSERT INTO orders VALUES (1, 1, 100, 1)
db> INSERT INTO orders VALUES (2, 1, 101, 2)
db> INSERT INTO orders VALUES (3, 2, 100, 1)
db> COMMIT

-- 创建索引
db> CREATE INDEX idx_customers_id ON customers(id)
db> CREATE INDEX idx_orders_customer_id ON orders(customer_id)

-- 查询：每个客户的订单总额
db> SELECT c.name, SUM(p.price * o.quantity) AS total
    FROM customers c
    JOIN orders o ON c.id = o.customer_id
    JOIN products p ON o.product_id = p.id
    GROUP BY c.name
    ORDER BY total DESC
+-------+-------+
| name  | total |
+-------+-------+
| Alice | 1250  |
| Bob   | 1200  |
+-------+-------+
```

---

## 总结

本手册涵盖了Mini DBMS的主要功能。更多高级用法请参阅:
- [架构设计文档](ARCHITECTURE.md) - 深入理解系统原理
- [开发者文档](DEVELOPER.md) - 扩展和二次开发

祝你使用愉快！
