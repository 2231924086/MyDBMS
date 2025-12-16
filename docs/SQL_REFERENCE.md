# Mini DBMS SQL参考手册

## 目录
- [1. 数据定义语言 (DDL)](#1-数据定义语言-ddl)
- [2. 数据操作语言 (DML)](#2-数据操作语言-dml)
- [3. 事务控制](#3-事务控制)
- [4. 管理命令](#4-管理命令)
- [5. 数据类型](#5-数据类型)
- [6. 运算符与表达式](#6-运算符与表达式)
- [7. 聚合函数](#7-聚合函数)
- [8. 最佳实践](#8-最佳实践)
- [9. 常见错误](#9-常见错误)

---

## 1. 数据定义语言 (DDL)

### 1.1 CREATE TABLE

**语法**:
```sql
CREATE TABLE table_name (
    column1 TYPE(length),
    column2 TYPE(length),
    ...
)
```

**支持的数据类型**:
- `INT(length)` - 整数类型
- `VARCHAR(length)` - 可变长字符串
- `FLOAT(length)` - 浮点数类型

**示例**:

```sql
-- 创建学生表
CREATE TABLE students (
    id INT(10),
    name VARCHAR(50),
    age INT(3),
    gpa FLOAT(5)
)

-- 创建订单表
CREATE TABLE orders (
    order_id INT(10),
    customer_name VARCHAR(100),
    amount FLOAT(10),
    order_date VARCHAR(20)
)

-- 创建员工表
CREATE TABLE employees (
    emp_id INT(10),
    first_name VARCHAR(30),
    last_name VARCHAR(30),
    department VARCHAR(50),
    salary FLOAT(10),
    hire_date VARCHAR(20)
)
```

**注意事项**:
- ⚠️ 表名和列名区分大小写
- ⚠️ 长度参数是建议值，主要用于显示格式化
- ⚠️ 当前不支持主键、外键、默认值等约束（在CREATE语句中）
- ⚠️ 表创建后立即可用，但Schema需要COMMIT或正常退出才持久化

---

### 1.2 CREATE INDEX

**语法**:
```sql
CREATE INDEX index_name ON table_name(column_name)
```

**示例**:

```sql
-- 在ID列创建索引（加速等值查询）
CREATE INDEX idx_students_id ON students(id)

-- 在姓名列创建索引
CREATE INDEX idx_students_name ON students(name)

-- 在订单ID创建索引
CREATE INDEX idx_orders_id ON orders(order_id)

-- 在部门列创建索引
CREATE INDEX idx_emp_dept ON employees(department)
```

**索引工作原理**:
```
无索引:
SELECT * FROM students WHERE id = 100
→ 全表扫描，时间复杂度 O(N)

有索引:
SELECT * FROM students WHERE id = 100
→ B+树查找，时间复杂度 O(log N)
```

**注意事项**:
- ✅ 索引会自动在INSERT/UPDATE/DELETE时维护
- ✅ 查询优化器会自动选择使用索引
- ⚠️ 当前仅支持单列索引
- ⚠️ 索引会增加写入开销（权衡）
- ⚠️ 索引文件存储在 `storage/indexes/` 目录

**最佳实践**:
```sql
-- ✅ 推荐：在经常用于WHERE条件的列上创建索引
CREATE INDEX idx_students_id ON students(id)
SELECT * FROM students WHERE id = 100  -- 快速

-- ✅ 推荐：在JOIN连接键上创建索引
CREATE INDEX idx_orders_customer ON orders(customer_id)
SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id

-- ❌ 不推荐：在低基数列上创建索引（如性别）
CREATE INDEX idx_students_gender ON students(gender)  -- 只有男/女，效果不佳

-- ❌ 不推荐：在很少查询的列上创建索引
CREATE INDEX idx_students_comment ON students(comment)  -- 浪费空间
```

---

## 2. 数据操作语言 (DML)

### 2.1 INSERT

**语法**:
```sql
INSERT INTO table_name VALUES (value1, value2, ...)
```

**示例**:

```sql
-- 插入学生记录
INSERT INTO students VALUES (1, 'Alice', 20, 3.8)
INSERT INTO students VALUES (2, 'Bob', 21, 3.5)
INSERT INTO students VALUES (3, 'Charlie', 19, 3.9)

-- 插入订单记录
INSERT INTO orders VALUES (1001, 'Alice', 99.99, '2025-01-15')
INSERT INTO orders VALUES (1002, 'Bob', 149.50, '2025-01-16')

-- 插入字符串（使用单引号）
INSERT INTO employees VALUES (101, 'John', 'Doe', 'Engineering', 75000.0, '2024-06-01')

-- 插入浮点数
INSERT INTO products VALUES (1, 'Laptop', 1299.99, 50)
```

**注意事项**:
- ⚠️ 值的数量必须与表的列数匹配
- ⚠️ 值的顺序必须与CREATE TABLE中的列顺序一致
- ⚠️ 字符串使用单引号 `'...'`
- ⚠️ 数字不加引号
- ⚠️ 当前不支持指定列名（如 `INSERT INTO table(col1, col2) ...`）
- ✅ 如果列上有索引，会自动更新索引
- ✅ 如果有唯一索引，重复值会报错

---

### 2.2 SELECT

**基本语法**:
```sql
SELECT [DISTINCT] column1, column2, ...
FROM table_name
[WHERE condition]
[GROUP BY columns]
[HAVING condition]
[ORDER BY columns [ASC|DESC]]
```

#### 2.2.1 简单查询

```sql
-- 查询所有列
SELECT * FROM students

-- 查询指定列
SELECT name, age FROM students

-- 查询所有学生（显式列出所有列）
SELECT id, name, age, gpa FROM students
```

#### 2.2.2 WHERE条件查询

```sql
-- 等值条件
SELECT * FROM students WHERE age = 20

-- 比较运算
SELECT * FROM students WHERE gpa > 3.5
SELECT * FROM students WHERE age >= 18
SELECT * FROM students WHERE age < 25

-- 字符串比较
SELECT * FROM students WHERE name = 'Alice'

-- 复杂条件（AND, OR, NOT）
SELECT * FROM students WHERE age > 18 AND gpa > 3.5
SELECT * FROM students WHERE age < 18 OR gpa < 2.0
SELECT * FROM students WHERE NOT (age < 18)

-- 多条件组合
SELECT * FROM students
WHERE (age > 18 AND gpa > 3.0) OR (age < 18 AND gpa > 3.8)

-- 算术表达式
SELECT * FROM employees WHERE salary * 12 > 100000
SELECT * FROM orders WHERE amount - discount > 50
```

**支持的比较运算符**:
```
=     等于
!=    不等于
<     小于
<=    小于等于
>     大于
>=    大于等于
```

**支持的逻辑运算符**:
```
AND   逻辑与
OR    逻辑或
NOT   逻辑非
```

**运算符优先级** (从高到低):
```
1. 括号 ()
2. 算术运算 (*, /, %)
3. 算术运算 (+, -)
4. 比较运算 (=, !=, <, <=, >, >=)
5. NOT
6. AND
7. OR
```

#### 2.2.3 排序 (ORDER BY)

```sql
-- 升序排序（默认）
SELECT * FROM students ORDER BY gpa

-- 显式升序
SELECT * FROM students ORDER BY gpa ASC

-- 降序排序
SELECT * FROM students ORDER BY gpa DESC

-- 多列排序
SELECT * FROM students ORDER BY age ASC, gpa DESC

-- 按计算列排序
SELECT name, age, gpa, age * gpa as score
FROM students
ORDER BY score DESC
```

#### 2.2.4 去重 (DISTINCT)

```sql
-- 去除重复年龄
SELECT DISTINCT age FROM students

-- 去除重复的年龄-GPA组合
SELECT DISTINCT age, gpa FROM students

-- DISTINCT + ORDER BY
SELECT DISTINCT department FROM employees ORDER BY department
```

**注意**:
- DISTINCT 作用于整个SELECT列表
- DISTINCT 会增加计算开销（需要哈希去重）

#### 2.2.5 聚合查询 (GROUP BY)

```sql
-- 按年龄分组，计算平均GPA
SELECT age, AVG(gpa) FROM students GROUP BY age

-- 多个聚合函数
SELECT age, COUNT(*) as count, AVG(gpa) as avg_gpa, MAX(gpa) as max_gpa
FROM students
GROUP BY age

-- 按部门统计
SELECT department, COUNT(*) as emp_count, AVG(salary) as avg_salary
FROM employees
GROUP BY department

-- 多列分组
SELECT age, department, AVG(salary)
FROM employees
GROUP BY age, department
```

**HAVING子句** (过滤分组结果):
```sql
-- 筛选平均GPA > 3.5的年龄组
SELECT age, AVG(gpa) as avg_gpa
FROM students
GROUP BY age
HAVING AVG(gpa) > 3.5

-- 筛选人数 > 10的部门
SELECT department, COUNT(*) as count
FROM employees
GROUP BY department
HAVING COUNT(*) > 10

-- 复杂HAVING条件
SELECT department, AVG(salary) as avg_sal, MAX(salary) as max_sal
FROM employees
GROUP BY department
HAVING AVG(salary) > 60000 AND MAX(salary) < 150000
```

**WHERE vs HAVING**:
```sql
-- WHERE: 过滤行（在分组前）
-- HAVING: 过滤分组（在分组后）

SELECT department, AVG(salary) as avg_salary
FROM employees
WHERE salary > 50000          -- 先过滤薪资 > 50000 的员工
GROUP BY department            -- 然后分组
HAVING AVG(salary) > 70000     -- 最后过滤平均薪资 > 70000 的部门
```

#### 2.2.6 连接查询 (JOIN)

**INNER JOIN** (内连接):
```sql
-- 基本内连接
SELECT students.name, courses.course_name
FROM students
JOIN courses ON students.id = courses.student_id

-- 使用表别名
SELECT s.name, c.course_name
FROM students s
JOIN courses c ON s.id = c.student_id

-- 多表连接
SELECT s.name, c.course_name, e.exam_score
FROM students s
JOIN courses c ON s.id = c.student_id
JOIN exams e ON c.course_id = e.course_id

-- 连接 + 过滤
SELECT s.name, c.course_name
FROM students s
JOIN courses c ON s.id = c.student_id
WHERE s.age > 18 AND c.course_name = 'Database Systems'
```

**LEFT JOIN** (左外连接):
```sql
-- 查询所有学生，包括没有选课的学生
SELECT s.name, c.course_name
FROM students s
LEFT JOIN courses c ON s.id = c.student_id

-- 找出没有选课的学生
SELECT s.name
FROM students s
LEFT JOIN courses c ON s.id = c.student_id
WHERE c.course_id IS NULL  -- 注意：当前不支持NULL，需调整数据设计
```

**RIGHT JOIN** (右外连接):
```sql
-- 查询所有课程，包括没有学生选的课程
SELECT s.name, c.course_name
FROM students s
RIGHT JOIN courses c ON s.id = c.student_id
```

**连接类型对比**:
```
INNER JOIN: 只返回两表匹配的行
LEFT JOIN:  返回左表所有行，右表无匹配则为NULL
RIGHT JOIN: 返回右表所有行，左表无匹配则为NULL
```

#### 2.2.7 复杂查询示例

```sql
-- 示例1: 查询每个部门薪资最高的员工
SELECT department, MAX(salary) as max_salary
FROM employees
GROUP BY department

-- 示例2: 查询选课数量 > 3的学生
SELECT student_id, COUNT(*) as course_count
FROM enrollments
GROUP BY student_id
HAVING COUNT(*) > 3

-- 示例3: 多表连接 + 聚合
SELECT s.name, AVG(e.score) as avg_score
FROM students s
JOIN exams e ON s.id = e.student_id
GROUP BY s.name
HAVING AVG(e.score) > 85
ORDER BY avg_score DESC

-- 示例4: DISTINCT + JOIN
SELECT DISTINCT s.name
FROM students s
JOIN courses c ON s.id = c.student_id
WHERE c.course_name = 'Database Systems'

-- 示例5: 复杂过滤
SELECT name, age, gpa
FROM students
WHERE (age BETWEEN 18 AND 22)  -- 注意：BETWEEN当前可能不支持，用 AND 代替
  AND gpa > 3.0
ORDER BY gpa DESC

-- 使用 AND 代替 BETWEEN
SELECT name, age, gpa
FROM students
WHERE age >= 18 AND age <= 22 AND gpa > 3.0
ORDER BY gpa DESC
```

---

### 2.3 UPDATE

**语法**:
```sql
UPDATE table_name
SET column1 = value1, column2 = value2, ...
WHERE condition
```

**示例**:

```sql
-- 更新单列
UPDATE students SET gpa = 4.0 WHERE name = 'Alice'

-- 更新多列
UPDATE students SET age = 21, gpa = 3.9 WHERE id = 1

-- 使用表达式
UPDATE employees SET salary = salary * 1.1 WHERE department = 'Engineering'

-- 更新所有行（谨慎！）
UPDATE students SET gpa = gpa + 0.1

-- 复杂条件更新
UPDATE orders
SET amount = amount * 0.9
WHERE order_date = '2025-01-15' AND amount > 100
```

**注意事项**:
- ⚠️ 没有WHERE子句会更新所有行！
- ✅ 如果列上有索引，索引会自动更新
- ✅ 在事务中的UPDATE可以回滚
- ⚠️ 当前不支持FROM子句（如 `UPDATE ... FROM ...`）

---

### 2.4 DELETE

**语法**:
```sql
DELETE FROM table_name WHERE condition
```

**示例**:

```sql
-- 删除特定行
DELETE FROM students WHERE id = 1

-- 按条件删除
DELETE FROM students WHERE age < 18

-- 删除所有行（危险！清空表）
DELETE FROM students

-- 复杂条件删除
DELETE FROM orders
WHERE order_date < '2024-01-01' AND amount < 10
```

**注意事项**:
- ⚠️ 没有WHERE子句会删除所有行！
- ✅ 删除采用软删除（标记deleted），可在事务中回滚
- ✅ 索引会自动更新
- ⚠️ 删除的空间不会立即回收，需运行 `VACUUM` 命令

**VACUUM vs DELETE**:
```sql
-- DELETE: 标记删除，不回收空间
DELETE FROM students WHERE id = 1

-- VACUUM: 回收已删除记录的空间
VACUUM students
```

---

## 3. 事务控制

### 3.1 BEGIN

开始一个新事务。

**语法**:
```sql
BEGIN
```

**示例**:
```sql
BEGIN;
INSERT INTO students VALUES (100, 'Test', 20, 3.5);
-- 此时数据在事务中，未提交
```

---

### 3.2 COMMIT

提交当前事务，使所有更改永久生效。

**语法**:
```sql
COMMIT
```

**示例**:
```sql
BEGIN;
INSERT INTO students VALUES (100, 'Test', 20, 3.5);
UPDATE students SET gpa = 3.6 WHERE id = 100;
COMMIT;  -- 更改持久化
```

---

### 3.3 ROLLBACK

回滚当前事务，撤销所有更改。

**语法**:
```sql
ROLLBACK
```

**示例**:
```sql
BEGIN;
INSERT INTO students VALUES (100, 'Test', 20, 3.5);
DELETE FROM students WHERE id = 1;
ROLLBACK;  -- 撤销INSERT和DELETE
```

---

### 3.4 事务示例

**示例1: 银行转账**
```sql
BEGIN;

-- 从账户A扣款
UPDATE accounts SET balance = balance - 100 WHERE account_id = 1;

-- 向账户B存款
UPDATE accounts SET balance = balance + 100 WHERE account_id = 2;

-- 检查是否正确
-- 如果正确，提交
COMMIT;

-- 如果出错，回滚
-- ROLLBACK;
```

**示例2: 批量插入**
```sql
BEGIN;
INSERT INTO students VALUES (101, 'Student1', 19, 3.5);
INSERT INTO students VALUES (102, 'Student2', 20, 3.6);
INSERT INTO students VALUES (103, 'Student3', 21, 3.7);
COMMIT;  -- 一次性提交所有插入
```

**示例3: 测试更新**
```sql
BEGIN;
UPDATE students SET gpa = gpa + 0.5;  -- 给所有学生加0.5分
-- 查看效果
SELECT * FROM students;
-- 如果满意
COMMIT;
-- 如果不满意
-- ROLLBACK;
```

**注意事项**:
- ⚠️ 当前不支持并发事务（单线程）
- ⚠️ 事务未持久化，崩溃后会丢失
- ✅ 支持回滚 INSERT、UPDATE、DELETE
- ⚠️ DDL语句（CREATE TABLE/INDEX）不能回滚

---

## 4. 管理命令

### 4.1 TABLES

列出所有表及其统计信息。

**语法**:
```sql
TABLES
```

**输出示例**:
```
┌────────────┬────────┬────────┐
│ Table      │ Blocks │ Records│
├────────────┼────────┼────────┤
│ students   │   5    │  1230  │
│ courses    │   2    │   45   │
│ orders     │   8    │  3450  │
└────────────┴────────┴────────┘
```

---

### 4.2 INDEXES

列出所有索引及其信息。

**语法**:
```sql
INDEXES
```

**输出示例**:
```
┌─────────────────────┬────────────┬────────┬─────────┐
│ Index               │ Table      │ Column │ Entries │
├─────────────────────┼────────────┼────────┼─────────┤
│ idx_students_id     │ students   │ id     │  1230   │
│ idx_courses_name    │ courses    │ name   │   45    │
└─────────────────────┴────────────┴────────┴─────────┘
```

---

### 4.3 DUMP

导出表数据（带分页）。

**语法**:
```sql
DUMP table_name [limit [offset]]
```

**示例**:
```sql
-- 导出所有数据
DUMP students

-- 导出前10条
DUMP students 10

-- 导出第11-20条（跳过前10条，取10条）
DUMP students 10 10
```

**输出格式**:
```
Record 0 @ (students, block_0, slot_0):
  id: 1
  name: Alice
  age: 20
  gpa: 3.8

Record 1 @ (students, block_0, slot_1):
  id: 2
  name: Bob
  age: 21
  gpa: 3.5
...
```

---

### 4.4 VACUUM

回收表中已删除记录的空间。

**语法**:
```sql
VACUUM table_name
```

**示例**:
```sql
-- 删除一些记录
DELETE FROM students WHERE age < 18;

-- 回收空间
VACUUM students;
```

**工作原理**:
```
VACUUM前:
[Record1] [Record2] [DELETED] [Record4] [DELETED] [Record6]
        空间利用率: 66%

VACUUM后:
[Record1] [Record2] [Record4] [Record6]
        空间利用率: 100%
```

**建议**:
- 在大量DELETE后运行VACUUM
- VACUUM是阻塞操作，会锁定表

---

### 4.5 PLANS

查看最近的访问计划缓存。

**语法**:
```sql
PLANS [n]
```

**示例**:
```sql
-- 查看最近10条计划（默认）
PLANS

-- 查看最近20条计划
PLANS 20
```

**输出示例**:
```
========== 访问计划缓存 (最近 10 条) ==========
[1] SELECT * FROM students WHERE age > 18
    Plan: TableScan(students, filter: age>18)
    Hits: 5

[2] SELECT name, AVG(gpa) FROM students GROUP BY name
    Plan: Aggregate(TableScan(students))
    Hits: 2
...
```

---

### 4.6 LOGS

查看最近的操作日志。

**语法**:
```sql
LOGS [n]
```

**示例**:
```sql
-- 查看最近20条日志（默认）
LOGS

-- 查看最近50条日志
LOGS 50
```

---

### 4.7 MEM

显示内存布局和使用情况。

**语法**:
```sql
MEM
```

**输出示例**:
```
========== 内存布局 ==========
总内存:     32.00 MB

分区分配:
  Buffer Pool:      19.20 MB (60%)
  Plan Cache:        4.80 MB (15%)
  Catalog:           4.80 MB (15%)
  Log Buffer:        3.20 MB (10%)

Buffer Pool 统计:
  容量:       4915 blocks
  命中率:     92.3%
  脏页数:     127
```

---

### 4.8 HELP

显示帮助信息。

**语法**:
```sql
HELP
```

---

### 4.9 EXIT

退出程序（会自动保存Schema）。

**语法**:
```sql
EXIT
```

---

## 5. 数据类型

### 5.1 INT

**描述**: 整数类型

**语法**: `INT(length)`

**示例**:
```sql
CREATE TABLE example (
    id INT(10),        -- 10位整数
    count INT(5),      -- 5位整数
    year INT(4)        -- 4位整数
)
```

**取值范围**:
- 理论: -2147483648 到 2147483647 (32位)
- length参数主要用于显示格式化

---

### 5.2 VARCHAR

**描述**: 可变长字符串

**语法**: `VARCHAR(length)`

**示例**:
```sql
CREATE TABLE example (
    name VARCHAR(50),      -- 最长50字符
    email VARCHAR(100),    -- 最长100字符
    description VARCHAR(500)
)
```

**注意**:
- 实际存储长度根据内容动态分配
- length是最大长度限制

---

### 5.3 FLOAT

**描述**: 浮点数类型

**语法**: `FLOAT(length)`

**示例**:
```sql
CREATE TABLE example (
    price FLOAT(10),       -- 价格
    gpa FLOAT(5),          -- GPA
    temperature FLOAT(8)   -- 温度
)
```

**精度**:
- 单精度浮点数 (32位)
- 约7位有效数字

---

## 6. 运算符与表达式

### 6.1 算术运算符

| 运算符 | 说明 | 示例 |
|--------|------|------|
| `+` | 加法 | `SELECT salary + bonus FROM employees` |
| `-` | 减法 | `SELECT price - discount FROM products` |
| `*` | 乘法 | `SELECT salary * 12 FROM employees` |
| `/` | 除法 | `SELECT total / count FROM statistics` |
| `%` | 取模 | `SELECT id % 10 FROM students` |

**示例**:
```sql
-- 计算年薪
SELECT name, salary * 12 as annual_salary FROM employees;

-- 计算折扣价
SELECT product_name, price * 0.8 as discounted_price FROM products;

-- 复杂表达式
SELECT (price - cost) / cost * 100 as profit_margin FROM products;
```

---

### 6.2 比较运算符

| 运算符 | 说明 | 示例 |
|--------|------|------|
| `=` | 等于 | `age = 20` |
| `!=` | 不等于 | `status != 'inactive'` |
| `<` | 小于 | `price < 100` |
| `<=` | 小于等于 | `age <= 18` |
| `>` | 大于 | `gpa > 3.5` |
| `>=` | 大于等于 | `salary >= 50000` |

---

### 6.3 逻辑运算符

| 运算符 | 说明 | 示例 |
|--------|------|------|
| `AND` | 逻辑与 | `age > 18 AND gpa > 3.0` |
| `OR` | 逻辑或 | `age < 18 OR gpa < 2.0` |
| `NOT` | 逻辑非 | `NOT (age < 18)` |

**真值表**:
```
AND:
true  AND true  = true
true  AND false = false
false AND false = false

OR:
true  OR true  = true
true  OR false = true
false OR false = false

NOT:
NOT true  = false
NOT false = true
```

---

### 6.4 表达式示例

```sql
-- 算术 + 比较
SELECT * FROM employees WHERE salary * 12 > 100000;

-- 多重逻辑
SELECT * FROM students
WHERE (age >= 18 AND age <= 22)
  AND (gpa > 3.5 OR scholarship = 'yes');

-- 嵌套括号
SELECT * FROM orders
WHERE ((amount > 100 AND status = 'pending')
    OR (amount > 500 AND status = 'shipped'))
  AND customer_type = 'premium';
```

---

## 7. 聚合函数

### 7.1 COUNT

**描述**: 计数

**语法**: `COUNT(*)` 或 `COUNT(column)`

**示例**:
```sql
-- 统计总行数
SELECT COUNT(*) FROM students;

-- 按组统计
SELECT department, COUNT(*) as emp_count
FROM employees
GROUP BY department;

-- 统计满足条件的行数
SELECT COUNT(*) as adult_count
FROM students
WHERE age >= 18;
```

---

### 7.2 SUM

**描述**: 求和

**语法**: `SUM(column)`

**示例**:
```sql
-- 计算总薪资
SELECT SUM(salary) as total_salary FROM employees;

-- 按部门计算总薪资
SELECT department, SUM(salary) as dept_total
FROM employees
GROUP BY department;
```

---

### 7.3 AVG

**描述**: 平均值

**语法**: `AVG(column)`

**示例**:
```sql
-- 计算平均GPA
SELECT AVG(gpa) as avg_gpa FROM students;

-- 按年龄计算平均GPA
SELECT age, AVG(gpa) as avg_gpa
FROM students
GROUP BY age;

-- 平均薪资（带过滤）
SELECT department, AVG(salary) as avg_sal
FROM employees
WHERE hire_date > '2020-01-01'
GROUP BY department;
```

---

### 7.4 MIN

**描述**: 最小值

**语法**: `MIN(column)`

**示例**:
```sql
-- 最低GPA
SELECT MIN(gpa) FROM students;

-- 每个部门最低薪资
SELECT department, MIN(salary) as min_salary
FROM employees
GROUP BY department;
```

---

### 7.5 MAX

**描述**: 最大值

**语法**: `MAX(column)`

**示例**:
```sql
-- 最高GPA
SELECT MAX(gpa) FROM students;

-- 每个部门最高薪资
SELECT department, MAX(salary) as max_salary
FROM employees
GROUP BY department;
```

---

### 7.6 聚合函数组合

```sql
-- 多个聚合函数
SELECT
    COUNT(*) as count,
    AVG(gpa) as avg_gpa,
    MIN(gpa) as min_gpa,
    MAX(gpa) as max_gpa,
    MAX(gpa) - MIN(gpa) as gpa_range
FROM students;

-- 按组多聚合
SELECT
    department,
    COUNT(*) as emp_count,
    AVG(salary) as avg_salary,
    MIN(salary) as min_salary,
    MAX(salary) as max_salary,
    SUM(salary) as total_salary
FROM employees
GROUP BY department
HAVING COUNT(*) > 5
ORDER BY avg_salary DESC;
```

---

## 8. 最佳实践

### 8.1 索引使用

```sql
-- ✅ 好的做法：在经常查询的列上创建索引
CREATE INDEX idx_students_id ON students(id);
SELECT * FROM students WHERE id = 100;  -- 使用索引，快速

-- ✅ 好的做法：JOIN列创建索引
CREATE INDEX idx_orders_customer ON orders(customer_id);
SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id;

-- ❌ 不好的做法：不必要的索引
CREATE INDEX idx_students_random ON students(random_column);  -- 从不查询的列
```

---

### 8.2 查询优化

```sql
-- ✅ 好：只查询需要的列
SELECT name, age FROM students WHERE id = 100;

-- ❌ 差：查询所有列
SELECT * FROM students WHERE id = 100;

-- ✅ 好：使用索引列
SELECT * FROM students WHERE id = 100;  -- id有索引

-- ❌ 差：对索引列进行函数运算（无法使用索引）
SELECT * FROM students WHERE id + 1 = 101;  -- 索引失效
```

---

### 8.3 事务使用

```sql
-- ✅ 好：批量操作使用事务
BEGIN;
INSERT INTO students VALUES (1, 'A', 20, 3.5);
INSERT INTO students VALUES (2, 'B', 21, 3.6);
INSERT INTO students VALUES (3, 'C', 19, 3.7);
COMMIT;

-- ❌ 差：每条语句自动提交
INSERT INTO students VALUES (1, 'A', 20, 3.5);
INSERT INTO students VALUES (2, 'B', 21, 3.6);
INSERT INTO students VALUES (3, 'C', 19, 3.7);
```

---

### 8.4 安全实践

```sql
-- ⚠️ 危险：没有WHERE的UPDATE
UPDATE students SET gpa = 4.0;  -- 更新所有行！

-- ✅ 安全：带WHERE的UPDATE
UPDATE students SET gpa = 4.0 WHERE id = 1;

-- ⚠️ 危险：没有WHERE的DELETE
DELETE FROM students;  -- 删除所有行！

-- ✅ 安全：带WHERE的DELETE
DELETE FROM students WHERE age < 18;
```

---

## 9. 常见错误

### 9.1 语法错误

```sql
-- ❌ 错误：缺少逗号
CREATE TABLE test (
    id INT(10)
    name VARCHAR(50)  -- 缺少逗号
)

-- ✅ 正确
CREATE TABLE test (
    id INT(10),
    name VARCHAR(50)
)

-- ❌ 错误：字符串使用双引号
SELECT * FROM students WHERE name = "Alice"

-- ✅ 正确：使用单引号
SELECT * FROM students WHERE name = 'Alice'
```

---

### 9.2 数据类型错误

```sql
-- ❌ 错误：值数量不匹配
CREATE TABLE students (id INT(10), name VARCHAR(50), age INT(3));
INSERT INTO students VALUES (1, 'Alice');  -- 缺少age

-- ✅ 正确
INSERT INTO students VALUES (1, 'Alice', 20);
```

---

### 9.3 聚合函数错误

```sql
-- ❌ 错误：非聚合列没有在GROUP BY中
SELECT name, AVG(gpa) FROM students;  -- name不在GROUP BY中

-- ✅ 正确：方式1
SELECT name, AVG(gpa) FROM students GROUP BY name;

-- ✅ 正确：方式2
SELECT AVG(gpa) FROM students;  -- 不选择非聚合列
```

---

### 9.4 WHERE vs HAVING

```sql
-- ❌ 错误：在WHERE中使用聚合函数
SELECT department, AVG(salary)
FROM employees
WHERE AVG(salary) > 60000  -- 错误！
GROUP BY department;

-- ✅ 正确：在HAVING中使用聚合函数
SELECT department, AVG(salary)
FROM employees
GROUP BY department
HAVING AVG(salary) > 60000;
```

---

## 10. 快速参考

### SQL语句速查

```sql
-- DDL
CREATE TABLE name (col TYPE(len), ...)
CREATE INDEX idx ON table(col)

-- DML
INSERT INTO table VALUES (v1, v2, ...)
SELECT [DISTINCT] cols FROM table [WHERE ...] [GROUP BY ...] [ORDER BY ...]
UPDATE table SET col=val WHERE ...
DELETE FROM table WHERE ...

-- 事务
BEGIN
COMMIT
ROLLBACK

-- 管理
TABLES
INDEXES
DUMP table [limit [offset]]
VACUUM table
PLANS [n]
LOGS [n]
MEM
HELP
EXIT
```

---

**最后更新**: 2025-12-16
