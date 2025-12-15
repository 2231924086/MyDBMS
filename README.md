# DBMS 迷你数据库

一个用 C++ 实现的教学级小型数据库，引入存储管理、缓冲池、B+ 树索引、查询解析/优化/执行等核心模块，并提供交互式 CLI。

## 功能概览
- 存储：可配置块大小/内存/磁盘容量，变长记录页，块级持久化。
- 缓冲池：LRU 替换、写回，支持统计命中/缺失。
- 索引：B+ 树索引，支持重建、损坏检测与重建。
- 查询：词法/语法/语义分析，逻辑计划、物理计划、执行器（扫描/过滤/投影/去重/连接/排序/聚合）。
- 系统：数据字典、计划缓存、日志缓冲、VACUUM 清理。
- CLI：简化版 SQL 与管理命令。

## 构建
```bash
cmake -S . -B build
cmake --build build
```

生成的可执行文件位于：
- `build/Debug/dbms.exe`：交互式数据库
- `build/Debug/dbms_tests.exe`：自带测试

## 运行
```bash
./build/Debug/dbms.exe [--block-size <字节|K|M|G>] [--memory <字节|K|M|G>] [--disk <字节|K|M|G>]
```

首次启动会自动创建示例表 `users`、`orders` 并写入样例数据，表模式持久化在 `storage/meta/schemas.meta`。

## CLI 命令
- `CREATE TABLE name (col TYPE(len), ...)` 或简写 `name col1:int:16,col2:string:64`
- `CREATE INDEX idx ON table(column)`
- `INSERT INTO table VALUES (v1, v2, ...)`
- `SELECT ...`（支持连接、排序、聚合、去重等）
- `TABLES` / `INDEXES` 查看元数据
- `DUMP <table> [limit [offset]]` 导出原始行
- `VACUUM <table|all>` 清理删除槽位
- `PLANS [n]` 查看最近的访问计划
- `LOGS [n]` 查看持久化日志
- `MEM` 查看内存分区与字典/缓存占用
- `HELP` / `EXIT`

## 测试
```bash
./build/Debug/dbms_tests.exe
```

## 存储目录
- 数据块：`storage/<table>/block_*.blk`
- 索引：`storage/indexes/*.tree`
- 元数据：`storage/meta/*`
- 日志：`storage/logs/operations.log`
