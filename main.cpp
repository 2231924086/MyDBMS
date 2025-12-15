#include <algorithm>
#include <cctype>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "common/types.h"
#include "common/utils.h"
#include "parser/query_processor.h"
#include "system/database.h"

using dbms::ColumnDefinition;
using dbms::ColumnType;
using dbms::DatabaseSystem;
using dbms::Record;
using dbms::TableSchema;

namespace {

std::string trim(const std::string &input) {
    const auto first = input.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) {
        return "";
    }
    const auto last = input.find_last_not_of(" \t\r\n");
    return input.substr(first, last - first + 1);
}

std::string toLowerCopy(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return s;
}

bool startsWithCaseInsensitive(const std::string &text, const std::string &prefix) {
    if (text.size() < prefix.size()) {
        return false;
    }
    const auto lhs = toLowerCopy(text.substr(0, prefix.size()));
    const auto rhs = toLowerCopy(prefix);
    return lhs == rhs;
}

std::size_t defaultLength(ColumnType type) {
    switch (type) {
    case ColumnType::Integer:
    case ColumnType::Double:
        return 16;
    case ColumnType::String:
    default:
        return 64;
    }
}

ColumnType parseColumnTypeToken(const std::string &token) {
    const auto lower = toLowerCopy(token);
    if (lower == "int" || lower == "integer") {
        return ColumnType::Integer;
    }
    if (lower == "double") {
        return ColumnType::Double;
    }
    return ColumnType::String;
}

std::optional<ColumnDefinition> parseColumnDefinition(const std::string &definitionText) {
    std::string text = trim(definitionText);
    if (text.empty()) {
        return std::nullopt;
    }

    // Support "name:type:length" shorthand as well as "name TYPE(length)".
    std::string name;
    std::string typeToken;
    std::string lengthToken;

    const auto colonPos = text.find(':');
    if (colonPos != std::string::npos) {
        name = trim(text.substr(0, colonPos));
        const auto rest = text.substr(colonPos + 1);
        const auto secondColon = rest.find(':');
        if (secondColon == std::string::npos) {
            typeToken = trim(rest);
        } else {
            typeToken = trim(rest.substr(0, secondColon));
            lengthToken = trim(rest.substr(secondColon + 1));
        }
    } else {
        std::istringstream iss(text);
        iss >> name;
        iss >> typeToken;
        auto open = typeToken.find('(');
        if (open != std::string::npos) {
            lengthToken = typeToken.substr(open + 1);
            typeToken = typeToken.substr(0, open);
            if (!lengthToken.empty() && lengthToken.back() == ')') {
                lengthToken.pop_back();
            }
        } else {
            char ch{};
            if (iss >> ch && ch == '(') {
                std::getline(iss, lengthToken, ')');
            }
        }
    }

    if (name.empty() || typeToken.empty()) {
        return std::nullopt;
    }

    ColumnType type = parseColumnTypeToken(typeToken);
    std::size_t length = lengthToken.empty() ? defaultLength(type)
                                             : static_cast<std::size_t>(std::stoull(lengthToken));
    if (length == 0) {
        length = defaultLength(type);
    }

    return ColumnDefinition{name, type, length};
}

std::vector<std::string> split(const std::string &text, char delim) {
    std::vector<std::string> parts;
    std::stringstream ss(text);
    std::string part;
    while (std::getline(ss, part, delim)) {
        parts.push_back(part);
    }
    return parts;
}

std::vector<ColumnDefinition> parseColumns(const std::string &rawColumns) {
    std::string columnsText = trim(rawColumns);
    if (!columnsText.empty() && columnsText.front() == '(') {
        columnsText.erase(columnsText.begin());
    }
    if (!columnsText.empty() && columnsText.back() == ')') {
        columnsText.pop_back();
    }

    std::vector<ColumnDefinition> columns;
    for (const auto &part : split(columnsText, ',')) {
        if (auto col = parseColumnDefinition(part)) {
            columns.push_back(*col);
        }
    }
    return columns;
}

std::optional<TableSchema> parseCreateTableCommand(const std::string &line) {
    const std::string keyword = "create table";
    if (!startsWithCaseInsensitive(trim(line), keyword)) {
        return std::nullopt;
    }

    std::string work = trim(line);
    if (!work.empty() && work.back() == ';') {
        work.pop_back();
    }
    std::string lower = toLowerCopy(work);
    auto pos = lower.find(keyword);
    if (pos == std::string::npos) {
        return std::nullopt;
    }
    pos += keyword.size();
    while (pos < work.size() && std::isspace(static_cast<unsigned char>(work[pos]))) {
        ++pos;
    }
    std::size_t nameStart = pos;
    while (pos < work.size() && !std::isspace(static_cast<unsigned char>(work[pos])) &&
           work[pos] != '(') {
        ++pos;
    }
    std::string tableName = trim(work.substr(nameStart, pos - nameStart));
    if (tableName.empty()) {
        return std::nullopt;
    }
    std::string columnPart;
    auto open = work.find('(', pos);
    if (open == std::string::npos) {
        columnPart = work.substr(pos);
    } else {
        auto close = work.find_last_of(')');
        if (close != std::string::npos && close > open) {
            columnPart = work.substr(open, close - open + 1);
        } else {
            columnPart = work.substr(open);
        }
    }

    auto columns = parseColumns(columnPart);
    if (columns.empty()) {
        return std::nullopt;
    }
    return TableSchema(tableName, std::move(columns));
}

bool parseCreateIndexCommand(const std::string &line,
                             std::string &indexName,
                             std::string &tableName,
                             std::string &columnName) {
    const std::string keyword = "create index";
    if (!startsWithCaseInsensitive(trim(line), keyword)) {
        return false;
    }
    std::string work = trim(line);
    if (!work.empty() && work.back() == ';') {
        work.pop_back();
    }
    std::string lower = toLowerCopy(work);
    auto pos = lower.find(keyword);
    if (pos == std::string::npos) {
        return false;
    }
    pos += keyword.size();
    while (pos < work.size() && std::isspace(static_cast<unsigned char>(work[pos]))) {
        ++pos;
    }
    std::size_t idxStart = pos;
    while (pos < work.size() && !std::isspace(static_cast<unsigned char>(work[pos]))) {
        ++pos;
    }
    indexName = trim(work.substr(idxStart, pos - idxStart));
    auto onPos = lower.find(" on ", pos);
    if (onPos == std::string::npos) {
        return false;
    }
    onPos += 4;
    while (onPos < work.size() && std::isspace(static_cast<unsigned char>(work[onPos]))) {
        ++onPos;
    }
    std::size_t tableStart = onPos;
    while (onPos < work.size() && work[onPos] != '(' &&
           !std::isspace(static_cast<unsigned char>(work[onPos]))) {
        ++onPos;
    }
    tableName = trim(work.substr(tableStart, onPos - tableStart));
    auto open = work.find('(', onPos);
    auto close = work.find(')', open);
    if (open == std::string::npos || close == std::string::npos || close <= open + 1) {
        return false;
    }
    columnName = trim(work.substr(open + 1, close - open - 1));
    return !(indexName.empty() || tableName.empty() || columnName.empty());
}

bool parseInsertCommand(const std::string &line,
                        std::string &tableName,
                        std::vector<std::string> &values) {
    const std::string keyword = "insert into";
    if (!startsWithCaseInsensitive(trim(line), keyword)) {
        return false;
    }
    std::string work = trim(line);
    if (!work.empty() && work.back() == ';') {
        work.pop_back();
    }
    std::string lower = toLowerCopy(work);
    auto pos = lower.find(keyword);
    if (pos == std::string::npos) {
        return false;
    }
    pos += keyword.size();
    while (pos < work.size() && std::isspace(static_cast<unsigned char>(work[pos]))) {
        ++pos;
    }
    std::size_t nameStart = pos;
    while (pos < work.size() && !std::isspace(static_cast<unsigned char>(work[pos])) &&
           work[pos] != '(') {
        ++pos;
    }
    tableName = trim(work.substr(nameStart, pos - nameStart));
    auto valuesPos = lower.find("values", pos);
    if (valuesPos == std::string::npos) {
        return false;
    }
    auto lparen = work.find('(', valuesPos);
    auto rparen = work.find_last_of(')');
    if (lparen == std::string::npos || rparen == std::string::npos || rparen <= lparen) {
        return false;
    }
    std::string payload = work.substr(lparen + 1, rparen - lparen - 1);
    values.clear();
    for (auto part : split(payload, ',')) {
        part = trim(part);
        if (!part.empty() && part.front() == '\'') {
            part.erase(part.begin());
        }
        if (!part.empty() && part.back() == '\'') {
            part.pop_back();
        }
        if (!part.empty() && part.front() == '"') {
            part.erase(part.begin());
        }
        if (!part.empty() && part.back() == '"') {
            part.pop_back();
        }
        values.push_back(part);
    }
    return !tableName.empty() && !values.empty();
}

std::string columnTypeToString(ColumnType type) {
    switch (type) {
    case ColumnType::Integer:
        return "int";
    case ColumnType::Double:
        return "double";
    case ColumnType::String:
    default:
        return "string";
    }
}

std::string serializeSchema(const TableSchema &schema) {
    std::ostringstream oss;
    oss << schema.name() << "|";
    const auto &cols = schema.columns();
    for (std::size_t i = 0; i < cols.size(); ++i) {
        if (i > 0) {
            oss << ",";
        }
        oss << cols[i].name << ":" << columnTypeToString(cols[i].type) << ":"
            << cols[i].length;
    }
    return oss.str();
}

std::optional<TableSchema> parseSchemaLine(const std::string &line) {
    const auto parts = split(line, '|');
    if (parts.size() < 2) {
        return std::nullopt;
    }
    auto columns = parseColumns(parts[1]);
    if (columns.empty()) {
        return std::nullopt;
    }
    return TableSchema(parts[0], std::move(columns));
}

class SchemaRegistry {
public:
    SchemaRegistry()
        : path_(dbms::pathutil::join(dbms::pathutil::join("storage", "meta"),
                                     "schemas.meta")) {}

    std::vector<TableSchema> load() const {
        std::vector<TableSchema> schemas;
        std::ifstream in(path_);
        if (!in) {
            return schemas;
        }
        std::string line;
        while (std::getline(in, line)) {
            line = trim(line);
            if (line.empty()) {
                continue;
            }
            if (auto schema = parseSchemaLine(line)) {
                schemas.push_back(*schema);
            }
        }
        return schemas;
    }

    void save(const std::vector<TableSchema> &schemas) const {
        dbms::pathutil::ensureParentDirectory(path_);
        std::ofstream out(path_, std::ios::trunc);
        for (const auto &schema : schemas) {
            out << serializeSchema(schema) << "\n";
        }
    }

    const std::string &path() const { return path_; }

private:
    std::string path_;
};

struct Config {
    std::size_t blockSizeBytes{4096};
    std::size_t memoryBytes{32 * 1024 * 1024}; // 32 MiB
    std::size_t diskBytes{256 * 1024 * 1024};  // 256 MiB
};

std::size_t parseBytes(const std::string &text) {
    if (text.empty()) {
        return 0;
    }
    std::string lower = toLowerCopy(text);
    std::size_t multiplier = 1;
    if (lower.back() == 'k') {
        multiplier = 1024ULL;
        lower.pop_back();
    } else if (lower.back() == 'm') {
        multiplier = 1024ULL * 1024ULL;
        lower.pop_back();
    } else if (lower.back() == 'g') {
        multiplier = 1024ULL * 1024ULL * 1024ULL;
        lower.pop_back();
    }
    return static_cast<std::size_t>(std::stoull(lower) * multiplier);
}

Config parseArgs(int argc, char **argv) {
    Config cfg;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto takeValue = [&](const std::string &name, std::size_t &target) {
            const std::string prefix = "--" + name + "=";
            if (arg.rfind(prefix, 0) == 0) {
                target = parseBytes(arg.substr(prefix.size()));
                return true;
            }
            if (arg == "--" + name && i + 1 < argc) {
                target = parseBytes(argv[++i]);
                return true;
            }
            return false;
        };
        takeValue("block-size", cfg.blockSizeBytes);
        takeValue("memory", cfg.memoryBytes);
        takeValue("disk", cfg.diskBytes);
    }
    return cfg;
}

void printHelp() {
    std::cout << "Commands:\n";
    std::cout << "  CREATE TABLE name (col TYPE(len), ...)  - define table schema\n";
    std::cout << "    Shorthand: name col1:int:16,col2:string:64\n";
    std::cout << "  CREATE INDEX idx ON table(column)       - build B+tree index\n";
    std::cout << "  INSERT INTO table VALUES (v1, v2, ...)  - append a record\n";
    std::cout << "  SELECT ...                              - run a query (supports joins, sort, agg)\n";
    std::cout << "  TABLES                                  - list registered tables\n";
    std::cout << "  INDEXES                                 - list indexes\n";
    std::cout << "  DUMP <table> [limit [offset]]           - dump raw table rows\n";
    std::cout << "  VACUUM <table|all>                      - reclaim deleted space\n";
    std::cout << "  PLANS [n]                               - show cached access plans\n";
    std::cout << "  LOGS [n]                                - show persisted log entries\n";
    std::cout << "  MEM                                     - show memory layout\n";
    std::cout << "  HELP                                    - show this help\n";
    std::cout << "  EXIT                                    - quit\n";
}

void printTableDump(DatabaseSystem &db,
                    const std::string &tableName,
                    std::size_t limit,
                    std::size_t offset) {
    auto dump = db.dumpTable(tableName, limit, offset);
    std::cout << "Table '" << tableName << "' rows (limit=" << limit
              << ", offset=" << offset << "):\n";
    std::size_t rowNo = offset + 1;
    for (const auto &row : dump.rows) {
        std::cout << "  #" << rowNo++ << " [block " << row.blockIndex << ", slot "
                  << row.slotIndex << "]: ";
        for (std::size_t i = 0; i < row.values.size(); ++i) {
            if (i > 0) {
                std::cout << " | ";
            }
            std::cout << row.values[i];
        }
        std::cout << "\n";
    }
    std::cout << "Total records: " << dump.totalRecords
              << " (blocks scanned: " << dump.blocksAccessed << ")\n";
    if (dump.truncated) {
        std::cout << "Result truncated; more rows are available.\n";
    }
}

void seedDemoData(DatabaseSystem &db) {
    auto addIfEmpty = [&](const std::string &table, const std::vector<Record> &records) {
        try {
            auto &t = db.getTable(table);
            if (t.totalRecords() == 0) {
                for (const auto &r : records) {
                    db.insertRecord(table, r);
                }
            }
        } catch (...) {
            // Table not present; ignore.
        }
    };

    addIfEmpty("users",
               {Record{"1", "Alice", "30"},
                Record{"2", "Bob", "42"},
                Record{"3", "Carol", "28"},
                Record{"4", "Dave", "55"}});
    addIfEmpty("orders",
               {Record{"100", "1", "200"},
                Record{"101", "2", "300"},
                Record{"102", "3", "150"},
                Record{"103", "4", "500"}});

    try {
        if (!db.findIndexForColumn("users", "id").has_value()) {
            db.createIndex("idx_users_id", "users", "id");
        }
    } catch (...) {
        // Index creation is best-effort for demo data.
    }
}

std::vector<TableSchema> defaultSchemas() {
    return {
        TableSchema("users",
                    {{"id", ColumnType::Integer, 16},
                     {"name", ColumnType::String, 64},
                     {"age", ColumnType::Integer, 8}}),
        TableSchema("orders",
                    {{"id", ColumnType::Integer, 16},
                     {"user_id", ColumnType::Integer, 16},
                     {"amount", ColumnType::Integer, 16}}),
    };
}

} // namespace

int main(int argc, char **argv) {
    Config cfg = parseArgs(argc, argv);

    try {
        DatabaseSystem db(cfg.blockSizeBytes, cfg.memoryBytes, cfg.diskBytes);
        SchemaRegistry registry;
        auto schemas = registry.load();

        if (schemas.empty()) {
            schemas = defaultSchemas();
            registry.save(schemas);
        }

        for (const auto &schema : schemas) {
            try {
                db.registerTable(schema);
            } catch (const std::exception &ex) {
                std::cerr << "Failed to register table " << schema.name()
                          << ": " << ex.what() << "\n";
            }
        }

        seedDemoData(db);

        std::cout << "Mini DBMS ready. Storage directory: storage\n";
        std::cout << "Block size: " << cfg.blockSizeBytes
                  << " bytes, buffer: " << db.buffer().capacity()
                  << " frame(s), disk blocks: " << db.diskBlocks() << "\n";
        std::cout << "Schema catalog: " << registry.path() << "\n";
        printHelp();

        std::string lineBuffer;
        while (true) {
            std::cout << "db> " << std::flush;
            std::string line;
            if (!std::getline(std::cin, line)) {
                break;
            }
            line = trim(line);
            if (line.empty()) {
                continue;
            }
            if (line == ";") {
                continue;
            }
            if (line == "exit" || line == "quit") {
                break;
            }
            if (startsWithCaseInsensitive(line, "help")) {
                printHelp();
                continue;
            }
            if (startsWithCaseInsensitive(line, "tables")) {
                for (const auto &row : db.tableSummaries()) {
                    std::cout << row << "\n";
                }
                continue;
            }
            if (startsWithCaseInsensitive(line, "indexes")) {
                for (const auto &row : db.indexSummaries()) {
                    std::cout << row << "\n";
                }
                continue;
            }
            if (startsWithCaseInsensitive(line, "dump")) {
                auto parts = split(line, ' ');
                if (parts.size() >= 2) {
                    std::size_t limit = 0;
                    std::size_t offset = 0;
                    if (parts.size() >= 3) {
                        limit = static_cast<std::size_t>(std::stoull(parts[2]));
                    }
                    if (parts.size() >= 4) {
                        offset = static_cast<std::size_t>(std::stoull(parts[3]));
                    }
                    try {
                        printTableDump(db, parts[1], limit, offset);
                    } catch (const std::exception &ex) {
                        std::cout << "Dump failed: " << ex.what() << "\n";
                    }
                } else {
                    std::cout << "Usage: DUMP <table> [limit] [offset]\n";
                }
                continue;
            }
            if (startsWithCaseInsensitive(line, "vacuum")) {
                auto parts = split(line, ' ');
                if (parts.size() >= 2 && toLowerCopy(parts[1]) != "all") {
                    try {
                        auto report = db.vacuumTable(parts[1]);
                        std::cout << "Vacuumed " << report.tableName << ": "
                                  << report.blocksVisited << " blocks visited, "
                                  << report.slotsCleared << " slots cleared\n";
                    } catch (const std::exception &ex) {
                        std::cout << "Vacuum failed: " << ex.what() << "\n";
                    }
                } else {
                    for (const auto &report : db.vacuumAllTables()) {
                        std::cout << "Vacuumed " << report.tableName << ": "
                                  << report.blocksVisited << " blocks visited, "
                                  << report.slotsCleared << " slots cleared\n";
                    }
                }
                continue;
            }
            if (startsWithCaseInsensitive(line, "plans")) {
                std::size_t limit = 10;
                auto parts = split(line, ' ');
                if (parts.size() >= 2) {
                    limit = static_cast<std::size_t>(std::stoull(parts[1]));
                }
                for (const auto &plan : db.cachedAccessPlans(limit)) {
                    std::cout << plan << "\n";
                }
                continue;
            }
            if (startsWithCaseInsensitive(line, "logs")) {
                std::size_t limit = 20;
                auto parts = split(line, ' ');
                if (parts.size() >= 2) {
                    limit = static_cast<std::size_t>(std::stoull(parts[1]));
                }
                for (const auto &log : db.persistedLogs(limit)) {
                    std::cout << log << "\n";
                }
                continue;
            }
            if (startsWithCaseInsensitive(line, "mem")) {
                std::cout << db.memoryLayoutDescription();
                continue;
            }

            if (auto schema = parseCreateTableCommand(line)) {
                try {
                    db.registerTable(*schema);
                    schemas.push_back(*schema);
                    registry.save(schemas);
                    std::cout << "Table '" << schema->name() << "' created.\n";
                } catch (const std::exception &ex) {
                    std::cout << "Create table failed: " << ex.what() << "\n";
                }
                continue;
            }

            std::string idxName, tblName, colName;
            if (parseCreateIndexCommand(line, idxName, tblName, colName)) {
                try {
                    auto pages = db.createIndex(idxName, tblName, colName);
                    std::cout << "Index '" << idxName << "' created (" << pages.size()
                              << " page(s)).\n";
                } catch (const std::exception &ex) {
                    std::cout << "Create index failed: " << ex.what() << "\n";
                }
                continue;
            }

            std::vector<std::string> insertValues;
            if (parseInsertCommand(line, tblName, insertValues)) {
                try {
                    db.insertRecord(tblName, Record{insertValues});
                    std::cout << "Inserted into " << tblName << ".\n";
                } catch (const std::exception &ex) {
                    std::cout << "Insert failed: " << ex.what() << "\n";
                }
                continue;
            }

            if (startsWithCaseInsensitive(line, "select")) {
                db.executeSQL(line);
                continue;
            }

            std::cout << "Unknown command. Type HELP for guidance.\n";
        }

        db.flushAll();
    } catch (const std::exception &ex) {
        std::cerr << "Fatal error: " << ex.what() << "\n";
        return 1;
    }

    return 0;
}
