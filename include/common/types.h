#pragma once

#include <cstddef>
#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace dbms {

enum class ColumnType {
    Integer,
    Double,
    String
};

enum class JoinType {
    kInner,
    kLeft,
    kRight
};

struct ColumnDefinition {
    std::string name;
    ColumnType type;
    std::size_t length;
};

struct Record {
    std::vector<std::string> values;

    Record() = default;

    Record(std::initializer_list<std::string> init)
        : values(init) {}

    explicit Record(std::vector<std::string> vals)
        : values(std::move(vals)) {}
};

inline std::string sliceIndexKey(const Record &record,
                                 std::size_t columnIndex,
                                 std::size_t keyLength) {
    if (columnIndex >= record.values.size()) {
        return {};
    }
    std::string key = record.values[columnIndex];
    if (key.size() > keyLength) {
        key.resize(keyLength);
    }
    return key;
}

struct BlockAddress {
    std::string table;
    std::size_t index;

    bool operator==(const BlockAddress &other) const {
        return table == other.table && index == other.index;
    }

    bool operator<(const BlockAddress &other) const {
        return table < other.table || (table == other.table && index < other.index);
    }
};

struct BlockAddressHash {
    std::size_t operator()(const BlockAddress &addr) const noexcept {
        std::size_t h1 = std::hash<std::string>{}(addr.table);
        std::size_t h2 = std::hash<std::size_t>{}(addr.index);
        return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6U) + (h1 >> 2U));
    }
};

struct IndexPointer {
    BlockAddress address;
    std::size_t slot{0};
};

} // namespace dbms
