#pragma once

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/types.h"
#include "index/b_plus_tree.h"

namespace dbms {

struct IndexDefinition {
    std::string name;
    std::string tableName;
    std::string columnName;
    std::size_t columnIndex{0};
    std::size_t keyLength{0};
    bool unique{true};
};

class BPlusTreeIndex {
public:
    BPlusTreeIndex() = default;

    BPlusTreeIndex(IndexDefinition def, std::size_t pageSizeBytes)
        : definition_(std::move(def)),
          tree_(pageSizeBytes, definition_.keyLength) {}

    void initialize(IndexDefinition def, std::size_t pageSizeBytes) {
        definition_ = std::move(def);
        tree_.initialize(pageSizeBytes, definition_.keyLength);
    }

    const IndexDefinition &definition() const {
        return definition_;
    }

    std::size_t entriesPerPage() const {
        return tree_.entriesPerPage();
    }

    void rebuild(const std::vector<std::pair<std::string, IndexPointer>> &entries) {
        tree_.bulkInsert(entries);
    }

    void insertRecord(const Record &record,
                      const BlockAddress &addr,
                      std::size_t slot) {
        const auto key = extractKey(record);
        tree_.insertUnique(key, IndexPointer{addr, slot});
    }

    void updateRecord(const Record &before,
                      const Record &after,
                      const BlockAddress &addr,
                      std::size_t slot) {
        const auto oldKey = extractKey(before);
        const auto newKey = extractKey(after);
        if (oldKey == newKey) {
            tree_.update(newKey, IndexPointer{addr, slot});
            return;
        }
        tree_.erase(oldKey);
        tree_.insertUnique(newKey, IndexPointer{addr, slot});
    }

    void deleteRecord(const Record &record) {
        const auto key = extractKey(record);
        tree_.erase(key);
    }

    std::optional<IndexPointer> find(const std::string &key) const {
        return tree_.find(key);
    }

    std::vector<std::string> describePages() const {
        return tree_.describePages();
    }

    std::string projectKey(const Record &record) const {
        return extractKey(record);
    }

    void saveToFile(const std::string &path) const {
        tree_.saveToFile(path);
    }

    void loadFromFile(const std::string &path) {
        tree_.loadFromFile(path, tree_.pageSizeBytes(), definition_.keyLength);
    }

private:
    std::string extractKey(const Record &record) const {
        return sliceIndexKey(record,
                             definition_.columnIndex,
                             definition_.keyLength);
    }

    IndexDefinition definition_;
    BPlusTree tree_;
};

} // namespace dbms
