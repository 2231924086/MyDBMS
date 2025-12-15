#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <fstream>
#include <functional>
#include <optional>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/types.h"
#include "common/utils.h"

namespace dbms {

class BPlusTree {
public:
    BPlusTree() = default;

    BPlusTree(std::size_t pageSizeBytes, std::size_t keyBytes) {
        initialize(pageSizeBytes, keyBytes);
    }

    void initialize(std::size_t pageSizeBytes, std::size_t keyBytes) {
        pageSize_ = pageSizeBytes;
        keyLength_ = keyBytes;
        maxKeys_ = computeMaxKeys(pageSizeBytes, keyBytes);
        if (maxKeys_ < 3) {
            maxKeys_ = 3;
        }
        minKeys_ = std::max<std::size_t>(1, maxKeys_ / 2);
        clearNodes();
    }

    std::size_t entriesPerPage() const {
        return maxKeys_;
    }

    std::size_t pageSizeBytes() const {
        return pageSize_;
    }

    void clearNodes() {
        nodes_.clear();
        rootId_ = kInvalidNode;
        nextNodeId_ = 1;
    }

    void bulkInsert(const     std::vector<std::pair<std::string, IndexPointer>> &entries) {
            clearNodes();
            if (entries.empty()) {
                return;
            }
            ensureRoot();
            std::vector<std::pair<std::string, IndexPointer>> sorted(entries);
            std::sort(sorted.begin(), sorted.end(),
                      [](const auto &lhs, const auto &rhs) {
                          return lhs.first < rhs.first;
                      });
            for (const auto &entry : sorted) {
                insertUnique(entry.first, entry.second);
            }
        }

        void insertUnique(const std::string &key, const IndexPointer &ptr) {
            ensureRoot();
            auto split = insertRecursive(rootId_, key, ptr, false);
            if (split.has_value()) {
                promoteToNewRoot(*split);
            }
        }

        void insertOrAssign(const std::string &key, const IndexPointer &ptr) {
            ensureRoot();
            auto split = insertRecursive(rootId_, key, ptr, false);
            if (split.has_value()) {
                promoteToNewRoot(*split);
            }
        }

        bool update(const std::string &key, const IndexPointer &ptr) {
            if (rootId_ == kInvalidNode) {
                return false;
            }
            const std::size_t leafId = locateLeaf(rootId_, key);
            auto &leaf = nodes_.at(leafId);
            auto it = std::lower_bound(leaf.keys.begin(), leaf.keys.end(), key);
            if (it == leaf.keys.end() || *it != key) {
                return false;
            }
            const std::size_t idx = static_cast<std::size_t>(std::distance(leaf.keys.begin(), it));
            leaf.values[idx] = ptr;
            return true;
        }

        bool erase(const std::string &key) {
            if (rootId_ == kInvalidNode) {
                return false;
            }
            const auto state = eraseRecursive(rootId_, key, kInvalidNode, 0);
            if (state == DeleteState::NotFound) {
                return false;
            }
            if (rootId_ != kInvalidNode) {
                auto rootIt = nodes_.find(rootId_);
                if (rootIt != nodes_.end()) {
                    auto &root = rootIt->second;
                    if (!root.leaf && root.keys.empty() && root.children.size() == 1) {
                        const std::size_t oldRoot = rootId_;
                        rootId_ = root.children.front();
                        nodes_.erase(oldRoot);
                    } else if (root.leaf && root.keys.empty()) {

                    }
                }
            }
            return true;
        }

        std::optional<IndexPointer> find(const std::string &key) const {
            if (rootId_ == kInvalidNode) {
                return std::nullopt;
            }
            const std::size_t leafId = locateLeaf(rootId_, key);
            const auto &leaf = nodes_.at(leafId);
            auto it = std::lower_bound(leaf.keys.begin(), leaf.keys.end(), key);
            if (it == leaf.keys.end() || *it != key) {
                return std::nullopt;
            }
            const std::size_t idx = static_cast<std::size_t>(std::distance(leaf.keys.begin(), it));
            return leaf.values[idx];
        }

    std::vector<std::string> describePages() const {
        std::vector<std::string> lines;
        std::ostringstream header;
        header << "Index file: " << nodes_.size() << " page(s), max "
               << maxKeys_ << " entry/entries per page.";
        lines.push_back(header.str());
        if (rootId_ == kInvalidNode || nodes_.empty()) {
            lines.push_back("  [empty tree]");
            return lines;
        }
        std::queue<std::pair<std::size_t, std::size_t>> bfs;
        bfs.emplace(rootId_, 0);
        while (!bfs.empty()) {
            auto [nodeId, level] = bfs.front();
            bfs.pop();
            const auto nodeIt = nodes_.find(nodeId);
            if (nodeIt == nodes_.end()) {
                continue;
            }
            const auto &node = nodeIt->second;
            std::ostringstream meta;
            meta << "  Page #" << node.id << " (level " << level << ", "
                 << (node.leaf ? "leaf" : "internal");
            if (node.id == rootId_) {
                meta << ", root";
            }
            meta << ") keys=" << node.keys.size();
            lines.push_back(meta.str());
            std::ostringstream keysLine;
            keysLine << "    Keys: ";
            if (node.keys.empty()) {
                keysLine << "[]";
            } else {
                for (std::size_t i = 0; i < node.keys.size(); ++i) {
                    if (i != 0) {
                        keysLine << " | ";
                    }
                    keysLine << "[" << node.keys[i] << "]";
                }
            }
            lines.push_back(keysLine.str());
            if (node.leaf) {
                std::ostringstream ptrLine;
                ptrLine << "    Pointers: ";
                if (node.values.empty()) {
                    ptrLine << "[]";
                } else {
                    for (std::size_t i = 0; i < node.values.size(); ++i) {
                        if (i != 0) {
                            ptrLine << " | ";
                        }
                        ptrLine << pointerToString(node.values[i]);
                    }
                }
                lines.push_back(ptrLine.str());
                if (node.hasNext) {
                    std::ostringstream nextLine;
                    nextLine << "    Next leaf -> #" << node.nextLeaf;
                    lines.push_back(nextLine.str());
                }
            } else {
                std::ostringstream childLine;
                childLine << "    Children: ";
                for (std::size_t i = 0; i < node.children.size(); ++i) {
                    if (i != 0) {
                        childLine << " | ";
                    }
                    childLine << "#" << node.children[i];
                    bfs.emplace(node.children[i], level + 1);
                }
                lines.push_back(childLine.str());
            }
        }
        return lines;
    }

    void saveToFile(const std::string &path) const {
        pathutil::ensureParentDirectory(path);
        std::ofstream out(path, std::ios::binary);
        if (!out) {
            std::ostringstream oss;
            oss << "failed to persist index file: " << path;
            throw std::runtime_error(oss.str());
        }
        out << "IDXTREE V1\n";
        out << "PAGE_SIZE " << pageSize_ << "\n";
        out << "KEY_LENGTH " << keyLength_ << "\n";
        out << "ROOT " << serializeNodeId(rootId_) << "\n";
        out << "NEXT " << nextNodeId_ << "\n";
        out << "NODE_COUNT " << nodes_.size() << "\n";
        std::vector<std::size_t> nodeOrder;
        nodeOrder.reserve(nodes_.size());
        for (const auto &entry : nodes_) {
            nodeOrder.push_back(entry.first);
        }
        std::sort(nodeOrder.begin(), nodeOrder.end());
        for (auto nodeId : nodeOrder) {
            const auto &node = nodes_.at(nodeId);
            out << "NODE " << node.id << " " << (node.leaf ? 1 : 0) << " "
                << (node.hasNext ? 1 : 0) << " "
                << serializeNodeId(node.nextLeaf) << "\n";
            out << "KEYS " << node.keys.size() << "\n";
            for (const auto &key : node.keys) {
                out << encodeHex(key) << "\n";
            }
            if (node.leaf) {
                out << "VALUES " << node.values.size() << "\n";
                for (const auto &value : node.values) {
                    out << encodeHex(value.address.table) << " "
                        << value.address.index << " " << value.slot << "\n";
                }
            } else {
                out << "CHILDREN " << node.children.size() << "\n";
                for (auto childId : node.children) {
                    out << childId << "\n";
                }
            }
        }
    }

    void loadFromFile(const std::string &path,
                      std::size_t expectedPageSize,
                      std::size_t expectedKeyLength) {
        std::ifstream in(path, std::ios::binary);
        if (!in) {
            std::ostringstream oss;
            oss << "failed to open index file: " << path;
            throw std::runtime_error(oss.str());
        }
        auto readLine = [&](const char *context) {
            std::string line;
            if (!std::getline(in, line)) {
                std::ostringstream oss;
                oss << "corrupted index file '" << path << "' missing " << context;
                throw std::runtime_error(oss.str());
            }
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            return line;
        };
        const std::string header = readLine("header");
        if (header != "IDXTREE V1") {
            std::ostringstream oss;
            oss << "unsupported index format in " << path;
            throw std::runtime_error(oss.str());
        }
        const auto pageSizeLine = readLine("page size");
        const auto filePageSize = parseHeaderValue(pageSizeLine, "PAGE_SIZE");
        if (filePageSize != expectedPageSize) {
            std::ostringstream oss;
            oss << "index page size mismatch in " << path;
            throw std::runtime_error(oss.str());
        }
        const auto keyLengthLine = readLine("key length");
        const auto fileKeyLength = parseHeaderValue(keyLengthLine, "KEY_LENGTH");
        if (fileKeyLength != expectedKeyLength) {
            std::ostringstream oss;
            oss << "index key length mismatch in " << path;
            throw std::runtime_error(oss.str());
        }
        const auto rootLine = readLine("root");
        const auto rootValue = parseSignedHeaderValue(rootLine, "ROOT");
        const auto nextLine = readLine("next node id");
        const auto nextValue = parseHeaderValue(nextLine, "NEXT");
        const auto countLine = readLine("node count");
        const auto nodeCount = parseHeaderValue(countLine, "NODE_COUNT");
        initialize(expectedPageSize, expectedKeyLength);
        nodes_.clear();
        nextNodeId_ = nextValue;
        rootId_ = rootValue < 0 ? kInvalidNode : static_cast<std::size_t>(rootValue);
        for (std::size_t idx = 0; idx < nodeCount; ++idx) {
            Node node;
            const auto nodeDesc = readLine("node descriptor");
            std::stringstream nodeStream(nodeDesc);
            std::string tag;
            int leafFlag{0};
            int nextFlag{0};
            long long nextLeafRaw{0};
            nodeStream >> tag >> node.id >> leafFlag >> nextFlag >> nextLeafRaw;
            if (tag != "NODE") {
                throw std::runtime_error("corrupted index node descriptor");
            }
            node.leaf = leafFlag != 0;
            node.hasNext = nextFlag != 0;
            node.nextLeaf = nextLeafRaw < 0 ? kInvalidNode
                                            : static_cast<std::size_t>(nextLeafRaw);
            const auto keysHeader = readLine("keys header");
            std::stringstream keyStream(keysHeader);
            std::string keysTag;
            std::size_t keyCount{0};
            keyStream >> keysTag >> keyCount;
            if (keysTag != "KEYS") {
                throw std::runtime_error("corrupted index keys header");
            }
            node.keys.reserve(keyCount);
            for (std::size_t k = 0; k < keyCount; ++k) {
                node.keys.push_back(decodeHex(readLine("key entry")));
            }
            if (node.leaf) {
                const auto valuesHeader = readLine("values header");
                std::stringstream valueStream(valuesHeader);
                std::string valuesTag;
                std::size_t valueCount{0};
                valueStream >> valuesTag >> valueCount;
                if (valuesTag != "VALUES") {
                    throw std::runtime_error("corrupted index values header");
                }
                node.values.reserve(valueCount);
                for (std::size_t v = 0; v < valueCount; ++v) {
                    const auto valueLine = readLine("value entry");
                    std::stringstream vs(valueLine);
                    std::string tableHex;
                    std::size_t blockIdx{0};
                    std::size_t slotIdx{0};
                    if (!(vs >> tableHex >> blockIdx >> slotIdx)) {
                        throw std::runtime_error("corrupted index pointer entry");
                    }
                    IndexPointer ptr;
                    ptr.address.table = decodeHex(tableHex);
                    ptr.address.index = blockIdx;
                    ptr.slot = slotIdx;
                    node.values.push_back(ptr);
                }
            } else {
                const auto childrenHeader = readLine("children header");
                std::stringstream childStream(childrenHeader);
                std::string childTag;
                std::size_t childCount{0};
                childStream >> childTag >> childCount;
                if (childTag != "CHILDREN") {
                    throw std::runtime_error("corrupted child header");
                }
                node.children.reserve(childCount);
                for (std::size_t c = 0; c < childCount; ++c) {
                    const auto childLine = readLine("child entry");
                    node.children.push_back(
                        static_cast<std::size_t>(std::stoull(childLine)));
                }
            }
            nodes_[node.id] = std::move(node);
        }
        if (nodes_.empty()) {
            rootId_ = kInvalidNode;
        }
    }

private:
        struct Node {
            std::size_t id{0};
            bool leaf{true};
            std::vector<std::string> keys;
            std::vector<IndexPointer> values;
            std::vector<std::size_t> children;
            bool hasNext{false};
            std::size_t nextLeaf{kInvalidNode};
        };

        enum class DeleteState { NotFound, Balanced, NeedsRebalance };

        static constexpr std::size_t kInvalidNode = std::numeric_limits<std::size_t>::max();

        void ensureRoot() {
            if (maxKeys_ == 0) {
                throw std::logic_error("B+ tree must be initialized before use");
            }
            if (rootId_ == kInvalidNode) {
                rootId_ = createNode(true);
            }
        }

        std::size_t createNode(bool leaf) {
            Node node;
            node.id = nextNodeId_++;
            node.leaf = leaf;
            node.hasNext = false;
            node.nextLeaf = kInvalidNode;
            return nodes_.emplace(node.id, std::move(node)).first->second.id;
        }

        std::size_t locateLeaf(std::size_t nodeId, const std::string &key) const {
            const auto &node = nodes_.at(nodeId);
            if (node.leaf) {
                return nodeId;
            }
            const std::size_t childIdx = findChildIndex(node, key);
            return locateLeaf(node.children[childIdx], key);
        }

        std::optional<std::pair<std::string, std::size_t>> insertRecursive(std::size_t nodeId,
                                                                           const std::string &key,
                                                                           const IndexPointer &ptr,
                                                                           bool failOnDuplicate) {
            auto &node = nodes_.at(nodeId);
            if (node.leaf) {
                auto it = std::lower_bound(node.keys.begin(), node.keys.end(), key);
                const std::size_t idx = static_cast<std::size_t>(std::distance(node.keys.begin(), it));
                if (it != node.keys.end() && *it == key) {
                    if (failOnDuplicate) {
                        std::ostringstream oss;
                        oss << "duplicate index key '" << key << "'";
                        throw std::runtime_error(oss.str());
                    }
                    node.values[idx] = ptr;
                    return std::nullopt;
                }
                node.keys.insert(it, key);
                node.values.insert(node.values.begin() + idx, ptr);
                if (node.keys.size() > maxKeys_) {
                    return splitLeaf(nodeId);
                }
                return std::nullopt;
            }
            const std::size_t childPos = findChildIndex(node, key);
            auto split = insertRecursive(node.children[childPos], key, ptr, failOnDuplicate);
            if (!split.has_value()) {
                return std::nullopt;
            }
            node.keys.insert(node.keys.begin() + childPos, split->first);
            node.children.insert(node.children.begin() + childPos + 1, split->second);
            if (node.keys.size() > maxKeys_) {
                return splitInternal(nodeId);
            }
            return std::nullopt;
        }

        void promoteToNewRoot(const std::pair<std::string, std::size_t> &splitInfo) {
            const std::size_t newRootId = createNode(false);
            auto &root = nodes_.at(newRootId);
            root.leaf = false;
            root.keys.push_back(splitInfo.first);
            root.children.push_back(rootId_);
            root.children.push_back(splitInfo.second);
            rootId_ = newRootId;
        }

        std::optional<std::pair<std::string, std::size_t>> splitLeaf(std::size_t nodeId) {
            auto &node = nodes_.at(nodeId);
            const std::size_t newNodeId = createNode(true);
            auto &right = nodes_.at(newNodeId);
            const std::size_t mid = node.keys.size() / 2;
            right.keys.assign(node.keys.begin() + mid, node.keys.end());
            right.values.assign(node.values.begin() + mid, node.values.end());
            node.keys.erase(node.keys.begin() + mid, node.keys.end());
            node.values.erase(node.values.begin() + mid, node.values.end());
            right.hasNext = node.hasNext;
            right.nextLeaf = node.nextLeaf;
            node.hasNext = true;
            node.nextLeaf = newNodeId;
            return std::make_pair(right.keys.front(), newNodeId);
        }

        std::optional<std::pair<std::string, std::size_t>> splitInternal(std::size_t nodeId) {
            auto &node = nodes_.at(nodeId);
            const std::size_t newNodeId = createNode(false);
            auto &right = nodes_.at(newNodeId);
            const std::size_t mid = node.keys.size() / 2;
            const std::string promote = node.keys[mid];
            right.leaf = false;
            right.keys.assign(node.keys.begin() + mid + 1, node.keys.end());
            right.children.assign(node.children.begin() + mid + 1, node.children.end());
            node.keys.erase(node.keys.begin() + mid, node.keys.end());
            node.children.erase(node.children.begin() + mid + 1, node.children.end());
            return std::make_pair(promote, newNodeId);
        }

        std::size_t findChildIndex(const Node &node, const std::string &key) const {
            auto it = std::upper_bound(node.keys.begin(), node.keys.end(), key);
            return static_cast<std::size_t>(std::distance(node.keys.begin(), it));
        }

        DeleteState eraseRecursive(std::size_t nodeId,
                                   const std::string &key,
                                   std::size_t parentId,
                                   std::size_t parentChildIndex) {
            auto &node = nodes_.at(nodeId);
            if (node.leaf) {
                auto it = std::lower_bound(node.keys.begin(), node.keys.end(), key);
                if (it == node.keys.end() || *it != key) {
                    return DeleteState::NotFound;
                }
                const std::size_t idx = static_cast<std::size_t>(std::distance(node.keys.begin(), it));
                node.keys.erase(node.keys.begin() + idx);
                node.values.erase(node.values.begin() + idx);
                if (nodeId == rootId_) {
                    return DeleteState::Balanced;
                }
                return node.keys.size() < minKeys_ ? DeleteState::NeedsRebalance
                                                   : DeleteState::Balanced;
            }
            std::size_t childIndex = findChildIndex(node, key);
            if (childIndex >= node.children.size()) {
                childIndex = node.children.size() - 1;
            }
            auto state = eraseRecursive(node.children[childIndex], key, nodeId, childIndex);
            if (state == DeleteState::NotFound) {
                return DeleteState::NotFound;
            }
            if (state == DeleteState::NeedsRebalance) {
                rebalanceChild(nodeId, childIndex);
            }
            if (nodeId == rootId_) {
                if (!node.leaf && node.keys.empty() && node.children.size() == 1) {
                    const std::size_t oldRoot = rootId_;
                    rootId_ = node.children.front();
                    nodes_.erase(oldRoot);
                }
                return DeleteState::Balanced;
            }
            return node.keys.size() < minKeys_ ? DeleteState::NeedsRebalance
                                               : DeleteState::Balanced;
        }

        void rebalanceChild(std::size_t parentId, std::size_t childIndex) {
            auto &parent = nodes_.at(parentId);
            if (parent.children.empty()) {
                return;
            }
            if (childIndex >= parent.children.size()) {
                childIndex = parent.children.size() - 1;
            }
            const std::size_t childId = parent.children[childIndex];
            auto &child = nodes_.at(childId);
            if (child.leaf) {
                if (childIndex > 0) {
                    auto &left = nodes_.at(parent.children[childIndex - 1]);
                    if (left.keys.size() > minKeys_) {
                        borrowFromLeftLeaf(parent, childIndex);
                        return;
                    }
                }
                if (childIndex + 1 < parent.children.size()) {
                    auto &right = nodes_.at(parent.children[childIndex + 1]);
                    if (right.keys.size() > minKeys_) {
                        borrowFromRightLeaf(parent, childIndex);
                        return;
                    }
                }
                if (childIndex > 0) {
                    mergeLeaves(parentId, childIndex - 1);
                } else if (parent.children.size() >= 2) {
                    mergeLeaves(parentId, 0);
                }
            } else {
                if (childIndex > 0) {
                    auto &left = nodes_.at(parent.children[childIndex - 1]);
                    if (left.keys.size() > minKeys_) {
                        borrowFromLeftInternal(parent, childIndex);
                        return;
                    }
                }
                if (childIndex + 1 < parent.children.size()) {
                    auto &right = nodes_.at(parent.children[childIndex + 1]);
                    if (right.keys.size() > minKeys_) {
                        borrowFromRightInternal(parent, childIndex);
                        return;
                    }
                }
                if (childIndex > 0) {
                    mergeInternal(parentId, childIndex - 1);
                } else if (parent.children.size() >= 2) {
                    mergeInternal(parentId, 0);
                }
            }
        }

        void borrowFromLeftLeaf(Node &parent, std::size_t childIndex) {
            auto &left = nodes_.at(parent.children[childIndex - 1]);
            auto &child = nodes_.at(parent.children[childIndex]);
            child.keys.insert(child.keys.begin(), left.keys.back());
            child.values.insert(child.values.begin(), left.values.back());
            left.keys.pop_back();
            left.values.pop_back();
            parent.keys[childIndex - 1] = child.keys.front();
        }

        void borrowFromRightLeaf(Node &parent, std::size_t childIndex) {
            auto &right = nodes_.at(parent.children[childIndex + 1]);
            auto &child = nodes_.at(parent.children[childIndex]);
            child.keys.push_back(right.keys.front());
            child.values.push_back(right.values.front());
            right.keys.erase(right.keys.begin());
            right.values.erase(right.values.begin());
            parent.keys[childIndex] = right.keys.front();
        }

        void mergeLeaves(std::size_t parentId, std::size_t leftIndex) {
            auto &parent = nodes_.at(parentId);
            if (leftIndex + 1 >= parent.children.size()) {
                return;
            }
            const std::size_t leftId = parent.children[leftIndex];
            const std::size_t rightId = parent.children[leftIndex + 1];
            auto &left = nodes_.at(leftId);
            auto &right = nodes_.at(rightId);
            left.keys.insert(left.keys.end(), right.keys.begin(), right.keys.end());
            left.values.insert(left.values.end(), right.values.begin(), right.values.end());
            left.hasNext = right.hasNext;
            left.nextLeaf = right.nextLeaf;
            parent.keys.erase(parent.keys.begin() + leftIndex);
            parent.children.erase(parent.children.begin() + leftIndex + 1);
            nodes_.erase(rightId);
        }

        void borrowFromLeftInternal(Node &parent, std::size_t childIndex) {
            auto &left = nodes_.at(parent.children[childIndex - 1]);
            auto &child = nodes_.at(parent.children[childIndex]);
            child.keys.insert(child.keys.begin(), parent.keys[childIndex - 1]);
            parent.keys[childIndex - 1] = left.keys.back();
            child.children.insert(child.children.begin(), left.children.back());
            left.keys.pop_back();
            left.children.pop_back();
        }

        void borrowFromRightInternal(Node &parent, std::size_t childIndex) {
            auto &right = nodes_.at(parent.children[childIndex + 1]);
            auto &child = nodes_.at(parent.children[childIndex]);
            child.keys.push_back(parent.keys[childIndex]);
            parent.keys[childIndex] = right.keys.front();
            child.children.push_back(right.children.front());
            right.keys.erase(right.keys.begin());
            right.children.erase(right.children.begin());
        }

        void mergeInternal(std::size_t parentId, std::size_t leftIndex) {
            auto &parent = nodes_.at(parentId);
            if (leftIndex + 1 >= parent.children.size()) {
                return;
            }
            const std::size_t leftId = parent.children[leftIndex];
            const std::size_t rightId = parent.children[leftIndex + 1];
            auto &left = nodes_.at(leftId);
            auto &right = nodes_.at(rightId);
            left.keys.push_back(parent.keys[leftIndex]);
            left.keys.insert(left.keys.end(), right.keys.begin(), right.keys.end());
            left.children.insert(left.children.end(), right.children.begin(), right.children.end());
            parent.keys.erase(parent.keys.begin() + leftIndex);
            parent.children.erase(parent.children.begin() + leftIndex + 1);
            nodes_.erase(rightId);
        }

        static std::size_t computeMaxKeys(std::size_t pageSizeBytes, std::size_t keyBytes) {
            constexpr std::size_t headerBytes = 32;
            constexpr std::size_t pointerBytes = sizeof(std::uint32_t) * 2;
            constexpr std::size_t slotBytes = sizeof(std::uint16_t);
            if (pageSizeBytes <= headerBytes) {
                return 3;
            }
            const std::size_t usable = pageSizeBytes - headerBytes;
            const std::size_t perEntry = std::max<std::size_t>(1, keyBytes + pointerBytes + slotBytes);
            return std::max<std::size_t>(3, usable / perEntry);
        }

    static std::string pointerToString(const IndexPointer &ptr) {
        std::ostringstream oss;
        oss << ptr.address.table << "#" << ptr.address.index << ":" << ptr.slot;
        return oss.str();
    }

    static std::string encodeHex(const std::string &input) {
        static constexpr char kDigits[] = "0123456789ABCDEF";
        std::string output;
        output.reserve(input.size() * 2);
        for (unsigned char ch : input) {
            output.push_back(kDigits[ch >> 4U]);
            output.push_back(kDigits[ch & 0x0FU]);
        }
        return output;
    }

    static unsigned char hexNibble(char ch) {
        if (ch >= '0' && ch <= '9') {
            return static_cast<unsigned char>(ch - '0');
        }
        if (ch >= 'A' && ch <= 'F') {
            return static_cast<unsigned char>(10 + (ch - 'A'));
        }
        if (ch >= 'a' && ch <= 'f') {
            return static_cast<unsigned char>(10 + (ch - 'a'));
        }
        throw std::runtime_error("invalid hex digit in index payload");
    }

    static std::string decodeHex(const std::string &input) {
        if (input.size() % 2 != 0) {
            throw std::runtime_error("corrupted index hex payload");
        }
        std::string output;
        output.reserve(input.size() / 2);
        for (std::size_t i = 0; i < input.size(); i += 2) {
            unsigned char high = hexNibble(input[i]);
            unsigned char low = hexNibble(input[i + 1]);
            output.push_back(static_cast<char>((high << 4U) | low));
        }
        return output;
    }

    static long long serializeNodeId(std::size_t nodeId) {
        if (nodeId == kInvalidNode) {
            return -1LL;
        }
        return static_cast<long long>(nodeId);
    }

    static std::size_t parseHeaderValue(const std::string &line,
                                        const std::string &expectedTag) {
        std::stringstream ss(line);
        std::string tag;
        std::size_t value{0};
        if (!(ss >> tag >> value) || tag != expectedTag) {
            std::ostringstream oss;
            oss << "corrupted header expecting '" << expectedTag << "'";
            throw std::runtime_error(oss.str());
        }
        return value;
    }

    static long long parseSignedHeaderValue(const std::string &line,
                                            const std::string &expectedTag) {
        std::stringstream ss(line);
        std::string tag;
        long long value{0};
        if (!(ss >> tag >> value) || tag != expectedTag) {
            std::ostringstream oss;
            oss << "corrupted header expecting '" << expectedTag << "'";
            throw std::runtime_error(oss.str());
        }
        return value;
    }

    std::unordered_map<std::size_t, Node> nodes_;
    std::size_t rootId_{kInvalidNode};
    std::size_t nextNodeId_{1};
    std::size_t maxKeys_{0};
        std::size_t minKeys_{0};
        std::size_t pageSize_{0};
        std::size_t keyLength_{0};
    };


} // namespace dbms
