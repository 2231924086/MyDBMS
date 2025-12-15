#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "executor/expression.h"
#include "executor/operator.h"

namespace dbms {

// Nested loop join (inner) using a generic predicate
class NestedLoopJoinOperator : public Operator {
public:
    NestedLoopJoinOperator(std::unique_ptr<Operator> left,
                           std::unique_ptr<Operator> right,
                           std::string condition,
                           JoinType joinType = JoinType::kInner);

    void init() override;
    std::optional<Tuple> next() override;
    void close() override;
    const Schema& getSchema() const override { return *outputSchema_; }
    void reset() override;

private:
    std::unique_ptr<Operator> left_;
    std::unique_ptr<Operator> right_;
    std::string condition_;
    std::unique_ptr<Expression> predicate_;
    JoinType joinType_;
    std::shared_ptr<Schema> outputSchema_;
    bool initialized_{false};
    std::optional<Tuple> currentLeft_;
    std::optional<Tuple> currentRight_;
    bool currentMatched_{false};

    Tuple combineTuples(const Tuple& left, const Tuple& right) const;
    Tuple combineWithNulls(bool nullLeft, const Tuple& other) const;
};

// Hash join (inner) on equality between two columns
class HashJoinOperator : public Operator {
public:
    HashJoinOperator(std::unique_ptr<Operator> left,
                     std::unique_ptr<Operator> right,
                     std::string condition,
                     std::string leftKey,
                     std::string rightKey,
                     JoinType joinType = JoinType::kInner);

    void init() override;
    std::optional<Tuple> next() override;
    void close() override;
    const Schema& getSchema() const override { return *outputSchema_; }
    void reset() override;

private:
    std::unique_ptr<Operator> left_;
    std::unique_ptr<Operator> right_;
    std::string condition_;
    std::unique_ptr<Expression> predicate_;
    std::string leftKey_;
    std::string rightKey_;
    JoinType joinType_;
    std::shared_ptr<Schema> outputSchema_;
    bool initialized_{false};

    std::unordered_map<std::string, std::vector<Tuple>> hashTable_;
    std::optional<Tuple> currentLeft_;
    const std::vector<Tuple>* currentMatches_{nullptr};
    std::size_t matchIndex_{0};

    void buildHashTable();
    Tuple combineTuples(const Tuple& left, const Tuple& right) const;
};

} // namespace dbms
