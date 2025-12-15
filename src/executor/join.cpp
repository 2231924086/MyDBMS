#include "executor/join.h"

#include "executor/expression_parser.h"
#include <stdexcept>

namespace dbms {

NestedLoopJoinOperator::NestedLoopJoinOperator(std::unique_ptr<Operator> left,
                                               std::unique_ptr<Operator> right,
                                               std::string condition,
                                               JoinType joinType)
    : left_(std::move(left)),
      right_(std::move(right)),
      condition_(std::move(condition)),
      joinType_(joinType) {}

void NestedLoopJoinOperator::init() {
    if (initialized_) {
        return;
    }
    left_->init();
    right_->init();

    // Build combined schema
    outputSchema_ = std::make_shared<Schema>();
    const auto& leftSchema = left_->getSchema();
    const auto& rightSchema = right_->getSchema();
    for (const auto& col : leftSchema.columns()) {
        outputSchema_->addColumn(col);
    }
    for (const auto& col : rightSchema.columns()) {
        outputSchema_->addColumn(col);
    }

    if (!condition_.empty()) {
        ExpressionParser parser;
        predicate_ = parser.parse(condition_);
    }
    currentLeft_.reset();
    currentRight_.reset();
    currentMatched_ = false;
    initialized_ = true;
}

std::optional<Tuple> NestedLoopJoinOperator::next() {
    if (!initialized_) {
        throw std::logic_error("operator not initialized");
    }

    if (joinType_ == JoinType::kRight) {
        while (true) {
            if (!currentRight_) {
                currentRight_ = right_->next();
                currentMatched_ = false;
                if (!currentRight_) {
                    return std::nullopt;
                }
                left_->reset();
                left_->init();
            }

            while (auto leftTuple = left_->next()) {
                Tuple combined = combineTuples(*leftTuple, *currentRight_);
                if (predicate_) {
                    ExprValue res = predicate_->evaluate(combined);
                    if (!res.asBool()) {
                        continue;
                    }
                }
                currentMatched_ = true;
                return combined;
            }

            if (!currentMatched_) {
                Tuple combined = combineWithNulls(true, *currentRight_);
                currentRight_.reset();
                return combined;
            }

            currentRight_.reset();
        }
    }

    while (true) {
        if (!currentLeft_) {
            currentLeft_ = left_->next();
            currentMatched_ = false;
            if (!currentLeft_) {
                return std::nullopt;
            }
            right_->reset();
            right_->init();
        }

        while (auto rightTuple = right_->next()) {
            Tuple combined = combineTuples(*currentLeft_, *rightTuple);
            if (predicate_) {
                ExprValue res = predicate_->evaluate(combined);
                if (!res.asBool()) {
                    continue;
                }
            }
            currentMatched_ = true;
            return combined;
        }

        if (!currentMatched_ && joinType_ == JoinType::kLeft) {
            Tuple combined = combineWithNulls(false, *currentLeft_);
            currentLeft_.reset();
            return combined;
        }

        currentLeft_.reset();
    }
}

void NestedLoopJoinOperator::close() {
    left_->close();
    right_->close();
    initialized_ = false;
    currentLeft_.reset();
    currentRight_.reset();
    currentMatched_ = false;
}

void NestedLoopJoinOperator::reset() {
    left_->reset();
    right_->reset();
    initialized_ = false;
    currentLeft_.reset();
    currentRight_.reset();
    currentMatched_ = false;
}

Tuple NestedLoopJoinOperator::combineTuples(const Tuple& left, const Tuple& right) const {
    Tuple combined;
    combined.values.reserve(left.values.size() + right.values.size());
    combined.values.insert(combined.values.end(), left.values.begin(), left.values.end());
    combined.values.insert(combined.values.end(), right.values.begin(), right.values.end());
    combined.schema = outputSchema_;
    return combined;
}

Tuple NestedLoopJoinOperator::combineWithNulls(bool nullLeft, const Tuple& other) const {
    Tuple combined;
    const std::size_t leftCount = left_->getSchema().columnCount();
    const std::size_t rightCount = right_->getSchema().columnCount();
    if (nullLeft) {
        combined.values.insert(combined.values.end(), leftCount, "NULL");
        combined.values.insert(combined.values.end(), other.values.begin(), other.values.end());
    } else {
        combined.values.insert(combined.values.end(), other.values.begin(), other.values.end());
        combined.values.insert(combined.values.end(), rightCount, "NULL");
    }
    combined.schema = outputSchema_;
    return combined;
}

HashJoinOperator::HashJoinOperator(std::unique_ptr<Operator> left,
                                   std::unique_ptr<Operator> right,
                                   std::string condition,
                                   std::string leftKey,
                                   std::string rightKey,
                                   JoinType joinType)
    : left_(std::move(left)),
      right_(std::move(right)),
      condition_(std::move(condition)),
      leftKey_(std::move(leftKey)),
      rightKey_(std::move(rightKey)),
      joinType_(joinType) {}

void HashJoinOperator::init() {
    if (initialized_) {
        return;
    }

    if (joinType_ != JoinType::kInner) {
        throw std::runtime_error("Hash join supports only inner joins");
    }

    // Build right hash table first
    right_->init();
    buildHashTable();
    right_->close();

    left_->init();

    outputSchema_ = std::make_shared<Schema>();
    const auto& leftSchema = left_->getSchema();
    const auto& rightSchema = right_->getSchema();
    for (const auto& col : leftSchema.columns()) {
        outputSchema_->addColumn(col);
    }
    for (const auto& col : rightSchema.columns()) {
        outputSchema_->addColumn(col);
    }

    if (!condition_.empty()) {
        ExpressionParser parser;
        predicate_ = parser.parse(condition_);
    }

    initialized_ = true;
}

std::optional<Tuple> HashJoinOperator::next() {
    if (!initialized_) {
        throw std::logic_error("operator not initialized");
    }

    while (true) {
        if (!currentLeft_) {
            currentLeft_ = left_->next();
            if (!currentLeft_) {
                return std::nullopt;
            }
            const std::string key = currentLeft_->getValue(leftKey_);
            auto it = hashTable_.find(key);
            if (it != hashTable_.end()) {
                currentMatches_ = &it->second;
                matchIndex_ = 0;
            } else {
                currentMatches_ = nullptr;
            }
        }

        if (currentMatches_) {
            while (matchIndex_ < currentMatches_->size()) {
                const Tuple& rightTuple = (*currentMatches_)[matchIndex_++];
                Tuple combined = combineTuples(*currentLeft_, rightTuple);
                if (predicate_) {
                    ExprValue res = predicate_->evaluate(combined);
                    if (!res.asBool()) {
                        continue;
                    }
                }
                return combined;
            }
        }

        currentLeft_.reset();
        currentMatches_ = nullptr;
    }
}

void HashJoinOperator::close() {
    left_->close();
    right_->close();
    initialized_ = false;
    currentLeft_.reset();
    currentMatches_ = nullptr;
    matchIndex_ = 0;
    hashTable_.clear();
}

void HashJoinOperator::reset() {
    left_->reset();
    right_->reset();
    initialized_ = false;
    currentLeft_.reset();
    currentMatches_ = nullptr;
    matchIndex_ = 0;
    hashTable_.clear();
}

void HashJoinOperator::buildHashTable() {
    hashTable_.clear();
    while (auto tuple = right_->next()) {
        const std::string key = tuple->getValue(rightKey_);
        hashTable_[key].push_back(*tuple);
    }
}

Tuple HashJoinOperator::combineTuples(const Tuple& left, const Tuple& right) const {
    Tuple combined;
    combined.values.reserve(left.values.size() + right.values.size());
    combined.values.insert(combined.values.end(), left.values.begin(), left.values.end());
    combined.values.insert(combined.values.end(), right.values.begin(), right.values.end());
    combined.schema = outputSchema_;
    return combined;
}

} // namespace dbms
