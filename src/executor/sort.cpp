#include "executor/sort.h"

#include <algorithm>
#include <stdexcept>

namespace dbms {

SortOperator::SortOperator(std::unique_ptr<Operator> child, std::vector<SortKey> keys)
    : child_(std::move(child)), keys_(std::move(keys)) {}

void SortOperator::init() {
    if (initialized_) {
        return;
    }
    if (!child_) {
        throw std::runtime_error("sort operator missing child");
    }

    child_->init();
    schema_ = std::make_shared<Schema>(child_->getSchema());
    resolveKeyIndices();

    sortedTuples_.clear();
    while (auto tuple = child_->next()) {
        tuple->schema = schema_;
        sortedTuples_.push_back(std::move(*tuple));
    }

    auto comparator = [this](const Tuple& a, const Tuple& b) {
        for (std::size_t i = 0; i < keyIndices_.size(); ++i) {
            const auto idx = keyIndices_[i];
            ExprValue left = makeTypedValue(a, idx);
            ExprValue right = makeTypedValue(b, idx);
            int cmp = left.compare(right);
            if (cmp == 0) {
                continue;
            }
            return keys_[i].ascending ? (cmp < 0) : (cmp > 0);
        }
        return false;
    };

    std::sort(sortedTuples_.begin(), sortedTuples_.end(), comparator);
    currentIndex_ = 0;
    initialized_ = true;
}

std::optional<Tuple> SortOperator::next() {
    if (!initialized_) {
        throw std::logic_error("operator not initialized");
    }

    if (currentIndex_ >= sortedTuples_.size()) {
        return std::nullopt;
    }

    return sortedTuples_[currentIndex_++];
}

void SortOperator::close() {
    if (child_) {
        child_->close();
    }
    sortedTuples_.clear();
    initialized_ = false;
    currentIndex_ = 0;
}

void SortOperator::reset() {
    if (child_) {
        child_->reset();
    }
    sortedTuples_.clear();
    initialized_ = false;
    currentIndex_ = 0;
}

void SortOperator::resolveKeyIndices() {
    if (keys_.empty()) {
        // Default to sorting by all columns if no keys specified
        for (std::size_t i = 0; i < schema_->columnCount(); ++i) {
            keys_.push_back({schema_->getColumn(i).name, true});
        }
    }

    keyIndices_.clear();
    for (const auto& key : keys_) {
        auto idx = schema_->findColumn(key.column);
        if (!idx) {
            throw std::runtime_error("sort key not found in schema: " + key.column);
        }
        keyIndices_.push_back(*idx);
    }
}

ExprValue SortOperator::makeTypedValue(const Tuple& tuple, std::size_t index) {
    if (!tuple.schema) {
        throw std::runtime_error("tuple missing schema for sorting");
    }
    const auto& col = tuple.schema->getColumn(index);
    ExprValue value;
    value.stringValue = tuple.getValue(index);
    switch (col.type) {
        case ColumnType::Integer:
            value.type = ExprValue::Type::INTEGER;
            break;
        case ColumnType::Double:
            value.type = ExprValue::Type::DOUBLE;
            break;
        case ColumnType::String:
            value.type = ExprValue::Type::STRING;
            break;
    }
    return value;
}

} // namespace dbms

