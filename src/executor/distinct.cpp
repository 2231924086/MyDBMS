#include "executor/distinct.h"

#include <stdexcept>

namespace dbms {

DistinctOperator::DistinctOperator(std::unique_ptr<Operator> child)
    : child_(std::move(child)) {}

void DistinctOperator::init() {
    if (initialized_) {
        return;
    }
    if (!child_) {
        throw std::runtime_error("distinct operator missing child");
    }

    child_->init();
    schema_ = std::make_shared<Schema>(child_->getSchema());
    uniqueTuples_.clear();
    seen_.clear();

    while (auto tuple = child_->next()) {
        tuple->schema = schema_;
        const std::string key = makeKey(*tuple);
        if (seen_.insert(key).second) {
            uniqueTuples_.push_back(std::move(*tuple));
        }
    }

    index_ = 0;
    initialized_ = true;
}

std::optional<Tuple> DistinctOperator::next() {
    if (!initialized_) {
        throw std::logic_error("operator not initialized");
    }

    if (index_ >= uniqueTuples_.size()) {
        return std::nullopt;
    }

    return uniqueTuples_[index_++];
}

void DistinctOperator::close() {
    if (child_) {
        child_->close();
    }
    uniqueTuples_.clear();
    seen_.clear();
    index_ = 0;
    initialized_ = false;
}

void DistinctOperator::reset() {
    if (child_) {
        child_->reset();
    }
    uniqueTuples_.clear();
    seen_.clear();
    index_ = 0;
    initialized_ = false;
}

std::string DistinctOperator::makeKey(const Tuple& tuple) const {
    std::string key;
    key.reserve(tuple.values.size() * 4);
    for (std::size_t i = 0; i < tuple.values.size(); ++i) {
        if (i > 0) {
            key.push_back('\x1f');
        }
        key.append(tuple.values[i]);
    }
    return key;
}

} // namespace dbms
