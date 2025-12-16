#include "executor/limit.h"

#include <stdexcept>

namespace dbms {

LimitOperator::LimitOperator(std::unique_ptr<Operator> child,
                             std::size_t limit,
                             std::size_t offset)
    : child_(std::move(child)),
      limit_(limit),
      offset_(offset) {}

void LimitOperator::init() {
    if (initialized_) {
        return;
    }
    if (!child_) {
        throw std::runtime_error("limit operator missing child");
    }
    child_->init();
    schema_ = std::make_shared<Schema>(child_->getSchema());

    // Skip offset rows eagerly
    while (skipped_ < offset_) {
        auto row = child_->next();
        if (!row) {
            break;
        }
        ++skipped_;
    }
    initialized_ = true;
}

std::optional<Tuple> LimitOperator::next() {
    if (!initialized_) {
        throw std::logic_error("operator not initialized");
    }
    if (limit_ != 0 && produced_ >= limit_) {
        return std::nullopt;
    }

    auto tuple = child_->next();
    if (!tuple) {
        return std::nullopt;
    }
    tuple->schema = schema_;
    ++produced_;
    return tuple;
}

void LimitOperator::close() {
    if (child_) {
        child_->close();
    }
    produced_ = 0;
    skipped_ = 0;
    initialized_ = false;
}

void LimitOperator::reset() {
    if (child_) {
        child_->reset();
    }
    produced_ = 0;
    skipped_ = 0;
    initialized_ = false;
}

} // namespace dbms

