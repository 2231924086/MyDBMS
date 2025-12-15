#include "executor/filter.h"
#include <stdexcept>

namespace dbms {

void FilterOperator::init() {
    if (!initialized_) {
        child_->init();
        initialized_ = true;
    }
}

std::optional<Tuple> FilterOperator::next() {
    if (!initialized_) {
        throw std::logic_error("operator not initialized");
    }

    // Loop through child tuples until we find one that satisfies the predicate
    while (auto tuple = child_->next()) {
        ExprValue result = predicate_->evaluate(*tuple);
        if (result.asBool()) {
            return tuple;
        }
    }

    // No more tuples satisfy the predicate
    return std::nullopt;
}

void FilterOperator::close() {
    if (initialized_) {
        child_->close();
        initialized_ = false;
    }
}

const Schema& FilterOperator::getSchema() const {
    return child_->getSchema();
}

void FilterOperator::reset() {
    child_->reset();
    initialized_ = false;
}

} // namespace dbms
