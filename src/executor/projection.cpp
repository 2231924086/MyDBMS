#include "executor/projection.h"

#include <stdexcept>

namespace dbms {

ProjectionOperator::ProjectionOperator(
    std::unique_ptr<Operator> child,
    std::vector<std::string> columnNames)
    : child_(std::move(child)),
      columnNames_(std::move(columnNames)),
      initialized_(false) {}

void ProjectionOperator::init() {
    if (!initialized_) {
        child_->init();
        resolveColumnIndices();
        initialized_ = true;
    }
}

std::optional<Tuple> ProjectionOperator::next() {
    if (!initialized_) {
        throw std::logic_error("operator not initialized");
    }

    auto childTuple = child_->next();
    if (!childTuple) {
        return std::nullopt;
    }

    // Create projected tuple
    Tuple projectedTuple;
    projectedTuple.values.reserve(columnIndices_.size());

    for (std::size_t idx : columnIndices_) {
        if (idx >= childTuple->values.size()) {
            throw std::runtime_error("column index out of range during projection");
        }
        projectedTuple.values.push_back(childTuple->values[idx]);
    }

    projectedTuple.schema = std::make_shared<Schema>(outputSchema_);
    return projectedTuple;
}

void ProjectionOperator::close() {
    if (initialized_) {
        child_->close();
        initialized_ = false;
    }
}

void ProjectionOperator::reset() {
    child_->reset();
    initialized_ = false;
}

void ProjectionOperator::resolveColumnIndices() {
    const Schema& childSchema = child_->getSchema();

    columnIndices_.clear();
    outputSchema_ = Schema();

    for (const auto& colName : columnNames_) {
        auto idx = childSchema.findColumn(colName);
        if (!idx) {
            throw std::runtime_error("column not found in child schema: " + colName);
        }

        columnIndices_.push_back(*idx);
        outputSchema_.addColumn(childSchema.getColumn(*idx));
    }
}

} // namespace dbms
