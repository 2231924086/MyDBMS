#pragma once

#include <memory>

#include "executor/expression.h"
#include "executor/operator.h"

namespace dbms {

// Filter operator - evaluates a predicate on each tuple from child
class FilterOperator : public Operator {
public:
    FilterOperator(std::unique_ptr<Operator> child, std::unique_ptr<Expression> predicate)
        : child_(std::move(child)), predicate_(std::move(predicate)), initialized_(false) {}

    void init() override;
    std::optional<Tuple> next() override;
    void close() override;
    const Schema& getSchema() const override;
    void reset() override;

private:
    std::unique_ptr<Operator> child_;
    std::unique_ptr<Expression> predicate_;
    bool initialized_;
};

} // namespace dbms
