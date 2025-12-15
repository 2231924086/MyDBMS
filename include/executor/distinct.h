#pragma once

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "executor/operator.h"

namespace dbms {

// Distinct operator - removes duplicate tuples while preserving first occurrence order
class DistinctOperator : public Operator {
public:
    explicit DistinctOperator(std::unique_ptr<Operator> child);

    void init() override;
    std::optional<Tuple> next() override;
    void close() override;
    const Schema& getSchema() const override { return *schema_; }
    void reset() override;

private:
    std::unique_ptr<Operator> child_;
    std::shared_ptr<Schema> schema_;
    std::vector<Tuple> uniqueTuples_;
    std::unordered_set<std::string> seen_;
    std::size_t index_{0};
    bool initialized_{false};

    std::string makeKey(const Tuple& tuple) const;
};

} // namespace dbms
