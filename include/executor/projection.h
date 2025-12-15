#pragma once

#include <memory>
#include <vector>

#include "executor/operator.h"

namespace dbms {

// Projection operator - selects specific columns from child tuples
class ProjectionOperator : public Operator {
public:
    ProjectionOperator(
        std::unique_ptr<Operator> child,
        std::vector<std::string> columnNames);

    void init() override;
    std::optional<Tuple> next() override;
    void close() override;
    const Schema& getSchema() const override { return outputSchema_; }
    void reset() override;

private:
    std::unique_ptr<Operator> child_;
    std::vector<std::string> columnNames_;
    Schema outputSchema_;
    std::vector<std::size_t> columnIndices_;  // Resolved indices
    bool initialized_;

    void resolveColumnIndices();
};

} // namespace dbms
