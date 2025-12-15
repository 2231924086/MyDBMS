#pragma once

#include <memory>
#include <string>
#include <vector>

#include "executor/expression.h"
#include "executor/operator.h"

namespace dbms {

struct SortKey {
    std::string column;
    bool ascending{true};
};

// Sort operator - materializes child tuples and orders them by given keys
class SortOperator : public Operator {
public:
    SortOperator(std::unique_ptr<Operator> child, std::vector<SortKey> keys);

    void init() override;
    std::optional<Tuple> next() override;
    void close() override;
    const Schema& getSchema() const override { return *schema_; }
    void reset() override;

private:
    std::unique_ptr<Operator> child_;
    std::vector<SortKey> keys_;
    std::vector<std::size_t> keyIndices_;
    std::shared_ptr<Schema> schema_;
    std::vector<Tuple> sortedTuples_;
    std::size_t currentIndex_{0};
    bool initialized_{false};

    void resolveKeyIndices();
    static ExprValue makeTypedValue(const Tuple& tuple, std::size_t index);
};

} // namespace dbms

