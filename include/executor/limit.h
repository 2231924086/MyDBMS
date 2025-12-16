#pragma once

#include <memory>

#include "executor/operator.h"

namespace dbms {

class LimitOperator : public Operator {
public:
    LimitOperator(std::unique_ptr<Operator> child, std::size_t limit, std::size_t offset);

    void init() override;
    std::optional<Tuple> next() override;
    void close() override;
    const Schema& getSchema() const override { return *schema_; }
    void reset() override;

private:
    std::unique_ptr<Operator> child_;
    std::size_t limit_;
    std::size_t offset_;
    std::size_t produced_{0};
    std::size_t skipped_{0};
    bool initialized_{false};
    std::shared_ptr<Schema> schema_;
};

} // namespace dbms

