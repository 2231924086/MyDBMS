#pragma once

#include <memory>
#include <string>

#include "executor/operator.h"

namespace dbms {

class AliasOperator : public Operator {
public:
    AliasOperator(std::unique_ptr<Operator> child, std::string alias);

    void init() override;
    std::optional<Tuple> next() override;
    void close() override;
    const Schema& getSchema() const override { return *schema_; }
    void reset() override;

private:
    std::unique_ptr<Operator> child_;
    std::string alias_;
    std::shared_ptr<Schema> schema_;
    bool initialized_{false};
};

} // namespace dbms

