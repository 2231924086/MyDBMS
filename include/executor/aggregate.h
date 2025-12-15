#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "executor/expression.h"
#include "executor/operator.h"

namespace dbms {

class AggregateOperator : public Operator {
public:
    enum class AggFunc { SUM, COUNT, AVG, MIN, MAX };

    struct AggregateSpec {
        AggFunc func;
        std::string expression; // Column or expression; "*" allowed for COUNT
        std::string alias;
    };

    AggregateOperator(std::unique_ptr<Operator> child,
                      std::vector<std::string> groupByColumns,
                      std::vector<AggregateSpec> aggregates,
                      std::string havingClause = "");

    void init() override;
    std::optional<Tuple> next() override;
    void close() override;
    const Schema& getSchema() const override { return *outputSchema_; }
    void reset() override;

private:
    struct PreparedAggregate {
        AggFunc func;
        std::string expression;
        std::string alias;
        std::unique_ptr<Expression> exprNode; // Null for COUNT(*)
        ColumnType resultType{ColumnType::Integer};
    };

    struct AggregateAccumulator {
        int64_t intSum{0};
        double doubleSum{0.0};
        int64_t count{0};
        ExprValue extreme;
        bool hasValue{false};
    };

    struct GroupKeyHash {
        std::size_t operator()(const std::vector<std::string>& key) const noexcept;
    };

    struct GroupKeyEqual {
        bool operator()(const std::vector<std::string>& a,
                        const std::vector<std::string>& b) const noexcept;
    };

    std::unique_ptr<Operator> child_;
    std::vector<std::string> groupByColumns_;
    std::vector<std::size_t> groupByIndices_;
    std::vector<PreparedAggregate> aggregates_;
    std::string havingClause_;
    std::unique_ptr<Expression> havingExpr_;
    std::shared_ptr<Schema> outputSchema_;
    std::vector<Tuple> results_;
    std::size_t resultIndex_{0};
    bool initialized_{false};

    void resolveGroupColumns(const Schema& childSchema);
    void prepareAggregates(const Schema& childSchema);
    void accumulateTuple(const Tuple& tuple,
                         std::unordered_map<std::vector<std::string>,
                                            std::vector<AggregateAccumulator>,
                                            GroupKeyHash,
                                            GroupKeyEqual>& groups);
    std::vector<std::string> buildGroupKey(const Tuple& tuple) const;
    Tuple buildOutputTuple(const std::vector<std::string>& key,
                           const std::vector<AggregateAccumulator>& accs) const;
    void buildOutputSchema(const Schema& childSchema);
    static ColumnType inferExpressionType(const Expression& expr, const Schema& schema);
    static std::string funcName(AggFunc func);
};

} // namespace dbms

