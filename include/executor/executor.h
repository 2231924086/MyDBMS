#pragma once

#include <memory>

#include "executor/operator.h"
#include "executor/result_set.h"
#include "parser/query_processor.h"
#include "system/database.h"

namespace dbms {

// Forward declarations
class Expression;

// Query executor - translates PhysicalPlanNode to Operator tree and executes it
class QueryExecutor {
public:
    explicit QueryExecutor(DatabaseSystem& db) : db_(db) {}

    // Execute physical plan and return results
    ResultSet execute(std::shared_ptr<PhysicalPlanNode> plan);

private:
    DatabaseSystem& db_;

    // Build operator tree from physical plan
    std::unique_ptr<Operator> buildOperatorTree(std::shared_ptr<PhysicalPlanNode> planNode);

    // Build specific operators
    std::unique_ptr<Operator> buildTableScan(std::shared_ptr<PhysicalPlanNode> planNode);
    std::unique_ptr<Operator> buildIndexScan(std::shared_ptr<PhysicalPlanNode> planNode);
    std::unique_ptr<Operator> buildFilter(
        std::shared_ptr<PhysicalPlanNode> planNode,
        std::unique_ptr<Operator> child);
    std::unique_ptr<Operator> buildProjection(
        std::shared_ptr<PhysicalPlanNode> planNode,
        std::unique_ptr<Operator> child);
    std::unique_ptr<Operator> buildDistinct(
        std::shared_ptr<PhysicalPlanNode> planNode,
        std::unique_ptr<Operator> child);
    std::unique_ptr<Operator> buildNestedLoopJoin(std::shared_ptr<PhysicalPlanNode> planNode);
    std::unique_ptr<Operator> buildHashJoin(std::shared_ptr<PhysicalPlanNode> planNode);
    std::unique_ptr<Operator> buildSort(
        std::shared_ptr<PhysicalPlanNode> planNode,
        std::unique_ptr<Operator> child);
    std::unique_ptr<Operator> buildAggregate(
        std::shared_ptr<PhysicalPlanNode> planNode,
        std::unique_ptr<Operator> child);

    // Helper: parse expression from string
    std::unique_ptr<Expression> parseExpression(const std::string& exprStr);
};

} // namespace dbms
