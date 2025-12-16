#include "executor/executor.h"

#include "executor/aggregate.h"
#include "executor/expression_parser.h"
#include "executor/filter.h"
#include "executor/index_scan.h"
#include "executor/join.h"
#include "executor/projection.h"
#include "executor/distinct.h"
#include "executor/limit.h"
#include "executor/alias.h"
#include "executor/sort.h"
#include "executor/table_scan.h"

#include <algorithm>
#include <cctype>
#include <sstream>
#include <stdexcept>

namespace {

std::string trim(const std::string& input) {
    std::size_t start = 0;
    while (start < input.size() && std::isspace(static_cast<unsigned char>(input[start]))) {
        ++start;
    }
    std::size_t end = input.size();
    while (end > start && std::isspace(static_cast<unsigned char>(input[end - 1]))) {
        --end;
    }
    return input.substr(start, end - start);
}

std::vector<std::string> split(const std::string& input, char delimiter) {
    std::vector<std::string> parts;
    std::stringstream ss(input);
    std::string item;
    while (std::getline(ss, item, delimiter)) {
        parts.push_back(item);
    }
    return parts;
}

std::string toUpper(const std::string& input) {
    std::string out = input;
    std::transform(out.begin(), out.end(), out.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return out;
}

bool iequals(const std::string& a, const std::string& b) {
    return toUpper(a) == toUpper(b);
}

dbms::JoinType parseJoinType(const std::string& input) {
    std::string upper = toUpper(trim(input));
    if (upper == "LEFT") return dbms::JoinType::kLeft;
    if (upper == "RIGHT") return dbms::JoinType::kRight;
    return dbms::JoinType::kInner;
}

std::vector<dbms::SortKey> parseSortKeys(const std::string& input) {
    std::vector<dbms::SortKey> keys;
    for (auto part : split(input, ',')) {
        part = trim(part);
        if (part.empty()) continue;

        std::string column = part;
        std::string direction;

        auto colon = part.find(':');
        if (colon != std::string::npos) {
            column = trim(part.substr(0, colon));
            direction = trim(part.substr(colon + 1));
        } else {
            auto space = part.find_last_of(' ');
            if (space != std::string::npos) {
                direction = trim(part.substr(space + 1));
                column = trim(part.substr(0, space));
            }
        }

        bool ascending = true;
        if (!direction.empty()) {
            ascending = !iequals(direction, "DESC") && !iequals(direction, "DESCENDING");
        }

        if (!column.empty()) {
            keys.push_back({column, ascending});
        }
    }
    return keys;
}

dbms::AggregateOperator::AggFunc parseAggFunc(const std::string& func) {
    const std::string upper = toUpper(trim(func));
    if (upper == "SUM") return dbms::AggregateOperator::AggFunc::SUM;
    if (upper == "COUNT") return dbms::AggregateOperator::AggFunc::COUNT;
    if (upper == "AVG") return dbms::AggregateOperator::AggFunc::AVG;
    if (upper == "MIN") return dbms::AggregateOperator::AggFunc::MIN;
    if (upper == "MAX") return dbms::AggregateOperator::AggFunc::MAX;
    if (upper == "STDDEV" || upper == "STDDEV_POP" || upper == "STDDEV_SAMP") {
        return dbms::AggregateOperator::AggFunc::STDDEV;
    }
    if (upper == "VARIANCE" || upper == "VAR" || upper == "VAR_POP" || upper == "VAR_SAMP") {
        return dbms::AggregateOperator::AggFunc::VARIANCE;
    }
    throw std::runtime_error("unknown aggregate function: " + func);
}

std::size_t findCaseInsensitive(const std::string& input, const std::string& patternUpper) {
    std::string upperInput = toUpper(input);
    return upperInput.find(patternUpper);
}

dbms::AggregateOperator::AggregateSpec parseAggregateToken(const std::string& token) {
    using AggFunc = dbms::AggregateOperator::AggFunc;
    dbms::AggregateOperator::AggregateSpec spec{};

    std::string work = trim(token);
    if (work.empty()) {
        throw std::runtime_error("empty aggregate token");
    }

    std::string funcPart;
    std::string exprPart;
    std::string aliasPart;

    const auto firstColon = work.find(':');
    if (firstColon != std::string::npos) {
        funcPart = trim(work.substr(0, firstColon));
        auto secondColon = work.find(':', firstColon + 1);
        if (secondColon != std::string::npos) {
            exprPart = trim(work.substr(firstColon + 1, secondColon - firstColon - 1));
            aliasPart = trim(work.substr(secondColon + 1));
        } else {
            exprPart = trim(work.substr(firstColon + 1));
        }
    } else {
        // SQL-like: FUNC(expr) [AS alias] or FUNC expr
        auto asPos = findCaseInsensitive(work, " AS ");
        if (asPos != std::string::npos) {
            aliasPart = trim(work.substr(asPos + 4));
            work = trim(work.substr(0, asPos));
        } else {
            auto eqPos = work.find('=');
            if (eqPos != std::string::npos) {
                aliasPart = trim(work.substr(eqPos + 1));
                work = trim(work.substr(0, eqPos));
            }
        }

        auto lp = work.find('(');
        auto rp = work.rfind(')');
        if (lp != std::string::npos && rp != std::string::npos && rp > lp) {
            funcPart = trim(work.substr(0, lp));
            exprPart = trim(work.substr(lp + 1, rp - lp - 1));
        } else {
            // Fallback: split by whitespace
            auto space = work.find(' ');
            if (space != std::string::npos) {
                funcPart = trim(work.substr(0, space));
                exprPart = trim(work.substr(space + 1));
            } else {
                funcPart = work;
                exprPart = "*";
            }
        }
    }

    spec.func = parseAggFunc(funcPart);
    spec.expression = exprPart;
    if (spec.expression.empty() && spec.func == AggFunc::COUNT) {
        spec.expression = "*";
    }
    if (aliasPart.empty()) {
        const std::string exprLabel = spec.expression.empty() ? "*" : spec.expression;
        aliasPart = toUpper(funcPart) + "(" + exprLabel + ")";
    }
    spec.alias = aliasPart;
    return spec;
}

std::vector<dbms::AggregateOperator::AggregateSpec> parseAggregateList(const std::string& input) {
    std::string normalized = input;
    std::replace(normalized.begin(), normalized.end(), ';', ',');

    std::vector<dbms::AggregateOperator::AggregateSpec> specs;
    for (auto part : split(normalized, ',')) {
        part = trim(part);
        if (part.empty()) continue;
        specs.push_back(parseAggregateToken(part));
    }
    return specs;
}

std::vector<std::string> parseGroupByList(const std::string& input) {
    std::vector<std::string> groups;
    for (auto part : split(input, ',')) {
        part = trim(part);
        if (!part.empty()) {
            groups.push_back(part);
        }
    }
    return groups;
}

} // namespace

namespace dbms {

ResultSet QueryExecutor::execute(std::shared_ptr<PhysicalPlanNode> plan) {
    if (!plan) {
        throw std::runtime_error("null physical plan");
    }

    // Build operator tree
    auto root = buildOperatorTree(plan);

    // Initialize operator
    root->init();

    // Collect all tuples
    ResultSet results(std::make_shared<Schema>(root->getSchema()));
    while (auto tuple = root->next()) {
        results.addTuple(std::move(*tuple));
    }

    // Cleanup
    root->close();

    return results;
}

std::unique_ptr<Operator> QueryExecutor::buildOperatorTree(std::shared_ptr<PhysicalPlanNode> planNode) {
    if (!planNode) {
        throw std::runtime_error("null plan node");
    }

    switch (planNode->opType) {
        case PhysicalOpType::kTableScan:
            return buildTableScan(planNode);

        case PhysicalOpType::kIndexScan:
            return buildIndexScan(planNode);

        case PhysicalOpType::kFilter:
            if (planNode->children.empty()) {
                throw std::runtime_error("FILTER node has no child");
            }
            return buildFilter(planNode, buildOperatorTree(planNode->children[0]));

        case PhysicalOpType::kProjection:
            if (planNode->children.empty()) {
                throw std::runtime_error("PROJECTION node has no child");
            }
            return buildProjection(planNode, buildOperatorTree(planNode->children[0]));

        case PhysicalOpType::kDistinct:
            if (planNode->children.empty()) {
                throw std::runtime_error("DISTINCT node has no child");
            }
            return buildDistinct(planNode, buildOperatorTree(planNode->children[0]));

        case PhysicalOpType::kNestedLoopJoin:
            return buildNestedLoopJoin(planNode);

        case PhysicalOpType::kHashJoin:
            return buildHashJoin(planNode);

        case PhysicalOpType::kSort:
            if (planNode->children.empty()) {
                throw std::runtime_error("SORT node has no child");
            }
            return buildSort(planNode, buildOperatorTree(planNode->children[0]));

        case PhysicalOpType::kAggregate:
            if (planNode->children.empty()) {
                throw std::runtime_error("AGGREGATE node has no child");
            }
            return buildAggregate(planNode, buildOperatorTree(planNode->children[0]));
        case PhysicalOpType::kLimit:
            if (planNode->children.empty()) {
                throw std::runtime_error("LIMIT node has no child");
            }
            return buildLimit(planNode, buildOperatorTree(planNode->children[0]));
        case PhysicalOpType::kAlias:
            if (planNode->children.empty()) {
                throw std::runtime_error("ALIAS node has no child");
            }
            return buildAlias(planNode, buildOperatorTree(planNode->children[0]));

        default:
            throw std::runtime_error("unsupported physical operator type");
    }
}

std::unique_ptr<Operator> QueryExecutor::buildTableScan(std::shared_ptr<PhysicalPlanNode> planNode) {
    // Extract table name from parameters
    auto it = planNode->parameters.find("table");
    if (it == planNode->parameters.end()) {
        throw std::runtime_error("TABLE_SCAN node missing 'table' parameter");
    }

    std::string tableName = it->second;
    return std::make_unique<TableScanOperator>(db_, tableName);
}

std::unique_ptr<Operator> QueryExecutor::buildIndexScan(std::shared_ptr<PhysicalPlanNode> planNode) {
    auto tableIt = planNode->parameters.find("table");
    auto indexIt = planNode->parameters.find("index");
    auto keyIt = planNode->parameters.find("key");
    if (tableIt == planNode->parameters.end() ||
        indexIt == planNode->parameters.end() ||
        keyIt == planNode->parameters.end()) {
        throw std::runtime_error("INDEX_SCAN node missing required parameters");
    }
    return std::make_unique<IndexScanOperator>(db_,
                                               tableIt->second,
                                               indexIt->second,
                                               keyIt->second);
}

std::unique_ptr<Operator> QueryExecutor::buildFilter(
    std::shared_ptr<PhysicalPlanNode> planNode,
    std::unique_ptr<Operator> child) {
    // Extract condition from parameters
    auto it = planNode->parameters.find("condition");
    if (it == planNode->parameters.end()) {
        throw std::runtime_error("FILTER node missing 'condition' parameter");
    }

    // Parse the condition string into an Expression tree
    auto predicate = parseExpression(it->second);

    return std::make_unique<FilterOperator>(std::move(child), std::move(predicate));
}

std::unique_ptr<Operator> QueryExecutor::buildProjection(
    std::shared_ptr<PhysicalPlanNode> planNode,
    std::unique_ptr<Operator> child) {
    // Extract output columns from planNode
    if (planNode->outputColumns.empty()) {
        throw std::runtime_error("PROJECTION node has no output columns");
    }

    return std::make_unique<ProjectionOperator>(std::move(child), planNode->outputColumns);
}

std::unique_ptr<Operator> QueryExecutor::buildDistinct(
    std::shared_ptr<PhysicalPlanNode> planNode,
    std::unique_ptr<Operator> child) {
    (void)planNode;
    return std::make_unique<DistinctOperator>(std::move(child));
}

std::unique_ptr<Operator> QueryExecutor::buildNestedLoopJoin(std::shared_ptr<PhysicalPlanNode> planNode) {
    if (planNode->children.size() < 2) {
        throw std::runtime_error("NESTED_LOOP_JOIN requires two children");
    }
    auto left = buildOperatorTree(planNode->children[0]);
    auto right = buildOperatorTree(planNode->children[1]);
    std::string condition;
    auto it = planNode->parameters.find("condition");
    if (it != planNode->parameters.end()) {
        condition = it->second;
    }
    auto jtIt = planNode->parameters.find("join_type");
    JoinType joinType = planNode->joinType;
    if (jtIt != planNode->parameters.end()) {
        joinType = parseJoinType(jtIt->second);
    }
    return std::make_unique<NestedLoopJoinOperator>(std::move(left),
                                                    std::move(right),
                                                    condition,
                                                    joinType);
}

std::unique_ptr<Operator> QueryExecutor::buildHashJoin(std::shared_ptr<PhysicalPlanNode> planNode) {
    if (planNode->children.size() < 2) {
        throw std::runtime_error("HASH_JOIN requires two children");
    }
    auto left = buildOperatorTree(planNode->children[0]);
    auto right = buildOperatorTree(planNode->children[1]);
    std::string condition;
    auto condIt = planNode->parameters.find("condition");
    if (condIt != planNode->parameters.end()) {
        condition = condIt->second;
    }
    auto leftKeyIt = planNode->parameters.find("left_key");
    auto rightKeyIt = planNode->parameters.find("right_key");
    if (leftKeyIt == planNode->parameters.end() ||
        rightKeyIt == planNode->parameters.end()) {
        throw std::runtime_error("HASH_JOIN missing join key parameters");
    }
    auto jtIt = planNode->parameters.find("join_type");
    JoinType joinType = planNode->joinType;
    if (jtIt != planNode->parameters.end()) {
        joinType = parseJoinType(jtIt->second);
    }
    return std::make_unique<HashJoinOperator>(std::move(left),
                                              std::move(right),
                                              condition,
                                              leftKeyIt->second,
                                              rightKeyIt->second,
                                              joinType);
}

std::unique_ptr<Operator> QueryExecutor::buildSort(
    std::shared_ptr<PhysicalPlanNode> planNode,
    std::unique_ptr<Operator> child) {
    std::vector<SortKey> keys;
    auto paramIt = planNode->parameters.find("order_by");
    if (paramIt == planNode->parameters.end()) {
        paramIt = planNode->parameters.find("sort_keys");
    }
    if (paramIt == planNode->parameters.end()) {
        paramIt = planNode->parameters.find("keys");
    }
    if (paramIt != planNode->parameters.end()) {
        keys = parseSortKeys(paramIt->second);
    }

    if (keys.empty() && !planNode->outputColumns.empty()) {
        for (const auto& col : planNode->outputColumns) {
            keys.push_back({col, true});
        }
    }

    if (keys.empty()) {
        throw std::runtime_error("SORT node missing sort keys");
    }

    return std::make_unique<SortOperator>(std::move(child), std::move(keys));
}

std::unique_ptr<Operator> QueryExecutor::buildAggregate(
    std::shared_ptr<PhysicalPlanNode> planNode,
    std::unique_ptr<Operator> child) {
    std::vector<std::string> groupBy;
    std::vector<AggregateOperator::AggregateSpec> aggregates;
    std::string havingClause;

    auto groupIt = planNode->parameters.find("group_by");
    if (groupIt == planNode->parameters.end()) {
        groupIt = planNode->parameters.find("groupby");
    }
    if (groupIt == planNode->parameters.end()) {
        groupIt = planNode->parameters.find("group");
    }
    if (groupIt != planNode->parameters.end()) {
        groupBy = parseGroupByList(groupIt->second);
    }

    auto aggIt = planNode->parameters.find("aggregates");
    if (aggIt == planNode->parameters.end()) {
        aggIt = planNode->parameters.find("aggs");
    }
    if (aggIt == planNode->parameters.end()) {
        aggIt = planNode->parameters.find("agg");
    }
    if (aggIt != planNode->parameters.end()) {
        aggregates = parseAggregateList(aggIt->second);
    }

    // Allow individual aggregate entries as parameters prefixed with "agg."
    for (const auto& kv : planNode->parameters) {
        if (kv.first.rfind("agg.", 0) == 0) {
            aggregates.push_back(parseAggregateToken(kv.second));
        }
    }

    auto havingIt = planNode->parameters.find("having");
    if (havingIt != planNode->parameters.end()) {
        havingClause = havingIt->second;
    }

    if (groupBy.empty() && !planNode->outputColumns.empty()) {
        std::size_t groupCount = planNode->outputColumns.size();
        if (!aggregates.empty() && groupCount >= aggregates.size()) {
            groupCount -= aggregates.size();
        }
        groupBy.assign(planNode->outputColumns.begin(),
                       planNode->outputColumns.begin() + groupCount);
    }

    // If outputColumns matches group + aggregate count, use them as aliases
    if (!planNode->outputColumns.empty() &&
        planNode->outputColumns.size() == groupBy.size() + aggregates.size()) {
        for (std::size_t i = 0; i < aggregates.size(); ++i) {
            aggregates[i].alias = planNode->outputColumns[groupBy.size() + i];
        }
    }

    return std::make_unique<AggregateOperator>(std::move(child),
                                               std::move(groupBy),
                                               std::move(aggregates),
                                               havingClause);
}

std::unique_ptr<Operator> QueryExecutor::buildLimit(
    std::shared_ptr<PhysicalPlanNode> planNode,
    std::unique_ptr<Operator> child) {
    std::size_t limit = 0;
    std::size_t offset = 0;
    auto itLimit = planNode->parameters.find("limit");
    if (itLimit != planNode->parameters.end()) {
        limit = static_cast<std::size_t>(std::stoull(itLimit->second));
    }
    auto itOffset = planNode->parameters.find("offset");
    if (itOffset != planNode->parameters.end()) {
        offset = static_cast<std::size_t>(std::stoull(itOffset->second));
    }
    return std::make_unique<LimitOperator>(std::move(child), limit, offset);
}

std::unique_ptr<Operator> QueryExecutor::buildAlias(
    std::shared_ptr<PhysicalPlanNode> planNode,
    std::unique_ptr<Operator> child) {
    auto it = planNode->parameters.find("alias");
    std::string alias = (it != planNode->parameters.end()) ? it->second : "";
    return std::make_unique<AliasOperator>(std::move(child), alias);
}

std::unique_ptr<Expression> QueryExecutor::parseExpression(const std::string& exprStr) {
    ExpressionParser parser;
    return parser.parse(exprStr);
}

} // namespace dbms
