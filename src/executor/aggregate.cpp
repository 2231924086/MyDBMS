#include "executor/aggregate.h"

#include <algorithm>
#include <cctype>
#include <stdexcept>
#include <unordered_map>

#include "executor/expression_parser.h"

namespace dbms {
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

} // namespace

AggregateOperator::AggregateOperator(std::unique_ptr<Operator> child,
                                     std::vector<std::string> groupByColumns,
                                     std::vector<AggregateSpec> aggregates,
                                     std::string havingClause)
    : child_(std::move(child)),
      groupByColumns_(std::move(groupByColumns)),
      havingClause_(trim(havingClause)) {
    aggregates_.reserve(aggregates.size());
    for (auto& spec : aggregates) {
        PreparedAggregate prepared;
        prepared.func = spec.func;
        prepared.expression = trim(spec.expression);
        prepared.alias = trim(spec.alias);
        aggregates_.push_back(std::move(prepared));
    }
}

void AggregateOperator::init() {
    if (initialized_) {
        return;
    }
    if (!child_) {
        throw std::runtime_error("aggregate operator missing child");
    }

    child_->init();
    const Schema& childSchema = child_->getSchema();

    resolveGroupColumns(childSchema);
    prepareAggregates(childSchema);
    buildOutputSchema(childSchema);

    // Parse HAVING after output schema is ready (names/aliases resolved)
    if (!havingClause_.empty()) {
        ExpressionParser parser;
        havingExpr_ = parser.parse(havingClause_);
    }

    std::unordered_map<std::vector<std::string>,
                       std::vector<AggregateAccumulator>,
                       GroupKeyHash,
                       GroupKeyEqual> groups;

    while (auto tuple = child_->next()) {
        accumulateTuple(*tuple, groups);
    }

    // Ensure at least one result for global aggregates even if input is empty
    if (groupByColumns_.empty() && groups.empty()) {
        groups[{}] = std::vector<AggregateAccumulator>(aggregates_.size());
    }

    for (auto& entry : groups) {
        Tuple tuple = buildOutputTuple(entry.first, entry.second);
        tuple.schema = outputSchema_;
        if (havingExpr_) {
            ExprValue res = havingExpr_->evaluate(tuple);
            if (!res.asBool()) {
                continue;
            }
        }
        results_.push_back(std::move(tuple));
    }

    resultIndex_ = 0;
    initialized_ = true;
}

std::optional<Tuple> AggregateOperator::next() {
    if (!initialized_) {
        throw std::logic_error("operator not initialized");
    }

    if (resultIndex_ >= results_.size()) {
        return std::nullopt;
    }

    return results_[resultIndex_++];
}

void AggregateOperator::close() {
    if (child_) {
        child_->close();
    }
    results_.clear();
    initialized_ = false;
    resultIndex_ = 0;
}

void AggregateOperator::reset() {
    if (child_) {
        child_->reset();
    }
    results_.clear();
    initialized_ = false;
    resultIndex_ = 0;
}

void AggregateOperator::resolveGroupColumns(const Schema& childSchema) {
    groupByIndices_.clear();
    for (const auto& name : groupByColumns_) {
        auto idx = childSchema.findColumn(name);
        if (!idx) {
            throw std::runtime_error("group by column not found: " + name);
        }
        groupByIndices_.push_back(*idx);
    }
}

void AggregateOperator::prepareAggregates(const Schema& childSchema) {
    ExpressionParser parser;
    for (auto& agg : aggregates_) {
        // Build default alias if needed
        if (agg.alias.empty()) {
            const std::string exprLabel = agg.expression.empty() ? "*" : agg.expression;
            agg.alias = funcName(agg.func) + "(" + exprLabel + ")";
        }

        if (agg.func == AggFunc::COUNT &&
            (agg.expression.empty() || agg.expression == "*")) {
            agg.exprNode.reset();
            agg.resultType = ColumnType::Integer;
            continue;
        }

        if (agg.expression.empty()) {
            throw std::runtime_error("aggregate expression missing for " + agg.alias);
        }

        agg.exprNode = parser.parse(agg.expression);
        agg.resultType = inferExpressionType(*agg.exprNode, childSchema);

        if (agg.func == AggFunc::AVG) {
            agg.resultType = ColumnType::Double;
        } else if (agg.func == AggFunc::SUM &&
                   agg.resultType == ColumnType::String) {
            // Promote string expressions to double for numeric aggregation
            agg.resultType = ColumnType::Double;
        }
    }
}

void AggregateOperator::accumulateTuple(
    const Tuple& tuple,
    std::unordered_map<std::vector<std::string>,
                       std::vector<AggregateAccumulator>,
                       GroupKeyHash,
                       GroupKeyEqual>& groups) {
    auto key = buildGroupKey(tuple);
    auto& accs = groups[key];
    if (accs.size() < aggregates_.size()) {
        accs.resize(aggregates_.size());
    }

    for (std::size_t i = 0; i < aggregates_.size(); ++i) {
        auto& agg = aggregates_[i];
        auto& acc = accs[i];

        switch (agg.func) {
            case AggFunc::COUNT:
                acc.count += 1;
                break;
            case AggFunc::SUM: {
                if (!agg.exprNode) {
                    acc.count += 1;
                    break;
                }
                ExprValue value = agg.exprNode->evaluate(tuple);
                if (agg.resultType == ColumnType::Double) {
                    acc.doubleSum += (value.type == ExprValue::Type::DOUBLE)
                                         ? value.asDouble()
                                         : static_cast<double>(value.asInt());
                } else {
                    acc.intSum += value.asInt();
                }
                acc.hasValue = true;
                break;
            }
            case AggFunc::AVG: {
                ExprValue value = agg.exprNode->evaluate(tuple);
                acc.doubleSum += (value.type == ExprValue::Type::DOUBLE)
                                     ? value.asDouble()
                                     : static_cast<double>(value.asInt());
                acc.count += 1;
                acc.hasValue = true;
                break;
            }
            case AggFunc::MIN:
            case AggFunc::MAX: {
                ExprValue value = agg.exprNode->evaluate(tuple);
                if (!acc.hasValue) {
                    acc.extreme = value;
                    acc.hasValue = true;
                } else {
                    int cmp = value.compare(acc.extreme);
                    if ((agg.func == AggFunc::MIN && cmp < 0) ||
                        (agg.func == AggFunc::MAX && cmp > 0)) {
                        acc.extreme = value;
                    }
                }
                break;
            }
        }
    }
}

std::vector<std::string> AggregateOperator::buildGroupKey(const Tuple& tuple) const {
    std::vector<std::string> key;
    key.reserve(groupByIndices_.size());
    for (auto idx : groupByIndices_) {
        key.push_back(tuple.getValue(idx));
    }
    return key;
}

Tuple AggregateOperator::buildOutputTuple(
    const std::vector<std::string>& key,
    const std::vector<AggregateAccumulator>& accs) const {
    Tuple tuple;
    tuple.values.reserve(key.size() + aggregates_.size());

    for (const auto& k : key) {
        tuple.values.push_back(k);
    }

    for (std::size_t i = 0; i < aggregates_.size(); ++i) {
        const auto& agg = aggregates_[i];
        const auto& acc = accs[i];

        switch (agg.func) {
            case AggFunc::COUNT:
                tuple.values.push_back(std::to_string(acc.count));
                break;
            case AggFunc::SUM:
                if (agg.resultType == ColumnType::Double) {
                    tuple.values.push_back(std::to_string(acc.doubleSum));
                } else {
                    tuple.values.push_back(std::to_string(acc.intSum));
                }
                break;
            case AggFunc::AVG: {
                if (acc.count == 0) {
                    tuple.values.push_back("0");
                } else {
                    double avg = acc.doubleSum / static_cast<double>(acc.count);
                    tuple.values.push_back(std::to_string(avg));
                }
                break;
            }
            case AggFunc::MIN:
                tuple.values.push_back(acc.hasValue ? acc.extreme.asString() : "NULL");
                break;
            case AggFunc::MAX:
                tuple.values.push_back(acc.hasValue ? acc.extreme.asString() : "NULL");
                break;
        }
    }

    return tuple;
}

void AggregateOperator::buildOutputSchema(const Schema& childSchema) {
    outputSchema_ = std::make_shared<Schema>();

    for (auto idx : groupByIndices_) {
        outputSchema_->addColumn(childSchema.getColumn(idx));
    }

    for (std::size_t i = 0; i < aggregates_.size(); ++i) {
        const auto& agg = aggregates_[i];
        ColumnInfo col;
        col.name = agg.alias;
        col.type = agg.resultType;
        col.sourceIndex = outputSchema_->columnCount();
        outputSchema_->addColumn(col);
    }
}

ColumnType AggregateOperator::inferExpressionType(const Expression& expr,
                                                  const Schema& schema) {
    if (const auto* col = dynamic_cast<const ColumnRefExpr*>(&expr)) {
        auto idx = schema.findColumn(col->columnName());
        if (idx) {
            return schema.getColumn(*idx).type;
        }
    }

    switch (expr.getType()) {
        case ExprValue::Type::DOUBLE:
            return ColumnType::Double;
        case ExprValue::Type::INTEGER:
            return ColumnType::Integer;
        case ExprValue::Type::STRING:
        case ExprValue::Type::BOOLEAN:
        case ExprValue::Type::NULL_VALUE:
        default:
            return ColumnType::String;
    }
}

std::string AggregateOperator::funcName(AggFunc func) {
    switch (func) {
        case AggFunc::SUM:
            return "SUM";
        case AggFunc::COUNT:
            return "COUNT";
        case AggFunc::AVG:
            return "AVG";
        case AggFunc::MIN:
            return "MIN";
        case AggFunc::MAX:
            return "MAX";
    }
    return "AGG";
}

std::size_t AggregateOperator::GroupKeyHash::operator()(
    const std::vector<std::string>& key) const noexcept {
    std::size_t seed = 0;
    for (const auto& part : key) {
        std::size_t h = std::hash<std::string>{}(part);
        seed ^= h + 0x9e3779b9 + (seed << 6U) + (seed >> 2U);
    }
    return seed;
}

bool AggregateOperator::GroupKeyEqual::operator()(
    const std::vector<std::string>& a,
    const std::vector<std::string>& b) const noexcept {
    return a == b;
}

} // namespace dbms

