#include "executor/expression.h"

#include <cmath>
#include <stdexcept>

namespace dbms {

// ============== ExprValue Implementation ==============

int64_t ExprValue::asInt() const {
    if (type == Type::NULL_VALUE) {
        throw std::runtime_error("cannot convert NULL to integer");
    }
    try {
        return std::stoll(stringValue);
    } catch (const std::exception& e) {
        throw std::runtime_error("failed to convert '" + stringValue + "' to integer: " + e.what());
    }
}

double ExprValue::asDouble() const {
    if (type == Type::NULL_VALUE) {
        throw std::runtime_error("cannot convert NULL to double");
    }
    try {
        return std::stod(stringValue);
    } catch (const std::exception& e) {
        throw std::runtime_error("failed to convert '" + stringValue + "' to double: " + e.what());
    }
}

bool ExprValue::asBool() const {
    if (type == Type::NULL_VALUE) {
        return false;
    }
    if (type == Type::BOOLEAN) {
        return stringValue == "true" || stringValue == "1";
    }
    return !stringValue.empty();
}

std::string ExprValue::asString() const {
    if (type == Type::NULL_VALUE) {
        return "NULL";
    }
    return stringValue;
}

int ExprValue::compare(const ExprValue& other) const {
    // NULL handling
    if (isNull() && other.isNull()) return 0;
    if (isNull()) return -1;
    if (other.isNull()) return 1;

    // Type-aware comparison
    if (type == Type::INTEGER && other.type == Type::INTEGER) {
        int64_t a = asInt();
        int64_t b = other.asInt();
        return (a < b) ? -1 : (a > b) ? 1 : 0;
    }

    if ((type == Type::DOUBLE || type == Type::INTEGER) &&
        (other.type == Type::DOUBLE || other.type == Type::INTEGER)) {
        double a = (type == Type::DOUBLE) ? asDouble() : static_cast<double>(asInt());
        double b = (other.type == Type::DOUBLE) ? other.asDouble() : static_cast<double>(other.asInt());
        if (std::abs(a - b) < 1e-9) return 0;
        return (a < b) ? -1 : 1;
    }

    // Default: string comparison
    return stringValue.compare(other.stringValue);
}

// ============== ColumnRefExpr Implementation ==============

ExprValue ColumnRefExpr::evaluate(const Tuple& tuple) const {
    if (!tuple.schema) {
        throw std::runtime_error("tuple has no schema");
    }

    // Cache column index for efficiency
    if (!columnIndex_) {
        columnIndex_ = tuple.schema->findColumn(columnName_);
        if (!columnIndex_) {
            throw std::runtime_error("column not found: " + columnName_);
        }
    }

    const std::string& value = tuple.getValue(*columnIndex_);
    const ColumnInfo& colInfo = tuple.schema->getColumn(*columnIndex_);

    // Create typed value
    if (value == "NULL") {
        return ExprValue(ExprValue::Type::NULL_VALUE, "NULL");
    }
    ExprValue result;
    result.stringValue = value;
    switch (colInfo.type) {
        case ColumnType::Integer:
            result.type = ExprValue::Type::INTEGER;
            break;
        case ColumnType::Double:
            result.type = ExprValue::Type::DOUBLE;
            break;
        case ColumnType::String:
            result.type = ExprValue::Type::STRING;
            break;
    }
    return result;
}

// ============== ComparisonExpr Implementation ==============

ExprValue ComparisonExpr::evaluate(const Tuple& tuple) const {
    ExprValue leftVal = left_->evaluate(tuple);
    ExprValue rightVal = right_->evaluate(tuple);

    bool result = false;
    int cmp = leftVal.compare(rightVal);

    switch (op_) {
        case Op::EQ:
            result = (cmp == 0);
            break;
        case Op::NE:
            result = (cmp != 0);
            break;
        case Op::LT:
            result = (cmp < 0);
            break;
        case Op::LE:
            result = (cmp <= 0);
            break;
        case Op::GT:
            result = (cmp > 0);
            break;
        case Op::GE:
            result = (cmp >= 0);
            break;
    }

    return ExprValue(ExprValue::Type::BOOLEAN, result ? "true" : "false");
}

// ============== LogicalExpr Implementation ==============

ExprValue LogicalExpr::evaluate(const Tuple& tuple) const {
    bool result = false;

    switch (op_) {
        case Op::AND: {
            bool leftBool = left_->evaluate(tuple).asBool();
            if (!leftBool) {
                // Short-circuit evaluation
                result = false;
            } else {
                result = right_->evaluate(tuple).asBool();
            }
            break;
        }
        case Op::OR: {
            bool leftBool = left_->evaluate(tuple).asBool();
            if (leftBool) {
                // Short-circuit evaluation
                result = true;
            } else {
                result = right_->evaluate(tuple).asBool();
            }
            break;
        }
        case Op::NOT:
            result = !left_->evaluate(tuple).asBool();
            break;
    }

    return ExprValue(ExprValue::Type::BOOLEAN, result ? "true" : "false");
}

// ============== BinaryOpExpr Implementation ==============

ExprValue BinaryOpExpr::evaluate(const Tuple& tuple) const {
    ExprValue leftVal = left_->evaluate(tuple);
    ExprValue rightVal = right_->evaluate(tuple);

    // Determine result type
    bool isDouble = (leftVal.type == ExprValue::Type::DOUBLE ||
                     rightVal.type == ExprValue::Type::DOUBLE);

    if (isDouble) {
        double left = (leftVal.type == ExprValue::Type::DOUBLE) ?
                      leftVal.asDouble() : static_cast<double>(leftVal.asInt());
        double right = (rightVal.type == ExprValue::Type::DOUBLE) ?
                       rightVal.asDouble() : static_cast<double>(rightVal.asInt());
        double result = 0.0;

        switch (op_) {
            case Op::ADD:
                result = left + right;
                break;
            case Op::SUB:
                result = left - right;
                break;
            case Op::MUL:
                result = left * right;
                break;
            case Op::DIV:
                if (std::abs(right) < 1e-9) {
                    throw std::runtime_error("division by zero");
                }
                result = left / right;
                break;
            case Op::MOD:
                result = std::fmod(left, right);
                break;
        }

        return ExprValue(ExprValue::Type::DOUBLE, std::to_string(result));
    } else {
        int64_t left = leftVal.asInt();
        int64_t right = rightVal.asInt();
        int64_t result = 0;

        switch (op_) {
            case Op::ADD:
                result = left + right;
                break;
            case Op::SUB:
                result = left - right;
                break;
            case Op::MUL:
                result = left * right;
                break;
            case Op::DIV:
                if (right == 0) {
                    throw std::runtime_error("division by zero");
                }
                result = left / right;
                break;
            case Op::MOD:
                if (right == 0) {
                    throw std::runtime_error("division by zero");
                }
                result = left % right;
                break;
        }

        return ExprValue(ExprValue::Type::INTEGER, std::to_string(result));
    }
}

ExprValue::Type BinaryOpExpr::getType() const {
    // If either operand is DOUBLE, result is DOUBLE; otherwise INTEGER
    ExprValue::Type leftType = left_->getType();
    ExprValue::Type rightType = right_->getType();

    if (leftType == ExprValue::Type::DOUBLE || rightType == ExprValue::Type::DOUBLE) {
        return ExprValue::Type::DOUBLE;
    }
    return ExprValue::Type::INTEGER;
}

} // namespace dbms
