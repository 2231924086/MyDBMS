#pragma once

#include <memory>
#include <string>

#include "executor/schema.h"

namespace dbms {

// Expression value (runtime typed value)
struct ExprValue {
    enum class Type { NULL_VALUE, INTEGER, DOUBLE, STRING, BOOLEAN };

    Type type;
    std::string stringValue;  // Stores all types as strings initially

    ExprValue() : type(Type::NULL_VALUE) {}
    explicit ExprValue(Type t, std::string val = "") : type(t), stringValue(std::move(val)) {}

    // Type conversion helpers
    int64_t asInt() const;
    double asDouble() const;
    bool asBool() const;
    std::string asString() const;

    // Null check
    bool isNull() const { return type == Type::NULL_VALUE; }

    // Comparison operators (type-aware)
    int compare(const ExprValue& other) const;
    bool operator==(const ExprValue& other) const { return compare(other) == 0; }
    bool operator!=(const ExprValue& other) const { return compare(other) != 0; }
    bool operator<(const ExprValue& other) const { return compare(other) < 0; }
    bool operator<=(const ExprValue& other) const { return compare(other) <= 0; }
    bool operator>(const ExprValue& other) const { return compare(other) > 0; }
    bool operator>=(const ExprValue& other) const { return compare(other) >= 0; }
};

// Abstract expression node
class Expression {
public:
    virtual ~Expression() = default;

    // Evaluate expression against a tuple
    virtual ExprValue evaluate(const Tuple& tuple) const = 0;

    // Get result type
    virtual ExprValue::Type getType() const = 0;
};

// Column reference expression
class ColumnRefExpr : public Expression {
public:
    explicit ColumnRefExpr(std::string name) : columnName_(std::move(name)) {}

    ExprValue evaluate(const Tuple& tuple) const override;
    ExprValue::Type getType() const override { return ExprValue::Type::STRING; }
    const std::string& columnName() const { return columnName_; }

private:
    std::string columnName_;
    mutable std::optional<std::size_t> columnIndex_;  // Cached index
};

// Literal expression
class LiteralExpr : public Expression {
public:
    explicit LiteralExpr(ExprValue val) : value_(std::move(val)) {}

    ExprValue evaluate(const Tuple& tuple) const override { return value_; }
    ExprValue::Type getType() const override { return value_.type; }
    const ExprValue& value() const { return value_; }

private:
    ExprValue value_;
};

// Comparison expression
class ComparisonExpr : public Expression {
public:
    enum class Op { EQ, NE, LT, LE, GT, GE };

    ComparisonExpr(Op op, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right)
        : op_(op), left_(std::move(left)), right_(std::move(right)) {}

    ExprValue evaluate(const Tuple& tuple) const override;
    ExprValue::Type getType() const override { return ExprValue::Type::BOOLEAN; }
    Op op() const { return op_; }
    const Expression* left() const { return left_.get(); }
    const Expression* right() const { return right_.get(); }

private:
    Op op_;
    std::unique_ptr<Expression> left_;
    std::unique_ptr<Expression> right_;
};

// Logical expression (AND, OR, NOT)
class LogicalExpr : public Expression {
public:
    enum class Op { AND, OR, NOT };

    // For AND and OR
    LogicalExpr(Op op, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right)
        : op_(op), left_(std::move(left)), right_(std::move(right)) {}

    // For NOT
    LogicalExpr(Op op, std::unique_ptr<Expression> expr)
        : op_(op), left_(std::move(expr)), right_(nullptr) {}

    ExprValue evaluate(const Tuple& tuple) const override;
    ExprValue::Type getType() const override { return ExprValue::Type::BOOLEAN; }
    Op op() const { return op_; }
    const Expression* left() const { return left_.get(); }
    const Expression* right() const { return right_.get(); }

private:
    Op op_;
    std::unique_ptr<Expression> left_;
    std::unique_ptr<Expression> right_;  // null for NOT
};

// Binary operation expression (+, -, *, /, %)
class BinaryOpExpr : public Expression {
public:
    enum class Op { ADD, SUB, MUL, DIV, MOD };

    BinaryOpExpr(Op op, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right)
        : op_(op), left_(std::move(left)), right_(std::move(right)) {}

    ExprValue evaluate(const Tuple& tuple) const override;
    ExprValue::Type getType() const override;
    Op op() const { return op_; }
    const Expression* left() const { return left_.get(); }
    const Expression* right() const { return right_.get(); }

private:
    Op op_;
    std::unique_ptr<Expression> left_;
    std::unique_ptr<Expression> right_;
};

} // namespace dbms
