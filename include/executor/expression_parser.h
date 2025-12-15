#pragma once

#include <memory>
#include <string>
#include <vector>

#include "executor/expression.h"

namespace dbms {

// Simple token for expression parsing
struct ExprToken {
    enum class Type {
        NUMBER,         // 123, 45.67
        STRING,         // 'hello'
        IDENTIFIER,     // column_name
        OPERATOR,       // =, <>, <, <=, >, >=, +, -, *, /, %
        KEYWORD,        // AND, OR, NOT
        LPAREN,         // (
        RPAREN,         // )
        END             // End of input
    };

    Type type;
    std::string value;

    ExprToken(Type t = Type::END, std::string v = "") : type(t), value(std::move(v)) {}
};

// Expression parser - parses string expressions from PhysicalPlanNode parameters
class ExpressionParser {
public:
    // Parse "age > 18" or "name = 'John' AND age < 50"
    std::unique_ptr<Expression> parse(const std::string& exprString);

private:
    std::vector<ExprToken> tokens_;
    std::size_t current_;

    // Lexer
    std::vector<ExprToken> tokenize(const std::string& input);

    // Parser (recursive descent with operator precedence)
    std::unique_ptr<Expression> parseExpression();
    std::unique_ptr<Expression> parseOrExpression();
    std::unique_ptr<Expression> parseAndExpression();
    std::unique_ptr<Expression> parseComparisonExpression();
    std::unique_ptr<Expression> parseAdditiveExpression();
    std::unique_ptr<Expression> parseMultiplicativeExpression();
    std::unique_ptr<Expression> parseUnaryExpression();
    std::unique_ptr<Expression> parsePrimaryExpression();

    // Helpers
    const ExprToken& current() const;
    const ExprToken& peek(int offset = 1) const;
    ExprToken advance();
    bool match(ExprToken::Type type);
    bool check(ExprToken::Type type) const;
    bool checkValue(const std::string& value) const;
    ExprToken consume(ExprToken::Type type, const std::string& message);
};

} // namespace dbms
