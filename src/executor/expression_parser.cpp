#include "executor/expression_parser.h"

#include <cctype>
#include <stdexcept>

namespace dbms {

std::unique_ptr<Expression> ExpressionParser::parse(const std::string& exprString) {
    tokens_ = tokenize(exprString);
    current_ = 0;
    return parseExpression();
}

// ============== Lexer ==============

std::vector<ExprToken> ExpressionParser::tokenize(const std::string& input) {
    std::vector<ExprToken> tokens;
    std::size_t pos = 0;

    while (pos < input.size()) {
        char ch = input[pos];

        // Skip whitespace
        if (std::isspace(ch)) {
            ++pos;
            continue;
        }

        // String literals
        if (ch == '\'' || ch == '\"') {
            char quote = ch;
            ++pos;
            std::string value;
            while (pos < input.size() && input[pos] != quote) {
                value += input[pos];
                ++pos;
            }
            if (pos >= input.size()) {
                throw std::runtime_error("unterminated string literal");
            }
            ++pos;  // Skip closing quote
            tokens.emplace_back(ExprToken::Type::STRING, value);
            continue;
        }

        // Numbers
        if (std::isdigit(ch) || (ch == '.' && pos + 1 < input.size() && std::isdigit(input[pos + 1]))) {
            std::string value;
            bool hasDecimal = false;
            while (pos < input.size() && (std::isdigit(input[pos]) || input[pos] == '.')) {
                if (input[pos] == '.') {
                    if (hasDecimal) break;
                    hasDecimal = true;
                }
                value += input[pos];
                ++pos;
            }
            tokens.emplace_back(ExprToken::Type::NUMBER, value);
            continue;
        }

        // Identifiers and keywords
        if (std::isalpha(ch) || ch == '_') {
            std::string value;
            while (pos < input.size() && (std::isalnum(input[pos]) || input[pos] == '_' || input[pos] == '.')) {
                value += input[pos];
                ++pos;
            }

            // Check for keywords
            if (value == "AND" || value == "and") {
                tokens.emplace_back(ExprToken::Type::KEYWORD, "AND");
            } else if (value == "OR" || value == "or") {
                tokens.emplace_back(ExprToken::Type::KEYWORD, "OR");
            } else if (value == "NOT" || value == "not") {
                tokens.emplace_back(ExprToken::Type::KEYWORD, "NOT");
            } else {
                tokens.emplace_back(ExprToken::Type::IDENTIFIER, value);
            }
            continue;
        }

        // Operators
        if (ch == '=' || ch == '<' || ch == '>' || ch == '!') {
            std::string op(1, ch);
            ++pos;
            if (pos < input.size()) {
                if ((ch == '<' || ch == '>' || ch == '!') && input[pos] == '=') {
                    op += '=';
                    ++pos;
                } else if (ch == '<' && input[pos] == '>') {
                    op = "<>";
                    ++pos;
                }
            }
            tokens.emplace_back(ExprToken::Type::OPERATOR, op);
            continue;
        }

        if (ch == '+' || ch == '-' || ch == '*' || ch == '/' || ch == '%') {
            tokens.emplace_back(ExprToken::Type::OPERATOR, std::string(1, ch));
            ++pos;
            continue;
        }

        // Parentheses
        if (ch == '(') {
            tokens.emplace_back(ExprToken::Type::LPAREN, "(");
            ++pos;
            continue;
        }
        if (ch == ')') {
            tokens.emplace_back(ExprToken::Type::RPAREN, ")");
            ++pos;
            continue;
        }

        throw std::runtime_error("unexpected character: " + std::string(1, ch));
    }

    tokens.emplace_back(ExprToken::Type::END, "");
    return tokens;
}

// ============== Parser Helpers ==============

const ExprToken& ExpressionParser::current() const {
    if (current_ >= tokens_.size()) {
        static ExprToken endToken(ExprToken::Type::END, "");
        return endToken;
    }
    return tokens_[current_];
}

const ExprToken& ExpressionParser::peek(int offset) const {
    std::size_t pos = current_ + offset;
    if (pos >= tokens_.size()) {
        static ExprToken endToken(ExprToken::Type::END, "");
        return endToken;
    }
    return tokens_[pos];
}

ExprToken ExpressionParser::advance() {
    if (current_ < tokens_.size()) {
        return tokens_[current_++];
    }
    return ExprToken(ExprToken::Type::END, "");
}

bool ExpressionParser::match(ExprToken::Type type) {
    if (check(type)) {
        advance();
        return true;
    }
    return false;
}

bool ExpressionParser::check(ExprToken::Type type) const {
    return current().type == type;
}

bool ExpressionParser::checkValue(const std::string& value) const {
    return current().value == value;
}

ExprToken ExpressionParser::consume(ExprToken::Type type, const std::string& message) {
    if (check(type)) {
        return advance();
    }
    throw std::runtime_error(message);
}

// ============== Parser (Recursive Descent) ==============

std::unique_ptr<Expression> ExpressionParser::parseExpression() {
    return parseOrExpression();
}

std::unique_ptr<Expression> ExpressionParser::parseOrExpression() {
    auto left = parseAndExpression();

    while (check(ExprToken::Type::KEYWORD) && checkValue("OR")) {
        advance();
        auto right = parseAndExpression();
        left = std::make_unique<LogicalExpr>(LogicalExpr::Op::OR, std::move(left), std::move(right));
    }

    return left;
}

std::unique_ptr<Expression> ExpressionParser::parseAndExpression() {
    auto left = parseComparisonExpression();

    while (check(ExprToken::Type::KEYWORD) && checkValue("AND")) {
        advance();
        auto right = parseComparisonExpression();
        left = std::make_unique<LogicalExpr>(LogicalExpr::Op::AND, std::move(left), std::move(right));
    }

    return left;
}

std::unique_ptr<Expression> ExpressionParser::parseComparisonExpression() {
    auto left = parseAdditiveExpression();

    if (check(ExprToken::Type::OPERATOR)) {
        std::string op = current().value;
        ComparisonExpr::Op compOp;

        if (op == "=") {
            compOp = ComparisonExpr::Op::EQ;
        } else if (op == "<>" || op == "!=") {
            compOp = ComparisonExpr::Op::NE;
        } else if (op == "<") {
            compOp = ComparisonExpr::Op::LT;
        } else if (op == "<=") {
            compOp = ComparisonExpr::Op::LE;
        } else if (op == ">") {
            compOp = ComparisonExpr::Op::GT;
        } else if (op == ">=") {
            compOp = ComparisonExpr::Op::GE;
        } else {
            return left;  // Not a comparison operator
        }

        advance();
        auto right = parseAdditiveExpression();
        return std::make_unique<ComparisonExpr>(compOp, std::move(left), std::move(right));
    }

    return left;
}

std::unique_ptr<Expression> ExpressionParser::parseAdditiveExpression() {
    auto left = parseMultiplicativeExpression();

    while (check(ExprToken::Type::OPERATOR) && (checkValue("+") || checkValue("-"))) {
        std::string op = advance().value;
        auto right = parseMultiplicativeExpression();
        BinaryOpExpr::Op binOp = (op == "+") ? BinaryOpExpr::Op::ADD : BinaryOpExpr::Op::SUB;
        left = std::make_unique<BinaryOpExpr>(binOp, std::move(left), std::move(right));
    }

    return left;
}

std::unique_ptr<Expression> ExpressionParser::parseMultiplicativeExpression() {
    auto left = parseUnaryExpression();

    while (check(ExprToken::Type::OPERATOR) && (checkValue("*") || checkValue("/") || checkValue("%"))) {
        std::string op = advance().value;
        auto right = parseUnaryExpression();
        BinaryOpExpr::Op binOp;
        if (op == "*") binOp = BinaryOpExpr::Op::MUL;
        else if (op == "/") binOp = BinaryOpExpr::Op::DIV;
        else binOp = BinaryOpExpr::Op::MOD;
        left = std::make_unique<BinaryOpExpr>(binOp, std::move(left), std::move(right));
    }

    return left;
}

std::unique_ptr<Expression> ExpressionParser::parseUnaryExpression() {
    if (check(ExprToken::Type::KEYWORD) && checkValue("NOT")) {
        advance();
        auto expr = parseUnaryExpression();
        return std::make_unique<LogicalExpr>(LogicalExpr::Op::NOT, std::move(expr));
    }

    return parsePrimaryExpression();
}

std::unique_ptr<Expression> ExpressionParser::parsePrimaryExpression() {
    // Parentheses
    if (match(ExprToken::Type::LPAREN)) {
        auto expr = parseExpression();
        consume(ExprToken::Type::RPAREN, "expected ')'");
        return expr;
    }

    // String literal
    if (check(ExprToken::Type::STRING)) {
        std::string value = advance().value;
        return std::make_unique<LiteralExpr>(ExprValue(ExprValue::Type::STRING, value));
    }

    // Number literal
    if (check(ExprToken::Type::NUMBER)) {
        std::string value = advance().value;
        // Determine if it's integer or double
        bool isDouble = (value.find('.') != std::string::npos);
        ExprValue::Type type = isDouble ? ExprValue::Type::DOUBLE : ExprValue::Type::INTEGER;
        return std::make_unique<LiteralExpr>(ExprValue(type, value));
    }

    // Identifier (column reference)
    if (check(ExprToken::Type::IDENTIFIER)) {
        std::string name = advance().value;
        return std::make_unique<ColumnRefExpr>(name);
    }

    throw std::runtime_error("unexpected token in expression: " + current().value);
}

} // namespace dbms
