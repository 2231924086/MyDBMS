#include "parser/query_processor.h"
#include "executor/executor.h"
#include "executor/expression_parser.h"
#include "executor/expression.h"
#include <cctype>
#include <sstream>
#include <iostream>
#include <algorithm>

namespace dbms {

// ============== ASTNode 实现 ==============
std::string ASTNode::toString(int indent) const {
    std::ostringstream oss;
    std::string indentStr(indent * 2, ' ');

    oss << indentStr << "Node(";
    switch (nodeType) {
        case ASTNodeType::SELECT_STATEMENT: oss << "SELECT_STMT"; break;
        case ASTNodeType::INSERT_STATEMENT: oss << "INSERT_STMT"; break;
        case ASTNodeType::UPDATE_STATEMENT: oss << "UPDATE_STMT"; break;
        case ASTNodeType::DELETE_STATEMENT: oss << "DELETE_STMT"; break;
        case ASTNodeType::ORDER_BY: oss << "ORDER_BY"; break;
        case ASTNodeType::SELECT_LIST: oss << "SELECT_LIST"; break;
        case ASTNodeType::FROM_CLAUSE: oss << "FROM"; break;
        case ASTNodeType::WHERE_CLAUSE: oss << "WHERE"; break;
        case ASTNodeType::JOIN_CLAUSE: oss << "JOIN"; break;
        case ASTNodeType::COLUMN_REF: oss << "COLUMN"; break;
        case ASTNodeType::TABLE_REF: oss << "TABLE"; break;
        case ASTNodeType::BINARY_OP: oss << "BINARY_OP"; break;
        case ASTNodeType::UNARY_OP: oss << "UNARY_OP"; break;
        case ASTNodeType::LITERAL: oss << "LITERAL"; break;
        case ASTNodeType::STAR: oss << "STAR"; break;
        case ASTNodeType::AND_EXPR: oss << "AND"; break;
        case ASTNodeType::OR_EXPR: oss << "OR"; break;
        case ASTNodeType::NOT_EXPR: oss << "NOT"; break;
        case ASTNodeType::COMPARISON: oss << "COMPARISON"; break;
    }

    if (!value.empty()) {
        oss << ", value=\"" << value << "\"";
    }
    oss << ")";

    if (!children.empty()) {
        oss << " {\n";
        for (const auto& child : children) {
            oss << child->toString(indent + 1) << "\n";
        }
        oss << indentStr << "}";
    }

    return oss.str();
}

// ============== RelAlgNode 实现 ==============
std::string RelAlgNode::toString(int indent) const {
    std::ostringstream oss;
    std::string indentStr(indent * 2, ' ');

    oss << indentStr;
    switch (opType) {
        case RelAlgOpType::kScan:
            oss << "σ SCAN(" << tableName << ")";
            break;
        case RelAlgOpType::kSelect:
            oss << "σ SELECT[" << condition << "]";
            break;
        case RelAlgOpType::kProject:
            oss << "π PROJECT[";
            for (size_t i = 0; i < columns.size(); ++i) {
                if (i > 0) oss << ", ";
                oss << columns[i];
            }
            oss << "]";
            break;
        case RelAlgOpType::kDistinct:
            oss << "DISTINCT";
            break;
        case RelAlgOpType::kJoin: {
            std::string typeLabel = "JOIN";
            if (joinType == JoinType::kLeft) {
                typeLabel = "LEFT JOIN";
            } else if (joinType == JoinType::kRight) {
                typeLabel = "RIGHT JOIN";
            }
            oss << "? " << typeLabel << "[" << condition << "]";
            break;
        }
        case RelAlgOpType::kCrossProduct:
            oss << "× CROSS_PRODUCT";
            break;
        case RelAlgOpType::kUnion:
            oss << "∪ UNION";
            break;
        case RelAlgOpType::kIntersect:
            oss << "∩ INTERSECT";
            break;
        case RelAlgOpType::kDifference:
            oss << "− DIFFERENCE";
            break;
        case RelAlgOpType::kSort:
            oss << "τ SORT";
            if (!orderByClause.empty()) {
                oss << "[" << orderByClause << "]";
            }
            break;
        case RelAlgOpType::kGroup:
            oss << "γ GROUP";
            break;
    }

    if (!operationDesc.empty()) {
        oss << " // " << operationDesc;
    }

    if (!children.empty()) {
        oss << "\n";
        for (const auto& child : children) {
            oss << child->toString(indent + 1) << "\n";
        }
    }

    return oss.str();
}

// ============== PhysicalPlanNode 实现 ==============
std::string PhysicalPlanNode::toString(int indent) const {
    std::ostringstream oss;
    std::string indentStr(indent * 2, ' ');

    oss << indentStr << "[";
    switch (opType) {
        case PhysicalOpType::kTableScan: oss << "TABLE_SCAN"; break;
        case PhysicalOpType::kIndexScan: oss << "INDEX_SCAN"; break;
        case PhysicalOpType::kFilter: oss << "FILTER"; break;
        case PhysicalOpType::kProjection: oss << "PROJECTION"; break;
        case PhysicalOpType::kDistinct: oss << "DISTINCT"; break;
        case PhysicalOpType::kNestedLoopJoin: oss << "NESTED_LOOP_JOIN"; break;
        case PhysicalOpType::kHashJoin: oss << "HASH_JOIN"; break;
        case PhysicalOpType::kMergeJoin: oss << "MERGE_JOIN"; break;
        case PhysicalOpType::kSort: oss << "SORT"; break;
        case PhysicalOpType::kAggregate: oss << "AGGREGATE"; break;
    }
    oss << "]";

    if (!description.empty()) {
        oss << " " << description;
    }

    oss << "\n" << indentStr << "  Algorithm: " << algorithm;
    oss << "\n" << indentStr << "  DataFlow: " << planFlow;
    oss << "\n" << indentStr << "  EstimatedCost: " << estimatedCost;

    if (!parameters.empty()) {
        oss << "\n" << indentStr << "  Parameters: {";
        bool first = true;
        for (const auto& p : parameters) {
            if (!first) oss << ", ";
            oss << p.first << "=" << p.second;
            first = false;
        }
        oss << "}";
    }

    if (!children.empty()) {
        oss << "\n" << indentStr << "  Children:\n";
        for (const auto& child : children) {
            oss << child->toString(indent + 2) << "\n";
        }
    }

    return oss.str();
}

// ============== Lexer 实现 ==============
Lexer::Lexer(std::string input)
    : input_(std::move(input)), position_(0), line_(1), column_(1) {}

char Lexer::currentChar() const {
    if (position_ >= input_.size()) return '\0';
    return input_[position_];
}

char Lexer::peekChar() const {
    if (position_ + 1 >= input_.size()) return '\0';
    return input_[position_ + 1];
}

void Lexer::advance() {
    if (position_ < input_.size()) {
        if (input_[position_] == '\n') {
            line_++;
            column_ = 1;
        } else {
            column_++;
        }
        position_++;
    }
}

void Lexer::skipWhitespace() {
    while (std::isspace(currentChar())) {
        advance();
    }
}

TokenType Lexer::getKeywordType(const std::string& word) const {
    static const std::map<std::string, TokenType> keywords = {
        {"SELECT", TokenType::SELECT}, {"FROM", TokenType::FROM},
        {"WHERE", TokenType::WHERE}, {"AND", TokenType::AND},
        {"OR", TokenType::OR}, {"NOT", TokenType::NOT},
        {"JOIN", TokenType::JOIN}, {"ON", TokenType::ON},
        {"INNER", TokenType::INNER}, {"LEFT", TokenType::LEFT},
        {"RIGHT", TokenType::RIGHT}, {"ORDER", TokenType::ORDER},
        {"BY", TokenType::BY}, {"GROUP", TokenType::GROUP},
        {"HAVING", TokenType::HAVING}, {"AS", TokenType::AS},
        {"DISTINCT", TokenType::DISTINCT}, {"ALL", TokenType::ALL},
        {"INSERT", TokenType::INSERT}, {"INTO", TokenType::INTO},
        {"VALUES", TokenType::VALUES}, {"UPDATE", TokenType::UPDATE},
        {"SET", TokenType::SET}, {"DELETE", TokenType::DELETE}
    };

    std::string upper = word;
    std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);

    auto it = keywords.find(upper);
    if (it != keywords.end()) {
        return it->second;
    }
    return TokenType::IDENTIFIER;
}

Token Lexer::readIdentifierOrKeyword() {
    int startColumn = column_;
    std::string lexeme;

    while (std::isalnum(currentChar()) || currentChar() == '_') {
        lexeme += currentChar();
        advance();
    }

    TokenType type = getKeywordType(lexeme);
    return Token(type, lexeme, line_, startColumn);
}

Token Lexer::readNumber() {
    int startColumn = column_;
    std::string lexeme;

    while (std::isdigit(currentChar()) || currentChar() == '.') {
        lexeme += currentChar();
        advance();
    }

    return Token(TokenType::NUMBER_LITERAL, lexeme, line_, startColumn);
}

Token Lexer::readString() {
    int startColumn = column_;
    std::string lexeme;
    char quote = currentChar();
    advance(); // Skip opening quote

    while (currentChar() != quote && currentChar() != '\0') {
        lexeme += currentChar();
        advance();
    }

    if (currentChar() == quote) {
        advance(); // Skip closing quote
    }

    return Token(TokenType::STRING_LITERAL, lexeme, line_, startColumn);
}

std::vector<Token> Lexer::tokenize() {
    std::vector<Token> tokens;

    while (currentChar() != '\0') {
        skipWhitespace();

        if (currentChar() == '\0') break;

        int startColumn = column_;
        char ch = currentChar();

        // Comments
        if (ch == '-' && peekChar() == '-') {
            while (currentChar() != '\n' && currentChar() != '\0') {
                advance();
            }
            continue;
        }

        // Identifiers and keywords
        if (std::isalpha(ch) || ch == '_') {
            tokens.push_back(readIdentifierOrKeyword());
            continue;
        }

        // Numbers
        if (std::isdigit(ch)) {
            tokens.push_back(readNumber());
            continue;
        }

        // String literals
        if (ch == '\'' || ch == '"') {
            tokens.push_back(readString());
            continue;
        }

        // Operators and delimiters
        TokenType type = TokenType::UNKNOWN;
        std::string lexeme(1, ch);

        switch (ch) {
            case '=':
                type = TokenType::EQUAL;
                advance();
                break;
            case '<':
                advance();
                if (currentChar() == '=') {
                    type = TokenType::LESS_EQUAL;
                    lexeme = "<=";
                    advance();
                } else if (currentChar() == '>') {
                    type = TokenType::NOT_EQUAL;
                    lexeme = "<>";
                    advance();
                } else {
                    type = TokenType::LESS;
                }
                break;
            case '>':
                advance();
                if (currentChar() == '=') {
                    type = TokenType::GREATER_EQUAL;
                    lexeme = ">=";
                    advance();
                } else {
                    type = TokenType::GREATER;
                }
                break;
            case '!':
                advance();
                if (currentChar() == '=') {
                    type = TokenType::NOT_EQUAL;
                    lexeme = "!=";
                    advance();
                }
                break;
            case '+':
                type = TokenType::PLUS;
                advance();
                break;
            case '-':
                type = TokenType::MINUS;
                advance();
                break;
            case '*':
                type = TokenType::STAR;
                advance();
                break;
            case '/':
                type = TokenType::SLASH;
                advance();
                break;
            case '%':
                type = TokenType::PERCENT;
                advance();
                break;
            case ',':
                type = TokenType::COMMA;
                advance();
                break;
            case ';':
                type = TokenType::SEMICOLON;
                advance();
                break;
            case '(':
                type = TokenType::LEFT_PAREN;
                advance();
                break;
            case ')':
                type = TokenType::RIGHT_PAREN;
                advance();
                break;
            case '.':
                type = TokenType::DOT;
                advance();
                break;
            default:
                advance();
                break;
        }

        if (type != TokenType::UNKNOWN) {
            tokens.push_back(Token(type, lexeme, line_, startColumn));
        }
    }

    tokens.push_back(Token(TokenType::END_OF_FILE, "", line_, column_));
    return tokens;
}

// ============== Parser 实现 ==============
Parser::Parser(std::vector<Token> tokens)
    : tokens_(std::move(tokens)), current_(0) {}

Token Parser::currentToken() const {
    if (current_ < tokens_.size()) {
        return tokens_[current_];
    }
    return Token(TokenType::END_OF_FILE, "", 0, 0);
}

Token Parser::peek(int offset) const {
    size_t pos = current_ + offset;
    if (pos < tokens_.size()) {
        return tokens_[pos];
    }
    return Token(TokenType::END_OF_FILE, "", 0, 0);
}

bool Parser::match(TokenType type) {
    if (check(type)) {
        advance();
        return true;
    }
    return false;
}

bool Parser::check(TokenType type) const {
    return currentToken().type == type;
}

Token Parser::advance() {
    if (current_ < tokens_.size()) {
        return tokens_[current_++];
    }
    return Token(TokenType::END_OF_FILE, "", 0, 0);
}

Token Parser::consume(TokenType type, const std::string& message) {
    if (check(type)) {
        return advance();
    }
    throw std::runtime_error(message + " at line " + std::to_string(currentToken().line));
}

std::shared_ptr<ASTNode> Parser::parse() {
    return parseStatement();
}

std::shared_ptr<ASTNode> Parser::parseStatement() {
    TokenType type = currentToken().type;

    if (type == TokenType::SELECT) {
        return parseSelectStatement();
    } else if (type == TokenType::INSERT) {
        return parseInsertStatement();
    } else if (type == TokenType::UPDATE) {
        return parseUpdateStatement();
    } else if (type == TokenType::DELETE) {
        return parseDeleteStatement();
    }

    throw std::runtime_error("Expected SQL statement");
}

std::shared_ptr<ASTNode> Parser::parseSelectStatement() {
    auto stmt = std::make_shared<ASTNode>(ASTNodeType::SELECT_STATEMENT);

    consume(TokenType::SELECT, "Expected SELECT");

    // Parse SELECT list
    stmt->addChild(parseSelectList());

    // Parse FROM clause
    if (match(TokenType::FROM)) {
        stmt->addChild(parseFromClause());
    }

    // Parse WHERE clause
    if (match(TokenType::WHERE)) {
        stmt->addChild(parseWhereClause());
    }

    // Parse ORDER BY clause
    if (match(TokenType::ORDER)) {
        consume(TokenType::BY, "Expected BY after ORDER");
        stmt->addChild(parseOrderByClause());
    }

    return stmt;
}

std::shared_ptr<ASTNode> Parser::parseInsertStatement() {
    auto stmt = std::make_shared<ASTNode>(ASTNodeType::INSERT_STATEMENT);
    consume(TokenType::INSERT, "Expected INSERT");
    consume(TokenType::INTO, "Expected INTO");

    Token tableName = consume(TokenType::IDENTIFIER, "Expected table name");
    auto tableNode = std::make_shared<ASTNode>(ASTNodeType::TABLE_REF, tableName.lexeme);
    stmt->addChild(tableNode);

    consume(TokenType::VALUES, "Expected VALUES");
    consume(TokenType::LEFT_PAREN, "Expected (");

    // Parse values
    do {
        if (check(TokenType::STRING_LITERAL) || check(TokenType::NUMBER_LITERAL)) {
            Token value = advance();
            stmt->addChild(std::make_shared<ASTNode>(ASTNodeType::LITERAL, value.lexeme));
        }
    } while (match(TokenType::COMMA));

    consume(TokenType::RIGHT_PAREN, "Expected )");

    return stmt;
}

std::shared_ptr<ASTNode> Parser::parseUpdateStatement() {
    auto stmt = std::make_shared<ASTNode>(ASTNodeType::UPDATE_STATEMENT);
    consume(TokenType::UPDATE, "Expected UPDATE");

    Token tableName = consume(TokenType::IDENTIFIER, "Expected table name");
    stmt->addChild(std::make_shared<ASTNode>(ASTNodeType::TABLE_REF, tableName.lexeme));

    consume(TokenType::SET, "Expected SET");

    // Parse SET clause (simplified)
    do {
        Token col = consume(TokenType::IDENTIFIER, "Expected column name");
        consume(TokenType::EQUAL, "Expected =");

        if (check(TokenType::STRING_LITERAL) || check(TokenType::NUMBER_LITERAL)) {
            advance();
        }
    } while (match(TokenType::COMMA));

    if (match(TokenType::WHERE)) {
        stmt->addChild(parseWhereClause());
    }

    return stmt;
}

std::shared_ptr<ASTNode> Parser::parseDeleteStatement() {
    auto stmt = std::make_shared<ASTNode>(ASTNodeType::DELETE_STATEMENT);
    consume(TokenType::DELETE, "Expected DELETE");
    consume(TokenType::FROM, "Expected FROM");

    Token tableName = consume(TokenType::IDENTIFIER, "Expected table name");
    stmt->addChild(std::make_shared<ASTNode>(ASTNodeType::TABLE_REF, tableName.lexeme));

    if (match(TokenType::WHERE)) {
        stmt->addChild(parseWhereClause());
    }

    return stmt;
}

std::shared_ptr<ASTNode> Parser::parseSelectList() {
    auto selectList = std::make_shared<ASTNode>(ASTNodeType::SELECT_LIST);
    bool distinct = false;

    if (match(TokenType::DISTINCT)) {
        distinct = true;
    } else if (match(TokenType::ALL)) {
        distinct = false;
    }

    do {
        if (match(TokenType::STAR)) {
            selectList->addChild(std::make_shared<ASTNode>(ASTNodeType::STAR, "*"));
        } else if (check(TokenType::IDENTIFIER)) {
            std::string name = parseQualifiedIdentifier();
            selectList->addChild(std::make_shared<ASTNode>(ASTNodeType::COLUMN_REF, name));
        }
    } while (match(TokenType::COMMA));

    if (distinct) {
        selectList->value = "DISTINCT";
    }

    return selectList;
}

std::shared_ptr<ASTNode> Parser::parseOrderByClause() {
    auto orderBy = std::make_shared<ASTNode>(ASTNodeType::ORDER_BY);

    auto toUpper = [](std::string s) {
        std::transform(s.begin(), s.end(), s.begin(), ::toupper);
        return s;
    };

    do {
        std::string column = parseQualifiedIdentifier();
        bool ascending = true;

        if (check(TokenType::IDENTIFIER)) {
            std::string dir = toUpper(currentToken().lexeme);
            if (dir == "ASC" || dir == "DESC") {
                ascending = (dir != "DESC");
                advance();
            }
        }

        std::string value = column + (ascending ? " ASC" : " DESC");
        orderBy->addChild(std::make_shared<ASTNode>(ASTNodeType::COLUMN_REF, value));
    } while (match(TokenType::COMMA));

    return orderBy;
}

std::shared_ptr<ASTNode> Parser::parseFromClause() {
    auto fromClause = std::make_shared<ASTNode>(ASTNodeType::FROM_CLAUSE);

    if (!check(TokenType::IDENTIFIER)) {
        throw std::runtime_error("Expected table name");
    }

    std::string tableName = parseQualifiedIdentifier();
    fromClause->addChild(std::make_shared<ASTNode>(ASTNodeType::TABLE_REF, tableName));

    while (true) {
        if (match(TokenType::COMMA)) {
            std::string extra = parseQualifiedIdentifier();
            fromClause->addChild(std::make_shared<ASTNode>(ASTNodeType::TABLE_REF, extra));
            continue;
        }

        TokenType joinToken = TokenType::UNKNOWN;
        if (match(TokenType::INNER)) {
            consume(TokenType::JOIN, "Expected JOIN after INNER");
            joinToken = TokenType::INNER;
        } else if (match(TokenType::LEFT)) {
            consume(TokenType::JOIN, "Expected JOIN after LEFT");
            joinToken = TokenType::LEFT;
        } else if (match(TokenType::RIGHT)) {
            consume(TokenType::JOIN, "Expected JOIN after RIGHT");
            joinToken = TokenType::RIGHT;
        } else if (match(TokenType::JOIN)) {
            joinToken = TokenType::JOIN;
        } else {
            break;
        }

        std::string joinTable = parseQualifiedIdentifier();
        auto joinNode = std::make_shared<ASTNode>(ASTNodeType::JOIN_CLAUSE);
        switch (joinToken) {
            case TokenType::LEFT:
                joinNode->value = "LEFT";
                break;
            case TokenType::RIGHT:
                joinNode->value = "RIGHT";
                break;
            default:
                joinNode->value = "INNER";
                break;
        }
        joinNode->addChild(std::make_shared<ASTNode>(ASTNodeType::TABLE_REF, joinTable));
        consume(TokenType::ON, "Expected ON after JOIN table");
        joinNode->addChild(parseExpression());
        fromClause->addChild(joinNode);
    }

    return fromClause;
}

std::shared_ptr<ASTNode> Parser::parseWhereClause() {
    auto whereClause = std::make_shared<ASTNode>(ASTNodeType::WHERE_CLAUSE);
    whereClause->addChild(parseExpression());
    return whereClause;
}

std::shared_ptr<ASTNode> Parser::parseExpression() {
    return parseOrExpression();
}

std::shared_ptr<ASTNode> Parser::parseOrExpression() {
    auto left = parseAndExpression();

    while (match(TokenType::OR)) {
        auto orNode = std::make_shared<ASTNode>(ASTNodeType::OR_EXPR, "OR");
        orNode->addChild(left);
        orNode->addChild(parseAndExpression());
        left = orNode;
    }

    return left;
}

std::shared_ptr<ASTNode> Parser::parseAndExpression() {
    auto left = parseComparisonExpression();

    while (match(TokenType::AND)) {
        auto andNode = std::make_shared<ASTNode>(ASTNodeType::AND_EXPR, "AND");
        andNode->addChild(left);
        andNode->addChild(parseComparisonExpression());
        left = andNode;
    }

    return left;
}

std::shared_ptr<ASTNode> Parser::parseComparisonExpression() {
    auto left = parseAdditiveExpression();

    TokenType type = currentToken().type;
    if (type == TokenType::EQUAL || type == TokenType::NOT_EQUAL ||
        type == TokenType::LESS || type == TokenType::LESS_EQUAL ||
        type == TokenType::GREATER || type == TokenType::GREATER_EQUAL) {

        Token op = advance();
        auto cmp = std::make_shared<ASTNode>(ASTNodeType::COMPARISON, op.lexeme);
        cmp->addChild(left);
        cmp->addChild(parseAdditiveExpression());
        return cmp;
    }

    return left;
}

std::shared_ptr<ASTNode> Parser::parseAdditiveExpression() {
    auto left = parseMultiplicativeExpression();

    while (check(TokenType::PLUS) || check(TokenType::MINUS)) {
        Token op = advance();
        auto binOp = std::make_shared<ASTNode>(ASTNodeType::BINARY_OP, op.lexeme);
        binOp->addChild(left);
        binOp->addChild(parseMultiplicativeExpression());
        left = binOp;
    }

    return left;
}

std::shared_ptr<ASTNode> Parser::parseMultiplicativeExpression() {
    auto left = parsePrimaryExpression();

    while (check(TokenType::STAR) || check(TokenType::SLASH) || check(TokenType::PERCENT)) {
        Token op = advance();
        auto binOp = std::make_shared<ASTNode>(ASTNodeType::BINARY_OP, op.lexeme);
        binOp->addChild(left);
        binOp->addChild(parsePrimaryExpression());
        left = binOp;
    }

    return left;
}

std::shared_ptr<ASTNode> Parser::parsePrimaryExpression() {
    if (match(TokenType::LEFT_PAREN)) {
        auto expr = parseExpression();
        consume(TokenType::RIGHT_PAREN, "Expected )");
        return expr;
    }

    if (check(TokenType::STRING_LITERAL) || check(TokenType::NUMBER_LITERAL)) {
        Token lit = advance();
        return std::make_shared<ASTNode>(ASTNodeType::LITERAL, lit.lexeme);
    }

    if (check(TokenType::IDENTIFIER)) {
        std::string name = parseQualifiedIdentifier();
        return std::make_shared<ASTNode>(ASTNodeType::COLUMN_REF, name);
    }

    throw std::runtime_error("Expected expression");
}

std::string Parser::parseQualifiedIdentifier() {
    Token first = consume(TokenType::IDENTIFIER, "Expected identifier");
    std::string name = first.lexeme;

    while (match(TokenType::DOT)) {
        Token part = consume(TokenType::IDENTIFIER, "Expected identifier after '.'");
        name += "." + part.lexeme;
    }

    return name;
}

// ============== SemanticAnalyzer 实现 ==============
SemanticAnalyzer::SemanticAnalyzer(DatabaseSystem& db) : db_(db) {}

void SemanticAnalyzer::analyze(std::shared_ptr<ASTNode> ast) {
    collectTableInfo(ast);
    analyzeNode(ast);
}

void SemanticAnalyzer::collectTableInfo(std::shared_ptr<ASTNode> node) {
    if (!node) return;

    if (node->nodeType == ASTNodeType::FROM_CLAUSE) {
        for (const auto& child : node->children) {
            if (child->nodeType == ASTNodeType::TABLE_REF) {
                std::string tableName = child->value;
                availableTables_.insert(tableName);

                try {
                    const auto& table = db_.getTable(tableName);
                    std::vector<std::string> cols;
                    for (const auto& col : table.schema().columns()) {
                        cols.push_back(col.name);
                    }
                    tableColumns_[tableName] = cols;
                } catch (...) {
                    // Table not found - will be handled in validation
                }
            }
        }
    }

    for (const auto& child : node->children) {
        collectTableInfo(child);
    }
}

void SemanticAnalyzer::analyzeNode(std::shared_ptr<ASTNode> node) {
    if (!node) return;

    if (node->nodeType == ASTNodeType::TABLE_REF) {
        validateTable(node->value);
    }

    // Simplified: we're not doing full column validation here
    // In a real system, you'd check column references against available tables

    for (const auto& child : node->children) {
        analyzeNode(child);
    }
}

void SemanticAnalyzer::validateTable(const std::string& tableName) {
    try {
        (void)db_.getTable(tableName);
    } catch (...) {
        throw std::runtime_error("Unknown table: " + tableName);
    }
}

void SemanticAnalyzer::validateColumn(const std::string& tableName,
                                      const std::string& columnName) {
    auto it = tableColumns_.find(tableName);
    if (it == tableColumns_.end()) {
        throw std::runtime_error("Table not in scope: " + tableName);
    }

    const auto& cols = it->second;
    if (std::find(cols.begin(), cols.end(), columnName) == cols.end()) {
        throw std::runtime_error("Unknown column: " + columnName + " in table " + tableName);
    }
}

// ============== LogicalPlanGenerator 实现 ==============
std::shared_ptr<RelAlgNode> LogicalPlanGenerator::generateLogicalPlan(
    std::shared_ptr<ASTNode> ast) {

    if (ast->nodeType == ASTNodeType::SELECT_STATEMENT) {
        return processSelectStatement(ast);
    }

    throw std::runtime_error("Unsupported statement type for logical plan generation");
}

std::shared_ptr<RelAlgNode> LogicalPlanGenerator::processSelectStatement(
    std::shared_ptr<ASTNode> node) {

    std::shared_ptr<RelAlgNode> plan;

    // Find FROM clause
    std::shared_ptr<ASTNode> fromNode;
    std::shared_ptr<ASTNode> whereNode;
    std::shared_ptr<ASTNode> selectNode;
    std::shared_ptr<ASTNode> orderNode;

    for (const auto& child : node->children) {
        if (child->nodeType == ASTNodeType::FROM_CLAUSE) {
            fromNode = child;
        } else if (child->nodeType == ASTNodeType::WHERE_CLAUSE) {
            whereNode = child;
        } else if (child->nodeType == ASTNodeType::SELECT_LIST) {
            selectNode = child;
        } else if (child->nodeType == ASTNodeType::ORDER_BY) {
            orderNode = child;
        }
    }

    // Build plan bottom-up: FROM -> WHERE -> SELECT
    if (fromNode) {
        plan = processFromClause(fromNode);
    }

    if (whereNode && plan) {
        plan = processWhereClause(plan, whereNode);
    }

    if (selectNode && plan) {
        plan = processSelectList(plan, selectNode);
    }

    bool distinct = (selectNode && selectNode->value == "DISTINCT");
    if (distinct && plan) {
        auto distinctNode = std::make_shared<RelAlgNode>(RelAlgOpType::kDistinct,
            "Distinct output");
        distinctNode->addChild(plan);
        plan = distinctNode;
    }

    if (orderNode && plan) {
        std::string clause;
        for (std::size_t i = 0; i < orderNode->children.size(); ++i) {
            if (i > 0) clause += ", ";
            clause += orderNode->children[i]->value;
        }
        auto sortNode = std::make_shared<RelAlgNode>(RelAlgOpType::kSort, "Order by");
        sortNode->orderByClause = clause;
        sortNode->condition = clause;
        sortNode->addChild(plan);
        plan = sortNode;
    }

    return plan;
}

std::shared_ptr<RelAlgNode> LogicalPlanGenerator::processFromClause(
    std::shared_ptr<ASTNode> node) {

    std::shared_ptr<RelAlgNode> current;

    auto makeScan = [](const std::string& table) {
        auto scan = std::make_shared<RelAlgNode>(RelAlgOpType::kScan,
            "Scan table " + table);
        scan->tableName = table;
        return scan;
    };

    for (const auto& child : node->children) {
        if (child->nodeType == ASTNodeType::TABLE_REF) {
            auto scan = makeScan(child->value);
            if (!current) {
                current = scan;
            } else {
                auto crossProd = std::make_shared<RelAlgNode>(RelAlgOpType::kCrossProduct,
                    "Cross product");
                crossProd->addChild(current);
                crossProd->addChild(scan);
                current = crossProd;
            }
        } else if (child->nodeType == ASTNodeType::JOIN_CLAUSE) {
            if (!current) {
                throw std::runtime_error("JOIN clause without left input");
            }
            if (child->children.empty() || child->children[0]->nodeType != ASTNodeType::TABLE_REF) {
                throw std::runtime_error("JOIN clause missing right table");
            }
            auto rightScan = makeScan(child->children[0]->value);
            std::string cond;
            if (child->children.size() > 1) {
                cond = extractCondition(child->children[1]);
            }
            auto join = std::make_shared<RelAlgNode>(RelAlgOpType::kJoin, "Join");
            join->condition = cond;
            if (child->value == "LEFT") {
                join->joinType = JoinType::kLeft;
            } else if (child->value == "RIGHT") {
                join->joinType = JoinType::kRight;
            } else {
                join->joinType = JoinType::kInner;
            }
            join->addChild(current);
            join->addChild(rightScan);
            current = join;
        }
    }

    if (!current) {
        throw std::runtime_error("No tables in FROM clause");
    }

    return current;
}

std::shared_ptr<RelAlgNode> LogicalPlanGenerator::processWhereClause(
    std::shared_ptr<RelAlgNode> input, std::shared_ptr<ASTNode> whereNode) {

    if (whereNode->children.empty()) return input;

    std::string condition = extractCondition(whereNode->children[0]);

    auto select = std::make_shared<RelAlgNode>(RelAlgOpType::kSelect,
        "Apply filter: " + condition);
    select->condition = condition;
    select->addChild(input);

    return select;
}

std::shared_ptr<RelAlgNode> LogicalPlanGenerator::processSelectList(
    std::shared_ptr<RelAlgNode> input, std::shared_ptr<ASTNode> selectNode) {

    std::vector<std::string> columns;
    bool hasStar = false;

    for (const auto& child : selectNode->children) {
        if (child->nodeType == ASTNodeType::STAR) {
            hasStar = true;
        } else if (child->nodeType == ASTNodeType::COLUMN_REF) {
            columns.push_back(child->value);
        }
    }

    // If SELECT *, no projection needed
    if (hasStar) {
        return input;
    }

    auto project = std::make_shared<RelAlgNode>(RelAlgOpType::kProject,
        "Project columns");
    project->columns = columns;
    project->addChild(input);

    return project;
}

std::string LogicalPlanGenerator::extractCondition(std::shared_ptr<ASTNode> node) {
    if (!node) return "";

    std::ostringstream oss;

    switch (node->nodeType) {
        case ASTNodeType::COMPARISON:
            if (node->children.size() >= 2) {
                oss << extractCondition(node->children[0]) << " "
                    << node->value << " "
                    << extractCondition(node->children[1]);
            }
            break;
        case ASTNodeType::AND_EXPR:
            if (node->children.size() >= 2) {
                oss << "(" << extractCondition(node->children[0]) << " AND "
                    << extractCondition(node->children[1]) << ")";
            }
            break;
        case ASTNodeType::OR_EXPR:
            if (node->children.size() >= 2) {
                oss << "(" << extractCondition(node->children[0]) << " OR "
                    << extractCondition(node->children[1]) << ")";
            }
            break;
        case ASTNodeType::COLUMN_REF:
            oss << node->value;
            break;
        case ASTNodeType::LITERAL:
            // Check if it's a string literal (not a number)
            {
                bool isNumber = !node->value.empty();
                for (char c : node->value) {
                    if (!std::isdigit(c) && c != '.' && c != '-') {
                        isNumber = false;
                        break;
                    }
                }
                if (isNumber) {
                    oss << node->value;
                } else {
                    oss << "'" << node->value << "'";
                }
            }
            break;
        default:
            oss << node->value;
            break;
    }

    return oss.str();
}

// ============== LogicalOptimizer 实现 ==============
std::shared_ptr<RelAlgNode> LogicalOptimizer::optimize(std::shared_ptr<RelAlgNode> plan) {
    if (!plan) return plan;

    // Apply multiple optimization passes
    plan = applyRule(plan);
    plan = pushDownSelection(plan);
    plan = combineSelections(plan);

    return plan;
}

std::shared_ptr<RelAlgNode> LogicalOptimizer::applyRule(std::shared_ptr<RelAlgNode> node) {
    if (!node) return node;

    // Recursively optimize children first
    for (auto& child : node->children) {
        child = applyRule(child);
    }

    return node;
}

std::shared_ptr<RelAlgNode> LogicalOptimizer::pushDownSelection(
    std::shared_ptr<RelAlgNode> node) {

    if (!node) return node;

    // If this is a SELECT over a CROSS_PRODUCT, try to push it down
    if (node->opType == RelAlgOpType::kSelect &&
        !node->children.empty() &&
        node->children[0]->opType == RelAlgOpType::kCrossProduct) {

        // This is a simplification - real optimizer would analyze predicates
        // and push down only those that can be evaluated on single relations

        // For now, we'll convert CROSS_PRODUCT + SELECT to JOIN if possible
        auto join = std::make_shared<RelAlgNode>(RelAlgOpType::kJoin,
            "Join with condition: " + node->condition);
        join->condition = node->condition;
        join->children = node->children[0]->children;

        return join;
    }

    // Recursively optimize children
    for (auto& child : node->children) {
        child = pushDownSelection(child);
    }

    return node;
}

std::shared_ptr<RelAlgNode> LogicalOptimizer::pushDownProjection(
    std::shared_ptr<RelAlgNode> node) {
    // Simplified - not implemented in this version
    return node;
}

std::shared_ptr<RelAlgNode> LogicalOptimizer::combineSelections(
    std::shared_ptr<RelAlgNode> node) {

    if (!node) return node;

    // If this is a SELECT over another SELECT, combine them
    if (node->opType == RelAlgOpType::kSelect &&
        !node->children.empty() &&
        node->children[0]->opType == RelAlgOpType::kSelect) {

        auto combined = std::make_shared<RelAlgNode>(RelAlgOpType::kSelect,
            "Combined selection");
        combined->condition = "(" + node->condition + ") AND (" +
                             node->children[0]->condition + ")";
        combined->children = node->children[0]->children;

        return combined;
    }

    // Recursively optimize children
    for (auto& child : node->children) {
        child = combineSelections(child);
    }

    return node;
}

std::shared_ptr<RelAlgNode> LogicalOptimizer::reorderJoins(
    std::shared_ptr<RelAlgNode> node) {
    // Simplified - not implemented in this version
    return node;
}

// ============== PhysicalPlanGenerator 实现 ==============
PhysicalPlanGenerator::PhysicalPlanGenerator(DatabaseSystem& db) : db_(db) {}

std::shared_ptr<PhysicalPlanNode> PhysicalPlanGenerator::generatePhysicalPlan(
    std::shared_ptr<RelAlgNode> logicalPlan) {

    if (!logicalPlan) return nullptr;

    return convertNode(logicalPlan);
}

std::shared_ptr<PhysicalPlanNode> PhysicalPlanGenerator::convertNode(
    std::shared_ptr<RelAlgNode> node) {

    if (!node) return nullptr;

    std::shared_ptr<PhysicalPlanNode> physNode;

    switch (node->opType) {
        case RelAlgOpType::kScan:
            physNode = chooseScanMethod(node);
            break;

        case RelAlgOpType::kSelect:
            // Attempt to turn simple equality predicate on a single table into an index scan
            if (!node->children.empty() && node->children[0]->opType == RelAlgOpType::kScan) {
                auto equality = extractColumnLiteralEquality(node->condition);
                if (equality) {
                    const std::string table = node->children[0]->tableName;
                    const std::string column = stripTablePrefix(equality->first);
                    auto indexName = db_.findIndexForColumn(table, column);
                    if (indexName) {
                        physNode = std::make_shared<PhysicalPlanNode>(PhysicalOpType::kIndexScan,
                            "Index scan on " + table + " using " + *indexName);
                        physNode->algorithm = "B+ tree equality lookup";
                        physNode->parameters["table"] = table;
                        physNode->parameters["index"] = *indexName;
                        physNode->parameters["key"] = equality->second;
                        physNode->planFlow = "pipeline";
                        physNode->estimatedCost = estimateCost(physNode);
                        return physNode;
                    }
                }
            }

            physNode = std::make_shared<PhysicalPlanNode>(PhysicalOpType::kFilter,
                "Filter: " + node->condition);
            physNode->algorithm = "Predicate evaluation";
            physNode->parameters["condition"] = node->condition;
            physNode->planFlow = "pipeline";
            break;

        case RelAlgOpType::kProject:
            physNode = std::make_shared<PhysicalPlanNode>(PhysicalOpType::kProjection,
                "Project columns");
            physNode->algorithm = "Column extraction";
            physNode->outputColumns = node->columns;
            physNode->planFlow = "pipeline";
            break;

        case RelAlgOpType::kDistinct:
            physNode = std::make_shared<PhysicalPlanNode>(PhysicalOpType::kDistinct,
                "Distinct");
            physNode->algorithm = "Hash-based deduplication";
            physNode->planFlow = "materialized";
            break;

        case RelAlgOpType::kJoin:
            physNode = chooseJoinMethod(node);
            break;

        case RelAlgOpType::kCrossProduct:
            physNode = std::make_shared<PhysicalPlanNode>(PhysicalOpType::kNestedLoopJoin,
                "Cross product");
            physNode->algorithm = "Nested loop (block-based)";
            physNode->joinType = JoinType::kInner;
            physNode->parameters["join_type"] = "INNER";
            physNode->planFlow = "materialized";
            break;

        case RelAlgOpType::kSort:
            physNode = std::make_shared<PhysicalPlanNode>(PhysicalOpType::kSort,
                "Sort results");
            if (!node->orderByClause.empty()) {
                physNode->parameters["order_by"] = node->orderByClause;
            } else if (!node->condition.empty()) {
                physNode->parameters["order_by"] = node->condition;
            }
            physNode->algorithm = "In-memory sort";
            physNode->planFlow = "materialized";
            break;

        default:
            physNode = std::make_shared<PhysicalPlanNode>(PhysicalOpType::kTableScan,
                "Unknown operation");
            physNode->algorithm = "Default";
            break;
    }

    // Convert children recursively
    for (const auto& child : node->children) {
        auto childPhys = convertNode(child);
        if (childPhys) {
            physNode->addChild(childPhys);
        }
    }

    // Estimate cost
    physNode->estimatedCost = estimateCost(physNode);

    return physNode;
}

std::shared_ptr<PhysicalPlanNode> PhysicalPlanGenerator::chooseScanMethod(
    std::shared_ptr<RelAlgNode> node) {

    std::shared_ptr<PhysicalPlanNode> physNode;

    // Check if we have an index on this table
    // For simplicity, always use table scan in this version
    // A real system would check available indexes and choose the best method

    physNode = std::make_shared<PhysicalPlanNode>(PhysicalOpType::kTableScan,
        "Scan table: " + node->tableName);
    physNode->algorithm = "Sequential scan (block-by-block)";
    physNode->parameters["table"] = node->tableName;
    physNode->planFlow = "pipeline";

    try {
        const auto& table = db_.getTable(node->tableName);
        physNode->parameters["blocks"] = std::to_string(table.blockCount());
        physNode->parameters["records"] = std::to_string(table.totalRecords());
    } catch (...) {
        // Table not found
    }

    return physNode;
}

std::shared_ptr<PhysicalPlanNode> PhysicalPlanGenerator::chooseJoinMethod(
    std::shared_ptr<RelAlgNode> node) {

    std::string joinTypeStr = "INNER";
    if (node->joinType == JoinType::kLeft) {
        joinTypeStr = "LEFT";
    } else if (node->joinType == JoinType::kRight) {
        joinTypeStr = "RIGHT";
    }

    if (node->joinType != JoinType::kInner) {
        auto physNode = std::make_shared<PhysicalPlanNode>(PhysicalOpType::kNestedLoopJoin,
            joinTypeStr + " join: " + node->condition);
        physNode->algorithm = "Nested loop (outer join capable)";
        physNode->parameters["condition"] = node->condition;
        physNode->parameters["join_type"] = joinTypeStr;
        physNode->joinType = node->joinType;
        physNode->planFlow = "materialized";
        return physNode;
    }

    auto eqCols = extractJoinColumns(node->condition);
    if (eqCols) {
        auto physNode = std::make_shared<PhysicalPlanNode>(PhysicalOpType::kHashJoin,
            "Hash join: " + node->condition);
        physNode->algorithm = "Hash join";
        physNode->parameters["condition"] = node->condition;
        physNode->parameters["left_key"] = eqCols->first;
        physNode->parameters["right_key"] = eqCols->second;
        physNode->parameters["join_type"] = joinTypeStr;
        physNode->joinType = node->joinType;
        physNode->planFlow = "materialized";
        return physNode;
    }

    auto physNode = std::make_shared<PhysicalPlanNode>(PhysicalOpType::kNestedLoopJoin,
        "Join: " + node->condition);
    physNode->algorithm = "Block nested loop join";
    physNode->parameters["condition"] = node->condition;
    physNode->parameters["join_type"] = joinTypeStr;
    physNode->joinType = node->joinType;
    physNode->planFlow = "materialized";

    return physNode;
}

int PhysicalPlanGenerator::estimateCost(std::shared_ptr<PhysicalPlanNode> node) {
    if (!node) return 0;

    int cost = 0;

    switch (node->opType) {
        case PhysicalOpType::kTableScan:
            // Cost = number of blocks to read
            if (node->parameters.find("blocks") != node->parameters.end()) {
                cost = std::stoi(node->parameters["blocks"]);
            } else {
                cost = 100; // Default estimate
            }
            break;

        case PhysicalOpType::kIndexScan:
            cost = 10; // Index scans are cheaper
            break;

        case PhysicalOpType::kFilter:
        case PhysicalOpType::kProjection:
        case PhysicalOpType::kDistinct:
            cost = 1; // Pipeline operations are cheap
            break;

        case PhysicalOpType::kNestedLoopJoin:
            cost = 1000; // Joins are expensive
            break;

        case PhysicalOpType::kHashJoin:
            cost = 200; // Hash joins are cheaper than nested loops
            break;
        case PhysicalOpType::kSort:
            cost = 150;
            break;

        default:
            cost = 50;
            break;
    }

    // Add cost of children
    for (const auto& child : node->children) {
        cost += estimateCost(child);
    }

    return cost;
}

bool PhysicalPlanGenerator::hasIndex(const std::string& tableName,
                                     const std::string& columnName) {
    return db_.findIndexForColumn(tableName, columnName).has_value();
}

std::optional<std::pair<std::string, std::string>>
PhysicalPlanGenerator::extractColumnLiteralEquality(const std::string& condition) {
    if (condition.empty()) {
        return std::nullopt;
    }
    try {
        ExpressionParser parser;
        auto expr = parser.parse(condition);
        auto cmp = dynamic_cast<ComparisonExpr*>(expr.get());
        if (!cmp || cmp->op() != ComparisonExpr::Op::EQ) {
            return std::nullopt;
        }
        auto leftCol = dynamic_cast<const ColumnRefExpr*>(cmp->left());
        auto rightCol = dynamic_cast<const ColumnRefExpr*>(cmp->right());
        auto leftLit = dynamic_cast<const LiteralExpr*>(cmp->left());
        auto rightLit = dynamic_cast<const LiteralExpr*>(cmp->right());

        if (leftCol && rightLit) {
            return std::make_pair(leftCol->columnName(), rightLit->value().asString());
        }
        if (rightCol && leftLit) {
            return std::make_pair(rightCol->columnName(), leftLit->value().asString());
        }
    } catch (...) {
        // Parsing or evaluation failed; ignore
    }
    return std::nullopt;
}

std::optional<std::pair<std::string, std::string>>
PhysicalPlanGenerator::extractJoinColumns(const std::string& condition) {
    if (condition.empty()) {
        return std::nullopt;
    }
    try {
        ExpressionParser parser;
        auto expr = parser.parse(condition);
        auto cmp = dynamic_cast<ComparisonExpr*>(expr.get());
        if (!cmp || cmp->op() != ComparisonExpr::Op::EQ) {
            return std::nullopt;
        }
        auto leftCol = dynamic_cast<const ColumnRefExpr*>(cmp->left());
        auto rightCol = dynamic_cast<const ColumnRefExpr*>(cmp->right());
        if (leftCol && rightCol) {
            return std::make_pair(leftCol->columnName(), rightCol->columnName());
        }
    } catch (...) {
        // Parsing failed; return no match
    }
    return std::nullopt;
}

std::string PhysicalPlanGenerator::stripTablePrefix(const std::string& name) {
    auto pos = name.find('.');
    if (pos == std::string::npos) {
        return name;
    }
    if (pos + 1 >= name.size()) {
        return name;
    }
    return name.substr(pos + 1);
}

// ============== QueryProcessor 实现 ==============
QueryProcessor::QueryProcessor(DatabaseSystem& db) : db_(db) {}

void QueryProcessor::processQuery(const std::string& sql) {
    std::cout << "\n========================================\n";
    std::cout << "Processing SQL Query:\n" << sql << "\n";
    std::cout << "========================================\n\n";

    try {
        // 1. Lexical Analysis
        std::cout << "==> Step 1: Lexical Analysis (词法分析)\n";
        Lexer lexer(sql);
        auto tokens = lexer.tokenize();

        std::cout << "Tokens generated: " << tokens.size() << "\n";
        for (size_t i = 0; i < tokens.size() && i < 20; ++i) {
            std::cout << "  Token[" << i << "]: type=" << static_cast<int>(tokens[i].type)
                     << ", lexeme=\"" << tokens[i].lexeme << "\"\n";
        }
        std::cout << "\n";

        // 2. Syntax Analysis
        std::cout << "==> Step 2: Syntax Analysis (语法分析)\n";
        Parser parser(tokens);
        lastAST_ = parser.parse();

        std::cout << "Abstract Syntax Tree (AST):\n";
        std::cout << lastAST_->toString() << "\n\n";

        // 3. Semantic Analysis
        std::cout << "==> Step 3: Semantic Analysis (语义分析)\n";
        SemanticAnalyzer analyzer(db_);
        analyzer.analyze(lastAST_);
        std::cout << "Semantic analysis passed - all tables and columns are valid\n\n";

        // 4. Logical Query Plan Generation
        std::cout << "==> Step 4: Logical Query Plan (逻辑查询计划 - 关系代数表达式)\n";
        LogicalPlanGenerator planGen;
        lastLogicalPlan_ = planGen.generateLogicalPlan(lastAST_);

        std::cout << "Initial Logical Plan:\n";
        std::cout << lastLogicalPlan_->toString() << "\n";

        // 5. Logical Query Optimization
        std::cout << "==> Step 5: Optimized Logical Plan (优化后的逻辑计划)\n";
        LogicalOptimizer optimizer;
        lastOptimizedPlan_ = optimizer.optimize(lastLogicalPlan_);

        std::cout << "Optimized Logical Plan:\n";
        std::cout << lastOptimizedPlan_->toString() << "\n";

        // 6. Physical Query Plan Generation
        std::cout << "==> Step 6: Physical Query Plan (物理查询计划)\n";
        PhysicalPlanGenerator physGen(db_);
        lastPhysicalPlan_ = physGen.generatePhysicalPlan(lastOptimizedPlan_);

        std::cout << "Physical Execution Plan:\n";
        std::cout << lastPhysicalPlan_->toString() << "\n";

        // 7. Execute the physical plan
        executePhysicalPlan(lastPhysicalPlan_);

        std::cout << "========================================\n";
        std::cout << "Query processing completed successfully!\n";
        std::cout << "========================================\n\n";

    } catch (const std::exception& ex) {
        std::cout << "\n[ERROR] Query processing failed: " << ex.what() << "\n\n";
    }
}

std::string QueryProcessor::getLastAST() const {
    if (lastAST_) {
        return lastAST_->toString();
    }
    return "[No AST available]";
}

std::string QueryProcessor::getLastLogicalPlan() const {
    if (lastLogicalPlan_) {
        return lastLogicalPlan_->toString();
    }
    return "[No logical plan available]";
}

std::string QueryProcessor::getLastOptimizedPlan() const {
    if (lastOptimizedPlan_) {
        return lastOptimizedPlan_->toString();
    }
    return "[No optimized plan available]";
}

std::string QueryProcessor::getLastPhysicalPlan() const {
    if (lastPhysicalPlan_) {
        return lastPhysicalPlan_->toString();
    }
    return "[No physical plan available]";
}

void QueryProcessor::executePhysicalPlan(std::shared_ptr<PhysicalPlanNode> plan) {
    std::cout << "\n==> Step 7: Query Execution\n";
    std::cout << std::string(60, '-') << "\n";

    try {
        QueryExecutor executor(db_);
        ResultSet results = executor.execute(plan);

        std::cout << "\nQuery executed successfully!\n";
        std::cout << "Rows returned: " << results.size() << "\n\n";

        // Print results
        results.print(std::cout);
    } catch (const std::exception& e) {
        std::cout << "Execution error: " << e.what() << "\n";
    }
}

} // namespace dbms
