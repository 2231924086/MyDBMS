#pragma once

#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "common/types.h"

namespace dbms {

class DatabaseSystem;

// 词法分析
enum class TokenType {
    // Keywords
    SELECT, FROM, WHERE, AND, OR, NOT, JOIN, ON, INNER, LEFT, RIGHT,
    ORDER, BY, GROUP, HAVING, AS, DISTINCT, ALL,
    INSERT, INTO, VALUES, UPDATE, SET, DELETE,
    // Operators
    EQUAL, NOT_EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL,
    PLUS, MINUS, STAR, SLASH, PERCENT,
    // Delimiters
    COMMA, SEMICOLON, LEFT_PAREN, RIGHT_PAREN, DOT,
    // Literals
    IDENTIFIER, STRING_LITERAL, NUMBER_LITERAL,
    // Special
    END_OF_FILE, UNKNOWN
};

struct Token {
    TokenType type;
    std::string lexeme;
    int line;
    int column;
    Token(TokenType t = TokenType::UNKNOWN, std::string lex = "", int l = 0, int c = 0)
        : type(t), lexeme(std::move(lex)), line(l), column(c) {}
};

// AST
enum class ASTNodeType {
    SELECT_STATEMENT,
    INSERT_STATEMENT,
    UPDATE_STATEMENT,
    DELETE_STATEMENT,
    ORDER_BY,
    SELECT_LIST,
    FROM_CLAUSE,
    WHERE_CLAUSE,
    JOIN_CLAUSE,
    COLUMN_REF,
    TABLE_REF,
    BINARY_OP,
    UNARY_OP,
    LITERAL,
    STAR,
    AND_EXPR,
    OR_EXPR,
    NOT_EXPR,
    COMPARISON
};

struct ASTNode {
    ASTNodeType nodeType;
    std::string value;
    std::vector<std::shared_ptr<ASTNode>> children;
    explicit ASTNode(ASTNodeType type, std::string val = "")
        : nodeType(type), value(std::move(val)) {}
    void addChild(std::shared_ptr<ASTNode> child) { children.push_back(std::move(child)); }
    std::string toString(int indent = 0) const;
};

// 关系代数节点
enum class RelAlgOpType {
    kScan,
    kSelect,
    kProject,
    kDistinct,
    kJoin,
    kCrossProduct,
    kUnion,
    kIntersect,
    kDifference,
    kSort,
    kGroup
};

struct RelAlgNode {
    RelAlgOpType opType;
    std::string operationDesc;
    std::vector<std::string> columns;
    std::string tableName;
    std::string condition;
    JoinType joinType{JoinType::kInner};
    std::string orderByClause;
    std::vector<std::shared_ptr<RelAlgNode>> children;
    explicit RelAlgNode(RelAlgOpType type, std::string desc = "")
        : opType(type), operationDesc(std::move(desc)) {}
    void addChild(std::shared_ptr<RelAlgNode> child) { children.push_back(std::move(child)); }
    std::string toString(int indent = 0) const;
};

// 物理计划节点
enum class PhysicalOpType {
    kTableScan,
    kIndexScan,
    kFilter,
    kProjection,
    kDistinct,
    kNestedLoopJoin,
    kHashJoin,
    kMergeJoin,
    kSort,
    kAggregate
};

struct PhysicalPlanNode {
    PhysicalOpType opType;
    std::string description;
    std::string algorithm;
    std::string planFlow;
    int estimatedCost;
    std::vector<std::string> outputColumns;
    std::map<std::string, std::string> parameters;
    JoinType joinType{JoinType::kInner};
    std::vector<std::shared_ptr<PhysicalPlanNode>> children;
    explicit PhysicalPlanNode(PhysicalOpType type, std::string desc = "")
        : opType(type), description(std::move(desc)), algorithm(""), planFlow("pipeline"), estimatedCost(0) {}
    void addChild(std::shared_ptr<PhysicalPlanNode> child) { children.push_back(std::move(child)); }
    std::string toString(int indent = 0) const;
};

// Lexer
class Lexer {
public:
    explicit Lexer(std::string input);
    std::vector<Token> tokenize();
private:
    std::string input_;
    size_t position_;
    int line_;
    int column_;
    char currentChar() const;
    char peekChar() const;
    void advance();
    void skipWhitespace();
    Token readIdentifierOrKeyword();
    Token readNumber();
    Token readString();
    TokenType getKeywordType(const std::string& word) const;
};

// Parser
class Parser {
public:
    explicit Parser(std::vector<Token> tokens);
    std::shared_ptr<ASTNode> parse();
private:
    std::vector<Token> tokens_;
    size_t current_;
    Token currentToken() const;
    Token peek(int offset = 1) const;
    bool match(TokenType type);
    bool check(TokenType type) const;
    Token advance();
    Token consume(TokenType type, const std::string& message);
    std::shared_ptr<ASTNode> parseStatement();
    std::shared_ptr<ASTNode> parseSelectStatement();
    std::shared_ptr<ASTNode> parseInsertStatement();
    std::shared_ptr<ASTNode> parseUpdateStatement();
    std::shared_ptr<ASTNode> parseDeleteStatement();
    std::shared_ptr<ASTNode> parseSelectList();
    std::shared_ptr<ASTNode> parseOrderByClause();
    std::shared_ptr<ASTNode> parseFromClause();
    std::shared_ptr<ASTNode> parseWhereClause();
    std::shared_ptr<ASTNode> parseExpression();
    std::shared_ptr<ASTNode> parseOrExpression();
    std::shared_ptr<ASTNode> parseAndExpression();
    std::shared_ptr<ASTNode> parseComparisonExpression();
    std::shared_ptr<ASTNode> parseAdditiveExpression();
    std::shared_ptr<ASTNode> parseMultiplicativeExpression();
    std::shared_ptr<ASTNode> parsePrimaryExpression();
    std::string parseQualifiedIdentifier();
};

// Semantic Analyzer
class SemanticAnalyzer {
public:
    explicit SemanticAnalyzer(DatabaseSystem& db);
    void analyze(std::shared_ptr<ASTNode> ast);
private:
    DatabaseSystem& db_;
    std::set<std::string> availableTables_;
    std::map<std::string, std::vector<std::string>> tableColumns_;
    void analyzeNode(std::shared_ptr<ASTNode> node);
    void validateTable(const std::string& tableName);
    void validateColumn(const std::string& tableName, const std::string& columnName);
    void collectTableInfo(std::shared_ptr<ASTNode> node);
};

// Logical Plan Generator
class LogicalPlanGenerator {
public:
    std::shared_ptr<RelAlgNode> generateLogicalPlan(std::shared_ptr<ASTNode> ast);
private:
    std::shared_ptr<RelAlgNode> processSelectStatement(std::shared_ptr<ASTNode> node);
    std::shared_ptr<RelAlgNode> processFromClause(std::shared_ptr<ASTNode> node);
    std::shared_ptr<RelAlgNode> processWhereClause(std::shared_ptr<RelAlgNode> input, std::shared_ptr<ASTNode> whereNode);
    std::shared_ptr<RelAlgNode> processSelectList(std::shared_ptr<RelAlgNode> input, std::shared_ptr<ASTNode> selectNode);
    std::string extractCondition(std::shared_ptr<ASTNode> node);
};

// Logical Optimizer
class LogicalOptimizer {
public:
    std::shared_ptr<RelAlgNode> optimize(std::shared_ptr<RelAlgNode> plan);
private:
    std::shared_ptr<RelAlgNode> pushDownSelection(std::shared_ptr<RelAlgNode> node);
    std::shared_ptr<RelAlgNode> pushDownProjection(std::shared_ptr<RelAlgNode> node);
    std::shared_ptr<RelAlgNode> combineSelections(std::shared_ptr<RelAlgNode> node);
    std::shared_ptr<RelAlgNode> reorderJoins(std::shared_ptr<RelAlgNode> node);
    std::shared_ptr<RelAlgNode> applyRule(std::shared_ptr<RelAlgNode> node);
};

// Physical Plan Generator
class PhysicalPlanGenerator {
public:
    explicit PhysicalPlanGenerator(DatabaseSystem& db);
    std::shared_ptr<PhysicalPlanNode> generatePhysicalPlan(std::shared_ptr<RelAlgNode> logicalPlan);
private:
    DatabaseSystem& db_;
    std::shared_ptr<PhysicalPlanNode> convertNode(std::shared_ptr<RelAlgNode> node);
    std::shared_ptr<PhysicalPlanNode> chooseScanMethod(std::shared_ptr<RelAlgNode> node);
    std::shared_ptr<PhysicalPlanNode> chooseJoinMethod(std::shared_ptr<RelAlgNode> node);
    int estimateCost(std::shared_ptr<PhysicalPlanNode> node);
    bool hasIndex(const std::string& tableName, const std::string& columnName);
    std::optional<std::pair<std::string, std::string>> extractColumnLiteralEquality(const std::string& condition);
    std::optional<std::pair<std::string, std::string>> extractJoinColumns(const std::string& condition);
    static std::string stripTablePrefix(const std::string& name);
};

// Query Processor
class QueryProcessor {
public:
    explicit QueryProcessor(DatabaseSystem& db);
    void processQuery(const std::string& sql);
    std::string getLastAST() const;
    std::string getLastLogicalPlan() const;
    std::string getLastOptimizedPlan() const;
    std::string getLastPhysicalPlan() const;
private:
    DatabaseSystem& db_;
    std::shared_ptr<ASTNode> lastAST_;
    std::shared_ptr<RelAlgNode> lastLogicalPlan_;
    std::shared_ptr<RelAlgNode> lastOptimizedPlan_;
    std::shared_ptr<PhysicalPlanNode> lastPhysicalPlan_;
    void executePhysicalPlan(std::shared_ptr<PhysicalPlanNode> plan);
};

} // namespace dbms
