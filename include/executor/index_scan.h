#pragma once

#include <optional>
#include <string>

#include "executor/operator.h"
#include "system/database.h"

namespace dbms {

// Index scan operator: performs equality lookup via a B+ tree index
class IndexScanOperator : public Operator {
public:
    IndexScanOperator(DatabaseSystem& db,
                      std::string table,
                      std::string index,
                      std::string key);

    void init() override;
    std::optional<Tuple> next() override;
    void close() override;
    const Schema& getSchema() const override { return schema_; }
    void reset() override;

private:
    DatabaseSystem& db_;
    std::string tableName_;
    std::string indexName_;
    std::string searchKey_;
    Schema schema_;
    bool initialized_{false};
    bool done_{false};

    Schema buildSchemaFromTable(const Table& table);
};

} // namespace dbms
