#pragma once

#include <iostream>
#include <memory>
#include <vector>

#include "executor/schema.h"

namespace dbms {

// Query result set container
class ResultSet {
public:
    ResultSet() = default;
    explicit ResultSet(std::shared_ptr<Schema> schema) : schema_(std::move(schema)) {}

    // Add a tuple to the result set
    void addTuple(Tuple tuple) { tuples_.push_back(std::move(tuple)); }

    // Get number of tuples
    std::size_t size() const { return tuples_.size(); }

    // Check if empty
    bool empty() const { return tuples_.empty(); }

    // Get schema
    const Schema& getSchema() const {
        if (!schema_) {
            throw std::logic_error("result set has no schema");
        }
        return *schema_;
    }

    std::shared_ptr<Schema> getSchemaPtr() const { return schema_; }

    // Get tuple by index
    const Tuple& getTuple(std::size_t index) const {
        if (index >= tuples_.size()) {
            throw std::out_of_range("tuple index out of range");
        }
        return tuples_[index];
    }

    // Iterator support
    auto begin() const { return tuples_.begin(); }
    auto end() const { return tuples_.end(); }
    auto begin() { return tuples_.begin(); }
    auto end() { return tuples_.end(); }

    // Display helper
    void print(std::ostream& os) const;

private:
    std::shared_ptr<Schema> schema_;
    std::vector<Tuple> tuples_;
};

} // namespace dbms
