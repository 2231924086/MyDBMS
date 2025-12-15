#pragma once

#include <optional>

#include "executor/schema.h"

namespace dbms {

// Abstract base class for all query operators (Volcano model)
class Operator {
public:
    virtual ~Operator() = default;

    // Initialize operator and allocate resources
    virtual void init() = 0;

    // Get next tuple/record (returns nullopt when exhausted)
    virtual std::optional<Tuple> next() = 0;

    // Release resources and cleanup
    virtual void close() = 0;

    // Get output schema for this operator
    virtual const Schema& getSchema() const = 0;

    // Reset iterator to beginning (for reusable operators)
    virtual void reset() = 0;
};

} // namespace dbms
