#include "executor/result_set.h"

#include <iomanip>
#include <sstream>

namespace dbms {

void ResultSet::print(std::ostream& os) const {
    if (!schema_ || schema_->columnCount() == 0) {
        os << "(No schema)\n";
        return;
    }

    if (tuples_.empty()) {
        os << "(No results)\n";
        return;
    }

    // Calculate column widths
    std::vector<std::size_t> columnWidths;
    for (std::size_t i = 0; i < schema_->columnCount(); ++i) {
        const auto& col = schema_->getColumn(i);
        std::size_t maxWidth = col.name.size();

        for (const auto& tuple : tuples_) {
            if (i < tuple.values.size()) {
                maxWidth = std::max(maxWidth, tuple.values[i].size());
            }
        }

        columnWidths.push_back(std::max(maxWidth, static_cast<std::size_t>(10)));
    }

    // Print header
    os << "+";
    for (auto width : columnWidths) {
        os << std::string(width + 2, '-') << "+";
    }
    os << "\n|";

    for (std::size_t i = 0; i < schema_->columnCount(); ++i) {
        const auto& col = schema_->getColumn(i);
        os << " " << std::left << std::setw(columnWidths[i]) << col.name << " |";
    }
    os << "\n+";

    for (auto width : columnWidths) {
        os << std::string(width + 2, '-') << "+";
    }
    os << "\n";

    // Print rows
    for (const auto& tuple : tuples_) {
        os << "|";
        for (std::size_t i = 0; i < schema_->columnCount(); ++i) {
            std::string value = (i < tuple.values.size()) ? tuple.values[i] : "NULL";
            os << " " << std::left << std::setw(columnWidths[i]) << value << " |";
        }
        os << "\n";
    }

    // Print footer
    os << "+";
    for (auto width : columnWidths) {
        os << std::string(width + 2, '-') << "+";
    }
    os << "\n";

    os << "(" << tuples_.size() << " row" << (tuples_.size() == 1 ? "" : "s") << ")\n";
}

} // namespace dbms
