#pragma once

#include "duckdb.hpp"
#include "duckdb/common/identifier.hpp"

namespace duckdb {
struct TableFunctionBindInput;

namespace ion {

unique_ptr<FunctionData> ReadIonBind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<Identifier> &names);

} // namespace ion
} // namespace duckdb
