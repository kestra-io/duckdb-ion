#pragma once

#include "duckdb.hpp"
#include "ion/ionc_shim.hpp"

namespace duckdb {
namespace ion {

Value IonReadValueImpl(ION_READER *reader, ION_TYPE type);
void SkipIonValueImpl(ION_READER *reader, ION_TYPE type);

} // namespace ion
} // namespace duckdb
