#pragma once

namespace duckdb {
class ExtensionLoader;
}

namespace duckdb {
namespace ion {

void RegisterReadIon(ExtensionLoader &loader);

} // namespace ion
} // namespace duckdb
