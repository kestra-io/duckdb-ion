#include "ion/read_ion.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/enums/on_create_conflict.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "ion/read_ion_bind.hpp"
#include "ion/read_ion_scan.hpp"

namespace duckdb {
namespace ion {

void RegisterReadIon(ExtensionLoader &loader) {
	TableFunction read_ion("read_ion", {LogicalType::VARCHAR}, ReadIonFunction, ReadIonBind, ReadIonInit,
	                       ReadIonInitLocal);
	read_ion.GetSignature().GetParameter(0).SetName(Identifier("path"));
	read_ion.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("columns", LogicalType::ANY)
		    .Add("format", LogicalType::VARCHAR)
		    .Add("records", LogicalType::ANY)
		    .Add("maximum_depth", LogicalType::BIGINT)
		    .Add("field_appearance_threshold", LogicalType::DOUBLE)
		    .Add("map_inference_threshold", LogicalType::BIGINT)
		    .Add("sample_size", LogicalType::BIGINT)
		    .Add("maximum_sample_files", LogicalType::BIGINT)
		    .Add("union_by_name", LogicalType::BOOLEAN)
		    .Add("conflict_mode", LogicalType::VARCHAR)
		    .Add("profile", LogicalType::BOOLEAN)
		    .Add("use_extractor", LogicalType::BOOLEAN);
	});
	read_ion.projection_pushdown = true;
	read_ion.filter_pushdown = false;
	read_ion.filter_prune = false;

	CreateTableFunctionInfo info(MultiFileReader::CreateFunctionSet(read_ion));
	FunctionDescription description;
	description.parameter_names = {"path"};
	description.description =
	    "Read Ion text or binary files into DuckDB rows, inferring column types unless a schema is provided.";
	description.examples = {"SELECT * FROM read_ion('test/ion/sample.ion')"};
	description.categories = {"data import"};
	info.descriptions.push_back(std::move(description));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	loader.RegisterFunction(std::move(info));
}

} // namespace ion
} // namespace duckdb
