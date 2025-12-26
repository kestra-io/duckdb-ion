#define DUCKDB_EXTENSION_MAIN

#include "ion_extension.hpp"
#include "ion_copy.hpp"
#include "ion_serialize.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/vector.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/common/helper.hpp"
#include <chrono>
#include <cstring>
#include <iostream>
#include <mutex>

#ifdef DUCKDB_IONC
#include <ionc/ion.h>
#include <ionc/ion_decimal.h>
#include <ionc/ion_extractor.h>
#include <ionc/ion_stream.h>
#include <ionc/ion_timestamp.h>
#include <decNumber/decQuad.h>
#endif
#include <cstdio>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

struct IonReadBindData : public TableFunctionData {
	string path;
	vector<LogicalType> return_types;
	vector<string> names;
	unordered_map<string, idx_t> name_map;
	enum class Format { AUTO, NEWLINE_DELIMITED, ARRAY, UNSTRUCTURED };
	enum class RecordsMode { AUTO, ENABLED, DISABLED };
	Format format = Format::AUTO;
	RecordsMode records_mode = RecordsMode::AUTO;
	bool records = true;
	bool profile = false;
};

struct IonStreamState {
	unique_ptr<FileHandle> handle;
	QueryContext query_context;
	vector<BYTE> buffer;
	idx_t offset = 0;
	idx_t end_offset = 0;
	bool bounded = false;
};

struct IonReadScanState {
#ifdef DUCKDB_IONC
	ION_READER *reader = nullptr;
	ION_READER_OPTIONS reader_options;
	IonStreamState stream_state;
	unordered_map<SID, idx_t> sid_map;
	hEXTRACTOR extractor = nullptr;
	vector<idx_t> extractor_cols;
	bool extractor_ready = false;
#endif
	bool finished = false;
	bool array_initialized = false;
	bool reader_initialized = false;
	struct Timing {
		uint64_t next_nanos = 0;
		uint64_t value_nanos = 0;
		uint64_t struct_nanos = 0;
		uint64_t rows = 0;
		uint64_t fields = 0;
		bool reported = false;
	} timing;
	vector<uint32_t> seen;
	uint32_t row_counter = 0;

	void ResetReader() {
#ifdef DUCKDB_IONC
		if (reader) {
			ion_reader_close(reader);
			reader = nullptr;
		}
#endif
		reader_initialized = false;
		array_initialized = false;
		finished = false;
#ifdef DUCKDB_IONC
		sid_map.clear();
#endif
	}

	~IonReadScanState() {
#ifdef DUCKDB_IONC
		if (reader) {
			ion_reader_close(reader);
		}
		if (extractor) {
			ion_extractor_close(extractor);
		}
		if (stream_state.handle) {
			stream_state.handle->Close();
		}
#endif
	}
};

struct IonReadGlobalState : public GlobalTableFunctionState {
	IonReadScanState scan_state;
	mutex lock;
	idx_t next_offset = 0;
	idx_t file_size = 0;
	idx_t chunk_size = 4 * 1024 * 1024;
	idx_t max_threads = 1;
	bool parallel_enabled = false;
	vector<column_t> column_ids;
	idx_t inflight_ranges = 0;
	IonReadScanState::Timing aggregate_timing;
	idx_t projected_columns = 0;

	idx_t MaxThreads() const override {
		return max_threads;
	}
};

struct IonReadLocalState : public LocalTableFunctionState {
	IonReadScanState scan_state;
	bool range_assigned = false;
};

static iERR IonStreamHandler(struct _ion_user_stream *pstream) {
	if (!pstream || !pstream->handler_state) {
		return IERR_EOF;
	}
	auto state = static_cast<IonStreamState *>(pstream->handler_state);
	if (!state->handle) {
		pstream->limit = nullptr;
		return IERR_EOF;
	}
	if (state->buffer.empty()) {
		state->buffer.resize(64 * 1024);
	}
	auto remaining = state->bounded ? (state->end_offset > state->offset ? state->end_offset - state->offset : 0)
	                                : state->buffer.size();
	if (state->bounded && remaining == 0) {
		pstream->limit = nullptr;
		return IERR_EOF;
	}
	auto read_size = state->bounded ? MinValue<idx_t>(state->buffer.size(), remaining) : state->buffer.size();
	auto read = state->handle->Read(state->query_context, state->buffer.data(), read_size);
	if (read <= 0) {
		pstream->limit = nullptr;
		return IERR_EOF;
	}
	state->offset += static_cast<idx_t>(read);
	pstream->curr = state->buffer.data();
	pstream->limit = pstream->curr + read;
	return IERR_OK;
}

static void InferIonSchema(const string &path, vector<string> &names, vector<LogicalType> &types,
                           IonReadBindData::Format format, IonReadBindData::RecordsMode records_mode,
                           bool &records_out, ClientContext &context);
static void ParseColumnsParameter(ClientContext &context, const Value &value, vector<string> &names,
                                  vector<LogicalType> &types);
static void ParseFormatParameter(const Value &value, IonReadBindData::Format &format);
static void ParseRecordsParameter(const Value &value, IonReadBindData::RecordsMode &records_mode);
static void ParseProfileParameter(const Value &value, bool &profile);

static unique_ptr<FunctionData> IonReadBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1 || input.inputs[0].IsNull()) {
		throw InvalidInputException("read_ion expects a single, non-null file path");
	}
	auto bind_data = make_uniq<IonReadBindData>();
	bind_data->path = StringValue::Get(input.inputs[0].CastAs(context, LogicalType::VARCHAR));
	for (auto &kv : input.named_parameters) {
		if (kv.second.IsNull()) {
			throw BinderException("read_ion does not allow NULL named parameters");
		}
		auto option = StringUtil::Lower(kv.first);
		if (option == "columns") {
			ParseColumnsParameter(context, kv.second, bind_data->names, bind_data->return_types);
		} else if (option == "format") {
			ParseFormatParameter(kv.second, bind_data->format);
		} else if (option == "records") {
			ParseRecordsParameter(kv.second, bind_data->records_mode);
		} else if (option == "profile") {
			ParseProfileParameter(kv.second, bind_data->profile);
		} else {
			throw BinderException("read_ion does not support named parameter \"%s\"", kv.first);
		}
	}
	if (bind_data->records_mode == IonReadBindData::RecordsMode::DISABLED && !bind_data->names.empty()) {
		throw BinderException("read_ion cannot use \"columns\" when records=false");
	}
	if (bind_data->names.empty()) {
		InferIonSchema(bind_data->path, bind_data->names, bind_data->return_types, bind_data->format,
		               bind_data->records_mode, bind_data->records, context);
	} else {
		bind_data->records = true;
	}
	for (idx_t i = 0; i < bind_data->names.size(); i++) {
		bind_data->name_map.emplace(bind_data->names[i], i);
	}
	return_types = bind_data->return_types;
	names = bind_data->names;
	return std::move(bind_data);
}

static LogicalType PromoteIonType(const LogicalType &existing, const LogicalType &incoming);
static LogicalType NormalizeInferredIonType(const LogicalType &type);
static Value IonReadValue(ION_READER *reader, ION_TYPE type);
static bool ReadIonValueToVector(ION_READER *reader, ION_TYPE field_type, Vector &vector, idx_t row,
                                 const LogicalType &target_type);
static iERR IonExtractorCallback(hREADER reader, hPATH matched_path, void *user_context,
                                 ION_EXTRACTOR_CONTROL *p_control);
#ifdef DUCKDB_IONC
static iERR IonSymbolTableChanged(void *context, ION_COLLECTION *imports);
#endif

static void EnsureIonExtractor(IonReadScanState &scan_state, const IonReadBindData &bind_data,
                               const vector<idx_t> &projected_cols) {
#ifdef DUCKDB_IONC
	if (scan_state.extractor_ready) {
		return;
	}
	if (projected_cols.empty()) {
		return;
	}
	ION_EXTRACTOR_OPTIONS options = {};
	options.match_relative_paths = true;
	if (ion_extractor_open(&scan_state.extractor, &options) != IERR_OK) {
		scan_state.extractor = nullptr;
		return;
	}
	scan_state.extractor_cols = projected_cols;
	for (auto &col_idx : scan_state.extractor_cols) {
		hPATH path = nullptr;
		if (ion_extractor_path_create(scan_state.extractor, 1, IonExtractorCallback, &col_idx, &path) != IERR_OK) {
			ion_extractor_close(scan_state.extractor);
			scan_state.extractor = nullptr;
			return;
		}
		ION_STRING field_name;
		field_name.value = reinterpret_cast<BYTE *>(const_cast<char *>(bind_data.names[col_idx].data()));
		field_name.length = bind_data.names[col_idx].size();
		if (ion_extractor_path_append_field(path, &field_name) != IERR_OK) {
			ion_extractor_close(scan_state.extractor);
			scan_state.extractor = nullptr;
			return;
		}
	}
	scan_state.extractor_ready = true;
#endif
}

static unique_ptr<GlobalTableFunctionState> IonReadInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<IonReadBindData>();
	auto result = make_uniq<IonReadGlobalState>();
#ifndef DUCKDB_IONC
	throw InvalidInputException("read_ion requires ion-c; rebuild with ion-c available");
#else
	auto &fs = FileSystem::GetFileSystem(context);
	result->scan_state.stream_state.handle = fs.OpenFile(bind_data.path, FileFlags::FILE_FLAGS_READ);
	result->scan_state.stream_state.query_context = QueryContext(context);
	result->scan_state.reader_options = {};
	result->scan_state.reader_options.skip_character_validation = TRUE;
	result->file_size = result->scan_state.stream_state.handle->GetFileSize();
	result->column_ids = input.column_ids;
	for (auto col_id : result->column_ids) {
		if (col_id != DConstants::INVALID_INDEX) {
			result->projected_columns++;
		}
	}
	result->parallel_enabled = bind_data.format == IonReadBindData::Format::NEWLINE_DELIMITED && bind_data.records &&
	                           result->scan_state.stream_state.handle->CanSeek();
	if (result->parallel_enabled) {
		auto &scheduler = TaskScheduler::GetScheduler(context);
		result->max_threads = MaxValue<idx_t>(1, scheduler.NumberOfThreads());
	} else {
		result->max_threads = 1;
	}
#endif
	return std::move(result);
}

static idx_t FindNextNewline(FileHandle &handle, QueryContext &context, idx_t start, idx_t file_size) {
	const idx_t buffer_size = 64 * 1024;
	vector<char> buffer(buffer_size);
	idx_t offset = start;
	while (offset < file_size) {
		auto to_read = MinValue<idx_t>(buffer_size, file_size - offset);
		handle.Read(context, buffer.data(), to_read, offset);
		for (idx_t i = 0; i < to_read; i++) {
			if (buffer[i] == '\n') {
				return offset + i + 1;
			}
		}
		offset += to_read;
	}
	return file_size;
}

static bool AssignIonRange(IonReadGlobalState &global_state, idx_t &start, idx_t &end) {
	lock_guard<mutex> guard(global_state.lock);
	if (global_state.next_offset >= global_state.file_size) {
		return false;
	}
	start = global_state.next_offset;
	auto provisional_end = MinValue<idx_t>(global_state.file_size, start + global_state.chunk_size);
	if (provisional_end >= global_state.file_size) {
		end = global_state.file_size;
	} else {
		end = FindNextNewline(*global_state.scan_state.stream_state.handle,
		                      global_state.scan_state.stream_state.query_context, provisional_end,
		                      global_state.file_size);
	}
	global_state.next_offset = end;
	return start < end;
}

static bool InitializeIonRange(IonReadGlobalState &global_state, IonReadLocalState &local_state) {
	idx_t start = 0;
	idx_t end = 0;
	if (!AssignIonRange(global_state, start, end)) {
		return false;
	}
	auto &scan_state = local_state.scan_state;
	scan_state.ResetReader();
	scan_state.stream_state.offset = start;
	scan_state.stream_state.end_offset = end;
	scan_state.stream_state.bounded = true;
	scan_state.stream_state.handle->Seek(start);
	local_state.range_assigned = true;
	return true;
}

static void ReportProfile(IonReadGlobalState &global_state, IonReadScanState &scan_state) {
	lock_guard<mutex> guard(global_state.lock);
	global_state.aggregate_timing.next_nanos += scan_state.timing.next_nanos;
	global_state.aggregate_timing.value_nanos += scan_state.timing.value_nanos;
	global_state.aggregate_timing.struct_nanos += scan_state.timing.struct_nanos;
	global_state.aggregate_timing.rows += scan_state.timing.rows;
	global_state.aggregate_timing.fields += scan_state.timing.fields;
	if (global_state.inflight_ranges > 0) {
		global_state.inflight_ranges--;
	}
	const bool done = !global_state.parallel_enabled ||
	                  (global_state.next_offset >= global_state.file_size && global_state.inflight_ranges == 0);
	if (done && !global_state.aggregate_timing.reported) {
		global_state.aggregate_timing.reported = true;
		auto to_ms = [](uint64_t nanos) { return static_cast<double>(nanos) / 1000000.0; };
		std::cout << "read_ion aggregate timing: rows=" << global_state.aggregate_timing.rows
		          << " fields=" << global_state.aggregate_timing.fields
		          << " next_ms=" << to_ms(global_state.aggregate_timing.next_nanos)
		          << " value_ms=" << to_ms(global_state.aggregate_timing.value_nanos)
		          << " struct_ms=" << to_ms(global_state.aggregate_timing.struct_nanos)
		          << " projected_columns=" << global_state.projected_columns << std::endl;
	}
}

static unique_ptr<LocalTableFunctionState> IonReadInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                            GlobalTableFunctionState *global_state_p) {
	if (!global_state_p) {
		return nullptr;
	}
	auto &global_state = global_state_p->Cast<IonReadGlobalState>();
	if (!global_state.parallel_enabled) {
		return nullptr;
	}
	auto &bind_data = input.bind_data->Cast<IonReadBindData>();
	auto result = make_uniq<IonReadLocalState>();
#ifndef DUCKDB_IONC
	throw InvalidInputException("read_ion requires ion-c; rebuild with ion-c available");
#else
	auto &fs = FileSystem::GetFileSystem(context.client);
	result->scan_state.stream_state.handle = fs.OpenFile(bind_data.path, FileFlags::FILE_FLAGS_READ);
	result->scan_state.stream_state.query_context = QueryContext(context.client);
	result->scan_state.reader_options = global_state.scan_state.reader_options;
	if (!InitializeIonRange(global_state, *result)) {
		result->scan_state.finished = true;
	} else {
		lock_guard<mutex> guard(global_state.lock);
		global_state.inflight_ranges++;
	}
#endif
	return std::move(result);
}

static inline bool IonStringEquals(const ION_STRING &ion_str, const string &value) {
	if (!ion_str.value) {
		return false;
	}
	if (ion_str.length != value.size()) {
		return false;
	}
	return std::memcmp(ion_str.value, value.data(), ion_str.length) == 0;
}

static inline void SkipIonValue(ION_READER *reader, ION_TYPE type) {
	(void)reader;
	(void)type;
}

struct IonExtractorMatchContext {
	IonReadScanState *scan_state = nullptr;
	const IonReadBindData *bind_data = nullptr;
	DataChunk *output = nullptr;
	const vector<idx_t> *column_to_output = nullptr;
	idx_t row = 0;
	idx_t remaining = 0;
	bool profile = false;
};

static thread_local IonExtractorMatchContext *extractor_context = nullptr;

static iERR IonExtractorCallback(hREADER reader, hPATH matched_path, void *user_context,
                                 ION_EXTRACTOR_CONTROL *p_control) {
	(void)matched_path;
	if (!extractor_context || !extractor_context->scan_state || !extractor_context->bind_data ||
	    !extractor_context->output || !extractor_context->column_to_output) {
		return IERR_INVALID_ARG;
	}
	auto &ctx = *extractor_context;
	auto col_idx_ptr = static_cast<idx_t *>(user_context);
	if (!col_idx_ptr) {
		return IERR_INVALID_ARG;
	}
	auto col_idx = *col_idx_ptr;
	auto out_idx = ctx.column_to_output->empty() ? col_idx : (*ctx.column_to_output)[col_idx];
	if (out_idx == DConstants::INVALID_INDEX) {
		return IERR_OK;
	}
	ION_TYPE type = tid_NULL;
	if (ion_reader_get_type(reader, &type) != IERR_OK) {
		return IERR_INVALID_ARG;
	}
	auto &vec = ctx.output->data[out_idx];
	auto target_type = ctx.bind_data->return_types[col_idx];
	auto value_start = ctx.profile ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
	auto value = IonReadValue(reader, type);
	if (!value.IsNull()) {
		ctx.output->SetValue(out_idx, ctx.row, value.DefaultCastAs(target_type));
	}
	if (ctx.profile) {
		auto elapsed = std::chrono::steady_clock::now() - value_start;
		ctx.scan_state->timing.value_nanos +=
		    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
	}
	if (ctx.remaining > 0) {
		auto marker = ctx.scan_state->row_counter;
		if (ctx.scan_state->seen[col_idx] != marker) {
			ctx.scan_state->seen[col_idx] = marker;
			ctx.remaining--;
		}
	}
	if (ctx.remaining == 0) {
		*p_control = ion_extractor_control_step_out(1);
	} else {
		*p_control = ion_extractor_control_next();
	}
	return IERR_OK;
}

static bool IonDecimalToHugeint(const ION_DECIMAL &decimal, hugeint_t &result, uint8_t width, uint8_t scale) {
	std::vector<char> buffer(ION_DECIMAL_STRLEN(&decimal) + 1);
	if (ion_decimal_to_string(&decimal, buffer.data()) != IERR_OK) {
		return false;
	}
	auto decimal_str = string(buffer.data());
	CastParameters parameters(false, nullptr);
	return TryCastToDecimal::Operation(string_t(decimal_str), result, parameters, width, scale);
}

static bool IonFractionToMicros(decQuad &fraction, int32_t &micros) {
	decContext ctx;
	decContextDefault(&ctx, DEC_INIT_DECQUAD);
	decQuad scale;
	decQuadFromUInt32(&scale, 1000000);
	decQuad scaled;
	decQuadMultiply(&scaled, &fraction, &scale, &ctx);
	if (!decQuadIsFinite(&scaled) || decQuadIsNegative(&scaled)) {
		return false;
	}
	micros = decQuadToInt32(&scaled, &ctx, DEC_ROUND_DOWN);
	return micros >= 0 && micros <= 1000000;
}

static bool IonTimestampToDuckDB(ION_TIMESTAMP &timestamp, timestamp_t &result) {
	int precision = 0;
	if (ion_timestamp_get_precision(&timestamp, &precision) != IERR_OK) {
		return false;
	}
	if (precision < ION_TS_DAY) {
		return false;
	}
	int year = 0;
	int month = 0;
	int day = 0;
	int hour = 0;
	int minute = 0;
	int second = 0;
	decQuad fraction;
	decQuadZero(&fraction);
	if (precision >= ION_TS_FRAC) {
		if (ion_timestamp_get_thru_fraction(&timestamp, &year, &month, &day, &hour, &minute, &second, &fraction) !=
		    IERR_OK) {
			return false;
		}
	} else if (precision >= ION_TS_SEC) {
		if (ion_timestamp_get_thru_second(&timestamp, &year, &month, &day, &hour, &minute, &second) != IERR_OK) {
			return false;
		}
	} else if (precision >= ION_TS_MIN) {
		if (ion_timestamp_get_thru_minute(&timestamp, &year, &month, &day, &hour, &minute) != IERR_OK) {
			return false;
		}
	} else {
		if (ion_timestamp_get_thru_day(&timestamp, &year, &month, &day) != IERR_OK) {
			return false;
		}
	}

	date_t date;
	if (!Date::TryFromDate(year, month, day, date)) {
		return false;
	}

	int32_t micros = 0;
	if (precision >= ION_TS_FRAC) {
		if (!IonFractionToMicros(fraction, micros)) {
			return false;
		}
	}
	if (!Time::IsValidTime(hour, minute, second, micros)) {
		return false;
	}

	auto time = Time::FromTime(hour, minute, second, micros);
	if (!Timestamp::TryFromDatetime(date, time, result)) {
		return false;
	}

	BOOL has_offset = FALSE;
	if (ion_timestamp_has_local_offset(&timestamp, &has_offset) != IERR_OK) {
		return false;
	}
	if (has_offset) {
		int offset_minutes = 0;
		if (ion_timestamp_get_local_offset(&timestamp, &offset_minutes) != IERR_OK) {
			return false;
		}
		const int64_t delta = int64_t(offset_minutes) * Interval::MICROS_PER_MINUTE;
		if (!TrySubtractOperator::Operation(result.value, delta, result.value)) {
			return false;
		}
	}
	return true;
}

static Value IonReadValue(ION_READER *reader, ION_TYPE type) {
	BOOL is_null = FALSE;
	auto status = ion_reader_is_null(reader, &is_null);
	if (status != IERR_OK) {
		throw IOException("read_ion failed while checking null status");
	}
	if (is_null) {
		return Value();
	}
	if (type == tid_NULL || type == tid_EOF) {
		return Value();
	}
	switch (ION_TYPE_INT(type)) {
	case tid_BOOL_INT: {
		BOOL value = FALSE;
		if (ion_reader_read_bool(reader, &value) != IERR_OK) {
			throw IOException("read_ion failed to read bool");
		}
		return Value::BOOLEAN(value != FALSE);
	}
	case tid_INT_INT: {
		int64_t value = 0;
		if (ion_reader_read_int64(reader, &value) != IERR_OK) {
			throw IOException("read_ion failed to read int");
		}
		return Value::BIGINT(value);
	}
	case tid_FLOAT_INT: {
		double value = 0.0;
		if (ion_reader_read_double(reader, &value) != IERR_OK) {
			throw IOException("read_ion failed to read float");
		}
		return Value::DOUBLE(value);
	}
	case tid_DECIMAL_INT: {
		ION_DECIMAL decimal;
		ion_decimal_zero(&decimal);
		if (ion_reader_read_ion_decimal(reader, &decimal) != IERR_OK) {
			throw IOException("read_ion failed to read decimal");
		}
		hugeint_t decimal_value;
		auto width = Decimal::MAX_WIDTH_DECIMAL;
		auto scale = static_cast<uint8_t>(18);
		if (IonDecimalToHugeint(decimal, decimal_value, width, scale)) {
			ion_decimal_free(&decimal);
			return Value::DECIMAL(decimal_value, width, scale);
		}
		std::vector<char> buffer(ION_DECIMAL_STRLEN(&decimal) + 1);
		ion_decimal_to_string(&decimal, buffer.data());
		ion_decimal_free(&decimal);
		auto decimal_str = string(buffer.data());
		CastParameters parameters(false, nullptr);
		if (TryCastToDecimal::Operation(string_t(decimal_str), decimal_value, parameters, width, scale)) {
			return Value::DECIMAL(decimal_value, width, scale);
		}
		return Value(decimal_str);
	}
	case tid_TIMESTAMP_INT: {
		ION_TIMESTAMP timestamp;
		if (ion_reader_read_timestamp(reader, &timestamp) != IERR_OK) {
			throw IOException("read_ion failed to read timestamp");
		}
		timestamp_t ts;
		if (IonTimestampToDuckDB(timestamp, ts)) {
			return Value::TIMESTAMPTZ(timestamp_tz_t(ts));
		}
		decContext ctx;
		decContextDefault(&ctx, DEC_INIT_DECQUAD);
		char buffer[ION_MAX_TIMESTAMP_STRING + 1];
		SIZE output_length = 0;
		if (ion_timestamp_to_string(&timestamp, buffer, sizeof(buffer), &output_length, &ctx) != IERR_OK) {
			throw IOException("read_ion failed to format timestamp");
		}
		if (output_length > ION_MAX_TIMESTAMP_STRING) {
			output_length = ION_MAX_TIMESTAMP_STRING;
		}
		buffer[output_length] = '\0';
		auto ts_str = string(buffer);
		ts = Timestamp::FromString(ts_str, true);
		return Value::TIMESTAMPTZ(timestamp_tz_t(ts));
	}
	case tid_STRING_INT:
	case tid_SYMBOL_INT:
	case tid_CLOB_INT: {
		ION_STRING value;
		value.value = nullptr;
		value.length = 0;
		if (ion_reader_read_string(reader, &value) != IERR_OK) {
			throw IOException("read_ion failed to read string");
		}
		return Value(string(reinterpret_cast<const char *>(value.value), value.length));
	}
	case tid_BLOB_INT: {
		SIZE length = 0;
		if (ion_reader_get_lob_size(reader, &length) != IERR_OK) {
			throw IOException("read_ion failed to get blob size");
		}
		std::vector<BYTE> buffer(length);
		SIZE read_bytes = 0;
		if (ion_reader_read_lob_bytes(reader, buffer.data(), length, &read_bytes) != IERR_OK) {
			throw IOException("read_ion failed to read blob");
		}
		string data(reinterpret_cast<const char *>(buffer.data()), read_bytes);
		return Value::BLOB(data);
	}
	case tid_LIST_INT: {
		if (ion_reader_step_in(reader) != IERR_OK) {
			throw IOException("read_ion failed to step into list");
		}
		vector<Value> values;
		LogicalType child_type = LogicalType::SQLNULL;
		while (true) {
			ION_TYPE elem_type = tid_NULL;
			auto elem_status = ion_reader_next(reader, &elem_type);
			if (elem_status == IERR_EOF || elem_type == tid_EOF) {
				break;
			}
			if (elem_status != IERR_OK) {
				throw IOException("read_ion failed while reading list element");
			}
			auto value = IonReadValue(reader, elem_type);
			values.push_back(value);
			if (!value.IsNull()) {
				child_type = PromoteIonType(child_type, value.type());
			}
		}
		if (ion_reader_step_out(reader) != IERR_OK) {
			throw IOException("read_ion failed to step out of list");
		}
		return Value::LIST(child_type, values);
	}
	case tid_STRUCT_INT: {
		if (ion_reader_step_in(reader) != IERR_OK) {
			throw IOException("read_ion failed to step into struct");
		}
		child_list_t<Value> values;
		unordered_map<string, idx_t> index_by_name;
		while (true) {
			ION_TYPE field_type = tid_NULL;
			auto field_status = ion_reader_next(reader, &field_type);
			if (field_status == IERR_EOF || field_type == tid_EOF) {
				break;
			}
			if (field_status != IERR_OK) {
				throw IOException("read_ion failed while reading struct field");
			}
			ION_STRING field_name;
			field_name.value = nullptr;
			field_name.length = 0;
			if (ion_reader_get_field_name(reader, &field_name) != IERR_OK) {
				throw IOException("read_ion failed to read field name");
			}
			auto name = string(reinterpret_cast<const char *>(field_name.value), field_name.length);
			auto value = IonReadValue(reader, field_type);
			auto it = index_by_name.find(name);
			if (it == index_by_name.end()) {
				index_by_name.emplace(name, values.size());
				values.emplace_back(name, value);
			} else {
				values[it->second].second = value;
			}
		}
		if (ion_reader_step_out(reader) != IERR_OK) {
			throw IOException("read_ion failed to step out of struct");
		}
		return Value::STRUCT(values);
	}
	default:
		throw NotImplementedException("read_ion currently supports scalar bool/int/float/decimal/timestamp/string/blob only (type id " +
		                              std::to_string((int)ION_TYPE_INT(type)) + ")");
	}
}

static LogicalType PromoteIonType(const LogicalType &existing, const LogicalType &incoming) {
	if (existing.id() == LogicalTypeId::SQLNULL) {
		return incoming;
	}
	if (existing == incoming) {
		return existing;
	}
	if (existing.id() == LogicalTypeId::STRUCT && incoming.id() == LogicalTypeId::STRUCT) {
		child_list_t<LogicalType> merged;
		unordered_map<string, idx_t> index_by_name;
		auto &existing_children = StructType::GetChildTypes(existing);
		for (auto &child : existing_children) {
			index_by_name.emplace(child.first, merged.size());
			merged.emplace_back(child.first, child.second);
		}
		auto &incoming_children = StructType::GetChildTypes(incoming);
		for (auto &child : incoming_children) {
			auto it = index_by_name.find(child.first);
			if (it == index_by_name.end()) {
				index_by_name.emplace(child.first, merged.size());
				merged.emplace_back(child.first, child.second);
			} else {
				merged[it->second].second = PromoteIonType(merged[it->second].second, child.second);
			}
		}
		return LogicalType::STRUCT(std::move(merged));
	}
	if (existing.id() == LogicalTypeId::LIST && incoming.id() == LogicalTypeId::LIST) {
		auto &existing_child = ListType::GetChildType(existing);
		auto &incoming_child = ListType::GetChildType(incoming);
		return LogicalType::LIST(PromoteIonType(existing_child, incoming_child));
	}
	if (existing.id() == LogicalTypeId::VARCHAR || incoming.id() == LogicalTypeId::VARCHAR) {
		return LogicalType::VARCHAR;
	}
	if (existing.id() == LogicalTypeId::DOUBLE || incoming.id() == LogicalTypeId::DOUBLE) {
		return LogicalType::DOUBLE;
	}
	if (existing.id() == LogicalTypeId::DECIMAL || incoming.id() == LogicalTypeId::DECIMAL) {
		return LogicalType::DECIMAL(Decimal::MAX_WIDTH_DECIMAL, 18);
	}
	if ((existing.id() == LogicalTypeId::BIGINT && incoming.id() == LogicalTypeId::BOOLEAN) ||
	    (existing.id() == LogicalTypeId::BOOLEAN && incoming.id() == LogicalTypeId::BIGINT)) {
		return LogicalType::BIGINT;
	}
	return LogicalType::VARCHAR;
}

static LogicalType NormalizeInferredIonType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::DECIMAL:
		return LogicalType::DOUBLE;
	case LogicalTypeId::STRUCT: {
		child_list_t<LogicalType> children;
		auto &child_types = StructType::GetChildTypes(type);
		children.reserve(child_types.size());
		for (auto &child : child_types) {
			children.emplace_back(child.first, NormalizeInferredIonType(child.second));
		}
		return LogicalType::STRUCT(std::move(children));
	}
	case LogicalTypeId::LIST: {
		auto &child = ListType::GetChildType(type);
		return LogicalType::LIST(NormalizeInferredIonType(child));
	}
	default:
		return type;
	}
}

static bool ReadIonValueToVector(ION_READER *reader, ION_TYPE field_type, Vector &vector, idx_t row,
                                 const LogicalType &target_type) {
	BOOL is_null = FALSE;
	if (ion_reader_is_null(reader, &is_null) != IERR_OK) {
		throw IOException("read_ion failed while checking null status");
	}
	if (is_null) {
		return true;
	}
	switch (target_type.id()) {
	case LogicalTypeId::BOOLEAN: {
		if (ION_TYPE_INT(field_type) != tid_BOOL_INT) {
			return false;
		}
		BOOL value = FALSE;
		if (ion_reader_read_bool(reader, &value) != IERR_OK) {
			throw IOException("read_ion failed to read bool");
		}
		auto data = FlatVector::GetData<bool>(vector);
		data[row] = value != FALSE;
		FlatVector::Validity(vector).SetValid(row);
		return true;
	}
	case LogicalTypeId::BIGINT: {
		if (ION_TYPE_INT(field_type) != tid_INT_INT && ION_TYPE_INT(field_type) != tid_BOOL_INT) {
			return false;
		}
		int64_t value = 0;
		if (ION_TYPE_INT(field_type) == tid_BOOL_INT) {
			BOOL bool_value = FALSE;
			if (ion_reader_read_bool(reader, &bool_value) != IERR_OK) {
				throw IOException("read_ion failed to read bool");
			}
			value = bool_value ? 1 : 0;
		} else if (ion_reader_read_int64(reader, &value) != IERR_OK) {
			throw IOException("read_ion failed to read int");
		}
		auto data = FlatVector::GetData<int64_t>(vector);
		data[row] = value;
		FlatVector::Validity(vector).SetValid(row);
		return true;
	}
	case LogicalTypeId::DOUBLE: {
		double value = 0.0;
		if (ION_TYPE_INT(field_type) == tid_FLOAT_INT) {
			if (ion_reader_read_double(reader, &value) != IERR_OK) {
				throw IOException("read_ion failed to read float");
			}
		} else if (ION_TYPE_INT(field_type) == tid_DECIMAL_INT) {
			ION_DECIMAL decimal;
			ion_decimal_zero(&decimal);
			if (ion_reader_read_ion_decimal(reader, &decimal) != IERR_OK) {
				throw IOException("read_ion failed to read decimal");
			}
			std::vector<char> buffer(ION_DECIMAL_STRLEN(&decimal) + 1);
			ion_decimal_to_string(&decimal, buffer.data());
			ion_decimal_free(&decimal);
			auto decimal_str = string(buffer.data());
			if (!TryCast::Operation(string_t(decimal_str), value, false)) {
				return false;
			}
		} else if (ION_TYPE_INT(field_type) == tid_INT_INT) {
			int64_t int_value = 0;
			if (ion_reader_read_int64(reader, &int_value) != IERR_OK) {
				throw IOException("read_ion failed to read int");
			}
			value = static_cast<double>(int_value);
		} else if (ION_TYPE_INT(field_type) == tid_BOOL_INT) {
			BOOL bool_value = FALSE;
			if (ion_reader_read_bool(reader, &bool_value) != IERR_OK) {
				throw IOException("read_ion failed to read bool");
			}
			value = bool_value ? 1.0 : 0.0;
		} else {
			return false;
		}
		auto data = FlatVector::GetData<double>(vector);
		data[row] = value;
		FlatVector::Validity(vector).SetValid(row);
		return true;
	}
	case LogicalTypeId::DECIMAL: {
		if (ION_TYPE_INT(field_type) != tid_DECIMAL_INT) {
			return false;
		}
		ION_DECIMAL decimal;
		ion_decimal_zero(&decimal);
		if (ion_reader_read_ion_decimal(reader, &decimal) != IERR_OK) {
			throw IOException("read_ion failed to read decimal");
		}
		hugeint_t decimal_value;
		auto width = DecimalType::GetWidth(target_type);
		auto scale = DecimalType::GetScale(target_type);
		if (!IonDecimalToHugeint(decimal, decimal_value, width, scale)) {
			std::vector<char> buffer(ION_DECIMAL_STRLEN(&decimal) + 1);
			ion_decimal_to_string(&decimal, buffer.data());
			ion_decimal_free(&decimal);
			auto decimal_str = string(buffer.data());
			CastParameters parameters(false, nullptr);
			if (!TryCastToDecimal::Operation(string_t(decimal_str), decimal_value, parameters, width, scale)) {
				return false;
			}
		} else {
			ion_decimal_free(&decimal);
		}
		auto data = FlatVector::GetData<hugeint_t>(vector);
		data[row] = decimal_value;
		FlatVector::Validity(vector).SetValid(row);
		return true;
	}
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ: {
		if (ION_TYPE_INT(field_type) != tid_TIMESTAMP_INT) {
			return false;
		}
		ION_TIMESTAMP timestamp;
		if (ion_reader_read_timestamp(reader, &timestamp) != IERR_OK) {
			throw IOException("read_ion failed to read timestamp");
		}
		timestamp_t ts;
		if (!IonTimestampToDuckDB(timestamp, ts)) {
			decContext ctx;
			decContextDefault(&ctx, DEC_INIT_DECQUAD);
			char buffer[ION_MAX_TIMESTAMP_STRING + 1];
			SIZE output_length = 0;
			if (ion_timestamp_to_string(&timestamp, buffer, sizeof(buffer), &output_length, &ctx) != IERR_OK) {
				throw IOException("read_ion failed to format timestamp");
			}
			if (output_length > ION_MAX_TIMESTAMP_STRING) {
				output_length = ION_MAX_TIMESTAMP_STRING;
			}
			buffer[output_length] = '\0';
			auto ts_str = string(buffer);
			ts = Timestamp::FromString(ts_str, true);
		}
		if (target_type.id() == LogicalTypeId::TIMESTAMP_TZ) {
			auto data = FlatVector::GetData<timestamp_tz_t>(vector);
			data[row] = timestamp_tz_t(ts);
		} else {
			auto data = FlatVector::GetData<timestamp_t>(vector);
			data[row] = ts;
		}
		FlatVector::Validity(vector).SetValid(row);
		return true;
	}
	case LogicalTypeId::VARCHAR: {
		if (ION_TYPE_INT(field_type) != tid_STRING_INT && ION_TYPE_INT(field_type) != tid_SYMBOL_INT &&
		    ION_TYPE_INT(field_type) != tid_CLOB_INT) {
			return false;
		}
		ION_STRING value;
		value.value = nullptr;
		value.length = 0;
		if (ion_reader_read_string(reader, &value) != IERR_OK) {
			throw IOException("read_ion failed to read string");
		}
		auto data = FlatVector::GetData<string_t>(vector);
		data[row] = StringVector::AddString(vector, reinterpret_cast<const char *>(value.value), value.length);
		FlatVector::Validity(vector).SetValid(row);
		return true;
	}
	case LogicalTypeId::BLOB: {
		if (ION_TYPE_INT(field_type) != tid_BLOB_INT) {
			return false;
		}
		SIZE length = 0;
		if (ion_reader_get_lob_size(reader, &length) != IERR_OK) {
			throw IOException("read_ion failed to get blob size");
		}
		std::vector<BYTE> buffer(length);
		SIZE read_bytes = 0;
		if (ion_reader_read_lob_bytes(reader, buffer.data(), length, &read_bytes) != IERR_OK) {
			throw IOException("read_ion failed to read blob");
		}
		auto data = FlatVector::GetData<string_t>(vector);
		data[row] = StringVector::AddStringOrBlob(vector, reinterpret_cast<const char *>(buffer.data()),
		                                          static_cast<idx_t>(read_bytes));
		FlatVector::Validity(vector).SetValid(row);
		return true;
	}
	default:
		return false;
	}
}

static void ParseColumnsParameter(ClientContext &context, const Value &value, vector<string> &names,
                                  vector<LogicalType> &types) {
	auto &child_type = value.type();
	if (child_type.id() != LogicalTypeId::STRUCT) {
		throw BinderException("read_ion \"columns\" parameter requires a struct as input.");
	}
	auto &struct_children = StructValue::GetChildren(value);
	D_ASSERT(StructType::GetChildCount(child_type) == struct_children.size());
	for (idx_t i = 0; i < struct_children.size(); i++) {
		auto &name = StructType::GetChildName(child_type, i);
		auto &val = struct_children[i];
		if (val.IsNull()) {
			throw BinderException("read_ion \"columns\" parameter type specification cannot be NULL.");
		}
		if (val.type().id() != LogicalTypeId::VARCHAR) {
			throw BinderException("read_ion \"columns\" parameter type specification must be VARCHAR.");
		}
		names.push_back(name);
		types.emplace_back(TransformStringToLogicalType(StringValue::Get(val), context));
	}
	if (names.empty()) {
		throw BinderException("read_ion \"columns\" parameter needs at least one column.");
	}
}

static void ParseFormatParameter(const Value &value, IonReadBindData::Format &format) {
	if (value.type().id() != LogicalTypeId::VARCHAR) {
		throw BinderException("read_ion \"format\" parameter must be VARCHAR.");
	}
	auto format_str = StringUtil::Lower(StringValue::Get(value));
	if (format_str == "auto") {
		format = IonReadBindData::Format::AUTO;
	} else if (format_str == "newline_delimited") {
		format = IonReadBindData::Format::NEWLINE_DELIMITED;
	} else if (format_str == "array") {
		format = IonReadBindData::Format::ARRAY;
	} else if (format_str == "unstructured") {
		format = IonReadBindData::Format::UNSTRUCTURED;
	} else {
		throw BinderException("read_ion \"format\" must be one of ['auto', 'newline_delimited', 'array', 'unstructured'].");
	}
}

static void ParseRecordsParameter(const Value &value, IonReadBindData::RecordsMode &records_mode) {
	if (value.type().id() == LogicalTypeId::BOOLEAN) {
		records_mode = BooleanValue::Get(value) ? IonReadBindData::RecordsMode::ENABLED
		                                        : IonReadBindData::RecordsMode::DISABLED;
		return;
	}
	if (value.type().id() != LogicalTypeId::VARCHAR) {
		throw BinderException("read_ion \"records\" parameter must be VARCHAR or BOOLEAN.");
	}
	auto records_str = StringUtil::Lower(StringValue::Get(value));
	if (records_str == "auto") {
		records_mode = IonReadBindData::RecordsMode::AUTO;
	} else if (records_str == "true") {
		records_mode = IonReadBindData::RecordsMode::ENABLED;
	} else if (records_str == "false") {
		records_mode = IonReadBindData::RecordsMode::DISABLED;
	} else {
		throw BinderException("read_ion \"records\" must be one of ['auto', 'true', 'false'] or a BOOLEAN.");
	}
}

static void ParseProfileParameter(const Value &value, bool &profile) {
	if (value.type().id() != LogicalTypeId::BOOLEAN) {
		throw BinderException("read_ion \"profile\" parameter must be BOOLEAN.");
	}
	profile = BooleanValue::Get(value);
}

static void InferIonSchema(const string &path, vector<string> &names, vector<LogicalType> &types,
                           IonReadBindData::Format format, IonReadBindData::RecordsMode records_mode,
                           bool &records_out, ClientContext &context) {
	ION_READER *reader = nullptr;
	IonStreamState stream_state;
	auto &fs = FileSystem::GetFileSystem(context);
	stream_state.handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	stream_state.query_context = QueryContext(context);
	ION_READER_OPTIONS reader_options = {};
	reader_options.skip_character_validation = TRUE;
	auto status = ion_reader_open_stream(&reader, &stream_state, IonStreamHandler, &reader_options);
	if (status != IERR_OK) {
		throw IOException("read_ion failed to open Ion reader for schema inference");
	}

	unordered_map<string, idx_t> index_by_name;
	unordered_map<SID, idx_t> sid_map;
	const idx_t max_rows = 1000;
	idx_t rows = 0;
	bool records_decided = (records_mode != IonReadBindData::RecordsMode::AUTO);
	if (records_decided) {
		records_out = (records_mode == IonReadBindData::RecordsMode::ENABLED);
	}

	auto next_value = [&](ION_TYPE &type) -> bool {
		auto status = ion_reader_next(reader, &type);
		if (status == IERR_EOF || type == tid_EOF) {
			return false;
		}
		if (status != IERR_OK) {
			ion_reader_close(reader);
			throw IOException("read_ion failed while inferring schema");
		}
		return true;
	};

	auto read_record = [&](ION_TYPE type) {
		if (type != tid_STRUCT) {
			ion_reader_close(reader);
			throw InvalidInputException("read_ion expects records to be structs");
		}
		if (ion_reader_step_in(reader) != IERR_OK) {
			ion_reader_close(reader);
			throw IOException("read_ion failed to step into struct during schema inference");
		}
		while (true) {
			ION_TYPE field_type = tid_NULL;
			auto field_status = ion_reader_next(reader, &field_type);
			if (field_status == IERR_EOF || field_type == tid_EOF) {
				break;
			}
			if (field_status != IERR_OK) {
				ion_reader_close(reader);
				throw IOException("read_ion failed while reading struct field");
			}
			ION_SYMBOL *field_symbol = nullptr;
			if (ion_reader_get_field_name_symbol(reader, &field_symbol) != IERR_OK) {
				ion_reader_close(reader);
				throw IOException("read_ion failed to read field name");
			}
			idx_t field_idx = 0;
			bool have_index = false;
			if (field_symbol && field_symbol->sid > 0) {
				auto sid_it = sid_map.find(field_symbol->sid);
				if (sid_it != sid_map.end()) {
					field_idx = sid_it->second;
					have_index = true;
				}
			}
			if (!have_index) {
				string name;
				if (field_symbol && field_symbol->value.value && field_symbol->value.length > 0) {
					name = string(reinterpret_cast<const char *>(field_symbol->value.value), field_symbol->value.length);
				} else {
					ION_STRING field_name;
					field_name.value = nullptr;
					field_name.length = 0;
					if (ion_reader_get_field_name(reader, &field_name) != IERR_OK) {
						ion_reader_close(reader);
						throw IOException("read_ion failed to read field name");
					}
					name = string(reinterpret_cast<const char *>(field_name.value), field_name.length);
				}
				auto it = index_by_name.find(name);
				if (it == index_by_name.end()) {
					field_idx = types.size();
					index_by_name.emplace(name, field_idx);
					names.push_back(name);
					types.push_back(LogicalType::SQLNULL);
				} else {
					field_idx = it->second;
				}
				if (field_symbol && field_symbol->sid > 0) {
					sid_map.emplace(field_symbol->sid, field_idx);
				}
			}
			auto value = IonReadValue(reader, field_type);
			if (value.IsNull()) {
				continue;
			}
			auto &current_type = types[field_idx];
			auto incoming_type = NormalizeInferredIonType(value.type());
			current_type = PromoteIonType(current_type, incoming_type);
		}
		if (ion_reader_step_out(reader) != IERR_OK) {
			ion_reader_close(reader);
			throw IOException("read_ion failed to step out of struct during schema inference");
		}
	};

	auto read_scalar = [&](ION_TYPE type) {
		auto value = IonReadValue(reader, type);
		if (!value.IsNull()) {
			if (types.empty()) {
				types.push_back(NormalizeInferredIonType(value.type()));
			} else {
				types[0] = PromoteIonType(types[0], NormalizeInferredIonType(value.type()));
			}
		}
		if (names.empty()) {
			names.push_back("ion");
		}
	};

	if (format == IonReadBindData::Format::ARRAY) {
		ION_TYPE outer_type = tid_NULL;
		if (!next_value(outer_type)) {
			ion_reader_close(reader);
			throw InvalidInputException("read_ion expects a top-level list when format='array'");
		}
		if (outer_type != tid_LIST) {
			ion_reader_close(reader);
			throw InvalidInputException("read_ion expects a top-level list when format='array'");
		}
		if (ion_reader_step_in(reader) != IERR_OK) {
			ion_reader_close(reader);
			throw IOException("read_ion failed to step into list during schema inference");
		}
		while (rows < max_rows) {
			ION_TYPE type = tid_NULL;
			if (!next_value(type)) {
				break;
			}
			BOOL is_null = FALSE;
			if (ion_reader_is_null(reader, &is_null) != IERR_OK) {
				ion_reader_close(reader);
				throw IOException("read_ion failed while checking null during schema inference");
			}
			if (is_null) {
				rows++;
				continue;
			}
			if (!records_decided) {
				records_out = (type == tid_STRUCT);
				records_decided = true;
			}
			if (records_out) {
				read_record(type);
			} else {
				read_scalar(type);
			}
			rows++;
		}
		if (ion_reader_step_out(reader) != IERR_OK) {
			ion_reader_close(reader);
			throw IOException("read_ion failed to step out of list during schema inference");
		}
	} else {
		while (rows < max_rows) {
			ION_TYPE type = tid_NULL;
			if (!next_value(type)) {
				break;
			}
			BOOL is_null = FALSE;
			if (ion_reader_is_null(reader, &is_null) != IERR_OK) {
				ion_reader_close(reader);
				throw IOException("read_ion failed while checking null during schema inference");
			}
			if (is_null) {
				rows++;
				continue;
			}
			if (!records_decided) {
				records_out = (type == tid_STRUCT);
				records_decided = true;
			}
			if (records_out) {
				read_record(type);
			} else {
				read_scalar(type);
			}
			rows++;
		}
	}

	ion_reader_close(reader);
	if (stream_state.handle) {
		stream_state.handle->Close();
	}

	if (names.empty()) {
		throw InvalidInputException("read_ion could not infer schema from input");
	}
	if (!records_decided) {
		records_out = true;
	}
}

static void IonReadFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &global_state = data_p.global_state->Cast<IonReadGlobalState>();
	IonReadLocalState *local_state =
	    data_p.local_state ? &data_p.local_state->Cast<IonReadLocalState>() : nullptr;
	IonReadScanState *scan_state = local_state ? &local_state->scan_state : &global_state.scan_state;
	if (scan_state->finished) {
		if (local_state && InitializeIonRange(global_state, *local_state)) {
			scan_state = &local_state->scan_state;
		} else {
			auto &bind_data = data_p.bind_data->Cast<IonReadBindData>();
			if (bind_data.profile && !scan_state->timing.reported) {
				scan_state->timing.reported = true;
				ReportProfile(global_state, *scan_state);
			}
			output.SetCardinality(0);
			return;
		}
	}
#ifndef DUCKDB_IONC
	throw InvalidInputException("read_ion requires ion-c; rebuild with ion-c available");
#else
	auto &bind_data = data_p.bind_data->Cast<IonReadBindData>();
	const auto profile = bind_data.profile;
	if (!scan_state->reader_initialized) {
		auto status =
		    ion_reader_open_stream(&scan_state->reader, &scan_state->stream_state, IonStreamHandler,
		                           &scan_state->reader_options);
		if (status != IERR_OK) {
			throw IOException("read_ion failed to open Ion reader");
		}
		scan_state->reader_initialized = true;
	}
	auto &column_ids = global_state.column_ids;
	vector<idx_t> column_to_output;
	if (!column_ids.empty()) {
		column_to_output.assign(bind_data.return_types.size(), DConstants::INVALID_INDEX);
		for (idx_t i = 0; i < column_ids.size(); i++) {
			auto col_id = column_ids[i];
			if (col_id != DConstants::INVALID_INDEX) {
				column_to_output[col_id] = i;
			}
		}
	}
	const bool all_columns = column_ids.empty();
	const idx_t required_columns = all_columns ? bind_data.return_types.size() : global_state.projected_columns;
	vector<idx_t> projected_cols;
	bool use_extractor = bind_data.records && !all_columns && required_columns > 0 && required_columns <= 3;
	bool use_fast_projection = !all_columns && required_columns > 0 && required_columns <= 3 && !use_extractor;
	if (use_fast_projection || use_extractor) {
		projected_cols.reserve(required_columns);
		for (auto col_id : column_ids) {
			if (col_id != DConstants::INVALID_INDEX) {
				projected_cols.push_back(col_id);
			}
		}
		if (use_extractor) {
			EnsureIonExtractor(*scan_state, bind_data, projected_cols);
			if (!scan_state->extractor_ready) {
				use_extractor = false;
				use_fast_projection = true;
			}
		}
	}
	if (!all_columns && required_columns > 0 && scan_state->seen.size() != bind_data.return_types.size()) {
		scan_state->seen.assign(bind_data.return_types.size(), 0);
		scan_state->row_counter = 0;
	}
	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE) {
		ION_TYPE type = tid_NULL;
		auto status = IERR_OK;
		auto next_start = profile ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
		if (bind_data.format == IonReadBindData::Format::ARRAY) {
			if (!scan_state->array_initialized) {
				status = ion_reader_next(scan_state->reader, &type);
				if (status == IERR_EOF || type == tid_EOF) {
					scan_state->finished = true;
					break;
				}
				if (status != IERR_OK) {
					throw IOException("read_ion failed while reading array");
				}
				if (type != tid_LIST) {
					throw InvalidInputException("read_ion expects a top-level list when format='array'");
				}
				if (ion_reader_step_in(scan_state->reader) != IERR_OK) {
					throw IOException("read_ion failed to step into list");
				}
				scan_state->array_initialized = true;
			}
			status = ion_reader_next(scan_state->reader, &type);
			if (status == IERR_EOF || type == tid_EOF) {
				ion_reader_step_out(scan_state->reader);
				scan_state->finished = true;
				break;
			}
			if (status != IERR_OK) {
				throw IOException("read_ion failed while reading array element");
			}
		} else {
			status = ion_reader_next(scan_state->reader, &type);
			if (status == IERR_EOF || type == tid_EOF) {
				scan_state->finished = true;
				break;
			}
			if (status != IERR_OK) {
				throw IOException("read_ion failed while reading next value");
			}
		}
		if (profile) {
			auto elapsed = std::chrono::steady_clock::now() - next_start;
			scan_state->timing.next_nanos +=
			    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
		}
		for (idx_t out_idx = 0; out_idx < output.ColumnCount(); out_idx++) {
			auto col_id = column_ids.empty() ? out_idx : column_ids[out_idx];
			if (col_id == DConstants::INVALID_INDEX) {
				output.SetValue(out_idx, count, Value());
			} else {
				output.SetValue(out_idx, count, Value(bind_data.return_types[col_id]));
			}
		}
		BOOL is_null = FALSE;
		if (ion_reader_is_null(scan_state->reader, &is_null) != IERR_OK) {
			throw IOException("read_ion failed while checking null status");
		}
		if (is_null) {
			count++;
			continue;
		}

		if (bind_data.records) {
			if (type != tid_STRUCT) {
				throw InvalidInputException("read_ion expects records to be structs");
			}
			if (!all_columns && required_columns == 0) {
				count++;
				if (profile) {
					scan_state->timing.rows++;
				}
				continue;
			}
			idx_t remaining = required_columns;
			scan_state->row_counter++;
			if (scan_state->row_counter == 0) {
				scan_state->row_counter = 1;
				std::fill(scan_state->seen.begin(), scan_state->seen.end(), 0);
			}
			auto struct_start = profile ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
			if (ion_reader_step_in(scan_state->reader) != IERR_OK) {
				throw IOException("read_ion failed to step into struct");
			}
			if (use_extractor && scan_state->extractor_ready) {
				IonExtractorMatchContext ctx;
				ctx.scan_state = scan_state;
				ctx.bind_data = &bind_data;
				ctx.output = &output;
				ctx.column_to_output = &column_to_output;
				ctx.row = count;
				ctx.remaining = required_columns;
				ctx.profile = profile;
				extractor_context = &ctx;
				status = ion_extractor_match(scan_state->extractor, scan_state->reader);
				extractor_context = nullptr;
				if (status != IERR_OK) {
					throw IOException("read_ion failed during extractor match");
				}
				if (ion_reader_step_out(scan_state->reader) != IERR_OK) {
					throw IOException("read_ion failed to step out of struct");
				}
				if (profile) {
					auto elapsed = std::chrono::steady_clock::now() - struct_start;
					scan_state->timing.struct_nanos += static_cast<uint64_t>(
					    std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
					scan_state->timing.rows++;
				}
				count++;
				continue;
			}
			while (true) {
				ION_TYPE field_type = tid_NULL;
				status = ion_reader_next(scan_state->reader, &field_type);
				if (status == IERR_EOF || field_type == tid_EOF) {
					break;
				}
				if (status != IERR_OK) {
					throw IOException("read_ion failed while reading struct field");
				}
				if (profile) {
					scan_state->timing.fields++;
				}
				ION_SYMBOL *field_symbol = nullptr;
				if (ion_reader_get_field_name_symbol(scan_state->reader, &field_symbol) != IERR_OK) {
					throw IOException("read_ion failed to read field name");
				}
				idx_t col_idx = 0;
				bool have_col = false;
				bool sid_known_miss = false;
				if (field_symbol && field_symbol->sid > 0) {
					auto sid_it = scan_state->sid_map.find(field_symbol->sid);
					if (sid_it != scan_state->sid_map.end()) {
						if (sid_it->second == DConstants::INVALID_INDEX) {
							sid_known_miss = true;
						} else {
							col_idx = sid_it->second;
							have_col = true;
						}
					}
				}
				if (!have_col && !sid_known_miss) {
					if (use_fast_projection) {
						bool matched = false;
						ION_STRING field_value;
						field_value.value = nullptr;
						field_value.length = 0;
						if (field_symbol && field_symbol->value.value && field_symbol->value.length > 0) {
							field_value = field_symbol->value;
						} else {
							if (ion_reader_get_field_name(scan_state->reader, &field_value) != IERR_OK) {
								throw IOException("read_ion failed to read field name");
							}
						}
						for (auto proj_col : projected_cols) {
							if (IonStringEquals(field_value, bind_data.names[proj_col])) {
								col_idx = proj_col;
								have_col = true;
								matched = true;
								break;
							}
						}
						if (field_symbol && field_symbol->sid > 0) {
							if (matched) {
								scan_state->sid_map.emplace(field_symbol->sid, col_idx);
							} else {
								scan_state->sid_map.emplace(field_symbol->sid, DConstants::INVALID_INDEX);
							}
						}
					}
				}
				if (!have_col && !sid_known_miss) {
					string name;
					if (field_symbol && field_symbol->value.value && field_symbol->value.length > 0) {
						name = string(reinterpret_cast<const char *>(field_symbol->value.value), field_symbol->value.length);
					} else {
						ION_STRING field_name;
						field_name.value = nullptr;
						field_name.length = 0;
						if (ion_reader_get_field_name(scan_state->reader, &field_name) != IERR_OK) {
							throw IOException("read_ion failed to read field name");
						}
						name = string(reinterpret_cast<const char *>(field_name.value), field_name.length);
					}
					auto map_it = bind_data.name_map.find(name);
					if (map_it != bind_data.name_map.end()) {
						col_idx = map_it->second;
						have_col = true;
						if (field_symbol && field_symbol->sid > 0) {
							scan_state->sid_map.emplace(field_symbol->sid, col_idx);
						}
					} else if (field_symbol && field_symbol->sid > 0) {
						scan_state->sid_map.emplace(field_symbol->sid, DConstants::INVALID_INDEX);
					}
				}
				if (have_col) {
					auto out_idx = column_to_output.empty() ? col_idx : column_to_output[col_idx];
					if (out_idx == DConstants::INVALID_INDEX) {
						auto value_start =
						    profile ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
						SkipIonValue(scan_state->reader, field_type);
						if (profile) {
							auto elapsed = std::chrono::steady_clock::now() - value_start;
							scan_state->timing.value_nanos += static_cast<uint64_t>(
							    std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
						}
						continue;
					}
					auto &vec = output.data[out_idx];
					auto target_type = bind_data.return_types[col_idx];
					auto value_start =
					    profile ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
					if (!ReadIonValueToVector(scan_state->reader, field_type, vec, count, target_type)) {
						auto value = IonReadValue(scan_state->reader, field_type);
						if (!value.IsNull()) {
							output.SetValue(out_idx, count, value.DefaultCastAs(target_type));
						}
					}
					if (profile) {
						auto elapsed = std::chrono::steady_clock::now() - value_start;
						scan_state->timing.value_nanos += static_cast<uint64_t>(
						    std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
					}
					if (!all_columns && remaining > 0) {
						auto marker = scan_state->row_counter;
						if (scan_state->seen[col_idx] != marker) {
							scan_state->seen[col_idx] = marker;
							remaining--;
							if (remaining == 0) {
								break;
							}
						}
					}
				} else {
					auto value_start =
					    profile ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
					SkipIonValue(scan_state->reader, field_type);
					if (profile) {
						auto elapsed = std::chrono::steady_clock::now() - value_start;
						scan_state->timing.value_nanos += static_cast<uint64_t>(
						    std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
					}
				}
			}
			if (ion_reader_step_out(scan_state->reader) != IERR_OK) {
				throw IOException("read_ion failed to step out of struct");
			}
			if (profile) {
				auto elapsed = std::chrono::steady_clock::now() - struct_start;
				scan_state->timing.struct_nanos +=
				    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
			}
		} else {
			auto value_start = profile ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
			auto value = IonReadValue(scan_state->reader, type);
			if (!value.IsNull()) {
				auto out_idx = column_to_output.empty() ? 0 : column_to_output[0];
				if (out_idx != DConstants::INVALID_INDEX) {
					output.SetValue(out_idx, count, value.DefaultCastAs(bind_data.return_types[0]));
				}
			}
			if (profile) {
				auto elapsed = std::chrono::steady_clock::now() - value_start;
				scan_state->timing.value_nanos +=
				    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
			}
		}
		if (profile) {
			scan_state->timing.rows++;
		}
		count++;
	}
	output.SetCardinality(count);
	if (profile && scan_state->finished && !scan_state->timing.reported && !local_state) {
		scan_state->timing.reported = true;
		ReportProfile(global_state, *scan_state);
	}
#endif
}

static void LoadInternal(ExtensionLoader &loader) {
	RegisterIonScalarFunctions(loader);
	TableFunction read_ion("read_ion", {LogicalType::VARCHAR}, IonReadFunction, IonReadBind, IonReadInit,
	                       IonReadInitLocal);
	read_ion.named_parameters["columns"] = LogicalType::ANY;
	read_ion.named_parameters["format"] = LogicalType::VARCHAR;
	read_ion.named_parameters["records"] = LogicalType::ANY;
	read_ion.named_parameters["profile"] = LogicalType::BOOLEAN;
	read_ion.projection_pushdown = true;
	read_ion.filter_pushdown = false;
	read_ion.filter_prune = false;
	loader.RegisterFunction(read_ion);
	RegisterIonCopyFunction(loader);
}

void IonExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string IonExtension::Name() {
	return "ion";
}

std::string IonExtension::Version() const {
#ifdef EXT_VERSION_ION
	return EXT_VERSION_ION;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(ion, loader) {
	duckdb::LoadInternal(loader);
}
}
