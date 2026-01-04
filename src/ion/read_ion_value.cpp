#include "ion/read_ion_value.hpp"

#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/cast/default_casts.hpp"

#include <cstring>

namespace duckdb {
namespace ion {

static LogicalType PromoteIonTypeLocal(const LogicalType &existing, const LogicalType &incoming) {
	if (existing.IsJSONType() || incoming.IsJSONType()) {
		return LogicalType::JSON();
	}
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
				merged[it->second].second = PromoteIonTypeLocal(merged[it->second].second, child.second);
			}
		}
		return LogicalType::STRUCT(std::move(merged));
	}
	if (existing.id() == LogicalTypeId::LIST && incoming.id() == LogicalTypeId::LIST) {
		auto &existing_child = ListType::GetChildType(existing);
		auto &incoming_child = ListType::GetChildType(incoming);
		return LogicalType::LIST(PromoteIonTypeLocal(existing_child, incoming_child));
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

static inline void InitIonDecimal(ION_DECIMAL &decimal) {
	std::memset(&decimal, 0, sizeof(decimal));
}

static bool IonDecimalToHugeint(const ION_DECIMAL &decimal, hugeint_t &result, uint8_t width, uint8_t scale) {
	std::vector<char> buffer(ION_DECIMAL_STRLEN(&decimal) + 1);
	if (ion_decimal_to_string(&decimal, buffer.data()) != IERR_OK) {
		return false;
	}
	auto decimal_str = string(buffer.data());
	for (auto &ch : decimal_str) {
		if (ch == 'd' || ch == 'D') {
			ch = 'e';
		}
	}
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

void SkipIonValueImpl(ION_READER *reader, ION_TYPE type) {
	if (!reader) {
		return;
	}
	BOOL is_null = FALSE;
	if (ion_reader_is_null(reader, &is_null) != IERR_OK) {
		throw IOException("read_ion failed while checking null status");
	}
	if (is_null || type == tid_NULL || type == tid_EOF) {
		return;
	}
	switch (ION_TYPE_INT(type)) {
	case tid_LIST_INT:
	case tid_SEXP_INT:
	case tid_STRUCT_INT: {
		if (ion_reader_step_in(reader) != IERR_OK) {
			throw IOException("read_ion failed to step into container while skipping");
		}
		while (true) {
			ION_TYPE child_type = tid_NULL;
			auto status = ion_reader_next(reader, &child_type);
			if (status == IERR_EOF || child_type == tid_EOF) {
				break;
			}
			if (status != IERR_OK) {
				throw IOException("read_ion failed while skipping container value");
			}
			if (ION_TYPE_INT(child_type) == tid_LIST_INT || ION_TYPE_INT(child_type) == tid_SEXP_INT ||
			    ION_TYPE_INT(child_type) == tid_STRUCT_INT) {
				SkipIonValueImpl(reader, child_type);
			}
		}
		if (ion_reader_step_out(reader) != IERR_OK) {
			throw IOException("read_ion failed to step out of container while skipping");
		}
		return;
	}
	default:
		return;
	}
}

Value IonReadValueImpl(ION_READER *reader, ION_TYPE type) {
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
		InitIonDecimal(decimal);
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
		auto normalized = decimal_str;
		for (auto &ch : normalized) {
			if (ch == 'd' || ch == 'D') {
				ch = 'e';
			}
		}
		CastParameters parameters(false, nullptr);
		if (TryCastToDecimal::Operation(string_t(normalized), decimal_value, parameters, width, scale)) {
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
			auto value = IonReadValueImpl(reader, elem_type);
			values.push_back(value);
			if (!value.IsNull()) {
				child_type = PromoteIonTypeLocal(child_type, value.type());
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
			auto value = IonReadValueImpl(reader, field_type);
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
		throw NotImplementedException(
		    "read_ion currently supports scalar bool/int/float/decimal/timestamp/string/blob only (type id " +
		    std::to_string((int)ION_TYPE_INT(type)) + ")");
	}
}

} // namespace ion
} // namespace duckdb
