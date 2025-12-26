#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <ionc/ion.h>
#include <ionc/ion_extractor.h>
#include <ionc/ion_reader.h>

struct CallbackState {
	int count = 0;
	int64_t last_value = 0;
	bool saw_value = false;
};

static void Check(iERR err, const char *what) {
	if (err == IERR_OK) {
		return;
	}
	std::fprintf(stderr, "ion-c error (%s): %d\n", what, static_cast<int>(err));
	std::exit(1);
}

static iERR ExtractorCallback(hREADER reader, hPATH matched_path, void *user_context,
                              ION_EXTRACTOR_CONTROL *p_control) {
	(void)matched_path;
	auto *state = static_cast<CallbackState *>(user_context);
	if (!state) {
		return IERR_INVALID_ARG;
	}
	ION_TYPE type = tid_NULL;
	if (ion_reader_get_type(reader, &type) != IERR_OK) {
		return IERR_INVALID_ARG;
	}
	if (ION_TYPE_INT(type) == tid_INT_INT) {
		int64_t value = 0;
		if (ion_reader_read_int64(reader, &value) != IERR_OK) {
			return IERR_INVALID_STATE;
		}
		state->last_value = value;
		state->saw_value = true;
	}
	state->count++;
	*p_control = ion_extractor_control_next();
	return IERR_OK;
}

static void RunCase(const char *label, bool match_relative, bool step_in) {
	const char *ion_text = "{id:1, name:\"alpha\", nested:{x:2}}";
	hREADER reader = nullptr;
	ION_READER_OPTIONS reader_opts = {};
	Check(ion_reader_open_buffer(&reader, reinterpret_cast<BYTE *>(const_cast<char *>(ion_text)),
	                             static_cast<SIZE>(std::strlen(ion_text)), &reader_opts),
	      "ion_reader_open_buffer");

	if (step_in) {
		ION_TYPE type = tid_NULL;
		Check(ion_reader_next(reader, &type), "ion_reader_next");
		if (type != tid_STRUCT) {
			std::fprintf(stderr, "expected struct, got %ld\n", static_cast<long>(ION_TYPE_INT(type)));
			std::exit(1);
		}
		Check(ion_reader_step_in(reader), "ion_reader_step_in");
	}

	ION_EXTRACTOR_OPTIONS extractor_opts = {};
	extractor_opts.max_path_length = 1;
	extractor_opts.max_num_paths = 1;
	extractor_opts.match_relative_paths = match_relative;

	hEXTRACTOR extractor = nullptr;
	Check(ion_extractor_open(&extractor, &extractor_opts), "ion_extractor_open");

	CallbackState state;
	hPATH path = nullptr;
	Check(ion_extractor_path_create(extractor, 1, ExtractorCallback, &state, &path),
	      "ion_extractor_path_create");

	ION_STRING field_name;
	field_name.value = reinterpret_cast<BYTE *>(const_cast<char *>("id"));
	field_name.length = 2;
	Check(ion_extractor_path_append_field(path, &field_name), "ion_extractor_path_append_field");

	iERR match_status = ion_extractor_match(extractor, reader);
	std::printf("%s: match_status=%d callbacks=%d saw_value=%s last_value=%lld\n", label,
	            static_cast<int>(match_status), state.count, state.saw_value ? "true" : "false",
	            static_cast<long long>(state.last_value));

	if (step_in) {
		Check(ion_reader_step_out(reader), "ion_reader_step_out");
	}
	ion_extractor_close(extractor);
	ion_reader_close(reader);
}

int main() {
	RunCase("depth0_match_relative=false", false, false);
	RunCase("depth0_match_relative=true", true, false);
	RunCase("depth1_match_relative=true", true, true);
	return 0;
}
