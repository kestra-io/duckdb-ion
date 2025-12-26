#include <ionc/ion_collection.h>
#include <ionc/ion_reader.h>
#include <ionc/ion_stream.h>
#include <ionc/ion_writer.h>

#include <cstdio>

static int Fail(const char *message) {
	std::fprintf(stderr, "%s\n", message);
	return 1;
}

int main(int argc, char **argv) {
	if (argc != 3) {
		std::fprintf(stderr, "Usage: %s <input.ion> <output.ion>\n", argv[0]);
		return 2;
	}
	const char *input_path = argv[1];
	const char *output_path = argv[2];

	FILE *input = std::fopen(input_path, "rb");
	if (!input) {
		std::perror("Failed to open input file");
		return 1;
	}
	FILE *output = std::fopen(output_path, "wb");
	if (!output) {
		std::perror("Failed to open output file");
		std::fclose(input);
		return 1;
	}

	ION_STREAM *in_stream = nullptr;
	ION_STREAM *out_stream = nullptr;
	if (ion_stream_open_file_in(input, &in_stream) != IERR_OK) {
		std::fclose(input);
		std::fclose(output);
		return Fail("Failed to open Ion input stream");
	}
	if (ion_stream_open_file_out(output, &out_stream) != IERR_OK) {
		ion_stream_close(in_stream);
		std::fclose(input);
		std::fclose(output);
		return Fail("Failed to open Ion output stream");
	}

	ION_READER_OPTIONS reader_options = {};
	reader_options.skip_character_validation = TRUE;
	hREADER reader = nullptr;
	if (ion_reader_open(&reader, in_stream, &reader_options) != IERR_OK) {
		ion_stream_close(in_stream);
		ion_stream_close(out_stream);
		std::fclose(input);
		std::fclose(output);
		return Fail("Failed to open Ion reader");
	}

	ION_WRITER_OPTIONS writer_options = {};
	writer_options.output_as_binary = TRUE;
	hWRITER writer = nullptr;
	if (ion_writer_open(&writer, out_stream, &writer_options) != IERR_OK) {
		ion_reader_close(reader);
		ion_stream_close(in_stream);
		ion_stream_close(out_stream);
		std::fclose(input);
		std::fclose(output);
		return Fail("Failed to open Ion writer");
	}

	auto status = ion_writer_write_all_values(writer, reader);
	ion_writer_close(writer);
	ion_reader_close(reader);
	ion_stream_close(in_stream);
	ion_stream_close(out_stream);
	std::fclose(input);
	std::fclose(output);

	if (status != IERR_OK) {
		return Fail("Failed to convert Ion input to binary");
	}
	return 0;
}
