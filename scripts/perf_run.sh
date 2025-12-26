#!/usr/bin/env bash
set -euo pipefail

DUCKDB_BIN="${DUCKDB_BIN:-./build/release/duckdb}"
DATA_DIR="${DATA_DIR:-perf/data}"
OUT_DIR="${OUT_DIR:-perf/results}"
ION_PROFILE="${ION_PROFILE:-0}"

ION_FILE="$DATA_DIR/data.ion"
JSON_FILE="$DATA_DIR/data.jsonl"
ION_BINARY_FILE="$DATA_DIR/data_binary.ion"
ION_WIDE_FILE="$DATA_DIR/data_wide.ion"
JSON_WIDE_FILE="$DATA_DIR/data_wide.jsonl"
ION_WIDE_BINARY_FILE="$DATA_DIR/data_wide_binary.ion"

mkdir -p "$OUT_DIR"

ION_PROFILE_ARG=""
if [[ "$ION_PROFILE" == "1" ]]; then
  ION_PROFILE_ARG=", profile := true"
fi

SQL_FILE="$OUT_DIR/run_perf.sql"
cat > "$SQL_FILE" <<SQL
INSTALL json;
LOAD json;
LOAD ion;

PRAGMA enable_profiling='json';

PRAGMA profiling_output='$OUT_DIR/ion_count.json';
SELECT COUNT(*) FROM read_ion('$ION_FILE'${ION_PROFILE_ARG});

PRAGMA profiling_output='$OUT_DIR/ion_count_binary.json';
SELECT COUNT(*) FROM read_ion('$ION_BINARY_FILE'${ION_PROFILE_ARG});

PRAGMA profiling_output='$OUT_DIR/ion_count_explicit.json';
SELECT COUNT(*) FROM read_ion(
  '$ION_FILE',
  columns := {
    id: 'BIGINT',
    category: 'VARCHAR',
    amount: 'DOUBLE',
    flag: 'BOOLEAN',
    ts: 'TIMESTAMP',
    nested: 'STRUCT(sub_id BIGINT, sub_name VARCHAR)',
    tags: 'VARCHAR[]'
  }${ION_PROFILE_ARG}
);

PRAGMA profiling_output='$OUT_DIR/json_count.json';
SELECT COUNT(*) FROM read_json('$JSON_FILE');

PRAGMA profiling_output='$OUT_DIR/ion_project.json';
SELECT id, category, amount::DOUBLE FROM read_ion('$ION_FILE'${ION_PROFILE_ARG});

PRAGMA profiling_output='$OUT_DIR/ion_project_binary.json';
SELECT id, category, amount::DOUBLE FROM read_ion('$ION_BINARY_FILE'${ION_PROFILE_ARG});

PRAGMA profiling_output='$OUT_DIR/ion_project_explicit.json';
SELECT id, category, amount::DOUBLE
FROM read_ion(
  '$ION_FILE',
  columns := {
    id: 'BIGINT',
    category: 'VARCHAR',
    amount: 'DOUBLE',
    flag: 'BOOLEAN',
    ts: 'TIMESTAMP',
    nested: 'STRUCT(sub_id BIGINT, sub_name VARCHAR)',
    tags: 'VARCHAR[]'
  }${ION_PROFILE_ARG}
);

PRAGMA profiling_output='$OUT_DIR/json_project.json';
SELECT id, category, amount::DOUBLE FROM read_json('$JSON_FILE');

PRAGMA profiling_output='$OUT_DIR/ion_project_min.json';
SELECT id FROM read_ion('$ION_FILE'${ION_PROFILE_ARG});

PRAGMA profiling_output='$OUT_DIR/ion_project_min_explicit.json';
SELECT id
FROM read_ion(
  '$ION_FILE',
  records := true,
  columns := {
    id: 'BIGINT',
    category: 'VARCHAR',
    amount: 'DOUBLE',
    flag: 'BOOLEAN',
    ts: 'TIMESTAMP',
    nested: 'STRUCT(sub_id BIGINT, sub_name VARCHAR)',
    tags: 'VARCHAR[]'
  }${ION_PROFILE_ARG}
);

PRAGMA profiling_output='$OUT_DIR/ion_project_min_extractor.json';
SELECT id
FROM read_ion(
  '$ION_FILE',
  records := true,
  columns := {
    id: 'BIGINT',
    category: 'VARCHAR',
    amount: 'DOUBLE',
    flag: 'BOOLEAN',
    ts: 'TIMESTAMP',
    nested: 'STRUCT(sub_id BIGINT, sub_name VARCHAR)',
    tags: 'VARCHAR[]'
  }${ION_PROFILE_ARG}
);

PRAGMA profiling_output='$OUT_DIR/json_project_min.json';
SELECT id FROM read_json('$JSON_FILE');

PRAGMA profiling_output='$OUT_DIR/ion_filter_agg.json';
SELECT category, COUNT(*)
FROM read_ion('$ION_FILE'${ION_PROFILE_ARG})
WHERE amount::DOUBLE > 5
GROUP BY category;

PRAGMA profiling_output='$OUT_DIR/ion_filter_agg_explicit.json';
SELECT category, COUNT(*)
FROM read_ion(
  '$ION_FILE',
  columns := {
    id: 'BIGINT',
    category: 'VARCHAR',
    amount: 'DOUBLE',
    flag: 'BOOLEAN',
    ts: 'TIMESTAMP',
    nested: 'STRUCT(sub_id BIGINT, sub_name VARCHAR)',
    tags: 'VARCHAR[]'
  }${ION_PROFILE_ARG}
)
WHERE amount::DOUBLE > 5
GROUP BY category;

PRAGMA profiling_output='$OUT_DIR/json_filter_agg.json';
SELECT category, COUNT(*)
FROM read_json('$JSON_FILE')
WHERE amount::DOUBLE > 5
GROUP BY category;

PRAGMA profiling_output='$OUT_DIR/ion_count_wide.json';
SELECT COUNT(*) FROM read_ion('$ION_WIDE_FILE'${ION_PROFILE_ARG});

PRAGMA profiling_output='$OUT_DIR/ion_count_wide_binary.json';
SELECT COUNT(*) FROM read_ion('$ION_WIDE_BINARY_FILE'${ION_PROFILE_ARG});

PRAGMA profiling_output='$OUT_DIR/json_count_wide.json';
SELECT COUNT(*) FROM read_json('$JSON_WIDE_FILE');

PRAGMA profiling_output='$OUT_DIR/ion_project_wide.json';
SELECT id, w_int_00, w_str_00, w_dec_00
FROM read_ion('$ION_WIDE_FILE'${ION_PROFILE_ARG});

PRAGMA profiling_output='$OUT_DIR/ion_project_wide_binary.json';
SELECT id, w_int_00, w_str_00, w_dec_00
FROM read_ion('$ION_WIDE_BINARY_FILE'${ION_PROFILE_ARG});

PRAGMA profiling_output='$OUT_DIR/json_project_wide.json';
SELECT id, w_int_00, w_str_00, w_dec_00
FROM read_json('$JSON_WIDE_FILE');

PRAGMA threads=4;

PRAGMA profiling_output='$OUT_DIR/ion_count_nd_parallel.json';
SELECT COUNT(*)
FROM read_ion('$ION_FILE', format := 'newline_delimited'${ION_PROFILE_ARG});

PRAGMA profiling_output='$OUT_DIR/json_count_nd_parallel.json';
SELECT COUNT(*)
FROM read_json('$JSON_FILE');

PRAGMA profiling_output='$OUT_DIR/ion_project_min_nd_parallel.json';
SELECT id
FROM read_ion('$ION_FILE', format := 'newline_delimited'${ION_PROFILE_ARG});

PRAGMA profiling_output='$OUT_DIR/json_project_min_nd_parallel.json';
SELECT id
FROM read_json('$JSON_FILE');

PRAGMA threads=1;
SQL

"$DUCKDB_BIN" < "$SQL_FILE"

python3 - <<'PY'
import json
import os

out_dir = os.environ.get("OUT_DIR", "perf/results")
summary_path = os.path.join(out_dir, "summary.md")
summary_lines = []

def load(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def fmt(val):
    return f"{val:.3f}s"

def ratio(a, b):
    if b == 0:
        return "n/a"
    return f"{a / b:.2f}x"

def summarize_pairs(title, pairs):
    print(title)
    print("Query | Ion CPU | JSON CPU | CPU Ratio | Ion Lat | JSON Lat | Lat Ratio")
    print("--- | --- | --- | --- | --- | --- | ---")
    summary_lines.append(f"## {title}")
    summary_lines.append("Query | Ion CPU | JSON CPU | CPU Ratio | Ion Lat | JSON Lat | Lat Ratio")
    summary_lines.append("--- | --- | --- | --- | --- | --- | ---")
    for label, ion_file, json_file in pairs:
        ion = load(os.path.join(out_dir, ion_file))
        js = load(os.path.join(out_dir, json_file))
        line = (
            f"{label} | {fmt(ion['cpu_time'])} | {fmt(js['cpu_time'])} | {ratio(ion['cpu_time'], js['cpu_time'])} | "
            f"{fmt(ion['latency'])} | {fmt(js['latency'])} | {ratio(ion['latency'], js['latency'])}"
        )
        print(line)
        summary_lines.append(line)
    print()
    summary_lines.append("")

def summarize_binary(title, pairs):
    print(title)
    print("Query | Text CPU | Binary CPU | Speedup | Text Lat | Binary Lat | Speedup")
    print("--- | --- | --- | --- | --- | --- | ---")
    summary_lines.append(f"## {title}")
    summary_lines.append("Query | Text CPU | Binary CPU | Speedup | Text Lat | Binary Lat | Speedup")
    summary_lines.append("--- | --- | --- | --- | --- | --- | ---")
    for label, text_file, bin_file in pairs:
        text = load(os.path.join(out_dir, text_file))
        binary = load(os.path.join(out_dir, bin_file))
        line = (
            f"{label} | {fmt(text['cpu_time'])} | {fmt(binary['cpu_time'])} | {ratio(text['cpu_time'], binary['cpu_time'])} | "
            f"{fmt(text['latency'])} | {fmt(binary['latency'])} | {ratio(text['latency'], binary['latency'])}"
        )
        print(line)
        summary_lines.append(line)
    print()
    summary_lines.append("")

summarize_pairs(
    "Ion vs JSON (text)",
    [
        ("COUNT(*)", "ion_count.json", "json_count.json"),
        ("Project 3 cols", "ion_project.json", "json_project.json"),
        ("Filter + group", "ion_filter_agg.json", "json_filter_agg.json"),
        ("Wide project (4 cols)", "ion_project_wide.json", "json_project_wide.json"),
    ],
)

summarize_pairs(
    "Ion vs JSON (parallel newline-delimited)",
    [
        ("COUNT(*)", "ion_count_nd_parallel.json", "json_count_nd_parallel.json"),
        ("Project min", "ion_project_min_nd_parallel.json", "json_project_min_nd_parallel.json"),
    ],
)

summarize_pairs(
    "Ion binary vs JSON (text)",
    [
        ("COUNT(*)", "ion_count_binary.json", "json_count.json"),
        ("Project 3 cols", "ion_project_binary.json", "json_project.json"),
        ("Wide project (4 cols)", "ion_project_wide_binary.json", "json_project_wide.json"),
    ],
)

summarize_binary(
    "Ion text vs binary",
    [
        ("COUNT(*)", "ion_count.json", "ion_count_binary.json"),
        ("Project 3 cols", "ion_project.json", "ion_project_binary.json"),
        ("Wide COUNT(*)", "ion_count_wide.json", "ion_count_wide_binary.json"),
        ("Wide project (4 cols)", "ion_project_wide.json", "ion_project_wide_binary.json"),
    ],
)

with open(summary_path, "w", encoding="utf-8") as f:
    f.write("# Performance Summary\n\n")
    f.write("\n".join(summary_lines).rstrip())
    f.write("\n")

print(f"Wrote profiling output to {out_dir}")
print(f"Wrote summary to {summary_path}")
PY
