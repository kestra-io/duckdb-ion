#!/usr/bin/env bash
set -euo pipefail

DUCKDB_BIN="${DUCKDB_BIN:-./build/release/duckdb}"
DATA_DIR="${DATA_DIR:-perf/data}"
OUT_DIR="${OUT_DIR:-perf/results}"

ION_FILE="$DATA_DIR/data.ion"
JSON_FILE="$DATA_DIR/data.jsonl"
ION_WIDE_FILE="$DATA_DIR/data_wide.ion"
JSON_WIDE_FILE="$DATA_DIR/data_wide.jsonl"

mkdir -p "$OUT_DIR"

SQL_FILE="$OUT_DIR/run_perf.sql"
cat > "$SQL_FILE" <<SQL
INSTALL json;
LOAD json;
LOAD ion;

PRAGMA enable_profiling='json';

PRAGMA profiling_output='$OUT_DIR/ion_count.json';
SELECT COUNT(*) FROM read_ion('$ION_FILE');

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
  }
);

PRAGMA profiling_output='$OUT_DIR/json_count.json';
SELECT COUNT(*) FROM read_json('$JSON_FILE');

PRAGMA profiling_output='$OUT_DIR/ion_project.json';
SELECT id, category, amount::DOUBLE FROM read_ion('$ION_FILE');

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
  }
);

PRAGMA profiling_output='$OUT_DIR/json_project.json';
SELECT id, category, amount::DOUBLE FROM read_json('$JSON_FILE');

PRAGMA profiling_output='$OUT_DIR/ion_project_min.json';
SELECT id FROM read_ion('$ION_FILE');

PRAGMA profiling_output='$OUT_DIR/ion_project_min_explicit.json';
SELECT id
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
  }
);

PRAGMA profiling_output='$OUT_DIR/json_project_min.json';
SELECT id FROM read_json('$JSON_FILE');

PRAGMA profiling_output='$OUT_DIR/ion_filter_agg.json';
SELECT category, COUNT(*)
FROM read_ion('$ION_FILE')
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
  }
)
WHERE amount::DOUBLE > 5
GROUP BY category;

PRAGMA profiling_output='$OUT_DIR/json_filter_agg.json';
SELECT category, COUNT(*)
FROM read_json('$JSON_FILE')
WHERE amount::DOUBLE > 5
GROUP BY category;

PRAGMA profiling_output='$OUT_DIR/ion_count_wide.json';
SELECT COUNT(*) FROM read_ion('$ION_WIDE_FILE');

PRAGMA profiling_output='$OUT_DIR/json_count_wide.json';
SELECT COUNT(*) FROM read_json('$JSON_WIDE_FILE');

PRAGMA profiling_output='$OUT_DIR/ion_project_wide.json';
SELECT id, w_int_00, w_str_00, w_dec_00
FROM read_ion('$ION_WIDE_FILE');

PRAGMA profiling_output='$OUT_DIR/json_project_wide.json';
SELECT id, w_int_00, w_str_00, w_dec_00
FROM read_json('$JSON_WIDE_FILE');

PRAGMA threads=4;

PRAGMA profiling_output='$OUT_DIR/ion_count_nd_parallel.json';
SELECT COUNT(*)
FROM read_ion('$ION_FILE', format := 'newline_delimited');

PRAGMA profiling_output='$OUT_DIR/ion_project_min_nd_parallel.json';
SELECT id
FROM read_ion('$ION_FILE', format := 'newline_delimited');

PRAGMA threads=1;
SQL

"$DUCKDB_BIN" < "$SQL_FILE"
echo "Wrote profiling output to $OUT_DIR"
