#!/usr/bin/env bash
set -euo pipefail

DUCKDB_BIN="${DUCKDB_BIN:-./build/release/duckdb}"
DATA_DIR="${DATA_DIR:-perf/data}"
OUT_DIR="${OUT_DIR:-perf/results}"

ION_FILE="$DATA_DIR/data.ion"
JSON_FILE="$DATA_DIR/data.jsonl"

mkdir -p "$OUT_DIR"

SQL_FILE="$OUT_DIR/run_perf.sql"
cat > "$SQL_FILE" <<SQL
INSTALL json;
LOAD json;
LOAD ion;

PRAGMA enable_profiling='json';

PRAGMA profiling_output='$OUT_DIR/ion_count.json';
SELECT COUNT(*) FROM read_ion('$ION_FILE');

PRAGMA profiling_output='$OUT_DIR/json_count.json';
SELECT COUNT(*) FROM read_json('$JSON_FILE');

PRAGMA profiling_output='$OUT_DIR/ion_project.json';
SELECT id, category, amount FROM read_ion('$ION_FILE');

PRAGMA profiling_output='$OUT_DIR/json_project.json';
SELECT id, category, amount FROM read_json('$JSON_FILE');

PRAGMA profiling_output='$OUT_DIR/ion_filter_agg.json';
SELECT category, COUNT(*)
FROM read_ion('$ION_FILE')
WHERE amount > 5
GROUP BY category;

PRAGMA profiling_output='$OUT_DIR/json_filter_agg.json';
SELECT category, COUNT(*)
FROM read_json('$JSON_FILE')
WHERE amount > 5
GROUP BY category;
SQL

"$DUCKDB_BIN" < "$SQL_FILE"
echo "Wrote profiling output to $OUT_DIR"
