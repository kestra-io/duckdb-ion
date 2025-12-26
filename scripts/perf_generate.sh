#!/usr/bin/env bash
set -euo pipefail

DUCKDB_BIN="${DUCKDB_BIN:-./build/release/duckdb}"
ROWS="${ROWS:-100000}"
OUT_DIR="${OUT_DIR:-perf/data}"

mkdir -p "$OUT_DIR"

WIDE_COLS=""
for i in $(seq -w 0 15); do
  WIDE_COLS+=", (i + $((10#$i)))::BIGINT AS w_int_$i"
done
for i in $(seq -w 0 15); do
  WIDE_COLS+=", ('s${i}_' || (i % 10)) AS w_str_$i"
done
for i in $(seq -w 0 15); do
  WIDE_COLS+=", ((i % 1000) * 0.01 + $((10#$i)) / 100.0)::DOUBLE AS w_dec_$i"
done

SQL_FILE="$OUT_DIR/generate_perf.sql"
cat > "$SQL_FILE" <<SQL
INSTALL json;
LOAD json;
LOAD ion;

CREATE OR REPLACE TABLE perf_source AS
SELECT
  i::BIGINT AS id,
  CASE (i % 5)
    WHEN 0 THEN 'alpha'
    WHEN 1 THEN 'bravo'
    WHEN 2 THEN 'charlie'
    WHEN 3 THEN 'delta'
    ELSE 'echo'
  END AS category,
  (i % 1000) * 0.01 AS amount,
  (i % 2) = 0 AS flag,
  (TIMESTAMP '2020-01-01' + i * INTERVAL '1 second') AS ts,
  struct_pack(sub_id := i % 10,
              sub_name := CASE (i % 3)
                WHEN 0 THEN 'x'
                WHEN 1 THEN 'y'
                ELSE 'z'
              END) AS nested,
  [CASE (i % 3)
     WHEN 0 THEN 't1'
     WHEN 1 THEN 't2'
     ELSE 't3'
   END, 't4'] AS tags
FROM range(0, $ROWS) tbl(i);

CREATE OR REPLACE TABLE perf_wide AS
SELECT
  i::BIGINT AS id${WIDE_COLS}
FROM range(0, $ROWS) tbl(i);

COPY perf_source TO '$OUT_DIR/data.ion' (FORMAT ION);
COPY perf_source TO '$OUT_DIR/data.jsonl' (FORMAT JSON);
COPY perf_wide TO '$OUT_DIR/data_wide.ion' (FORMAT ION);
COPY perf_wide TO '$OUT_DIR/data_wide.jsonl' (FORMAT JSON);
SQL

"$DUCKDB_BIN" < "$SQL_FILE"
echo "Wrote datasets to $OUT_DIR"
