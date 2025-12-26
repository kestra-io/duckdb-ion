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

os_name="$(uname -s | tr '[:upper:]' '[:lower:]')"
if [[ "$os_name" == "darwin" ]]; then
  os_name="osx"
fi
triplet="${VCPKG_TARGET_TRIPLET:-$(uname -m)-$os_name}"
local_prefix="$PWD/vcpkg_installed/$triplet"
vcpkg_prefix="${VCPKG_ROOT:-}/installed/$triplet"
if [[ -d "$local_prefix" ]]; then
  include_dir="$local_prefix/include"
  lib_dir="$local_prefix/lib"
elif [[ -d "$vcpkg_prefix" ]]; then
  include_dir="$vcpkg_prefix/include"
  lib_dir="$vcpkg_prefix/lib"
else
  include_dir=""
  lib_dir=""
fi

binary_tool="build/ion_text_to_binary"
if [[ -n "$include_dir" && -n "$lib_dir" ]]; then
  if [[ ! -x "$binary_tool" || "scripts/ion_text_to_binary.cpp" -nt "$binary_tool" ]]; then
    c++ -std=c++17 -O2 -I"$include_dir" "scripts/ion_text_to_binary.cpp" \
      "$lib_dir/libionc_static.a" "$lib_dir/libdecNumber_static.a" -o "$binary_tool"
  fi
  "$binary_tool" "$OUT_DIR/data.ion" "$OUT_DIR/data_binary.ion"
  "$binary_tool" "$OUT_DIR/data_wide.ion" "$OUT_DIR/data_wide_binary.ion"
else
  echo "Skipping binary Ion generation (vcpkg ion-c not found)." >&2
fi

echo "Wrote datasets to $OUT_DIR"
