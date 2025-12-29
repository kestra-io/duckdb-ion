#!/usr/bin/env bash
set -euo pipefail

UNITTEST_BIN="${UNITTEST_BIN:-./build/release/test/unittest}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-5}"
GDB_ATTEMPTS="${GDB_ATTEMPTS:-3}"

if [[ "$#" -lt 1 ]]; then
  echo "Usage: $0 <test-path> [test-path ...]" >&2
  exit 2
fi

for test_path in "$@"; do
  attempt=1
  while [[ "$attempt" -le "$MAX_ATTEMPTS" ]]; do
    if "$UNITTEST_BIN" "$test_path"; then
      attempt=$((attempt + 1))
      continue
    fi

    if ! command -v gdb >/dev/null 2>&1; then
      echo "gdb not available; install gdb to capture a backtrace." >&2
      exit 1
    fi

    echo "Test $test_path failed on attempt $attempt; running gdb for backtrace." >&2
    gdb --batch \
      -ex "set pagination off" \
      -ex "run" \
      -ex "bt" \
      --args "$UNITTEST_BIN" "$test_path"

    gdb_attempt=1
    while [[ "$gdb_attempt" -le "$GDB_ATTEMPTS" ]]; do
      echo "Retrying $test_path under gdb (attempt $gdb_attempt/$GDB_ATTEMPTS)" >&2
      gdb --batch \
        -ex "set pagination off" \
        -ex "run" \
        -ex "bt" \
        --args "$UNITTEST_BIN" "$test_path" && gdb_attempt=$((gdb_attempt + 1)) || exit 1
    done
    exit 1
  done
done
