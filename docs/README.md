# DuckDB Ion Extension

This repository implements a DuckDB extension for reading (and eventually writing) AWS Ion data. It is based on the DuckDB extension template (`docs/TEMPLATE_README.md`) and is intended for eventual distribution via community extensions.

## Status
- `read_ion(path)` reads newline-delimited Ion structs and maps fields to columns using schema inference.
- `read_ion(path, columns := {field: 'TYPE', ...})` uses an explicit schema and skips inference.
- `read_ion(path, records := 'false')` reads scalar values into a single `ion` column.
- `read_ion(path, format := 'array')` reads from a top-level Ion list (array) instead of top-level values.
- Complex types (list/struct) and richer binary support are planned next.

## Parameters
- `columns`: struct of `name: 'SQLTYPE'` pairs; skips inference and uses that schema.
- `format`: `'auto'` (default), `'newline_delimited'`, `'array'`, or `'unstructured'`.
- `records`: `'auto'` (default), `'true'`, `'false'`, or a BOOLEAN.
- You can combine `format` with `records` (e.g., `format := 'array', records := 'false'`). `columns` requires `records=true`.

## Building
### Dependencies
This extension uses vcpkg for dependencies (ion-c, OpenSSL). Ensure `VCPKG_ROOT` points to your vcpkg checkout and that you have built it (`./bootstrap-vcpkg.sh`).

```sh
CCACHE_DISABLE=1 VCPKG_TOOLCHAIN_PATH="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" \
VCPKG_TARGET_TRIPLET="$(uname -m)-osx" make
```

The repo uses a local vcpkg overlay in `vcpkg_ports/`.

### Build Outputs
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/ion/ion.duckdb_extension
```

## Running
```sh
./build/release/duckdb
```

Example:
```sql
SELECT bigint, varchar, bool FROM read_ion('test/ion/sample.ion');
SELECT bigint, varchar, bool
FROM read_ion('test/ion/sample.ion', columns := {bigint: 'BIGINT', varchar: 'VARCHAR', bool: 'BOOLEAN'});
SELECT ion FROM read_ion('test/ion/scalars.ion', records := false);
SELECT bigint, varchar, bool
FROM read_ion('test/ion/array.ion', format := 'array');
```

## Tests
SQLLogicTests live in `test/sql`:
```sh
make test
```

## Installing (Unsigned)
If you are loading a local or custom build, you may need `allow_unsigned_extensions`:
```sql
INSTALL ion;
LOAD ion;
```

For custom repositories:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/ion/latest';
INSTALL ion;
LOAD ion;
```
