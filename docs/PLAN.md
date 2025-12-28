# Open Topics

## Product + Semantics
- Typed Ion nulls (`null.int`, `null.struct`, etc.): should they influence inference or be preserved?
- Ion annotations: ignore, preserve as metadata, or map into columns?
- Ion symbol tables: expose or ignore?
- Mapping for Ion types with no direct DuckDB equivalent (e.g., sexp).

## Writer
- Writer options: pretty printing, timestamp formatting, array vs newline-delimited defaults.
- Decide what to do for unsupported DuckDB types (explicit error vs string fallback).

## Performance
- Field lookup fast path (per-scan name -> column map; SID-aware where possible).
- Extractor path strategy and fallback behavior.
- Vectorized batch transform to avoid per-row `Value` construction.
- Parallel tuning for newline-delimited inputs once traversal cost is reduced.

## Validation
- Type fidelity across text vs binary Ion for nested values.
