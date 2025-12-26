# Ion vs JSON: Implementation Comparison

This note summarizes how `read_ion` (Ion extension) differs from `read_json` (DuckDB JSON extension) and where
the biggest optimization opportunities likely sit. It focuses on the ingestion path used in the perf suite.

## Parsing & Data Flow
- `read_json` uses `yyjson` to parse JSONL buffers into a DOM and then extracts typed values in batches.
- `read_ion` uses `ion-c`’s streaming reader to walk Ion values/fields and materialize values row-by-row.
- JSON parsing benefits from tight loops over already-parsed DOM nodes; Ion parsing currently does more per-value
  reader calls and branching.

## Schema Inference
- JSON inference (`read_json`) builds a candidate structure tree and refines types across samples.
- Ion inference (`read_ion`) inspects up to a fixed number of rows (default 1000) and merges types with a
  promotion heuristic (e.g., DECIMAL vs DOUBLE vs VARCHAR).

## Projection & Field Lookup
- JSON has native projection pushdown in its reader path and can skip irrelevant fields while parsing.
- Ion uses a field-name lookup per struct field, with a SID cache to accelerate repeated field names.
- An experimental extractor path exists for very small projections, but it still routes through value parsing.

## Type Materialization
- JSON transforms `yyjson` numeric and string types directly into vectors without round-trips through string
  formatting, and applies casting rules inline.
- Ion currently converts DECIMAL via `ion_decimal_to_string` + `TryCastToDecimal`, which is correct but slower.
- Both paths write into DuckDB vectors, but Ion does more per-row `Value` construction and branching.

## Parallelism & I/O
- JSON reader uses a buffered scan with line/object parsing; the perf suite uses JSONL with good parallelism.
- Ion parallel scans are only enabled for newline-delimited formats and are not currently used in the baseline
  comparisons.
- Bytes read are similar for comparable inputs; gaps are dominated by parse and materialization costs.

## Behavioral Differences
- Ion supports typed values (decimals, timestamps, blobs, symbols) that do not have JSON equivalents.
- JSON allows unstructured ingestion into `JSON` type when inference conflicts; Ion currently promotes to
  `VARCHAR` as a last resort.

## Improvement Options
- **Vectorized materialization:** avoid per-row `Value` creation; write directly into vectors whenever possible.
- **Fast field matching:** precompute symbol IDs for projected columns when available to skip string comparisons.
- **Skip logic:** add a real `SkipIonValue` implementation to avoid traversing unprojected fields.
- **Decimal/timestamp fast path:** restore a correct binary-to-decimal conversion path to avoid string casts.
- **Extractor path:** validate and extend the ion-c extractor usage for small projections.
- **Parallel scans:** enable and tune newline-delimited parallelism in the perf suite for apples-to-apples runs.

## Recent Insights (Perf)
- Text Ion is still ~3–4x slower than JSON on CPU with much larger latency deltas; overhead is likely in per-row
  materialization and conversions rather than I/O.
- Binary Ion narrows the gap or beats JSON on CPU for several queries, but latency remains higher, suggesting
  remaining overhead outside the parser.
- Wide-schema wins for binary Ion indicate text parsing cost dominates in that scenario.
