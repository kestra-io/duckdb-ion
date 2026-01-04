# Refactoring Plan: `src/ion_extension.cpp`

Goal: improve maintainability and testability of the Ion extension implementation without changing behavior, SQL surface area, or performance characteristics (unless explicitly called out).

Scope: refactor `read_ion` implementation currently concentrated in `src/ion_extension.cpp` into smaller modules, with clearer ownership boundaries around Ion-C handles, schema inference, scanning, and value conversion.

Non-goals:
- Changing the SQL API (`read_ion` parameters, defaults, output semantics).
- Changing binary/text Ion semantics.
- Replacing Ion-C or DuckDB’s table function framework.
- Reworking unrelated extension functionality (COPY, scalar functions) unless required by the split.

## Current Structure (for orientation)

`src/ion_extension.cpp` currently contains:
- DuckDB table-function lifecycle: `IonReadBind` → `IonReadInit` → `IonReadInitLocal` → `IonReadFunction`
- Parameter parsing helpers: `Parse*Parameter`, `ParseIonPaths`, `ParseColumnsParameter`
- Schema inference: `InferIonSchema`, `ExtractIonStructure`, `IonStructureToType`, `PromoteIonType`, etc.
- Runtime reading: file/range management (`OpenIonFile`, range splitting), optional extractor path, vectorized read helpers
- Value conversion: `IonReadValue` (+ helpers for decimals/timestamps/skip)
- Profiling/timing plumbing: `IonTimingScope`, `ReportProfile`

## Refactoring Principles

- Keep function signatures stable at module boundaries (prefer small “state objects” over wide parameter lists).
- Prefer RAII wrappers for Ion-C resources (`ION_READER*`, `hEXTRACTOR`, etc.).
- Avoid “hidden” compile-time behavior changes; preserve `#ifdef DUCKDB_IONC` gating but confine it to a small compatibility layer.
- Split by responsibility and dependency direction:
  - `scan` depends on `value` conversion and `ionc` shim
  - `infer` depends on `value` type promotion and `ionc` shim
  - `bind` depends on `infer` and DuckDB types
- Incremental: each step should be a mechanical move + build/test, not a rewrite.

## Proposed Target Layout

### Public headers (`src/include/`)
- `src/include/ion/read_ion.hpp`
  - Declares `RegisterReadIon(ExtensionLoader&)` or similar registration entry.
  - Declares bind data structs if they’re used across translation units.
- `src/include/ion/ionc_shim.hpp`
  - Tiny adapter layer that isolates Ion-C includes, types, and `#ifdef DUCKDB_IONC`.

### Implementation files (`src/`)
- `src/ion/read_ion_register.cpp`
  - Contains only `LoadInternal`-level registration of `read_ion` (and `MultiFileReader::CreateFunctionSet` wiring).
- `src/ion/read_ion_bind.cpp`
  - `IonReadBind` + parameter parsing helpers.
  - Calls into schema inference module.
- `src/ion/read_ion_infer.cpp`
  - `InferIonSchema` + type-structure accumulation (`IonStructureNode`, `ExtractIonStructure`, `IonStructureToType`).
- `src/ion/read_ion_scan.cpp`
  - `IonReadInit`, `IonReadInitLocal`, `IonReadFunction`.
  - Range splitting, file iteration, extractor usage.
- `src/ion/read_ion_value.cpp`
  - `IonReadValue`, `ReadIonValueToVector`, decimal/timestamp conversion helpers, `SkipIonValue`.
- `src/ion/read_ion_profile.cpp`
  - `IonTimingScope`, `ReportProfile`, timing counters (if this can be cleanly separated).

Notes:
- Keep existing `src/ion_extension.cpp` temporarily as a thin forwarder during transition, then delete it.
- Keep existing `src/include/ion_extension.hpp` if it’s part of the extension template contract; otherwise migrate declarations.

## Step-by-Step Plan (Incremental)

### Step 0: Safety net (no behavior changes)
- Ensure `make test` passes on main before refactor begins.
- Add one SQLLogicTest that covers:
  - `read_ion` with explicit `columns := {...}`
  - `read_ion` with inference (at least one nested struct/list)
  - `format := 'array'` and `format := 'newline_delimited'` on small fixtures
  - projection pushdown (`SELECT id FROM read_ion(...)`) if not already covered

### Step 1: Introduce module scaffolding (compile-only)
- Add the new file structure and headers with minimal declarations.
- Keep all logic in `src/ion_extension.cpp` initially; new files just include headers and compile.

### Step 2: Extract Ion-C adapter (“ionc shim”)
- Move Ion-C includes and small wrapper helpers behind `src/include/ion/ionc_shim.hpp`.
- Introduce RAII wrappers:
  - `IonReaderHandle` (owns `ION_READER*` and closes in destructor)
  - `IonExtractorHandle` (owns `hEXTRACTOR` and closes)
- Replace raw pointers in scan state with these wrappers where possible.
- Keep behavior identical; no algorithm changes.

### Step 3: Split value conversion
- Move `IonReadValue`, `SkipIonValue`, `ReadIonValueToVector`, decimal/timestamp helpers into `read_ion_value.cpp`.
- Keep `IonReadScanState` in a shared header if required by both scan and value modules.
- Ensure compilation and `make test` pass.

### Step 4: Split schema inference
- Move schema inference logic (`InferIonSchema`, `IonStructureNode`, etc.) into `read_ion_infer.cpp`.
- Bind module calls the inference module; avoid circular dependencies by keeping “type promotion” helpers in `read_ion_value.cpp` or a shared `read_ion_types.cpp`.
- Run `make test`.

### Step 5: Split bind and scan
- Move bind parsing (`Parse*Parameter`, `ParseIonPaths`, `IonReadBind`) into `read_ion_bind.cpp`.
- Move init and scan (`IonReadInit`, `IonReadInitLocal`, `IonReadFunction`, range helpers, extractor wiring) into `read_ion_scan.cpp`.
- `read_ion_register.cpp` becomes the only place that registers the `TableFunction`.
- Run `make test` and `make tidy-check`.

### Step 6: Remove legacy monolith file
- Replace `src/ion_extension.cpp` with a small file that only wires the extension entrypoints (or remove it and update `CMakeLists.txt` sources accordingly).
- Ensure the extension still builds as static and loadable.

### Step 7: Optional cleanups (only if safe)
- Reduce global/static state where feasible (e.g., thread_local contexts) by threading through explicit pointers in scan state.
- Clarify ownership and mutability in the extractor path (avoid copying strings unless necessary).
- Consider moving profiling logic behind a feature flag or a small interface to reduce clutter in scan loop.

## Validation Checklist (per step)
- `make` (release build)
- `make test`
- `make format-check`
- `make tidy-check` (at least on one platform)
- Run one targeted perf check (`scripts/perf_run.sh` small dataset) to ensure no obvious regressions.

## Risk Notes
- DuckDB’s extension build pipeline and export sets are sensitive to how libraries/targets are linked. Prefer linking via imported targets from vcpkg and avoid introducing new non-exported build targets into DuckDB export sets.
- Keep `#ifdef DUCKDB_IONC` behavior stable: the extension should fail clearly at runtime if built without Ion-C support.

