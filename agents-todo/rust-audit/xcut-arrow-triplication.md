# XCUT arrow triplication

Scope: `packages/columine/crates/columine-arrow/Cargo.toml` (17), `src/lib.rs` (36), `src/schema.rs` (434), `src/ipc.rs`
(688), `src/record_batch.rs` (713; framing + `DynamicColumn` read; encoder body sampled), `src/columns.rs` (1138;
header + `DynamicColumns::new` read; remainder not audited — ColArrow owns internals).
`packages/lmao/crates/lmao-arrow/Cargo.toml` (23), `src/lib.rs` (53), `src/convert.rs` (317), `src/dict.rs` (577),
`src/archive.rs` (155), `src/source.rs` (136), `tests/convert.rs` (168), `tests/properties.rs` (646; IPC helper + schema
asserts), `benches/flush.rs` (120; imports only). `packages/cowshed/crates/cowshed-core/Cargo.toml` (39),
`src/storage/audit.rs` (637; Arrow sink + `StreamWriter` seal), `src/storage/job_artifact.rs` (4574; Arrow
schema/codec/IPC sites only — CsCoreJobArtifact owns the rest). `packages/cowshed/crates/cowshed-gateway/Cargo.toml`
(45), `src/telemetry.rs` (1114). Workspace pins: `packages/columine/Cargo.toml` (62), `packages/lmao/Cargo.toml` (50).
Locks: `packages/columine/Cargo.lock` (arrow-* 56.2.1), `packages/lmao/Cargo.lock` (arrow 55.2.0 + datafusion 47),
`packages/cowshed/Cargo.lock` (arrow-* 56.2.1). Targeted greps across `packages/*/crates` and `packages/lmao/src` for
schema/IPC/entry-type restatements.

## Summary

- Four Arrow dependents, two versions: columine-arrow + cowshed-core + cowshed-gateway on 56.2.1; lmao-arrow on 55.2.0
  because `lmao-query`'s optional `datafusion 47` pulls the `arrow` 55 umbrella.
- They are three products, not three copies of one codec: columine hand-rolls flat IPC (no `arrow-array` in prod); lmao
  builds dictionary `RecordBatch`es; cowshed uses `StreamWriter` on nested/utf8 batches.
- The live SSOT bug is the **trace row**: `lmao_arrow::trace_schema` and gateway `event_batch` share 01f column _names_
  and already disagree on types (Int64 vs Timestamp, dict vs Utf8, UInt32 vs UInt64 span ids).
- `StreamWriter::try_new` / `write` / `finish` is restated at five sites; cowshed-core's own comment says a second Arrow
  dep would fork the version — gateway did exactly that.
- ONE crate should own the **version pin + IPC helper + 01f system-column schema**. Do not merge the three column
  builders. Blocker: datafusion 47.
- lmao workspace pin omits `default-features = false` and therefore compiles `chrono-tz` (absent from the 56 locks).
- Gateway `event_batch` clones every string field 2–4× per request into `StringArray` (lmao already does borrowed
  dicts).
- Dictionary handling exists only in lmao-arrow. columine cannot encode dict/list/struct. cowshed `CSARROW1` is an
  envelope, not a second IPC codec.

## Findings

### F1 — HIGH — SSOT — Arrow is pinned twice (55 and 56); no crate owns the version

Evidence: `packages/columine/Cargo.toml:20-22`, `packages/lmao/Cargo.toml:23-27`,
`packages/cowshed/crates/cowshed-core/Cargo.toml:14-17`, `packages/cowshed/crates/cowshed-gateway/Cargo.toml:9-11`,
`packages/lmao/Cargo.lock:64-68`, `packages/lmao/Cargo.lock:583-591`, `packages/columine/Cargo.lock:29-30`,
`packages/cowshed/Cargo.lock:44-45`

```
# columine workspace
arrow-array = { version = "56", default-features = false }
arrow-ipc = { version = "56", default-features = false }
arrow-schema = { version = "56", default-features = false }

# lmao workspace
# Arrow subcrates only (not the `arrow` umbrella) to keep compile times down.
arrow-array = "55"
arrow-buffer = "55"
arrow-schema = "55"
arrow-ipc = "55"

# cowshed-core / cowshed-gateway manifests (no workspace pin)
arrow-array = "56"
arrow-ipc = "56"
arrow-schema = "56"
```

`packages/lmao/Cargo.lock` resolves `arrow` 55.2.0 (umbrella) as a dependency of `datafusion` 47.0.0. columine and
cowshed locks resolve `arrow-array`/`arrow-ipc`/`arrow-schema` 56.2.1 only — no 55, no umbrella. Problem: three Cargo
workspaces, two Arrow generations, four dependents. Types cannot be shared. A `RecordBatch` from lmao-arrow is a
different crate than one from cowshed. The version split is the SSOT failure; it is not itself a wire-format bug (IPC is
consumed in-process per workspace). Fix: introduce one path crate (name: `smoothbricks-arrow`) that pins
`arrow-array`/`arrow-buffer`/`arrow-schema`/`arrow-ipc` at a single version with `default-features = false`, re-exports
them, and owns `write_ipc_stream` / `read_ipc_stream` plus `trace_schema` (see F2). columine-arrow, lmao-arrow,
cowshed-core, and cowshed-gateway depend on it. Decision: pin **56**, matching columine/cowshed HEAD. Keep lmao on 55
only until `lmao-query`'s `datafusion = "47"` is upgraded or the datafusion feature is dropped — same workspace cannot
compile 55 and 56. Cost/Risk: lmao-query datafusion feature is the blast radius (cross-slice). columine-arrow must keep
`arrow-array` off the wasm prod graph (today it is a dev-dep only). Do not fold columine's hand-rolled encoder or
cowshed's nested job schema into this crate.

### F2 — HIGH — SSOT — 01f trace schema restated incompatibly (live type split)

Evidence: `packages/lmao/crates/lmao-arrow/src/convert.rs:52-63` vs
`packages/cowshed/crates/cowshed-gateway/src/telemetry.rs:700-728`

```
pub fn trace_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("trace_id", dict_type(DataType::UInt32), false),
        Field::new("thread_id", DataType::UInt64, false),
        Field::new("span_id", DataType::UInt32, false),
        Field::new("parent_thread_id", DataType::UInt64, true),
        Field::new("parent_span_id", DataType::UInt32, true),
        Field::new("entry_type", dict_type(DataType::UInt8), false),
        Field::new("message", dict_type(DataType::UInt32), true),
        Field::new("line_number", DataType::UInt32, false),
    ]))
}
```

```
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("thread_id", DataType::UInt64, false),
        Field::new("span_id", DataType::UInt64, false),
        Field::new("parent_thread_id", DataType::UInt64, true),
        Field::new("parent_span_id", DataType::UInt64, true),
        Field::new("entry_type", DataType::Utf8, false),
        Field::new("message", DataType::Utf8, false),
        // + sequence/workspace_id/repo_id/... gateway attributes
```

Gateway also emits the lmao entry-type strings (`telemetry.rs:667-773`: `"span-start"`, `"span-ok"`, `"span-exception"`,
`"span-err"`). Problem: same concept (01f system columns), two schemas. They already disagree: timestamp Int64 vs
Timestamp(ns); trace_id/entry_type/message dictionary vs Utf8; span_id/parent_span_id UInt32 vs UInt64; gateway drops
`line_number` and adds 15 extra columns. A flechette/lmao-query reader pointed at a gateway `.arrow` segment cannot use
`trace_schema()`. This is divergence that already happened — a live interop bug if those files are ever treated as
traces. Fix: `smoothbricks-arrow::trace_schema()` (F1) is the single source for the 9 system columns. Gateway
`event_batch` uses that schema as a prefix and appends gateway fields; it dictionary-encodes
`trace_id`/`entry_type`/`message` the way `convert_span_trees` already does. Decision: keep timestamp as `Int64`
epoch-ns (lmao 01f / columine physical Int64) rather than `Timestamp` — columine's `logical_type_matches` already treats
Timestamp as physical Int64 (`schema.rs:229`), and lmao stores i64 ticks. Cost/Risk: every gateway segment reader/test
(`telemetry.rs` tests, `packages/cowshed/crates/cowshed-gateway/tests/gateway.rs`) must accept dictionary columns and
UInt32 span ids. Existing on-disk gateway segments become a new schema generation (greenfield: migrate and delete).

### F3 — HIGH — DUPLICATION — `StreamWriter` try_new/write/finish copied five times

Evidence:

- `packages/cowshed/crates/cowshed-core/src/storage/job_artifact.rs:2463-2475` (`encode_batch`)
- `packages/cowshed/crates/cowshed-core/src/storage/job_artifact.rs:3415-3432` (`encode_controller_commitment`; comment
  admits the fork)
- `packages/cowshed/crates/cowshed-core/src/storage/audit.rs:413-421` (`ArrowAuditSink::seal`)
- `packages/cowshed/crates/cowshed-gateway/src/telemetry.rs:571-579` (`write_segment`)
- `packages/lmao/crates/lmao-arrow/tests/properties.rs:116-123` (`ipc_bytes`); same pattern `tests/convert.rs:144-146`

```
/// One commitment as a self-contained Arrow IPC stream — the byte form an
/// external [`crate::storage::audit::AuditSink`] stores. Owned here beside
/// the batch codec so the schema and the encoding drift together or not at
/// all; a host that carried its own Arrow dependency would fork the version.
pub fn encode_controller_commitment(...) {
    ...
    let mut writer = StreamWriter::try_new(&mut out, batch.schema_ref()) ...
    writer.write(&batch) ...
    writer.finish() ...
}
```

```
            let mut writer = StreamWriter::try_new(&mut file, &batch.schema())
                .map_err(|error| AuditError(format!("creating Arrow stream: {error}")))?;
            writer.write(&batch) ...
            writer.finish() ...
```

Problem: the IPC stream write is one function. cowshed-core already centralized controller-commitment bytes so hosts
would not take a second Arrow dep — then gateway (which core depends on, so it cannot import core) took Arrow 56 anyway
and rewrote the same three calls. `audit.rs::seal` does not even call `encode_controller_commitment`; it rebuilds the
batch and writes the stream itself. columine-arrow does **not** share this helper: it hand-emits IPC (`ipc.rs:159-229`)
because prod must not link `arrow-array`. Fix: put
`write_ipc_stream<W: Write>(sink: W, batch: &RecordBatch) -> Result<W, ArrowError>` (and the inverse reader that asserts
one batch) in the F1 crate. Replace the five call sites. `ArrowAuditSink::seal` should `encode_controller_commitment`
then write bytes, or call the shared helper on the batch it already built — not a third copy. columine-arrow stays on
`write_arrow_ipc_from_borrowed_columns`; do not route wasm events through `StreamWriter`. Cost/Risk: cowshed-gateway
cannot depend on cowshed-core (core already depends on gateway). The helper must live in a crate beneath both. Error
types today wrap `to_string()`; keep that at the callsite.

### F4 — MEDIUM — SSOT — `ENTRY_TYPE_NAMES` restated three ways

Evidence: `packages/lmao/crates/lmao-arrow/src/convert.rs:21-46`,
`packages/lmao/src/lib/schema/systemSchema.ts:314-340`,
`packages/cowshed/crates/cowshed-gateway/src/telemetry.rs:667-774`

```
pub const ENTRY_TYPE_NAMES: [&str; 24] = [
    "span-start", "span-ok", "span-err", "span-exception", "span-retry",
    "trace", "debug", "info", "warn", "error", ...
];
```

```
export const ENTRY_TYPE_NAMES = [
  '', // 0 - unused
  'span-start', // 1
  'span-ok', // 2
  ...
  'buffer-capacity', // 24
] as const;
```

Gateway does not import either table; `end_entry_type` hardcodes `"span-ok"` / `"span-exception"` / `"span-err"` and
`push_row` hardcodes `"span-start"`. Problem: one discriminant→name table. Rust omits the unused index-0 slot and uses
`entry_type - 1` as the dict key (`convert.rs:253, 286-288`). TS indexes by raw code. They currently agree, but nothing
enforces it. Gateway can already emit a string that is not in the table. Fix: generate the Rust `ENTRY_TYPE_NAMES` from
the TS table (or the reverse) in one place; gateway calls that table instead of literals. lmao-arrow is the Rust source;
TS is the documented original (`01a`). Cost/Risk: XcutRustTs owns the TS side. A generator or a shared JSON table; do
not hand-sync.

### F5 — MEDIUM — DEP-BLOAT — lmao Arrow 55 default features pull `chrono-tz` and the `arrow` umbrella

Evidence: `packages/lmao/Cargo.toml:23-27`, `packages/lmao/Cargo.lock:100-114`, `packages/lmao/Cargo.lock:64-83`,
`packages/columine/Cargo.toml:20-22`, `packages/columine/Cargo.lock:29-42`,
`packages/lmao/crates/lmao-query/Cargo.toml:10-21`

```
# lmao lock, arrow-array 55.2.0
dependencies = [
 "ahash", "arrow-buffer", "arrow-data", "arrow-schema",
 "chrono", "chrono-tz", "half", "hashbrown 0.15.5", "num",
]
# columine lock, arrow-array 56.2.1 (default-features = false)
dependencies = [
 "ahash", "arrow-buffer", "arrow-data", "arrow-schema",
 "chrono", "half", "hashbrown", "num",
]
```

lmao-arrow stores timestamps as `Int64Array` (`convert.rs:54, 303`), not `Timestamp`. It never needs tz databases. The
workspace comment says "subcrates only (not the `arrow` umbrella)" but `datafusion 47` still locks the umbrella
(`arrow-arith`/`arrow-csv`/`arrow-json`/`arrow-ord`/`arrow-row`/`arrow-string`/…). Problem: columine already proved
`default-features = false` on 56 drops `chrono-tz`. lmao did not. The umbrella is optional-feature lock residue, but it
is the reason the workspace cannot move to 56 (F1). Fix: set `arrow-array`/`arrow-buffer`/`arrow-schema`/`arrow-ipc` to
`{ version = "…", default-features = false }` in `packages/lmao/Cargo.toml` now (safe even on 55). Treat datafusion's
umbrella as LmaoQuery's problem: either drop the feature or bump datafusion until it accepts 56. Cost/Risk: wrong "just
shell out" would be worse — Arrow is load-bearing in-process (typed `RecordBatch`, wasm-adjacent flush, error typing).
Do not replace these crates with `pyarrow` CLI. rustc-hash in lmao-arrow is also load-bearing (dict.rs:4-9 records the
hasher measurement).

### F6 — MEDIUM — COPIES — gateway builds Utf8 columns by cloning every field 2–4 times per event

Evidence: `packages/cowshed/crates/cowshed-gateway/src/telemetry.rs:591-665`, contrast
`packages/lmao/crates/lmao-arrow/src/dict.rs:4-7, 447-454` and `src/convert.rs:223-233`

```
    let row_capacity = events.len().saturating_mul(4);
    ...
        let mut push_row = |...| {
            timestamp.push(i64::try_from(at).unwrap_or(i64::MAX));
            trace_id.push(trace.clone());
            ...
            entry_type.push(entry.to_owned());
            message.push(span_name.to_owned());
            workspace_id.push(event.workspace_id.clone());
            repo_id.push(event.repo_id.clone());
            ...
        };
```

Regime: per gateway audit flush (`DEFAULT_BATCH_CAPACITY = 64`, `DEFAULT_FLUSH_INTERVAL = 25ms`, `telemetry.rs:24-27`) —
not a per-byte parse loop, but it is the request-audit path, not once-per-process. Each event expands to 2 or 4 rows
(`saturating_mul(4)`), each row cloning ~15 `String`s, then `StringArray::from` copies again into Arrow buffers.
Problem: lmao-arrow already solved this column (borrowed `&str` dict keys, exact-size Vecs handed to arrow-buffer).
Gateway re-derives the same 01f columns as owned Utf8 (F2) and pays the copies. Byproduct L0: the entry-type string is a
closed 24-name table; the index IS the value. Fix: after F2, pass 1 observe distinct `trace_id`/`entry_type`/`message`
(reuse `ColumnDictionary`), pass 2 write keys. Numeric columns stay `Vec<u64>` → `UInt64Array::from` (that copy is the
Arrow buffer; do not add a second). Stop `enum_name`'s serde_json round-trip (`telemetry.rs:760-764`) for
`AuditStatus`/`AuditKind` — match to `&'static str`. Cost/Risk: telemetry.rs only. Schema change couples to F2.

### F7 — MEDIUM — STRUCTURE — `event_batch` is a 168-line schema+fanout+builder; two `ArrowAuditSink`s

Evidence: `packages/cowshed/crates/cowshed-gateway/src/telemetry.rs:123-159, 519-758`;
`packages/cowshed/crates/cowshed-core/src/storage/audit.rs:351-461` Problem: `event_batch` (591-758) constructs the
schema, expands one `AuditEvent` into span rows, and materializes 24 arrays. That is three functions. Independently,
cowshed-core and cowshed-gateway both export `ArrowAuditSink` that date-partitions `*.arrow` segments via `StreamWriter`
(F3) — same name, different schemas (controller commitments vs gateway traces). Fix: split `gateway_trace_schema()` (F2
prefix + extras), `expand_event(&AuditEvent) -> impl Iterator<Row>`, `rows_to_batch`. Rename gateway type to
`GatewayTraceSink` (or similar) so it cannot be confused with `cowshed_core::storage::audit::ArrowAuditSink`. Segment
publish (temp file, 0o600, rename, dirsync) can share a helper later; not required to fix the schema SSOT. Cost/Risk:
gateway tests address `ArrowAuditSink` by name. Core audit sink stays.

### F8 — LOW — TESTS — `pyarrow_reads_our_ipc` cannot go red without pyarrow

Evidence: `packages/lmao/crates/lmao-arrow/tests/convert.rs:125-136`

```
    if !probe.map(|o| o.status.success()).unwrap_or(false) {
        eprintln!("SKIP: python3/pyarrow not available; relying on arrow-rs roundtrip");
        return;
    }
```

Problem: PERFORMANCE-HANDBOOK §7.10bb — a guard that cannot go red is not a guard. The comment admits the skip.
`properties.rs::ipc_bytes` still round-trips through arrow-rs, so self-consistency is covered; cross-implementation is
not. Fix: make the test `#[ignore]` by default, or fail closed in CI when `LMAO_PYARROW=1`. Do not `return` on a missing
oracle while named as a pyarrow check. Cost/Risk: CI image either gains pyarrow or the test is explicitly ignored. No
production code.

### F9 — LOW — COPIES — `protected_record_schema` rebuilds 33 fields on every call; sibling is `LazyLock`

Evidence: `packages/cowshed/crates/cowshed-core/src/storage/job_artifact.rs:2493-2533` vs `3169-3173`

```
pub fn protected_record_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![ field("record_kind", ...), ... ]))
}
/// The controller commitment schema is compared against every decoded segment
/// during replay, so it is built once rather than allocating twenty-three fields
/// per comparison.
pub fn controller_commitment_schema() -> Arc<Schema> {
    static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(build_controller_commitment_schema);
    Arc::clone(&SCHEMA)
}
```

Regime: once per job record append/recover — not a hot loop. The comment on the sibling already names the cost they
chose to pay on the other schema. Fix: same `LazyLock` pattern as `controller_commitment_schema`.
`batch.schema() != protected_record_schema()` (`2699`, `2886`) currently allocates a fresh schema per comparison.
Cost/Risk: job_artifact.rs only (CsCoreJobArtifact). Pointer equality of `Arc<Schema>` becomes stable; `PartialEq` on
Schema still compares fields.

## Cross-slice questions

- **LmaoQuery** (`packages/lmao/crates/lmao-query/Cargo.toml`): `datafusion = { version = "47", …, optional = true }` is
  the only reason `packages/lmao/Cargo.lock` contains `arrow` 55.2.0 umbrella. Can datafusion be bumped to a
  56-compatible line, or is the feature dead enough to delete? F1 cannot close without that answer.
- **XcutRustTs / LmaoCore**: `packages/lmao/src/lib/schema/systemSchema.ts:314` vs `lmao-arrow` `ENTRY_TYPE_NAMES` (F4).
  Which is generated from which?
- **XcutRustTs / ColTypes**: `columine-arrow/src/schema.rs:13` claims `ArrowType` matches a TypeScript `ArrowType` enum.
  Grep of `packages/columine`, `packages/arrow-builder`, and `packages/lmao` found no TS `enum ArrowType`. Is the TS
  source gone (comment lie) or outside those trees?
- **ColArrow**: do not merge `columns.rs` / `record_batch.rs` / `ipc.rs` into the pin crate. Confirm wasm prod must stay
  free of `arrow-array` (current: dev-dep only).
- **LmaoArrow**: `convert_span_trees` / `ColumnDictionary` stay in lmao-arrow; the pin crate only takes `trace_schema` +
  IPC helper.
- **CsCoreJobArtifact**: F9 and the nested `protected_record_schema` / `visible_jobs_type` codecs stay in
  `job_artifact.rs`. The 4574-line file is a god-file finding for that slice, not this one.
- **CsGwActor / CsGwPolicy**: `telemetry.rs` is the gateway Arrow surface; F2/F6/F7 land there.

## Non-findings (checked, clean)

- **Column builders are not triplicated.** columine `ColumnStorage`/`EventColumns` (offset+bitmap byte vecs,
  `columns.rs:1-78`), lmao exact-size `Vec` → `DictionaryArray` (`convert.rs:221-316`), cowshed
  `StringArray::from`/`StructArray`/`ListArray` (`job_artifact.rs:2614+`) are three different layouts for three
  different schemas. Merging them would be the wrong SSOT.
- **Dictionary handling is not triplicated.** Only lmao-arrow (`dict.rs` `ColumnDictionary` / `StableVocabularyCatalog`
  / `vocabulary_dictionary`). Gateway and job_artifact use plain Utf8/Binary. columine `ArrowType` has no Dictionary
  variant (`schema.rs:16-26`).
- **IPC framing is not triplicated.** columine hand-emits continuation + RecordBatch FlatBuffers + `EOS_MARKER`
  (`record_batch.rs:9-10`, `ipc.rs:15, 159-229`) because it cannot take `arrow-array`. cowshed/lmao-tests use
  `arrow_ipc::writer::StreamWriter`. cowshed `CSARROW1`/`CSBATCH1`/`CSEND001` (`job_artifact.rs:36-38`) is a
  length+checksum envelope _around_ a StreamWriter payload, not a second Arrow codec.
- **Arrow crates are load-bearing.** In-process `RecordBatch`, typed errors, wasm flush, and gateway audit cannot shell
  out to `pyarrow`/`python3`. columine's `default-features = false` and lmao's "subcrates not umbrella" comment are the
  right instinct (F5 is the incomplete application).
- **No dual Arrow versions inside one lock.** Each workspace is internally consistent. The split is cross-workspace only
  (F1).
- **columine-arrow prod deps are already minimal:** `arrow-ipc` + `arrow-schema` only; `arrow-array` is
  `[dev-dependencies]` (`columine-arrow/Cargo.toml:11-17`). lmao-arrow keeps `arrow-ipc` as a dev-dep
  (`lmao-arrow/Cargo.toml:16-18`).
- **`controller_commitment_schema` is already a single source** for core audit replay; gateway traces are the schema
  that escaped (F2). `encode_controller_commitment` is the intended IPC SSOT that `audit.rs::seal` and gateway failed to
  use (F3).
- **Unsafe / unwrap on operational Arrow paths:** `telemetry.rs:642` `i64::try_from(at).unwrap_or(i64::MAX)` saturates
  rather than panicking. job_artifact Arrow errors are `ArtifactError::Arrow(String)`. Not a finding here.
- **Tests asserting rendered IPC bytes:** columine ipc tests round-trip through `StreamReader`/`StreamWriter` as an
  oracle for the hand encoder — that is typed, not stringy. lmao properties compare dictionary keys/values as arrays.
