# lmao-arrow

Scope: `packages/lmao-rs/crates/lmao-arrow/src/lib.rs` (53), `dict.rs` (577), `convert.rs` (317), `archive.rs` (155),
`source.rs` (136); `Cargo.toml` (23); `benches/flush.rs` (120); `tests/convert.rs` (168), `tests/properties.rs` (646).
Doctrine: BYPRODUCT-ENGINEERING.md, PERFORMANCE-HANDBOOK §4.1 / §7 / §7.12. Neighbour peeks (duplication only):
`packages/lmao-rs/Cargo.toml`, `packages/lmao-rs/Cargo.lock` (arrow-array 55.2.0), `packages/columine/Cargo.toml`,
`packages/columine/crates/columine-arrow/{Cargo.toml,src/lib.rs,src/schema.rs:1-83}`,
`packages/cowshed/crates/cowshed-core/Cargo.toml`, `packages/cowshed/crates/cowshed-gateway/Cargo.toml`,
`packages/lmao-rs/crates/lmao-core/src/{entry_type.rs,packed_header.rs,identity.rs,buffer.rs}`,
`packages/lmao/src/lib/{schema/systemSchema.ts,arrow/vocabularyDictionary.ts,archive/chunkEnvelope.ts,convertToArrow.ts}`,
`specs/lmao/{01f_arrow_table_structure.md,01t_trace_archive_pipeline.md}`.

## Summary

- Arrow table schema in this crate is not the 01f/TS table: missing
  `package_name`/`package_file`/`git_sha`/`uint64_value`, `timestamp` is `Int64` not `timestamp[ns]`, and the line
  column is named `line_number` not `line`.
- `ENTRY_TYPE_NAMES` is a hand restatement of TS `ENTRY_TYPE_NAMES` / `lmao_core::EntryType`; the Rust dictionary drops
  the unused slot 0 so IPC keys are discriminant−1 vs TS discriminant.
- `build_trace_chunk_envelope` comments claim TS parity; the canonical bytes, field set, `chunk_id` type, and time units
  do not match `chunkEnvelope.ts`.
- Arrow is pinned at 55 here vs 56 in columine and cowshed. Do not merge with `columine-arrow` — different job — but the
  version pin must move.
- Flush rebuilds the entry-type UTF-8 dictionary and a fresh `Schema` on every batch; static vocabulary UTF-8 is cached,
  numeric `Vec` fill is the concat copy into Arrow-owned buffers, not a second payload copy.
- `rustc-hash` is load-bearing for first-seen dynamic strings; keys are not uniform. Packed-header unpack is restated
  instead of using `lmao-core`.

## Findings

### F1 — HIGH — SSOT — `trace_schema` is not the 01f / TS system table

Evidence: `packages/lmao-rs/crates/lmao-arrow/src/convert.rs:52-63`

```rust
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

`specs/lmao/01f_arrow_table_structure.md:236-247` lists core columns `timestamp` as `timestamp[ns]`, plus
`package_name`, `package_file`, `message`. TS `convertToArrow.ts:1091-1150` emits `package_name`, `package_file`,
`git_sha` after `entry_type`. TS system field name for the line column is `line` (`systemSchema.ts:54-56`), not
`line_number`. Problem: Three restatements of one table (spec, TS flush, Rust flush) already disagree. `lib.rs:3` claims
this crate implements 01f. IPC from this crate is a different schema than flechette flush. `timestamp` stored as `Int64`
rather than Arrow timestamp[ns] is a type-level wire break, not a rename. Fix: Make 01f the single schema. Put field
names, nullability, and Arrow types in one table next to `lmao_core::EntryType` (or generate both TS and Rust from it).
Add the missing attribution columns when `SpanSource` can supply them; rename `line_number` → `line`; use
`DataType::Timestamp(TimeUnit::Nanosecond, None)`. Until then, stop claiming 01f compliance in `lib.rs`. Cost/Risk:
`lmao-query` `load_batches` and every test that hard-codes column index 7/8 (`tests/properties.rs:208`,
`tests/convert.rs:83-90`, `archive.rs:37,75`) must move with the schema.

### F2 — HIGH — SSOT — `ENTRY_TYPE_NAMES` restated; dictionary layout already diverged

Evidence: `packages/lmao-rs/crates/lmao-arrow/src/convert.rs:21-46,253,286-288`

```rust
pub const ENTRY_TYPE_NAMES: [&str; 24] = [
    "span-start",
    "span-ok",
    // ...
    "buffer-capacity",
];
entry_keys.push(entry_type - 1);
let entry_col = DictionaryArray::try_new(
    UInt8Array::from(entry_keys),
    Arc::new(StringArray::from_iter_values(ENTRY_TYPE_NAMES)) as ArrayRef,
)?;
```

TS `packages/lmao/src/lib/schema/systemSchema.ts:314-340` is 25 entries with `''` at index 0 so
`ENTRY_TYPE_NAMES[entryType]` is the name. `lmao_core::EntryType` (`entry_type.rs:10-38`) already owns discriminants
1..=24 and `COUNT = 24`. Problem: Names live in three places. Rust keys are discriminant−1; TS prebuilt dictionary
(`vocabularyDictionary.ts:6-12,119-123`) includes the empty slot so keys equal the discriminant. Decoded strings match;
dictionary keys and IPC bytes do not. That is live encoding divergence, not a cosmetic copy. Fix: Delete
`ENTRY_TYPE_NAMES` from this crate. Add `EntryType::name(self) -> &'static str` (and `ALL: [EntryType; 24]`) in
`lmao-core`. Build one process-lifetime `DictionaryArray` from that table, including or excluding slot 0 by an explicit
choice shared with TS — prefer matching TS (key == discriminant, unused ordinal 0) so IPC is comparable. Cost/Risk:
Every test that asserts `et.keys().value(1) == 1` for span-ok (`tests/convert.rs:69-73`, `tests/properties.rs:626-627`)
flips if slot 0 is restored. `lmao-core` becomes the SSOT; this crate only consumes it.

### F3 — HIGH — SSOT — archive envelope identity does not match TS (comment is false)

Evidence: `packages/lmao-rs/crates/lmao-arrow/src/archive.rs:21-62`

```rust
/// `fnv1a64` over the canonicalized content descriptor (`file_ref`, refs, row
/// count, time bounds) — NOT over the payload bytes, matching the TS
/// `buildTraceChunkEnvelope` behavior of hashing the canonical descriptor.
pub chunk_id: u64,
// ...
let canonical =
    format!("v1\x1f{file_ref}\x1f{row_count}\x1f{min_timestamp}\x1f{max_timestamp}");
TraceChunkEnvelope {
    chunk_id: fnv1a64(canonical.as_bytes()),
    file_ref: file_ref.to_string(),
```

TS `packages/lmao/src/lib/archive/chunkEnvelope.ts:23-44,69-80` hashes a stable-serialized object
`{chunk_ref, ended_at_ms, file_ref, metadata, partition_keys, row_count, started_at_ms}` with FNV-1a over UTF-16 code
units via `charCodeAt`, then formats `chunk_${hex}`. Spec 01t (`01t_trace_archive_pipeline.md:33-34,58-63`) says
`chunk_id: string` and time bounds `started_at_ms`/`ended_at_ms`. Problem: Same function name, different canonical
bytes, different hash domain (`u64` vs hex string), different fields (`chunk_ref`/`metadata`/`partition_keys` absent; ns
vs ms). The “matching the TS” comment is a live lie. FNV-1a itself is also copied (`archive.rs:13-19` vs
`chunkEnvelope.ts:69-77` vs `chunkCompaction.ts:35-37`). Fix: Pick one envelope struct (01t shipped contract: the TS
one). Implement `fnv1a64` once. Stop inventing a `v1\x1f` descriptor. If Rust cannot yet carry `chunk_ref`, do not emit
a `chunk_id` and claim it is the archive identity. Cost/Risk: Any store that mixed TS and Rust chunk ids would see
retries as new chunks. Tests in `tests/properties.rs:633-645` pin the Rust-only function, not TS parity.

### F4 — HIGH — SSOT — Arrow 55 vs 56 across workspaces; do not merge the crates

Evidence: `packages/lmao-rs/Cargo.toml:23-27` and `packages/lmao-rs/crates/lmao-arrow/Cargo.toml:11-13`

```toml
arrow-array = "55"
arrow-buffer = "55"
arrow-schema = "55"
arrow-ipc = "55"
```

`packages/columine/Cargo.toml:20-22` pins the same crates at `"56"` with `default-features = false`.
`packages/cowshed/crates/cowshed-core/Cargo.toml:14-17` and `cowshed-gateway/Cargo.toml:9-11` depend on arrow-* `"56"`.
Lock: `packages/lmao-rs/Cargo.lock` `arrow-array` `55.2.0`. `columine-arrow` public surface
(`packages/columine/crates/columine-arrow/src/lib.rs:1-35`) is `EventColumns` / `DynamicColumns` plus a hand-emitted IPC
writer (`write_arrow_ipc_from_borrowed_columns`). Prod deps are `arrow-ipc` + `arrow-schema` only; `arrow-array` is a
dev-dep. This crate’s job is SpanBuffer-tree → `RecordBatch` via `arrow-array`. Problem: Version skew is real SSOT
drift. Merging the libraries is not: different input (span trees vs event column bytes), different output (arrow-rs
`RecordBatch` vs raw IPC), different schema (lmao 01f vs columine dynamic IPC schema). A wrong “one crate”
recommendation would couple unrelated flush kernels. Fix: Keep three crates. Bump `packages/lmao-rs` workspace arrow-*
to 56 with `default-features = false`, matching columine. Do not route lmao flush through `columine-arrow` or vice
versa. Cost/Risk: `lmao-query` (arrow-array 55, datafusion 47) must move with the pin. [INFERENCE] datafusion 47’s arrow
major may force a datafusion bump — confirm in the lmao-query slice.

### F5 — HIGH — COPIES — entry-type UTF-8 dictionary and `Schema` rebuilt every flush

Evidence: `packages/lmao-rs/crates/lmao-arrow/src/convert.rs:286-301` and `dict.rs:311-376`

```rust
Arc::new(StringArray::from_iter_values(ENTRY_TYPE_NAMES)) as ArrayRef,
// ...
Arc::new(StringArray::from_iter_values(trace_dict.values.iter())) as ArrayRef,
let message_values: ArrayRef = vocabulary_dictionary(vocabulary, &dynamic_messages.values)?;
Ok(RecordBatch::try_new(
    trace_schema(),
```

`trace_schema()` (`convert.rs:52-64`) allocates nine `Field` name `String`s per call. Contrast TS
`PREBUILT_ENTRY_TYPE_DICTIONARY` (`vocabularyDictionary.ts:6-12,119-123`) and “Record text is decoded and copied into
Arrow UTF-8 buffers once per generation” (`vocabularyDictionary.ts:36-39`). Static vocab in Rust is cached
(`dict.rs:165,311-315,371-372`); entry-type names are not. Problem: Regime is the flush kernel (once per batch, not per
event, not once per process). Doctrine: UTF-8 encode once on the cold path; immutable dictionaries are leased.
Entry-type names are a closed 24-row table known at compile time — rebuilding them is evaporating work (Byproduct L0).
`from_iter_values` copies UTF-8 + offsets every batch. Numeric columns (`Int64Array::from(timestamps)` etc.) take the
`Vec` by value; [INFERENCE] arrow-rs `Buffer::from_vec` does not copy that payload again. The tree walk still copies
primitives into those vecs because `SpanSource` only offers per-row getters (`source.rs:20-26`) even though `SpanBuffer`
already holds `timestamps: Vec<i64>` (`buffer.rs:42-45`). Fix: `OnceLock<ArrayRef>` for the entry-type dictionary and
`OnceLock<Arc<Schema>>` for `trace_schema`. Expose `&[i64]` / `&[u32]` on `SpanSource` (or a `ColumnarSpan` subtrait)
and `extend_from_slice` into the pre-sized column vecs; keep the per-row trait only for tests/`MockSpan`. Leave
static-vocab `OnceLock` as-is. Trace-id and dynamic-message dictionaries must rebuild per flush (values are
batch-local). Cost/Risk: Schema OnceLock must die if F1 changes fields. Slice getters touch `lmao-core::SpanBuffer`.

### F6 — MEDIUM — COPIES — pass 2 re-derives pass 1 proofs; first-seen map double-hashes

Evidence: `packages/lmao-rs/crates/lmao-arrow/src/convert.rs:167-214,252-273` and `dict.rs:387-395`

```rust
let (entry_type, vocabulary_id) = split_packed_header(buffer.packed_header(row));
// pass 1: key_for_id / key_for_value / dynamic_messages.observe
// pass 2, same row:
let (entry_type, vocabulary_id) = split_packed_header(buffer.packed_header(row));
vocabulary.key_for_id(...).expect("validated static vocabulary ID in pass 1")
vocabulary.len() as u32 + dynamic_messages.index_of(message).expect("dynamic message observed in pass 1")
```

```rust
if self.index.contains_key(value) {
    return;
}
self.values.push(value);
self.index.insert(value, index);
```

Problem: Two full pre-order walks. Pass 1 already resolved vocabulary ordinals and first-seen dynamic indices; pass 2
binary-searches and hashes again (Byproduct L0 / handbook §7.7). Only sorted trace-id keys are unknown until
`finalize_indexed`. `FirstSeenDictionary::observe` probes the map twice per novel value. Regime: flush kernel, per row.
Fix: In pass 1, push resolved `message_keys` / validity (and entry_type) into the already-capacity-reserved vecs. Pass 2
only writes identity columns that need `trace_dict.index_of`. Replace contains+insert with `HashMap::entry`. Cost/Risk:
Slightly larger pass-1 scratch; deletes the `expect` invariant comments on pass 2.

### F7 — MEDIUM — SSOT — packed-header unpack restated beside `lmao-core`

Evidence: `packages/lmao-rs/crates/lmao-arrow/src/convert.rs:134-145`

```rust
fn required_vocabulary_kind(entry_type: u8) -> Option<StableVocabularyKind> {
    match entry_type {
        1 => Some(StableVocabularyKind::SpanName),
        6..=10 => Some(StableVocabularyKind::LogTemplate),
        _ => None,
    }
}
fn split_packed_header(header: u32) -> (u8, u32) {
    (header as u8, header >> 8)
}
```

`lmao-core` already has `entry_type_from_header`, `vocabulary_id_from_header`, `supports_static_vocabulary`
(`packed_header.rs:101-121`) covering SpanStart + Trace..=Error — the same allow-list as `1` and `6..=10`. Validation
uses `ENTRY_TYPE_NAMES.len()` (`convert.rs:169`) instead of `EntryType::from_u8`. Problem: Two parsers of one u32
layout. If the shift/mask changes, this crate keeps compiling and silently mis-reads rows. `required_vocabulary_kind`
adds SpanName vs LogTemplate, which core does not have — that mapping should live next to `supports_static_vocabulary`,
not as magic integers here. Fix: Call `entry_type_from_header` / `vocabulary_id_from_header`. Put
`fn vocabulary_kind(self) -> Option<StableVocabularyKind>` on `EntryType` (or a shared enum in core). Delete
`split_packed_header`. Cost/Risk: `lmao-core` grows a kind mapping; `StableVocabularyKind` today lives in this crate —
move it with the function.

### F8 — MEDIUM — DEP-BLOAT — arrow default features pull chrono/chrono-tz

Evidence: `packages/lmao-rs/Cargo.toml:24-27` (no `default-features = false`) vs lock `packages/lmao-rs/Cargo.lock`
`arrow-array 55.2.0` dependencies: `chrono`, `chrono-tz`, `half`, `num`, `ahash`, `hashbrown 0.15.5`. Columine’s pin
(`packages/columine/Cargo.toml:20-22`) turns defaults off. This crate uses
`arrow_array::{Array, DictionaryArray, RecordBatch, StringArray, *Array}`,
`arrow_buffer::{Buffer, OffsetBuffer, ScalarBuffer, NullBuffer}`, `arrow_schema::{DataType, Field, Schema}` — no
timezone conversion. Problem: Default features are paying for a tz database this flush path does not call. `arrow-ipc`
is correctly a dev-dep (`lmao-arrow/Cargo.toml:16-18`). `rustc-hash` stays (see Non-findings). Workspace lock also
carries `hashbrown` 0.14.5 / 0.15.5 / 0.17.1 — not introduced by this manifest, but default arrow features feed that
graph. Fix: Workspace `arrow-* = { version = "56", default-features = false }` as columine. Do not shell out Arrow work;
the crate is in-process and needs typed `RecordBatch` errors. Cost/Risk: Same as F4. Feature-flag audit of
`lmao-query`/datafusion after the bump.

### F9 — MEDIUM — TESTS — skip-green pyarrow oracle; bench controls are a different kernel

Evidence: `packages/lmao-rs/crates/lmao-arrow/tests/convert.rs:128-166` and `benches/flush.rs:37-46`

```rust
if !probe.map(|o| o.status.success()).unwrap_or(false) {
    eprintln!("SKIP: python3/pyarrow not available; relying on arrow-rs roundtrip");
    return;
}
assert_eq!(
    lines.next().unwrap(),
    "timestamp,trace_id,thread_id,span_id,parent_thread_id,parent_span_id,entry_type,message,line_number"
);
```

```rust
g.bench_function("std_hashmap_siphash", |b| {
    b.iter(|| {
        let mut counts: std::collections::HashMap<&str, u64> = Default::default();
        for s in &strings {
            *counts.entry(black_box(s.as_str())).or_default() += 1;
        }
```

Problem: Handbook §7.10bb / §4.2b: a guard that cannot go red is not a guard. Missing pyarrow → green. The assertion
that can fire is a rendered CSV of field names, not typed `Schema` fields. `benches/flush.rs` `std_hashmap_siphash`
counts occurrences; `ColumnDictionary` (`dict.rs:410-453`) does not — hasher A/B is confounded by extra work.
`dictionary_build_256` never calls `StringArray` / `convert_span_trees`, so it cannot catch F5’s UTF-8 rebuild.
`fxhashmap_column_dictionary` does exercise `observe`+`finalize_indexed` (live). Full flush bench (`flush.rs:103-116`)
is the production shape; fixture `MockSpan` owns `Vec<Option<String>>` allocated outside `iter` (acceptable), but
`line_number` is hard-wired 0 (`source.rs:125-126`) so that column is unmeasured. Fix: Fail the pyarrow test when the
binary is missing in CI, or drop it and keep the typed IPC roundtrip in `properties.rs`. Assert
`batch.schema().field(i).name()` / `data_type()`. Make hasher controls call the same observe/finalize body with a type
parameter. Add a flush cell that includes a non-zero `line_number` source. Cost/Risk: CI needs pyarrow or the test goes
away. Bench numbers are not comparable to the quoted 5.8 µs SipHash vs 3.3 µs JS Map until the kernels match.

### F10 — LOW — STRUCTURE — flush function is the file; archive uses positional columns; `Option` swallowed

Evidence: `packages/lmao-rs/crates/lmao-arrow/src/convert.rs:148-317` (170 lines), `archive.rs:36-37,74-80`,
`source.rs:45-50`

```rust
fn timestamp(&self, row: usize) -> i64 {
    self.timestamp_at(row).unwrap_or(0)
}
fn packed_header(&self, row: usize) -> u32 {
    self.packed_header_at(row).unwrap_or(0)
}
```

```rust
let timestamps = batch.column(0).as_primitive::<Int64Type>();
batch.column(1).as_dictionary::<UInt32Type>().keys().value(row)
```

Problem: `convert_span_trees` is pass 1 + pass 2 + array wrap in one function (>100 lines). Archive predicates on
`column(0)`/`column(1)` duplicate `trace_schema` order (F1 blast radius). `unwrap_or(0)` on `SpanBuffer` turns a missing
row (operational: `write_index` vs vec length) into a plausible timestamp/header instead of a `ConvertError`. `line_at`
in core already does the same (`buffer.rs:224-226`) — this crate repeats the swallow. Fix: Split pass 1 / pass 2 /
`finish_batch`. Name columns from the schema (`schema.index_of("timestamp")`) or a const index enum generated with the
schema. `timestamp_at(row).ok_or(ConvertError::...)` — do not invent 0. Cost/Risk: Low. Tests that never hit the
short-vec case stay green.

## Cross-slice questions

- `lmao-core` (`entry_type.rs`, `packed_header.rs`): F2/F7 want names + vocabulary kind on `EntryType`. LmaoCore owns
  those files.
- `lmao-query`: consumes `trace_schema` RecordBatches (`sqlite_backend.rs`). F1/F4 change its load path and possibly
  datafusion’s arrow major.
- `columine-arrow` (ColArrow): public surface is IPC-from-columns, not this crate’s job. Confirm they are not planning
  to take a `RecordBatch` dependency that would fight F4’s “do not merge”.
- cowshed-core / cowshed-gateway: arrow 56 RecordBatches for telemetry, not lmao span trees. No shared schema found from
  this slice.
- TS `convertToArrow.ts` / `chunkEnvelope.ts` / `systemSchema.ts`: not a Rust slice; F1–F3 are the Rust/TS SSOT breaks.

## Non-findings (checked, clean)

- **rustc-hash is load-bearing.** `TraceId` is a validated ASCII `Arc<str>` ≤128, not a uniform index
  (`identity.rs:38-63`). Dynamic messages are arbitrary `&str`. `FirstSeenDictionary` must preserve first-observation
  order (`dict.rs:378-400`, asserted in `tests/properties.rs:457-462`) — sort-then-index would break that. Vocabulary
  lookup is already binary search on sorted ids/values (`dict.rs:272-309`), no hashmap. Do not replace with shell
  `shasum`/`uuidgen`. Do not delete the crate.
- **Static vocabulary UTF-8 is once-per-catalog.** `OnceLock<Arc<StringArray>>` + empty-dynamic fast path
  (`dict.rs:311-376`). Mixed batches copy prefix+suffix once into a combined dictionary, which is required.
- **No `unsafe` in this crate.** Pass-2 `expect`s are pass-1 invariants, not operational unwraps.
- **Numeric handoff is not builder-append.** Pre-sized `Vec::with_capacity(total_rows)` then `PrimitiveArray::from(vec)`
  / `Buffer::from_vec` for offsets (`convert.rs:223-233`, `dict.rs:347-361`). Per-value Arrow builders are absent.
  Remaining cost is F5/F6, not `BooleanBufferBuilder` as a second payload copy.
- **`arrow-ipc` / `criterion` / `proptest` are dev-deps** and do not leak into the lib artifact (`Cargo.toml:16-23`).
- **`ColumnDictionary` savings heuristic** (`dict.rs:456-468`) is unused by `convert_span_trees` (message is always
  dictionary-encoded, matching 01f). It is the 01a text-column API, tested at the `>` vs `>=` boundary
  (`dict.rs:516-532`) — not dead, not a flush bug.
- **No `cfg(target_os)` arms.** No 5k-line files. `MockSpan` is public because `SpanBuffer` still cannot build child
  trees for benches (`source.rs:90-92`) — justified test seam.
- **Intra-Rust IPC determinism** (`tests/properties.rs:583-598`) asserts typed roundtrip of the same tree, not rendered
  JSON. Catalog validation tests assert enum error values, not strings.
