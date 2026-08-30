# columine-arrow

Scope: `packages/columine/crates/columine-arrow/src/lib.rs` (36), `columns.rs` (1138), `record_batch.rs` (713), `ipc.rs`
(688), `schema.rs` (434), `Cargo.toml` (17). Neighbor reads (public surface only):
`packages/lmao-rs/crates/lmao-arrow/{Cargo.toml,src/lib.rs,src/convert.rs:1-90}`; targeted greps for Arrow versions,
`ArrowType`, `EventLogError`, batch limits, `has_extraction_fields`. Rubric: `BYPRODUCT-ENGINEERING.md`,
`docs/handbook/{02-measurement.md §4.1, 04-mechanisms.md, 05-memory-toolkit.md}`.

## Summary

- `has_extraction_fields` is `field_count != 4`, not schema identity; the EventColumns IPC writer then hard-codes
  utf8/utf8/int64/binary.
- `EventColumns` is a second 4-column store; the crate’s own test pins it byte-identical to `DynamicColumns`.
- Physical `ArrowType` tags and batch ceilings are restated in TS (`COMPACT_KIND_TAG`, `parse-backend.ts` constants);
  `ParseError` claims a TS `EventLogError` that does not exist.
- This crate is not a copy of `lmao-arrow`. RecordBatch IPC is fully hand-rolled; `arrow-ipc`/`arrow-schema` are used
  only to decode the inbound Schema message. Arrow 56 here vs 55 in `lmao-rs`.
- Int32 rides `ColumnType::Int64` with width 4; `read_fixed_i64` zero-extends, so signed Int32 reads are wrong. Null
  columns allocate Utf8 storage they never emit.
- Null counts are re-derived by walking rows despite an already-packed validity bitmap (once per IPC encode).
- Operational “buffer too small” is a `0`/`false` sentinel on the encoder, not `Result`.
- `MAX_STRING_BYTES` and the `proptest` dev-dep are unused in this crate.

## Findings

### F1 — HIGH — SSOT — `has_extraction_fields` is a field-count, not a schema

Evidence: `packages/columine/crates/columine-arrow/src/schema.rs:170-176`

```rust
        Ok(Self {
            has_extraction_fields: field_metadata.len() != 4,
            schema_bytes: schema_bytes.to_vec(),
            field_metadata,
            logical_types,
            field_names,
        })
```

and `packages/columine/crates/columine-arrow/src/ipc.rs:332-376`

```rust
    if schema_config.field_metadata.len() != 4 {
        return Err(IpcError::InvalidColumn);
    }
    ...
            0 => Ok(DynamicColumn::utf8(0, false, None, columns.id_offsets_bytes(), columns.id_data_bytes())),
            1 => Ok(DynamicColumn::utf8(1, false, None, columns.type_offsets_bytes(), columns.type_data_bytes())),
            2 => Ok(DynamicColumn::int64(2, false, None, columns.timestamps_bytes())),
            3 => Ok(DynamicColumn::binary(3, true, Some(columns.value_nulls_bytes()), ...)),
```

Problem: Any 4-field schema is classified as the base event log. The EventColumns writer then emits
id/type/timestamp/value buffers regardless of `field_metadata` / `schema_bytes`. A 4-field extraction schema (e.g.
utf8/utf8/int64/int64 quantity) produces an IPC stream whose Schema message and RecordBatch body disagree. Downstream
`columine-event-processor` routes on this flag (`use_base = wiring.base_path && !schema_config.has_extraction_fields`).
Fix: Delete the flag. Identify the base schema by the four physical tags (Utf8, Utf8, Int64, Binary) plus names if
present, or drop the EventColumns path entirely (see F2). `write_arrow_ipc_from_columns_with_schema` must reject type
mismatch, not just `len != 4`. Cost/Risk: Event-processor init and any caller of `has_extraction_fields` move with it.
Blast radius is that crate, not this one’s encode kernel.

### F2 — HIGH — DUPLICATION — `EventColumns` is a second 4-column `DynamicColumns`

Evidence: `packages/columine/crates/columine-arrow/src/columns.rs:54-78` (hard-coded id/type/timestamp/value planes) vs
`541-552` (`DynamicColumns { columns: Vec<ColumnStorage>, ... }`), and the unification test at `ipc.rs:432-475`:

```rust
    /// End-to-end: base and dynamic writers produce byte-identical streams
    /// for the same 4-column content — the unification claim in one test.
    #[test]
    fn base_and_dynamic_streams_are_byte_identical() {
        ...
        assert_eq!(base_out[..base_len], dyn_out[..dyn_len]);
```

Problem: Two stores, two append APIs, two IPC adapters, two null-count walks, two reset paths. EventColumns cannot grow
(hits `BufferOverflow` at the 36/64/256-byte estimates); DynamicColumns can. The test proves there is no wire difference
to preserve. Event-processor still constructs both when `use_base` is true. Fix: Delete `EventColumns`,
`write_arrow_ipc_from_columns_with_schema`, and the base-path branch. One `DynamicColumns` for every schema, including
the four-column log. Cost/Risk: `columine-event-processor` and `columine-parsing` re-exports. Scanners that call
`add_event` move onto `begin_row`/`append_*`/`end_row`.

### F3 — HIGH — SSOT — physical type tags and batch ceilings restated in TypeScript; claimed `EventLogError` does not exist

Evidence: Rust `schema.rs:16-26`

```rust
pub enum ArrowType {
    Null = 0,
    Int32 = 1,
    Float64 = 2,
    Binary = 3,
    Utf8 = 4,
    Bool = 5,
    Int64 = 6,
}
```

TS `packages/columine/src/parse-backend.ts:98-119`

```ts
const MAX_EVENTS_PER_BATCH = 65_536;
const MAX_FIELDS = 256;
const MAX_VARIABLE_DATA_BYTES = 16 * 1024 * 1024;
const MIN_COMPACT_ARROW_CAPACITY = 4 * 1024;
const COMPACT_KIND_TAG = {
  null: 0,
  u32: 1,
  f64: 2,
  binary: 3,
  utf8: 4,
  bool: 5,
  i64: 6,
} as const satisfies Record<CompactColumn['kind'], number>;
```

Rust originals: `columns.rs:16-22` (`MAX_EVENTS_PER_BATCH = 65536`, `MAX_VALUE_BYTES = 16MB`), `schema.rs:11`
(`MAX_SCHEMA_FIELDS = 256`), `ipc.rs:16` (`MIN_ARROW_OUTPUT_CAPACITY = 4096`). `columns.rs:24-37` and `789-798`:

```rust
/// TypeScript `EventLogError` discriminants (the JS interop contract), pinned
/// by `parse_error_codes_match_ts`.
pub enum ParseError { Ok = 0, InvalidJson = 1, ... OutOfMemory = 7 }
...
        // Values match the TypeScript EventLogError codes.
        assert_eq!(ParseError::Ok as u32, 0);
```

Repo grep for `EventLogError` hits only these two comments. There is no TS type to drift against.
`parse_error_codes_match_ts` asserts the Rust enum against itself (PERFORMANCE-HANDBOOK §7.10bb). Problem: Two sources
for the 4-byte physical-type table and the batch/schema ceilings. They agree today; nothing compiles them from one
table. The ParseError↔TS pin is already a lie. Fix: Generate `ArrowType` / `COMPACT_KIND_TAG` / the four MAX_* constants
from one table (Rust `#[repr]` + a TS emit, or the reverse). Delete the `EventLogError` claim; if a JS numeric contract
exists, point the test at that file’s literals. SSOT is the Rust `ArrowType`/`MAX_*` in this crate — wasm consumes them
— and TS must be generated from it. Cost/Risk: `parse-backend.ts` compact encoder and compact tests. Numbers currently
match; generation is mechanical.

### F4 — MEDIUM — DEP-BLOAT — `arrow-ipc` decodes Schema only; RecordBatch IPC is hand-rolled (not a `lmao-arrow` clone)

Evidence: production `arrow-ipc` imports, `schema.rs:7-8` and `199-218`:

```rust
use arrow_ipc::{MessageHeader, convert::try_schema_from_ipc_buffer, root_as_message};
use arrow_schema::DataType;
...
    let message = root_as_message(&bytes[8..]).map_err(|_| SchemaError::InvalidMessage)?;
    if message.header_type() != MessageHeader::Schema || message.bodyLength() != 0 {
        return Err(SchemaError::InvalidMessage);
    }
    try_schema_from_ipc_buffer(bytes).map_err(|_| SchemaError::InvalidMessage)
```

Hand-rolled writer: `record_batch.rs:1-5, 367-424` (in-place Message/RecordBatch FlatBuffers, continuation prefix) and
`ipc.rs:1-6, 155-229` (schema-bytes copy + body + EOS). `Cargo.toml:10-17`: runtime `arrow-ipc` + `arrow-schema`;
`arrow-array` is dev-only (StreamReader oracle in `ipc.rs:388-617`). `lmao-arrow` public surface
(`packages/lmao-rs/crates/lmao-arrow/src/lib.rs:1-35`, `convert.rs:52-64, 148-151`): `convert_span_trees` →
`arrow_array::RecordBatch` with dictionary-encoded trace columns (`timestamp`, `trace_id`, …). No IPC writer, no
event-log schema, no byte-backed columns. Workspace versions: columine/cowshed `arrow-* = "56"`;
`packages/lmao-rs/Cargo.toml` `arrow-* = "55"`. cowshed uses official `StreamWriter`/`StreamReader` on `RecordBatch`
values (not this crate). Problem: Three Arrow stacks, two jobs. columine-arrow exists to emit IPC bytes in wasm without
`arrow-array`. The hand-rolled path does **not** duplicate `arrow-ipc`’s writer (that writer is unused) and does **not**
duplicate `lmao-arrow`. Dropping `arrow-ipc`/`arrow-schema` is a real wasm-size win **if and only if** Schema-message
validation is rewritten. That decoder is not ~30 lines: untrusted FFI schema bytes need a real FlatBuffers Schema
reader. `logical_types: Vec<DataType>` (`schema.rs:93, 167`) is retained after validation and read only by
`schema.rs:345` — it does not justify the dep on its own. `default-features = false` is already set; keep it. `proptest`
is listed in this crate’s dev-deps and never used (`Cargo.toml:16`; grep in the crate is the manifest line only). Fix:
Keep `arrow-ipc`/`arrow-schema` for Schema decode until a dedicated decoder exists. Delete `logical_types` from the
retained config. Delete the unused `proptest` dev-dep. Do not merge with `lmao-arrow`. Align `lmao-rs` to arrow 56 in
that workspace (cross-slice), not by sharing this crate. Cost/Risk: Schema decode is the processor-create boundary; a
wrong hand-roll is a security/compat hole. Wrong “just drop the crate” recommendation would ship invalid-schema
acceptance.

### F5 — MEDIUM — STRUCTURE — Int32 is `ColumnType::Int64` width 4; `read_fixed_i64` zero-extends

Evidence: `columns.rs:329-334, 414-423, 699-704`

```rust
    fn new_int32(capacity: u32) -> Self {
        Self::with_fixed_width(ColumnType::Int64, capacity, MAX_VALUE_BYTES, 4)
    }
    pub fn read_fixed_i32(&self, row: u32) -> Option<u32> { ... }
    pub fn read_fixed_i64(&self, row: u32) -> Option<i64> {
        ...
        if self.fixed_width == 4 {
            return self.read_fixed_i32(row).map(i64::from);
        }
```

```rust
            (ColumnType::Int64, 4) => {
                if !(i64::from(i32::MIN)..=i64::from(u32::MAX)).contains(&value) {
                    return Err(ParseError::InvalidFieldType);
                }
                fixed[row_idx * 4..row_idx * 4 + 4].copy_from_slice(&(value as u32).to_le_bytes());
```

`DynamicColumns::new` maps `ArrowType::Null` to `ColumnType::Utf8` (`columns.rs:571`). IPC then special-cases Null and
emits no buffers (`record_batch.rs:260-266`). Problem: Two logical types (Int32 and UInt32) share one physical tag and
one storage kind that is named Int64. Append of `-1` stores `0xFFFF_FFFF`; `i64::from(u32)` yields `4294967295`. IPC
body bytes are correct (the reader uses the logical schema). The Rust read helper is not. `columine-parsing` `read_cell`
matches on `ColumnType::Int64` and calls this helper (test/differential view). Null columns still allocate offsets+data
they never publish. Fix: Give `ColumnType` an `Int32` variant (or store width in the type). Sign-extend when the logical
type is Int32; zero-extend only for UInt32 — which means the reader needs the logical type, already sitting in
`SignalSchemaField`/`ArrowType`. Map `ArrowType::Null` to a Null storage that allocates nothing. Cost/Risk: `read_cell`
and any width-4 `ColumnType::Int64` match. IPC bytes stay stable if the physical layout does.

### F6 — MEDIUM — COPIES — null_count walks every row instead of popcounting the validity bitmap

Evidence: `ipc.rs:307-313, 341-343`

```rust
fn dynamic_null_count(columns: &DynamicColumns, index: usize) -> i64 {
    ...
    (0..columns.count)
        .filter(|row| columns.is_null(field_index, *row))
        .count() as i64
}
    let value_null_count = (0..columns.count)
        .filter(|row| !columns.has_value(*row))
        .count() as i64;
```

Regime: once per IPC encode (per batch, not per event). At `MAX_EVENTS_PER_BATCH` × N columns this is tens of millions
of bit tests on the write path, re-deriving what the LSB-first validity plane already is (Byproduct L0/L1; handbook
§7.8). Fix: `null_count = row_count - popcount(validity[0..ceil(n/8)])` with a masked last byte; or maintain a running
count in `ColumnStorage` on append. Same for `EventColumns` until F2 deletes it. Cost/Risk: Local to the two IPC
adapters. Must match Arrow’s definition (null bit = 0). The mixed-schema round-trip test (`ipc.rs:519-671`) is the
oracle.

### F7 — MEDIUM — STRUCTURE — encoder operational failure is `0` / `false`, not `Result`

Evidence: `record_batch.rs:361-387, 244-252, 294-316` and `schema.rs:190-196`

```rust
/// Result of [`encode_record_batch_dynamic`]: total bytes written into
/// `output` from its start, or `0` when the buffer is too small.
pub fn encode_record_batch_dynamic(...) -> usize {
    ...
    if output.len() < total_size {
        return 0;
    }
```

`DynamicBodyBuilder::add_column` / `add_buffer` return `bool`. `write_schema_message` returns `0` on short output.
`ipc.rs:138-139` then maps `rb_written == 0` to `IpcError::BufferTooSmall`. Problem: Buffer-too-small is operational,
not an invariant. The `0` sentinel collides with a legitimate empty write only because this encoder always writes at
least 8 prefix bytes — a convention, not a type. Doctrine: `Result` for operational failure. Fix:
`encode_record_batch_dynamic` → `Result<usize, IpcError>`; `add_column` → `Result<(), IpcError>`. Collapse the bool/`0`
translation in `finish_stream`. Cost/Risk: Internal plus any external `encode_record_batch_dynamic` caller (exported
from `lib.rs:29-32`). Tests that assert `result > 8` become `unwrap` on `Ok`.

### F8 — LOW — SSOT — `compute_buffer_count` and the 256-field cap are written twice

Evidence: `record_batch.rs:36-38`

```rust
pub fn compute_buffer_count(fields: &[SignalSchemaField]) -> u32 {
    fields.iter().map(|f| f.buffer_count()).sum()
}
```

`schema.rs:179-183` (`DynamicSchemaConfig::compute_buffer_count` is the same fold). `MetadataLimits::default`
(`record_batch.rs:57-63`) hard-codes `max_fields: 256, max_buffers: 768` beside `MAX_SCHEMA_FIELDS = 256`
(`schema.rs:11`). `align_to_8` (`record_batch.rs:27-29`) is re-implemented in
`columine-event-processor/src/checkpoint.rs:17-19` instead of imported. Fix: One function
(`SignalSchemaField::buffer_count` fold on `DynamicSchemaConfig`). `MetadataLimits::default()` uses `MAX_SCHEMA_FIELDS`
and `MAX_SCHEMA_FIELDS * 3`. Checkpoint alignment is the other slice’s copy. Cost/Risk: None if the formulas stay
identical; they already are.

### F9 — LOW — STRUCTURE — dead `MAX_STRING_BYTES`; Null storage is Utf8; `read_u32` swallows OOB

Evidence: `columns.rs:18-19` (`MAX_STRING_BYTES = 1024 * 1024`) — definition and `lib.rs:21-22` re-export only;
`columine-parsing` re-exports it and never uses it. EventColumns sizes id/type from 36/64-byte estimates
(`columns.rs:90-93`), not this cap. `columns.rs:39-42`:

```rust
fn read_u32(bytes: &[u8], index: usize) -> u32 {
    let start = index * 4;
    u32::from_le_bytes(bytes[start..start + 4].try_into().unwrap_or([0; 4]))
}
```

`write_u32` panics on OOB via `copy_from_slice`; `read_u32` returns 0. Null→Utf8 allocation: `columns.rs:571`. Regime:
`read_u32` is on get/append paths but only with indices the caller already bounded — swallow is a silent-corruption
shape, not a measured hot cost. `EventColumns::new` zero-fills estimate-sized planes once per processor
(`vec![0; cap * 256]` etc.); once-per-open, not a finding. Fix: Delete `MAX_STRING_BYTES`. Make `read_u32` index a
proven in-bounds slice (or `try_into().ok()` → `Option`). See F5 for Null storage. Cost/Risk: Re-export deletion in
`columine-parsing`.

### F10 — LOW — TESTS — several tests cannot go red on the thing they name

Evidence: `columns.rs:789-798` (`parse_error_codes_match_ts` — see F3). `record_batch.rs:649-656, 705-710`:

```rust
        assert_eq!(u32::from_le_bytes(output[0..4].try_into().unwrap()), 0xFFFF_FFFF);
        assert!(result > 8);
        ...
        assert!(metadata_size > 100 && metadata_size < 400);
```

`ipc.rs:519-671` (`mixed_schema_with_null_type_round_trips_through_arrow_reader`) is the real contract test: typed
values through `StreamReader`. The record-batch tests assert on continuation bytes and a size band, not
nodes/buffers/body. Fix: Pin `encode_record_batch_dynamic` against `record_batch_metadata_size` and against a
`StreamReader` batch, or delete the weak tests and keep the ipc round-trip. Point the ParseError test at a real foreign
literal or delete it. Cost/Risk: Test-only.

## Cross-slice questions

- `columine-event-processor` (`lib.rs:198-201`): `use_base = wiring.base_path && !schema_config.has_extraction_fields`.
  If `EpWiring::columine().base_path` is true for any 4-field non-base schema, F1 is a live wire-corruptor. This slice
  cannot close that without owning that file.
- `columine-parsing` `read_cell` (`lib.rs:73-87`) consumes `read_fixed_i64` on every `ColumnType::Int64`, including
  width-4 Int32. Confirm whether any differential test feeds negative Int32.
- `lmao-arrow` / `lmao-rs` workspace: arrow 55 vs columine/cowshed 56. Not a duplicate library; version pin only.
  `LmaoArrow` owns whether 55 is load-bearing.
- cowshed-core/cowshed-gateway official `StreamWriter` on arrow 56: different job (RecordBatch in-process). No merge
  with this wasm byte-writer.
- `columine-event-processor/src/checkpoint.rs` local `align_to_8` vs this crate’s export.

## Non-findings (checked, clean)

- **Not a clone of `lmao-arrow`.** lmao-arrow builds dictionary `RecordBatch`es from span trees via `arrow-array`. This
  crate owns byte-backed event columns and a hand-emitted IPC stream. Sharing a crate would complect two data models.
- **Hand-rolled RecordBatch writer is load-bearing for wasm.** Production path never constructs
  `arrow_array::RecordBatch`. `arrow-array` is correctly a dev-dependency (reader oracle). Do not replace the writer
  with `arrow-ipc::writer::StreamWriter` in the wasm artifact.
- **`arrow-ipc` Schema decode is load-bearing** until a replacement decoder exists. Untrusted FFI schema bytes; not a
  `git2`/`openssl` “shell out” case (`plutil` etc. are irrelevant).
- **Workspace `default-features = false` on arrow-*** already set in `packages/columine/Cargo.toml`.
- No `unsafe`. `columine_types::die!` is used only for broken `ColumnType`/storage pairing (programmer invariant), not
  parse errors.
- No `cfg(target_os)`. No 5k-line god file. Functions stay near or under ~100 lines (`encode_record_batch_dynamic` is
  the long one, ~115).
- Column planes are LE `Vec<u8>`; IPC borrows `&[u8]` without `to_vec` on the write path (`lib.rs:9-12`, `DynamicColumn`
  borrows). Body is copied once into the caller buffer (`record_batch.rs:313`, in-place skip at `477-480`).
- `columns_seen` looks unused in this crate but is the extraction presence workspace written by `columine-parsing` — not
  dead.
- Variable-width geometric growth (`ensure_variable_capacity_preserving`) is retained across `reset` — L4-ish for the
  unknown payload size; not closed-form, but not a per-row realloc after warm.
- IPC tests that round-trip through `arrow_ipc::reader::StreamReader` with typed downcasts (`ipc.rs:519-671`) are real
  oracles, not string asserts.
- `ParseError` as `Result` on append paths is the right operational type; `Ok = 0` as a success discriminant on an error
  enum is unused in this crate and not worth a finding.
