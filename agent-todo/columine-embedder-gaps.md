# columine: what an embedder still cannot do through the parse plane

Each item states the need, its red test, and what main holds (checked at `d9e8e8da0c`). Read
`specs/columine/10-physical-vocabulary.md` before designing; state any rule the spec lacks there before the code. Where
the code contradicts the spec, the code is the defect.

## The host numbers still typed by hand

Need: §9 "The generated host table" says no host number is typed by hand, and a consumer that generates its own host
table from its Cargo pin must emit columine's text byte for byte. On main:

- `packages/columine/src/parse-backend.ts` types `EP_MAX_HANDLE = 255` (`:180`; the handle table is in
  `columine-ep-wasm/src/lib.rs`), the CPB1 header and descriptor field offsets (`:910-927`, owners
  `CompactBatchView::parse` and `decode_descriptor` in `columine-event-processor/src/compact.rs`), the diagnostic
  offsets (`:943-949`, `write_diagnostic_bytes`), the result-header offsets (`:1085-1087`, `:1160-1163`,
  `write_result_header`) and the RecordBatch metadata estimate (`:846-847`, `76` and `4 + 16 * n`,
  `record_batch_metadata_size` in `columine-arrow/src/record_batch.rs`). Lines 123-126 are host-only planning bounds and
  stay.
- The renderer is a private `fn render()` in `columine-event-processor/tests/host_abi.rs:345`, so a consumer cannot call
  it.
- The schema dictionary table's null-entry length is an inline `u32::MAX` (`columine-arrow/src/schema.rs:1500`), not an
  item a generated table can carry.

Change: `host_abi.rs` generates the layouts above (offsets found by encoding probe values, as it already does for
`FIELD_DESCRIPTOR` with `SignalSchemaField::to_wire()`), `EP_MAX_HANDLE` becomes one `pub const` the handle table and
the generator share, the renderer becomes a public library function, and the null-entry length an exported item.

Red first: the rendered text gains the layouts, so the committed generated file differs until regenerated; then
`parse-backend.ts` imports them and `rg -n '\+ (4|8|12|16|20|24|28)\b' packages/columine/src/parse-backend.ts` finds no
ABI offset.

## A named dynamic member whose column cannot hold a null as data is accepted

Need: §7 "A dynamic member's null is data". On main a `DynamicMember { event_type, member }` whose member resolves to a
typed (non-Binary) column is accepted and has no effect (`build_extraction_config_with_semantic_schemas`,
`columine-parsing/src/lib.rs`); `ConfigError::InvalidDynamicMember` does not exist (the nearest is
`InvalidDynamicColumn`, `lib.rs:1113`).

Change: resolve the member as the lanes do (`variant_map` for `(event_type, member)`, then `payload_map`, then a record
routing row); a column that is not a Binary or LargeBinary plane, or no place at all, is a new
`ConfigError::InvalidDynamicMember`, mapped to `CreateFailure::Init`.

Red first, through `EventProcessor`: a config naming a Utf8 member dynamic is refused at creation; one naming a Binary
member or a record-routed member is created.

## Route payload members by a subtype inside the payload

Need: an event whose payload shape depends on a member inside the payload (an operation event split by its operation
kind) puts one member name in different columns and record paths per subtype. On main a routing row is keyed by
`(discriminant, member)` (`PayloadDiscriminator`, `DiscriminatedMember`, `RecordMember` in
`columine-arrow/src/schema.rs:753-843`; `MemberTable` in `columine-parsing/src/lib.rs:269`), and nothing reads a member
of the payload to route. The discriminant is already any envelope key in code (`discriminates_by_type`,
`event_discriminant`, `lib.rs:469-487`), while §7 says "A routing row's discriminant is the event's `type`"
(`10-physical-vocabulary.md:150`): state the envelope-key rule in §7 in the same unit.

Change: `PayloadDiscriminator` gains `subtype_key: Option<String>`; `DiscriminatedMember` and `RecordMember` gain
`subtype: Option<String>`; `MemberTable` keys become `(discriminant, subtype, member)`, a row without a subtype matching
every subtype of its discriminant and a row with one winning over it. Both lanes read ahead inside the nested `value`
object for `subtype_key` before resolving any member (the pattern of `lookahead_event_type` in `json_extractor.rs` and
its probe `Reader` twin in `msgpack_extractor.rs`, one level down). Placement, the enum-dictionary check and
`ValueCells::prepare` targets are keyed by the triple.

Red first, through `EventProcessor::new_with_cells`, on JSON and MessagePack, with the subtype member spelled before and
after the members it routes: two subtypes of one event type put the same member name in different columns and record
paths, and the two lanes produce identical streams (`cross_format.rs`).

## Dictionary columns from the TypeScript backend, and a shared-id Schema writer

Need: §3 and §4 (a schema dictionary has fixed values supplied at creation; fields of one slot may share one Arrow id,
so a Schema message that shares the id pays for one dictionary message). On main the wasm export
`ep_create_with_schema_and_dictionaries` exists (`columine-ep-wasm/src/lib.rs:285`) and `parse-backend.ts` binds it
(`:59`, `:91-116`) but never calls it; `pipeline.ts` takes no dictionary table. Rust has only the method
`DynamicSchemaConfig::schema_message(&self)` (`columine-arrow/src/schema.rs:1119`); arrow-ipc's writer numbers every
dictionary field separately, and the one-id builder is test-only (`columine-arrow/tests/common/mod.rs`
`schema_message_declaring`).

Change:

1. `ParseCompactBackend` config gains `dictionaries?: readonly (readonly (string | null)[])[]`; the backend encodes the
   wire table (`[count u32]`, per dictionary `[values u32]`, per value `[length u32][UTF-8]`, little-endian, length
   `0xFFFFFFFF` a null entry; its layout constants generated from Rust) and creates the handle through
   `ep_create_with_schema_and_dictionaries`; the cached-handle comparison includes the table's bytes.
2. A compact (CPB1) caller of a stream-dictionary column needs to add values: state in the spec how (an export that
   appends values to the handle's image and returns their ordinals, called before `ep_compact`), then build it.
3. A Rust writer `columine_arrow::schema_message(fields)` that gives every field of one slot one Arrow id (the test
   helper promoted); `logical_schema_ipc_bytes` callers that declare dictionary fields move to it.

Red first: TypeScript over the real wasm, a batch parsed through a schema with an enum column reads back (flechette) as
a dictionary column of the declared values, and creation without the table fails with `SCHEMA_DICTIONARY`; Rust,
`schema_message` over three fields of two slots yields two Arrow ids, and the preamble holds one message per schema
dictionary.

## A value-object frame nested inside an embedder-written member

Need: §7 ends "a frame nested inside a member the embedder writes has no cell of its own and refuses the row"
(`10-physical-vocabulary.md:168-169`); an embedder that writes a declared object member holding a value-object field
needs the certified bytes at any depth. On main the embedder path reads through `msgpack_value` and never consumes the
projection (`msgpack_extractor.rs:912-930`), so `ExtractionError::UnwrittenProjection` (`:111`) follows. That error is
also wider than §7: `unwritten_projection()` (`columine-event-processor/src/lib.rs:890`) fails the whole call with
`VALIDATION`/`PAYLOAD_VIOLATION`, not the row.

Change: state the rule in §7 first, then a `ValueView` wrapper holding the `FrameProjections` answers a nested frame's
`as_bytes()` with its certified bytes, so `member`, `put` and `put_undeclared` receive certified frames at any depth.

Red first, through `EventProcessor::new_with_cells` with an admission that projects: a declared object member holding a
value-object field, written to an embedder column; the embedder receives the certified bytes, where today the call
fails.

## The opaque extension on record and declared-record columns

Need: an embedder's TypeScript host encodes a whole record cell itself, so values its MessagePack view cannot carry (a
set, a map with non-string keys, an integer past 64 bits) reach a record column and the `value.$declared` column as they
already reach a dynamic cell. On main §7 lets only a dynamic column and a dynamic member of the declared record take the
reserved extension (`10-physical-vocabulary.md:156-164`): `takes_opaque` (`columine-parsing/src/lib.rs:414`), the
record-member path in `msgpack_extractor.rs:536-544`, refused elsewhere as `OPAQUE_VALUE`.

Change: state in §7 that a record column and the `value.$declared` column take the extension at the cell's top, unread;
on `value.$declared` the seam also receives the row's discriminant, since the record's shape depends on the event type.
The JSON lane has no opaque form and needs none.

Red first, through `EventProcessor::new_with_cells`: a record column and the `value.$declared` column each receive the
extension's payload unread, the second with the discriminant; today both are refused as `OPAQUE_VALUE`.

## Exact semantic kinds for u64, category, text, timestamp and duration

Need: a declared field's column type is exact, but its admission node can only be one of the kinds the judge parses, so
an embedder lowers `u64` to `bigint` with bounds, `category` to `string`, a text to `union(string, binary)` (the 32-byte
width checked only by the column), and `timestamp(unit)` or `duration(unit)` to `bigint` or `number`. On main
`parse_schema` (`columine-parsing/src/validate.rs:2076-2167`) accepts unknown, string, key, axId, number, boolean,
binary, bigint, i32, u32, enum, range, array, vo, object, union, map, record and optional, and answers `UnknownKind` for
every other kind.

Change: `u64`, `category`, `text` (a content hash of exactly 32 bytes, or the inline text with its hash),
`timestamp(unit)` and `duration(unit)` as semantic kinds with their own messages (path, expected, observed), and their
TypeScript twin and wire form with exact members.

Red first, per kind: a value the coarse node admits and the exact one refuses, naming the member and the kind (a text
hash of 31 bytes; a `timestamp('ms')` that is not an integer; a `u64` given as a fraction).

## One declaration of the time-unit codes

Need: one owner per constant. On main the same four codes (0 seconds, 1 milliseconds, 2 microseconds, 3 nanoseconds) are
declared in `columine-vm/src/temporal.rs:12` (`NANOS_PER_UNIT`, indexed by the temporal table's `field_unit` byte) and
`columine-arrow/src/schema.rs:291-310` (`TIME_UNITS`, `time_unit`, `time_unit_code`, the Timestamp plane's
`type_param`), because `columine-vm` depends only on `columine-types`; the generated host table writes a third copy for
TypeScript.

Change: the codes and their nanosecond scales move to `columine-types`, and both crates and the host-table generator
read that one declaration.

Red first: a test in `columine-types` that each unit's code equals its index in the scale table, and a compile failure
in each crate when its private table goes before the import is added.

## Struct maps keyed by text

Need: an embedder's reducer keys maps by strings and by pairs of strings; interning them to u32 ids exists only for lack
of a text lane. On main `LaneKind` is `Raw` and `Interned` only (`columine-vm/src/key_index.rs:42-47`). The change is
written and not on main: `9ad6790813` (a one-key struct map keyed by a text) and `5309dfdc6d` (a two-key struct map that
holds frames and is keyed by texts; its upserts 0x83/0x87/0x89 carry the array tail with `num_arrays` always present),
on `d7acb489f8`. Governing: `specs/columine/04-key-index.md` §3, §5.

1. Land those two commits.
2. A text key in logical export and import: `columine-vm/src/logical_export.rs` decodes and encodes every key lane as a
   u32 through the interner (`decode_u32` `:527`, `encode_u32` `:548`, `key_u32` `:571`).
3. The change feed's prior value of a two-key row whose leading fields are frames: `struct_map2_value`
   (`columine-vm/src/vm.rs:3139`) reads fields 0 and 1 as u32 numbers.
4. Admission refuses a writer of a text-keyed slot that does not map its frames.

Red first, per item: a text-keyed map exported and imported holds the same keys in the same order, NUL bytes and shared
prefixes included; the change feed answers a two-key row's prior value with its frames; a writer that leaves the frames
unmapped is refused at creation.

## The wasm smoke tests depend on one global runtime

Need: tests that run in parallel without a lock (AGENTS.md rule 6: test serialization is a workaround). On main the
extern layer holds one global runtime (`columine-wasm/src/lib.rs:73`, `static mut RUNTIME`), and
`columine-wasm/tests/smoke.rs:33-36` serializes every test with a `SERIAL` mutex. Run together without it, the tests
answered `InvalidState` (4) at their first `execute_batch`, the failing set varying by run, and passed alone.

Change: each runtime is an explicit instance behind a handle, and the mutex goes.

Red first: the smoke tests without `SERIAL`, run in parallel, pass on every run.
