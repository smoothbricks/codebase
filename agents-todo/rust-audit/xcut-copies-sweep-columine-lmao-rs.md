# XCUT copies sweep: columine + lmao-rs

Scope: grep-driven over every `packages/columine/crates/*/src/**/*.rs` and `packages/lmao-rs/crates/*/src/**/*.rs` (74
src files, 31 066 lines including in-file `#[cfg(test)]`; 27 test-crate files / 11 647 lines excluded from production
counts). Close-read of hot-path modules listed below; remaining files covered by pattern census only. Doctrine:
`BYPRODUCT-ENGINEERING.md`, `docs/handbook/04-mechanisms.md`, `05-memory-toolkit.md`, `02-measurement.md` §4.1.

Close-read (line counts are whole-file): `columine-wasm/src/lib.rs` (884), `columine-ep-wasm/src/lib.rs` (264),
`columine-parsing/src/{lib.rs` (382), `json_parser.rs` (684), `json_scanner.rs` (345), `json_extractor.rs` (1304),
`msgpack_scanner.rs` (526), `msgpack_extractor.rs` (634)}, `columine-arrow/src/{columns.rs` (1138), `schema.rs` (434),
`ipc.rs` (688)}, `columine-vm/src/{vm.rs` (5146), `hooks.rs` (167), `undo_log.rs` (349), `intern.rs` (257), `minroar.rs`
(724), `bitmap_ops.rs` (598)}, `columine-types/src/{abort.rs` (49), `types.rs` (1114)}, `lmao-core/src/{buffer.rs`
(289), `context.rs` (310), `identity.rs` (124), `columns.rs` (217)}, `lmao-wasm/src/lib.rs` (523),
`lmao-timestamp-proof/src/lib.rs` (234), `lmao-arrow/src/convert.rs` (317), `lmao-arena/src/lib.rs` (284).

Pattern census (production, `tests/` + `benches/` + `examples/` excluded; in-file `#[cfg(test)]` stripped):

| pattern              | prod count                                                                  |
| -------------------- | --------------------------------------------------------------------------- |
| `.to_vec()`          | 16                                                                          |
| `.clone()`           | 14                                                                          |
| `.to_owned()`        | 6                                                                           |
| `.to_string()`       | 17                                                                          |
| `format!`            | 15                                                                          |
| `Vec::new`           | 33                                                                          |
| `Vec::with_capacity` | 23                                                                          |
| `HashMap::`          | 2                                                                           |
| `.unwrap()`          | 16 (almost all leftover in-file tests; 2 real in `lmao-arena` native `Mem`) |
| `.expect(`           | 23                                                                          |
| `panic!`             | 11                                                                          |
| `unsafe` keyword     | 328 / 210 excl. `#[unsafe(no_mangle)]`                                      |
| `#[inline]`          | 148 (99 `#[inline]` + 49 `#[inline(always)]`)                               |

Wasm-reachable panic/unwrap/expect (same prod filter, crates that feed `columine-wasm` / `columine-ep-wasm` /
`lmao-wasm` / `lmao-timestamp-proof`): **unwrap 2** (native-only `VecMem`), **expect 15**, **panic! 11** (incl. `die!`
expansion + comment line), **unreachable! 4**. `panic = "abort"` turns every one of these into a trap and keeps the
format/`Location` machinery in the size budget unless the site is `die!`/`trap()`.

Regime (PH §4.1): these are the production wasm/native kernels (`opt-level = "z"` size profile for shipped wasm). An
allocation on a per-event / per-opcode / per-span path is HIGH regardless of wall time, because the stated design is
zero-allocation hot paths plus a wasm size budget.

## Summary

- JSON lexer owns `String` for every key/number/string token, then `peek_token` clones it on every field-boundary check
  — two heap allocs per JSON field before columns ever see the bytes.
- Scanners and typed extractors materialize `ColumnValue`/`Vec<u8>` per cell, then `append_*`/`add_event` copy the same
  bytes into pre-sized column buffers (double copy on the per-event path).
- `vm_execute_batch` heap-allocates a `Vec<&[u8]>` of column views per batch; bitmap opcodes `to_vec` the source payload
  and minroar algebra `Box::new([0u64; 1024])` (8 KiB) per container.
- Every lmao span pays three `vec![0; capacity]` plus `SharedStr::Owned(name.into())`; overflow repeats it. `expect` in
  the VM batch loop and timestamp-proof `span_start` pull panic formatting into wasm.
- No CRITICAL live correctness bug found on this axis. Highest-value deletes are Token-as-span, ColumnValue-as-borrow,
  reuse of `Runtime`/BitmapEnv scratch, and replacing `expect` with `die!` or `try_into().ok()`.

## Findings

### F1 — HIGH — COPIES — JSON `Token` owns `String` for every key, number, and string

Evidence: `packages/columine/crates/columine-parsing/src/json_parser.rs:16-27`, `292-308`, `372-411`

```
pub enum Token {
    ...
    String(String),
    Number(String),
    ...
}
fn parse_string(&mut self) -> Result<String, ParserError> {
    ...
    let mut value = Vec::new();
    loop {
        ...
        value.extend_from_slice(&self.input[self.cursor..run_end]);
        ...
        b'"' => return String::from_utf8(value).map_err(|_| ParserError::InvalidJson),
```

```
fn parse_number(&mut self) -> Result<String, ParserError> {
    let start = self.cursor;
    ...
    std::str::from_utf8(&self.input[start..self.cursor])
        .map(str::to_owned)
```

Problem: Every JSON string and number on the per-event path becomes a heap `String`. Unescaped strings are a pure copy
of a subslice of `input`. Numbers are already a contiguous digit run in `input` and are immediately re-parsed
(`parse::<i64>`) by scanners/extractors — evaporating work (Byproduct L0) plus an alloc. `parse_string` starts
`Vec::new()` with no capacity (L4 / PH §7.2 grow). Fix: `Token` becomes a copyable span: `String { start, end }` /
`Number { start, end }` over `parser.input`. Unescape into a caller-supplied scratch (the extractor already has
`work_buffer`) only when `\\` is present. `parse_number` returns the byte range; parse i64/f64 from it without an
intermediate `String`. Cost/Risk: All `Token` match sites in `json_scanner.rs`, `json_extractor.rs`, and parser
`expect_*`. Tests that assert `Token::String("id".into())` become span/equality-on-bytes. This is the single biggest
alloc cut in columine parsing.

### F2 — HIGH — COPIES — `peek_token` clones the owned `Token` on every field-boundary check

Evidence: `packages/columine/crates/columine-parsing/src/json_parser.rs:414-422`, `462-466`; callers
`json_scanner.rs:51-52`, `json_extractor.rs:346-350`

```
pub fn peek_token(&mut self) -> Result<Token, ParserError> {
    if self.peeked.is_none() {
        self.peeked = Some(self.advance()?);
    }
    Ok(self
        .peeked
        .as_ref()
        .map(|spanned| spanned.token.clone())
        .expect("peeked was just populated"))
}
pub fn is_object_end(&mut self) -> bool {
    matches!(self.peek_token(), Ok(Token::ObjectEnd))
}
```

Problem: Per-event object walk is `while !is_object_end() { expect_field_name() }`. When the next token is a key,
`is_object_end` peeks a `Token::String(String)` and **clones it**, then `expect_field_name` `take()`s the original. That
is a second heap alloc per JSON field, plus an `.expect` (format machinery) on the wasm parse path. Unit variants
(`ObjectEnd`) clone cheaply; the damage is the String arm. Fix: `peek_token(&self) -> Result<&Token, _>` or
`fn peeked_is(&self, TokenKind) -> bool` that does not clone. Drop the `expect`; the `None` branch is an invariant after
the `is_none` fill — `die!` or restructure so it cannot be `None`. Cost/Risk: Local to `JsonParser`. F1 makes the clone
cheap even if left; do both.

### F3 — HIGH — COPIES — JSON scanner `to_vec`s the value payload, then `add_event` copies it again

Evidence: `packages/columine/crates/columine-parsing/src/json_scanner.rs:52-88`,
`packages/columine/crates/columine-arrow/src/columns.rs:113-154`

```
"id" => { id = Some(parser.expect_string()...) }          // owned String
"type" => { event_type = Some(parser.expect_string()...) } // owned String
"value" => {
    let start = token.start;
    let end = parser.skip_value_from(token)...;
    value = Some(parser.input()[start..end].to_vec());    // owned Vec
}
...
output.add_event(id.as_bytes(), event_type.as_bytes(), ..., value.as_deref())
```

```
/// Add an event: strings are copied into internal data buffers
self.id_data[...].copy_from_slice(id);
self.type_data[...].copy_from_slice(event_type);
self.value_data[...].copy_from_slice(v);
```

Problem: Per-event path allocates 2–3 heap buffers (`id`, `type`, `value`) whose only consumer immediately memcpy's them
into `EventColumns`' already-reserved planes (columns.rs:84-101 pre-sizes — the L4 reservation already exists). The
intermediate owns are evaporating (L0). Fix: Pass `&[u8]` spans from `input` (and from F1 token spans) straight into
`add_event`. Keep `id`/`event_type` as `Option<&[u8]>` with lifetime of `input`. Cost/Risk: `parse_event_object` locals
change lifetime; `add_event` already takes `&[u8]`. Scanner tests that inspect the `Option<String>` go away.

### F4 — HIGH — COPIES — MessagePack scanner `to_owned`s id/type and `to_vec`s value, then `add_event` copies again

Evidence: `packages/columine/crates/columine-parsing/src/msgpack_scanner.rs:69-123`

```
b"id" => { id = Some(std::str::from_utf8(reader.read_string()?)?.to_owned()) }
b"type" => { event_type = Some(...to_owned()) }
b"value" => {
    let raw = &reader.input()[start..reader.position()];
    value = if raw == [0xc0] { None } else { Some(raw.to_vec()) };
}
output.add_event(id?.as_bytes(), event_type?.as_bytes(), ..., value.as_deref())
```

Problem: Same double-copy as F3. Worse: `read_string()` already returned `&[u8]` from the input — `to_owned` is a pure
tax. JSON at least had to unescape; msgpack keys/values are raw slices. Fix: `id: Option<&[u8]>`, `value: Option<&[u8]>`
over `reader.input()`. Identical `add_event` callsite. Cost/Risk: Local to `parse_event_map`. Pair with F3 so both
scanners share one "span then copy-once into columns" shape.

### F5 — HIGH — COPIES — MessagePack typed extractor allocates `ColumnValue` per cell, then `append_cell` copies again

Evidence: `packages/columine/crates/columine-parsing/src/msgpack_extractor.rs:135-175`, `245-264`;
`packages/columine/crates/columine-parsing/src/lib.rs:43-67`

```
Some(ColumnValue::Binary(work_buffer[..extra_end].to_vec()))
Some(ColumnValue::Utf8(std::str::from_utf8(reader.read_string()?)?.to_owned()))
Some(ColumnValue::Binary(payload.to_vec()))
Some(ColumnValue::Binary(reader.input()[start..reader.position()].to_vec()))
```

```
Some(ColumnValue::Utf8(s)) => columns.append_utf8(column, s.as_bytes()),
Some(ColumnValue::Binary(b)) => columns.append_binary(column, &b),
```

Problem: Per-event, per-declared-field: heap `String`/`Vec<u8>`, then a second copy into `DynamicColumns`. Extra-fields
already live in `work_buffer`; line 144 `to_vec`s that scratch only to hand `&[u8]` to `append_binary`. The json extra
path already does this right (F16). Fix: `extract_typed_value` calls `columns.append_utf8(column, bytes)` /
`append_binary(column, payload)` / `append_int64` directly. Delete `ColumnValue` from the production extract path (keep
it as a test-only view in `read_cell`, which already documents itself as "test/differential view" at lib.rs:71-72).
Cost/Risk: `append_cell` and `ColumnValue` become test-only. Extractor error paths that currently build a `ColumnValue`
then `append` flatten to one call.

### F6 — HIGH — COPIES — JSON typed extractor pays F1's `String` token, then wraps it in `ColumnValue` again

Evidence: `packages/columine/crates/columine-parsing/src/json_extractor.rs:346-350`, `477-478`; `lib.rs:61-67`

```
let name = parser.expect_field_name()?;          // String
if let Some(lookup) = config.field_map.get(&name) {
```

```
ArrowType::Utf8 => match token {
    Token::String(value) => Some(ColumnValue::Utf8(value)),
```

Problem: Per-field: owned key `String` (F1) hashed against `HashMap<String, FieldLookup>` (F15), then for Utf8 the token
`String` is moved into `ColumnValue::Utf8` and `append_utf8` copies the bytes a second time. Int/float also
`value.parse()` after the number was already copied into a `String` (F1). Fix: After F1, `expect_field_name` returns
`&str` into `input`. `field_map` keys should be interned `&'static str` or a small linear scan / FNV table over schema
names (schema width is tiny). Utf8 appends `input[start..end]` directly. Cost/Risk: Tied to F1 and F15. json extra path
(lines 444-446) already appends from the workspace slice — copy that pattern to declared fields.

### F7 — HIGH — COPIES — `vm_execute_batch` heap-allocates column-view `Vec` on every batch

Evidence: `packages/columine/crates/columine-wasm/src/lib.rs:136-144`, `236-264`

```
unsafe fn cols_vec<'a>(col_ptrs: *const *const u8, num_cols: u32) -> Vec<&'a [u8]> {
    let mut cols = Vec::with_capacity(num_cols as usize);
    for i in 0..num_cols as usize {
        let p = unsafe { *col_ptrs.add(i) };
        cols.push(unsafe { state_ref(p) });
    }
    cols
}
pub unsafe extern "C" fn vm_execute_batch(...) -> u32 {
    ...
    let cols = unsafe { cols_vec(col_ptrs_ptr, num_cols) };
    rt().vm.execute_batch(state, program, &cols, batch_len)
}
```

Same alloc on `vm_execute_batch_delta` (252-264). Problem: Per-batch (and therefore per-event-batch, the VM's hottest
export) a heap `Vec`. `num_cols` is schema-bounded and known. `Runtime` is already a process-lifetime static
(lib.rs:33-67) — this is exactly L7 "pre-allocate at startup". Fix: Store `cols: Vec<&'static [u8]>` (or a
`[Option<&[u8]>; MAX_COLS]` if there is a closed-form cap) on `Runtime`; `clear` + `extend` into it. After warmup,
grow=0. Cost/Risk: Lifetime: views are only valid for the call; a reused vec of raw slices is the same contract as
today's stack `Vec`. `MAX_COLS` already exists in types if the schema cap is pinned — use it.

### F8 — HIGH — COPIES — Bitmap algebra opcodes `to_vec` the source payload because of a borrow split

Evidence: `packages/columine/crates/columine-vm/src/vm.rs:2957-3008`; scratch already exists at `bitmap_ops.rs:71-78`
and `vm.rs:585-716`

```
let source_data = if source_len > 0 {
    let off = source_storage.payload_offset() as usize;
    state[off..off + source_len as usize].to_vec()
} else {
    Vec::new()
};
...
let source_data = self.bitmap_env.algebra_result().to_vec();
```

Problem: Per-opcode (AND/OR/ANDNOT/XOR and the Scratch variants) a heap copy of the serialized bitmap. Cause is `&state`
vs `&mut state` into `batch_bitmap_algebra`. `BitmapEnv` already documents "Reusable buffers avoid allocation churn on
store and algebra paths" (`store_temp`, `algebra_result`) and `UndoState.capture_scratch` is the same pattern for undo.
The copy is not consumed more than once (PH §7.2 counterexample does not apply). Fix: Copy source into
`bitmap_env.store_temp` (reuse, `clear`+`extend`/`resize`), then pass `&env.store_temp` while mutating `state`. Scratch
variants: if `algebra_result` is overwritten in-place, rotate two buffers in `BitmapEnv` instead of `to_vec`. Cost/Risk:
`batch_bitmap_algebra` signature stays `&[u8]` source. Only the VM opcode arm and env buffers move. ColVmCore owns
`vm.rs`.

### F9 — HIGH — COPIES — minroar algebra allocates an 8 KiB `Box<[u64; 1024]>` per container, twice

Evidence: `packages/columine/crates/columine-vm/src/minroar.rs:188-190`, `410-452`

```
fn to_words(&self) -> Box<[u64; 1024]> {
    let mut words = Box::new([0u64; 1024]);
```

```
let a = self.containers[i].to_words();
let b = other.containers[j].to_words();
let mut out = Box::new([0u64; 1024]);
for k in 0..1024 { out[k] = op(a[k], b[k]); }
```

Problem: Per overlapping container on a bitmap opcode: two 8 KiB heap bitsets from `to_words`, plus a third for the
result. Array/run containers also scatter into that box. `algebra` then `Vec::new()`s keys/containers with no capacity
(L4). Clone of whole containers on the keep-left/keep-right arms (428, 434) copies those boxes again. Fix: One
thread-local / `BitmapEnv`-owned `[u64; 1024; 3]` (or three boxes allocated once). `to_words_into(&mut [u64; 1024])`.
Pre-size `keys`/`containers` to `self.keys.len() + other.keys.len()`. Prefer in-place word ops on bitset containers
without materializing array/run to words when both sides are already bitsets. Cost/Risk: minroar is the roaring backend
for every bitmap opcode (F8). ColVmMaps / bitmap slice.

### F10 — HIGH — COPIES — Every lmao span allocates three capacity-sized `Vec`s plus an owned span name

Evidence: `packages/lmao-rs/crates/lmao-core/src/buffer.rs:90-118`;
`packages/lmao-rs/crates/lmao-core/src/context.rs:132-144`; `packages/lmao-rs/crates/lmao-core/src/columns.rs:22-29`

```
let mut timestamps = vec![0i64; capacity];
let mut headers = vec![0u32; capacity];
let line_numbers = vec![0u32; capacity];
...
messages: StrColumn::new(),
children: Vec::new(),
```

```
let buf = SpanBuffer::start_dynamic(
    identity, capacity,
    SharedStr::Owned(name.into()),   // Arc<str> even for literals
    ...
);
```

Problem: Per-span path. `capacity` is 8..=1024; three zeroed vecs + identity `Arc` (context.rs:62-68
`Arc::new(SpanIdentity { trace_id: self.trace_id.clone(), ... })`). `SharedStr` exists specifically so `'static` names
are free (`columns.rs:22-24`) but `start` takes `&str` and forces `Owned`. First `messages.set` then allocates
`Box<[Option<SharedStr>]>` of `capacity` (columns.rs:162-165). Fix: `start_dynamic` takes `impl Into<SharedStr>` so
macros pass `SharedStr::Static("...")`. Pre-allocate span buffers from a pool keyed by capacity tier (lmao-arena already
has size classes / tiers — that is the SSOT for this). Zeroing three planes per span is the write-side cost; reuse by
`fill(0)` on a pooled buffer. Cost/Risk: `lmao-macros` span! callers, `SpanContext::start` signature. LmaoCore /
LmaoArena.

### F11 — HIGH — COPIES — Span overflow allocates another full buffer (same three vecs) on the append path

Evidence: `packages/lmao-rs/crates/lmao-core/src/buffer.rs:197-214`, `229-233`

```
if target.write_index == target.capacity {
    let mut next = Box::new(SpanBuffer {
        identity: target.identity.clone(),
        ...
        timestamps: vec![0i64; target.capacity],
        headers: vec![0u32; target.capacity],
        line_numbers: vec![0u32; target.capacity],
        ...
        children: Vec::new(),
    });
```

```
while target.overflow.is_some() {
    target = target.overflow.as_deref_mut().unwrap();
}
```

Problem: Per-append once the span exceeds capacity — still a per-span-log hot path, not startup. `unwrap` after
`is_some` is an invariant but still a panic symbol in the crate. `identity.clone()` is an `Arc` bump (cheap) plus a new
`SpanBuffer` heap object. Fix: Same pool as F10. Walk overflow with
`while let Some(o) = target.overflow.as_deref_mut()`. Closed-form: if overflow is a documented rare path, keep the alloc
but delete the `unwrap` (structure). If logs routinely overflow 8-row buffers, the capacity default is the bug (L4: size
is a formula of expected rows). Cost/Risk: Local to `SpanBuffer`. Measure overflow rate before changing the default
capacity (PH §4.1 / L8).

### F12 — HIGH — STRUCTURE — `expect` in the per-event VM batch loop pulls panic formatting into wasm

Evidence: `packages/columine/crates/columine-vm/src/vm.rs:3151-3159`, `3212-3213`, `3252-3253`, `1801-1802`, `4004-4005`

```
for i in 0..batch_len {
    let key = keys[usize::try_from(i).expect("batch index fits usize")];
    ...
    comparison.expect("max comparison resolved"),
```

Problem: This is the interpreter's per-event loop, compiled into `columine.wasm`. `u32 as usize` is infallible on
wasm32/aarch64; `try_from`+`expect` is a panic path with a string, `Location`, and fmt glue (exactly what `abort.rs`
exists to prevent). `comparison.expect` is an opcode-invariant: if it can fail, it is an operational `ErrorCode`; if it
cannot, it is `die!`. Fix: `let i = i as usize;` (or `keys[i as usize]` with a hoisted `let n = batch_len as usize` so
LLVM elides bounds — PH §7.3). Replace `comparison.expect` with
`let Some(c) = comparison else { return ErrorCode::... }` or `die!` if it is a programmer bug. Count: 6 `expect` in
`vm.rs` production. Cost/Risk: Interpreter arms only. Wasm size delta is the acceptance figure (census the panic symbols
before/after).

### F13 — HIGH — STRUCTURE — timestamp-proof `span_start` `expect`s on a 8-byte slice every span

Evidence: `packages/lmao-rs/crates/lmao-timestamp-proof/src/lib.rs:40-70`; `layout.rs:50`

```
unsafe fn timestamp_nanos(trace_root_ptr: u32) -> i64 {
    let root = unsafe { buf_at(trace_root_ptr, 16) };
    let wall_clock = i64::from_le_bytes(
        root[TRACE_ROOT_WALL_CLOCK_OFFSET..TRACE_ROOT_WALL_CLOCK_OFFSET + 8]
            .try_into()
            .expect("trace root wall clock"),
    );
```

Called from exported `span_start` / `span_end_ok` / `span_end_err` (67-80). Problem: Per-span wasm export. The slice
length is a constant 8; `try_into` cannot fail unless `buf_at` lied. `expect` still ships the message. Same shape at
`layout.rs:50` (`"write-index slot"`). Fix: `root[off..off+8].try_into().unwrap_or([0;8])` is still a panic-free but
silent path — wrong. Use `let arr: [u8; 8] = unsafe { *(root.as_ptr().add(off) as *const [u8; 8]) }` with a SAFETY
comment citing `buf_at(..., 16)` and the offset constant, or `die!`. The layout is the certified-open (PH §7.7):
validate the 16-byte root once in `init_trace_root`, then trust. Cost/Risk: Local to timestamp-proof. LmaoTsProof owns
the crate.

### F14 — HIGH — COPIES — `parse_string` grows a `Vec` from 0 on every JSON string

Evidence: `packages/columine/crates/columine-parsing/src/json_parser.rs:292-301`

```
let mut value = Vec::new();
loop {
    let run_end = crate::scan::find_string_special(self.input, self.cursor);
    value.extend_from_slice(&self.input[self.cursor..run_end]);
```

Problem: Even after F1, if unescape stays, this is L4: the unescaped length is ≤ `run_end - start` (escapes only
shrink). `Vec::new` + `extend` reallocates in the per-token loop (PH §7.2 `grow > 0`). Fix: With F1, skip the vec
entirely for the no-escape fast path (the common case). Escape path: `Vec::with_capacity(end - start)` into caller
scratch. Cost/Risk: Subsumed by F1. If F1 slips, this one line is still the grow.

### F15 — MEDIUM — SSOT — `ExtractionConfig` stores every field name twice as `String` and hashes with `HashMap<String, _>`

Evidence: `packages/columine/crates/columine-parsing/src/lib.rs:98-104`, `148-176`

```
pub struct ExtractionConfig {
    pub(crate) field_entries: Vec<(usize, ArrowType, String)>,
    pub(crate) field_map: HashMap<String, FieldLookup>,
    ...
}
field_map.insert((*name).to_owned(), FieldLookup { ... });
field_entries.push((column, field.arrow_type, (*name).to_owned()));
```

Problem: Schema-build (once per processor, not per event) still duplicates each name. The per-event cost is hashing
`&str` against owned `String` keys (F6) instead of a direct index. Schema width is a closed form; a
`Vec<(&str, FieldLookup)>` linear scan or a perfect map from interned names beats `HashMap<String,_>` (Byproduct: "Hash
a key that is already uniform" — names are a tiny static set). Fix: One table: `entries: Vec<FieldLookup>` plus
`names: Vec<&str>` borrowed from `DynamicSchemaConfig.field_names` (which already owns them — schema.rs:95). Lookup:
linear scan, or intern names at config build to u32 column ids. Cost/Risk: `build_extraction_config` and both
extractors' `field_map.get`. Config is built at `ep_create_*` (startup). Per-event win is the hash + F6.

### F16 — MEDIUM — DUPLICATION — JSON extra-fields append from the work buffer; MessagePack extra-fields `to_vec`

Evidence: `packages/columine/crates/columine-parsing/src/json_extractor.rs:439-446` vs `msgpack_extractor.rs:135-145`

JSON (correct):

```
// Append directly from the workspace slice; no intermediate materialization is needed.
if let Err(err) = columns.append_binary(column as u32, bytes) {
```

MessagePack:

```
Some(ColumnValue::Binary(work_buffer[..extra_end].to_vec())),
```

Problem: The copies have already diverged. JSON comments the zero-copy; msgpack re-introduced the intermediate. Live
waste on the per-event extra-fields path, not just style. Fix: Copy the JSON arm.
`append(columns, column, Some(ColumnValue::Binary(...)))` becomes
`columns.append_binary(column as u32, &work_buffer[..extra_end])`. Cost/Risk: One function. Tests asserting
`ColumnValue::Binary(vec![...])` via `cell()` stay (that API is the test view).

### F17 — MEDIUM — STRUCTURE — `FlatUndoEntry::read_from` `expect`s on a fixed `[u8; 24]`

Evidence: `packages/columine/crates/columine-vm/src/undo_log.rs:119-145`

```
pub fn read_from(buf: &[u8; FLAT_UNDO_ENTRY_SIZE as usize]) -> Option<Self> {
    ...
    key: u32::from_le_bytes(buf[4..8].try_into().expect("4-byte slice")),
    prev_value: u32::from_le_bytes(buf[8..12].try_into().expect("4-byte slice")),
    aux: u64::from_le_bytes(buf[16..24].try_into().expect("8-byte slice")),
```

Problem: Input is already `[u8; N]`. `buf[4..8]` is structurally 4 bytes; `try_into` cannot fail. Three `expect` strings
in the delta/undo path compiled into wasm. Same class as F12/F13. Fix:
`u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]])` or `array::from_fn`. No `Result`, no panic. Cost/Risk: Local.
Undo/delta export is on the batch path when undo is enabled.

### F18 — MEDIUM — STRUCTURE — `NoVm` uses `panic!("...")` instead of `die!`, so messages survive in the VM crate

Evidence: `packages/columine/crates/columine-vm/src/hooks.rs:123-165`; contrast
`packages/columine/crates/columine-types/src/abort.rs:1-37`

```
panic!("TTL slot reached NoVm — the vm slice's eviction machinery is required")
panic!("BITMAP slot reached NoVm — the bitmap_ops slice is required")
```

`abort.rs` exists so invariant panics "drop the message tokens entirely on wasm32". These five `panic!` plus two
`unreachable!(...)` do not. Problem: If LTO does not DCE `NoVm` (it is a public `VmHooks` impl in the same crate as the
interpreter), the strings and fmt glue land in `columine.wasm`. Even if DCE'd today, a future use from a test-in-wasm or
a stub path reintroduces them. Fix: `die!("TTL slot reached NoVm")`. Or make the methods return `ErrorCode`
(operational: the configuration asked for TTL/bitmap without the slice — that is not a programmer bug in the wasm
artifact, it is a wiring error). Prefer `ErrorCode` (Cantrill: errors are values). Cost/Risk: `VmHooks` signatures.
ColVmCore.

### F19 — MEDIUM — STRUCTURE — `SlotMeta::slot_type` uses `panic!()` on wasm instead of `trap()`

Evidence: `packages/columine/crates/columine-types/src/types.rs:626-636`

```
// `die!` is not const-callable; a bare `panic!()` keeps this fn
// const while shipping no message string in the wasm artifact.
#[cfg(target_arch = "wasm32")]
None => panic!(),
```

Problem: The comment is half-right: no message string. `panic!()` still references the panic runtime / `Location`.
`abort.rs:trap` is `core::arch::wasm32::unreachable()` with `#[inline(never)] #[cold]` — that is the intended
size-budget primitive. `die!` not being const is a real constraint; `trap()` is a plain `fn() -> !` and _is_ callable
from a const fn on wasm only via a `const { }` / split, or drop `const` on `slot_type` (it already reads runtime flags).
Fix: On wasm, `None => crate::abort::trap()`. If `const fn` must stay, a `const` wasm `unreachable` intrinsic, or stop
being `const` — nothing in the hot path needs `slot_type` at compile time. Cost/Risk: One function. Types crate is in
every wasm artifact.

### F20 — MEDIUM — COPIES — String intern tables grow under load (L4)

Evidence: `packages/columine/crates/columine-vm/src/intern.rs:7-8`, `41-70`

```
//! Buffers grow on demand; handles stay stable because growth only appends.
if (self.count + 1) * 2 > self.hash_cap {
    self.grow_hash();
}
```

`new` sizes from `initial_cap` but `intern` reallocates `data` / `offsets` / hash when exceeded. Problem: Intern is on
the wasm export path (`intern_*` mentioned at intern.rs:9). Growth is a mid-flight reallocation (Byproduct L4: "growth
is a contract violation surfaced at admission"). First-seen strings on a hot batch pay memcpy of the whole table. Fix:
Admission: intern capacity is a formula of program/schema. `intern` returns a fail code on full instead of growing, or
`grow` is a distinct export. Handles-stable growth can stay as an explicit `intern_reserve` call, not a silent realloc
inside `intern`. Cost/Risk: Bindings that assume intern never fails. Cross-slice: wasm intern exports.

### F21 — MEDIUM — COPIES — `DynamicSchemaConfig` copies schema bytes and `DataType`s at build

Evidence: `packages/columine/crates/columine-arrow/src/schema.rs:88-175`

```
logical_types.push(field.data_type().clone());
...
schema_bytes: schema_bytes.to_vec(),
```

Problem: Once-per-processor (ep_create / vm_init adjacent), not per-event — so not HIGH. Still a second copy of bytes
the caller already owns for the duration of `from_wire`, plus `arrow_schema::DataType` clones that exist only to
re-check what `field_metadata: Vec<SignalSchemaField>` already encoded. Fix: Borrow schema bytes if the config's
lifetime can be `'a` (wasm handle owns a `Box` already — the copy is to detach from the caller's JS buffer, which **is**
load-bearing: JS may reuse the input buffer). Keep `schema_bytes: Vec<u8>`. Drop `logical_types` if
`field_metadata.arrow_type` is the SSOT (it is validated equal at build). Cost/Risk: IPC writer that reads
`logical_types`. ColArrow. The `to_vec` of schema bytes is **not** deletable without a lifetime change; the `DataType`
clone vector is.

### F22 — MEDIUM — COPIES — Arrow convert clones `NullBuffer` to share parent-nulls across two columns

Evidence: `packages/lmao-rs/crates/lmao-arrow/src/convert.rs:284-311`

```
let parent_nulls = NullBuffer::new(parent_valid.finish());
...
Arc::new(UInt64Array::new(parent_thread_ids.into(), Some(parent_nulls.clone()))),
Arc::new(UInt32Array::new(parent_span_ids.into(), Some(parent_nulls))),
```

Problem: Per-flush (not per-span write). `NullBuffer::clone` is typically a buffer refcount, not a byte copy —
[INFERENCE] on arrow-rs internals; if it is refcount this is LOW. `expect("trace ID observed in pass 1")` etc. on the
per-row walk (239, 260, 262, 272) are panic paths in the flush kernel. Fix: Confirm clone is `Arc` (if yes, leave).
Replace per-row `expect` with `ok_or(ConvertError::...)?` — pass-1 already proved it; an `Err` is a programmer bug in
pass-2, `die!` if so, not `expect` with a string. Cost/Risk: convert.rs flush. LmaoArrow. Flush is colder than
span-write (F10) but still production.

### F23 — MEDIUM — STRUCTURE — `vm.rs` is a 5146-line interpreter god file

Evidence: `packages/columine/crates/columine-vm/src/vm.rs` (5146 lines). Opcode arms sampled at 2937-3035 (bitmap),
3147-3168 (struct-map per-event). Problem: A 5k–10k-line file is itself a finding. Natural seams already exist as
modules (`hashmap_ops`, `hashset_ops`, `struct_map`, `bitmap_ops`, `aggregates`, `undo_log`) but the dispatch `match op`
and per-event loops still live in one function. Fix: One module per opcode family, each exporting
`fn execute_*(vm, state, cols, batch_len, pc) -> Result<new_pc, ErrorCode>`. The match becomes a jump table of those
fns. This also makes F8/F12 local. Cost/Risk: Large mechanical move. ColVmCore owns this file — do not edit from this
slice.

### F24 — MEDIUM — STRUCTURE — Three wasm crates `allow(clippy::missing_safety_doc)` instead of per-block invariants

Evidence: `packages/columine/crates/columine-wasm/src/lib.rs:15-22, 110-144`;
`packages/columine/crates/columine-ep-wasm/src/lib.rs:17, 43-45, 125-129, 210-211`;
`packages/lmao-rs/crates/lmao-wasm/src/lib.rs:22, 67-109`

columine-wasm documents the policy (one contract for all externs) and `state_mut` has a `# Safety` section. `state_ref`
/ `buf` / `cols_vec` / `from_raw_parts` sites do not restate length vs linear-memory bounds. ep-wasm
`from_raw_parts(schema_ptr, schema_len)` does not check `schema_len` against wasm memory (only null). lmao-wasm
`laundered(off).read()` has a module comment for the null-at-0 trick but no per-load bounds check (host-owned memory is
the contract). Problem: 210 production `unsafe` tokens excluding `#[unsafe(no_mangle)]`; most lack a local SAFETY
comment in the previous 6 lines. The crate-level allow is an explicit choice, not an accident. Still: `cols_vec`
dereferences `col_ptrs.add(i)` with no documented bound on `num_cols` vs the pointer table length. Fix: Keep the
crate-level allow for the identical extern contract. Add a 3-line SAFETY on `cols_vec`, `buf`, and ep-wasm
`from_raw_parts` stating the length source. Bound `num_cols` against a closed-form max (F7). Cost/Risk: Comments + one
bounds check on `num_cols`. Not a copy finding except that unbounded `num_cols` makes F7's Vec unbounded.

### F25 — MEDIUM — COPIES — `TraceId::generate` `format!`s 32 hex chars

Evidence: `packages/lmao-rs/crates/lmao-core/src/identity.rs:54-57`

```
pub fn generate(entropy: &mut dyn Entropy) -> Self {
    let (a, b) = (entropy.next_u64(), entropy.next_u64());
    Self(format!("{a:016x}{b:016x}").into())
}
```

Problem: Once per trace (startup of a tree, not per span). `format!` + `String` + `Arc<str>` is three steps; the result
is 32 ASCII bytes with a closed-form size. Fix: `[0u8; 32]` stack buf, write hex by nibble,
`Arc::<str>::from(str::from_utf8(&buf).unwrap())` — or a 32-byte inline type, not `Arc<str>`, if trace ids are always
this width (W3C). Cost/Risk: `TraceId` is `Arc<str>` to share across the tree (identity.rs:7). Keep the Arc; drop
`format!`. Once-per-trace = not HIGH.

### F26 — MEDIUM — DUPLICATION — JSON and MessagePack typed extractors are the same presence/fallback/append state machine

Evidence: `json_extractor.rs:339-461` vs `msgpack_extractor.rs:77-149` (columns_seen fill, field lookup, extra fallback,
presence_entries, field_entries null-fill, end_row). Problem: Two implementations of one concept. They have already
diverged on extra-fields (F16). Next change will diverge again. Fix: One
`fn finish_row(columns, config, extra: Option<&[u8]>)` for presence/null-fill/extra append. Per-format loops only
tokenize and call `append_*`. Cost/Risk: ColParseJson + ColParseMsgpack. Do it as a single cutover after F5/F16.

### F27 — LOW — COPIES — lmao-query builds SQL with `format!` / `to_string` per constraint

Evidence: `packages/lmao-rs/crates/lmao-query/src/datafusion_backend.rs:47-76`, `sqlite_backend.rs:87-168`,
`arrow_backend.rs:45-62` Problem: Query-assertion path, not the span write path. `to_string()` on dictionary values per
row when reading Arrow back is a test/query convenience copy. sqlite `HashMap<(String, u32), _>` keys
`trace_id.to_string()` per row at ingest (sqlite_backend.rs:87-88). Fix: If this crate stays a test oracle, leave it. If
it becomes a production query lane, intern trace ids (they are already `Arc<str>` upstream) and stop `format!`ing SQL —
use bound parameters throughout (sqlite already does for values; datafusion inlines literals). Cost/Risk: LmaoQuery.
Regime: once-per-assertion, not per-span.

### F28 — LOW — COPIES — `parsed_event` / `read_cell` copy columns back into owned `String`/`Vec`

Evidence: `packages/columine/crates/columine-parsing/src/lib.rs:23-87`

```
id: String::from_utf8_lossy(cols.get_id(row)?).into_owned(),
...
value: cols.get_value(row).map(<[u8]>::to_vec),
ColumnType::Utf8 => ColumnValue::Utf8(String::from_utf8_lossy(...).into_owned()),
ColumnType::Binary => ColumnValue::Binary(storage.read_variable(row_idx)?.to_vec()),
```

Problem: Documented as "test/differential view". Production consumers "read the Arrow buffers". Not a hot-path finding
unless something in ep-wasm calls it — grep of production src shows extractors write via `append_cell`, not
`parsed_event`. Fix: Keep test-only; `#[cfg(test)]` the functions so they cannot leak into wasm. Cost/Risk: Tests that
use `parsed_event`. Wasm size if currently linked — [INFERENCE] LTO may already drop them.

### F29 — LOW — STRUCTURE — `#[inline]` / `#[inline(always)]` volume

Evidence: census 148 `#[inline]`, 49 `#[inline(always)]`. Justified at module level: `columine-vm/src/bytes.rs:7-11`
("compile to a bounds check plus one load/store, and the interpreter hot loops call them per event"). `hash_table.rs` /
`meta.rs` / `struct_map.rs` leaf accessors follow PH §7.1 (inline leaf helpers). `lmao-wasm` `laundered` is
`#[inline(always)]` with a comment (lib.rs:63-70). Problem: Many `#[inline]` on trivial getters (`capacity()`, `len()`,
`as_str()`) have no local justification. Harmless under LTO; under the size profile (opt-z, PH §4.1) `inline(always)`
can **increase** wasm size via duplication, which is the opposite of the budget. Fix: Keep `inline(always)` only on the
bytes/meta/hash_table load/store leaves (already documented). Demote the rest to `#[inline]` or nothing; let opt-z
outline. Do not spray new `always` without a census. Cost/Risk: Size, not correctness. Measure wasm size, do not guess
(PH §4.1).

### F30 — LOW — COPIES — `EventColumns` `read_u32` swallows a short slice with `unwrap_or([0;4])`

Evidence: `packages/columine/crates/columine-arrow/src/columns.rs:39-41`

```
fn read_u32(bytes: &[u8], index: usize) -> u32 {
    let start = index * 4;
    u32::from_le_bytes(bytes[start..start + 4].try_into().unwrap_or([0; 4]))
}
```

Problem: Not an alloc. Operational failure (short buffer) becomes `0` instead of `Err` / `die!`. Cantrill: silence is
the enemy. Off the per-event write path (write_u32 does not do this). Fix: `debug_assert!(start + 4 <= bytes.len());` +
`try_into().unwrap()` is still a panic. Prefer indexing `[u8;4]` after a length check that returns `ParseError`, or
`die!` if the offsets plane is an invariant of `new()`. Cost/Risk: Readers of id/type/value offsets. ColArrow.

## Cross-slice questions

- **ColVmCore** (`columine-vm/src/vm.rs`): F8/F12/F23 live in the interpreter you own. `capture_scratch` /
  `BitmapEnv.store_temp` look like the intended reuse buffers — is F8 waiting on a borrow-checker refactor you already
  planned?
- **ColParseJson / ColParseMsgpack**: F1–F6 and F16/F26 are the extractors. Token-as-span is the cut that unlocks the
  rest; please do not "fix" msgpack extra `to_vec` in isolation without the shared `finish_row` (F26).
- **ColArrow**: F3's `add_event` is already the right copy-once sink. F21 `logical_types` — is anything outside
  schema.rs matching on `DataType` rather than `ArrowType`?
- **LmaoCore / LmaoArena**: F10/F11 want the arena size-class pool to own span buffers. `columns.rs:14-18` already says
  single-block bundling "lives in lmao-arena". Is a `SpanBuffer` pool in-bounds for that crate?
- **LmaoTsProof**: F13 `expect` on 8-byte slices in `span_start`.
- **XcutRustTs**: `Token` / `ColumnValue` / `ParsedEvent` look like a hand-restated TS DTO. Not verified against TS in
  this slice.
- **columine-wasm vs "superset wasm wrapper"** (`columine-wasm/src/lib.rs:9-10`): "adapted copy of the superset wasm
  wrapper". If that superset still exists, `cols_vec` (F7) is duplicated there — not opened here.

## Non-findings (checked, clean)

- `columine-types` `die!`/`trap()`/`check!` are the correct wasm panic posture (abort.rs). Native `panic!` inside `die!`
  is cfg-gated and does not ship in wasm.
- `EventColumns::new` / `DynamicColumns` pre-size planes (L4 reservation). The waste is the _extra_ owns in front of
  them (F3–F6), not the columns themselves.
- JSON extra-fields path (`json_extractor.rs:444-446`) already appends from workspace memory — the pattern to copy.
- MessagePack field _lookup_ already uses `&[u8]`/`&str` without allocating the key (msgpack_extractor.rs:92-98). Only
  the JSON side allocates keys (F6).
- lmao `log(template: &'static str)` uses `SharedStr::Static` (context.rs:165-167) — zero-alloc on that path, as
  documented.
- lmao-wasm `WasmMem` loads are unaligned pointer reads, no `to_vec`. Native `VecMem` `try_into().unwrap()`
  (arena/lib.rs:165, 173) is the test backend, not the wasm artifact.
- `#[inline(always)]` on `bytes.rs` accessors is justified and matches PH §7.1.
- No `HashMap<String,_>` on the VM opcode path. The only production `HashMap::` sites are extraction config (F15) and
  sqlite test ingest.
- Schema `to_vec` of caller schema bytes (F21) is load-bearing for the JS-owned input buffer; not a delete.
- Tests' `.unwrap()` / `format!` / `clone` (bloom false-positive loop, intern growth test, ipc
  `Field::new(format!(...))`) are not production findings.
- No CRITICAL correctness bug on this axis: copies waste work and wasm size; they do not (as read) drop events or
  corrupt slots.
