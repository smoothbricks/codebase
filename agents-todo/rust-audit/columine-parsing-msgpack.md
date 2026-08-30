# columine-parsing/msgpack

Scope: `packages/columine/crates/columine-parsing/src/msgpack_extractor.rs` (634 lines),
`packages/columine/crates/columine-parsing/src/msgpack_scanner.rs` (526 lines). Doctrine: `BYPRODUCT-ENGINEERING.md`,
`docs/handbook/04-mechanisms.md`, `docs/handbook/05-memory-toolkit.md`, `docs/handbook/02-measurement.md` §4.1. Neighbor
reads (comparison only, not audited): `json_extractor.rs` (extract loop + `extract_typed_value` + `ExtractionError` +
`reserve_map32`), `json_scanner.rs` (`parse_event_object` + timestamp), `lib.rs` (`append_cell` / `ColumnValue`),
`columine-arrow` `EventColumns::add_event`, `columine-event-processor` `extract_with_growth`. Manifest:
`packages/columine/crates/columine-parsing/Cargo.toml`.

## Summary

- `Reader::skip_value` map32 does `n * 2` in `u32`; a 5-byte header with `n >= 2^31` wraps in release, skip succeeds,
  parent parse continues inside the skipped value (parser confusion on untrusted input).
- Typed extraction already disagrees with the JSON twin: `Int32 | Int64` share one arm (floats + ISO strings accepted
  for Int32; Int64 strings are ISO-only, not bigint-as-string).
- Malformed MessagePack is reported as `ExtractionError::InvalidJson`; the scanner ABI (`ParseError::InvalidMsgpack`) is
  never produced on the extractor path.
- JSON and MessagePack pipelines are hand-copied control flow, not two instantiations of one generic; they have already
  diverged (null `value`, diagnostics, extra encoding, Int32 policy).
- Per-event ingest copies through `String`/`Vec` into columns that already take `&[u8]`; JSON extra already appends the
  workspace slice.
- Marker-class predicates and big-endian length prefixes are restated in `read_*`, `skip_value`, `read_timestamp`, and
  `is_integer`/`is_string`.
- No `rmp`/`rmpv` (or other parser crate): the hand-rolled `Reader` is load-bearing. Do not shell out.

## Findings

### F1 — CRITICAL — STRUCTURE — `skip_value` map32 `n * 2` wraps and silently succeeds

Evidence: `packages/columine/crates/columine-parsing/src/msgpack_scanner.rs:360-372`

```
            0xde => {
                self.pos += 1;
                let n = u32::from(u16::from_be_bytes(self.take_slice(2)?.try_into().ok()?));
                for _ in 0..n * 2 {
                    self.skip_value()?;
                }
            }
            0xdf => {
                self.pos += 1;
                let n = u32::from_be_bytes(self.take_slice(4)?.try_into().ok()?);
                for _ in 0..n * 2 {
                    self.skip_value()?;
                }
            }
```

Problem: `0xde` is map16 (`n` fits `u32 * 2`). `0xdf` is map32: `n * 2` overflows `u32` when `n >= 0x8000_0000`. Debug
panics; release wraps to a small loop bound (0 when `n == 0x8000_0000`). `skip_value` then returns `Some(())` after
consuming only the 5-byte header. Callers treat the value as skipped. The extra path (`msgpack_extractor.rs:103-110`)
and undeclared-field skip (`:122`) then parse the map body as the parent event's next keys — parser confusion on a
5-byte untrusted input. Regime: skip of undeclared / extra values during ingest (hot, attacker-controlled). Fix: never
multiply the count in `u32`. `for _ in 0..n { self.skip_value()?; self.skip_value()?; }` (or `u64::from(n) * 2`). Same
for any future `* 2` on a u32 header. Add a pin test: `0xdf 80 00 00 00` as an undeclared field must return
`InvalidMsgpack` / fail skip, not extract inner keys. Cost/Risk: `skip_value` only; extractor extra/skip paths inherit
the fix. No JSON change.

### F2 — HIGH — SSOT — Int32/Int64 coercion already diverged from JSON (live spec bug)

Evidence: `packages/columine/crates/columine-parsing/src/msgpack_extractor.rs:179-207`

```
        ArrowType::Int32 | ArrowType::Int64 => {
            if first == 0xc0 {
                reader.skip_value();
                None
            } else if is_integer(first) {
                Some(ColumnValue::Int64(reader.read_integer()...))
            } else if matches!(first, 0xca | 0xcb) {
                Some(ColumnValue::Int64(reader.read_float()? as i64))
            } else if is_string(first) {
                ...
                    parse_iso8601_to_micros(value)...
```

Versus JSON (neighbor, not owned): `json_extractor.rs:490-533` keeps Int32 strict (integer numbers only; no string, no
decimal) and Int64 strings as `parse::<i64>()` **or** ISO-8601 (`parse_timestamp_to_micros`). JSON Int64 rejects
decimals (`parse::<i64>` fails). Problem: one MessagePack arm implements a third policy. Int32 accepts truncated floats
and ISO strings (JSON Int32 does not). Int64 strings that are decimal integers (`"12345"`) fail ISO parse here and
become `InvalidFieldType`; JSON accepts them as bigint-as-string. Same schema, two answers. Byproduct L0: the coercion
table evaporated into two copies and they no longer agree. Fix: split `Int32` and `Int64` to match
`json_extractor::extract_typed_value`. Int32: integer markers only (reject `0xca`/`0xcb` and strings). Int64: integers;
optional float policy named in one comment if MessagePack timestamps need it (they belong in `read_timestamp`, not Int64
cells); strings via the same `parse::<i64>().or_else(parse_iso8601_to_micros)` order JSON uses. JSON is the SSOT for the
Arrow-type table; this file instantiates it for MessagePack markers. Cost/Risk: any producer that stuffed ISO strings or
floats into an Int32 MessagePack column will start failing. That is the point. Tests must pin both arms (none do today —
see Non-findings / F7).

### F3 — HIGH — SSOT — extractor reports `InvalidJson` for malformed MessagePack

Evidence: `packages/columine/crates/columine-parsing/src/msgpack_extractor.rs:33-35,57-59,93,104`

```
        let size = reader
            .read_array_header()
            .ok_or(ExtractionError::InvalidJson)?;
...
        .read_map_header()
        .ok_or(ExtractionError::InvalidJson)?;
...
        let key = reader.read_string().ok_or(ExtractionError::InvalidJson)?;
...
            reader.skip_value().ok_or(ExtractionError::InvalidJson)?;
```

`ExtractionError` (`json_extractor.rs:12-19`) has `InvalidJson` and `MsgpackError`, not `InvalidMsgpack`. The scanner
maps correctly: `msgpack_scanner.rs:17-18` `MsgpackScannerError::InvalidMsgpack => ParseError::InvalidMsgpack`.
`ParseError` ABI: `InvalidJson = 1`, `InvalidMsgpack = 2` (`columine-arrow` columns.rs). Event-processor
`extract_with_growth` calls this extractor with no diagnostic (`columine-event-processor/src/lib.rs:529-536`) and folds
leftover errors to `INVALID_JSON`. Problem: two error vocabularies for one format. Extractor failures are
indistinguishable from JSON parse failures at the only type this path returns. `MsgpackError` exists and is unused here.
Fix: add `ExtractionError::InvalidMsgpack` next to `InvalidJson` (json_extractor owns the enum — cross-slice) and use it
at every `ok_or(InvalidJson)` in this file. Until that variant exists, `MsgpackError` is less wrong than `InvalidJson`.
Thread `&mut ExtractionDiagnostic` like `extract_json_events` so EP can stage-tag MSGPACK instead of JSON. Cost/Risk: EP
diagnostic mapping and any TS `DIAGNOSTIC_*` tables. Scanner path already correct; do not change it.

### F4 — HIGH — DUPLICATION — JSON/MessagePack pipelines are hand copies, already diverged

Evidence (scanners): `msgpack_scanner.rs:54-129` vs `json_scanner.rs:42-93` — same `id`/`type`/`timestamp`/`value`
Options, same `add_event`, same `ParseError` remap. Live divergence on null `value`: `msgpack_scanner.rs:105-109`

```
                value = if raw == [0xc0] {
                    None
                } else {
                    Some(raw.to_vec())
                };
```

JSON always `Some(input[start..end].to_vec())` when the field is present (`json_scanner.rs:69-73`), including JSON
`null`. Present-nil MessagePack == missing field; present-null JSON == raw bytes. Evidence (extractors):
`msgpack_extractor.rs:81-148` vs `json_extractor.rs:341-460` — same `columns_seen.fill`, presence loop, missing-field
fill, `$extra` map32 (`0xdf` + count). Extra envelope restated: `msgpack_extractor.rs:139-144`
`work_buffer[0] = 0xdf; ... extra_count.to_be_bytes(); ... Binary(work_buffer[..extra_end].to_vec())` vs
`json_extractor.rs:758-763` `reserve_map32` / `finish_map32` + `columns.append_binary` (no intermediate `Vec`). Problem:
not two monomorphizations of one scanner/extractor. A `trait FormatCursor` over `JsonParser` vs `Reader` would hide the
real split (JSON must re-encode extra; MessagePack splices already-encoded pairs; JSON carries diagnostics). The copies
are the _control flow after a key is classified_ and the _Stage-4B four-field event_. They have already drifted (F2,
null `value`, diagnostics, extra commit). Fix: do **not** introduce a generic scanner trait. One source each for:

1. Event keys: a single `b"id"|b"type"|b"timestamp"|b"value"` table used by both scanners;
   `fn finish_event(id, ty, ts, value)`.
2. Declared-row commit: `fn commit_declared_row(columns, config, extra: Option<&[u8]>)` (presence + missing + fallback)
   — delete the duplicated loops.
3. Extra map32 header: `MsgpackValueWriter::reserve_map32`/`finish_map32` (json_extractor) is the encoder SSOT; this
   file should write the same 5-byte header through that helper or a 5-byte function next to `Reader`, then
   `columns.append_binary` on the workspace slice. Keep `Reader` and `JsonParser` native. Cost/Risk: json_extractor +
   json_scanner must move in lockstep (other slices). Null-`value` behavior needs an explicit product decision; I would
   treat present nil/null as null in **both** scanners (MessagePack is the honest one).

### F5 — MEDIUM — COPIES — evaporating `String`/`Vec` on the per-event ingest path

Evidence: `msgpack_scanner.rs:70-78,81-89,108,116-123`

```
                    .to_owned(),
...
                    Some(raw.to_vec())
...
            id.ok_or(...)?.as_bytes(),
            event_type.ok_or(...)?.as_bytes(),
...
            value.as_deref(),
```

`EventColumns::add_event` already takes `&[u8]` and copies into column buffers (`columine-arrow` `columns.rs:116-154`).
`msgpack_extractor.rs:168-175` Utf8 `.to_owned()` into `ColumnValue::Utf8`, then `append` → `append_cell` →
`append_utf8(..., s.as_bytes())` (`lib.rs:55-67`). Binary: `payload.to_vec()` (`:255`) / span `.to_vec()` (`:262-263`)
then `append_binary`. Extra: `:144` `work_buffer[..extra_end].to_vec()`. JSON extra already appends the workspace slice
(`json_extractor.rs:444-446`). Problem: MessagePack strings/bins are borrowed from `input`. `ColumnValue` is documented
as a test carrier (`lib.rs:51-54`). Each declared Utf8/Binary field allocates, copies into the carrier, then copies
again into Arrow storage. Regime: per-field per-event ingest (hot for the extractor/scanner, not a per-byte skip loop,
not startup). Intermediate alloc is evaporating work (Byproduct L0); the column copy is the one that must remain. Fix:
keep `id`/`type`/`value` as `Option<&'a [u8]>` (value as input span, not `to_vec`) in `parse_event_map`. On the typed
path, `columns.append_utf8` / `append_binary` / `append_null` directly from the `Reader` slice after UTF-8 check; drop
`ColumnValue` from the production match (keep it for tests via `read_cell`). Extra:
`columns.append_binary(column as u32, &work_buffer[..extra_end])` like JSON. Cost/Risk: `append` helper becomes
type-specific or disappears. Tests that round-trip `ColumnValue` still go through `read_cell`.

### F6 — MEDIUM — DUPLICATION — marker-class and length-prefix logic restated four times

Evidence: `msgpack_extractor.rs:284-288`

```
fn is_integer(byte: u8) -> bool {
    byte & 0x80 == 0 || byte & 0xe0 == 0xe0 || (0xcc..=0xd3).contains(&byte)
}
fn is_string(byte: u8) -> bool {
    byte & 0xe0 == 0xa0 || matches!(byte, 0xd9..=0xdb)
}
```

Same predicates inlined in `read_timestamp` (`msgpack_scanner.rs:255-264`) and again as match arms in `read_integer`
(`:216-235`), `read_string` (`:203-210`), `skip_value` (`:268-373`). Every multi-byte length does
`take_slice(N)?.try_into().ok()?` even though `take_slice` already returned exactly N bytes (`:183-186`, `:206-208`,
`:294`, `:369`, …). Problem: skip vs read can (and will) drift independently — F1 is the skip arm of a header
`read_map_header` already decoded. `try_into().ok()?` is evaporating validation of an invariant `take_slice` proved
(handbook §7.7). Regime: per-value on the ingest path; the duplication cost is correctness, not nanoseconds. Fix: put
`is_integer`/`is_string`/`is_float` on `Reader`. Add `read_be_u16`/`read_be_u32` (direct `[s[0], s[1]]`, no `try_into`).
Drive `skip_value` leaf sizes from a `[u8; 256]` skip-fixed table; containers recurse. `read_timestamp` calls the same
classifiers. Cost/Risk: `Reader` only. Extractor helpers become one-liners or vanish.

### F7 — LOW — TESTS — no test can go red on the JSON/MessagePack coercion split

Evidence: extractor tests (`msgpack_extractor.rs:305-345`, `:347-388`) pin Utf8/presence/capacity/extra bytes; none
construct `ArrowType::Int32` with a float or ISO string, or `Int64` with a decimal-integer string. Scanner tests
(`msgpack_scanner.rs:442-458`) document float timestamps on the **event** timestamp field, not on typed Int32 columns.
`parsed_event` (`lib.rs:32-37`) re-allocates via `from_utf8_lossy` / `to_vec`; scanner tests assert those Strings
(`msgpack_scanner.rs:414-417`), so invalid UTF-8 in `id` cannot surface as a red test through this view (the production
path already rejects non-UTF-8 — `:72-77`). Problem: handbook §7.10bb / §4.2b — the Int32/Int64 divergence (F2) has no
oracle that a working JSON-matched path must meet and a collapsed arm cannot. Extra-byte tests (`:548-553`) correctly
pin the `0xdf` envelope (typed contract, keep). Fix: add extractor pins: Int32 + `0xcb` float → `InvalidFieldType`;
Int64 + fixstr `"12345"` → `12345`; Int64 + ISO-8601 → micros. Assert `get_id` bytes, not `parsed_event().id`, for UTF-8
tests. Cost/Risk: test module only.

## Cross-slice questions

- `json_extractor.rs` owns `ExtractionError`, `ExtractionDiagnostic`, and `MsgpackValueWriter::{reserve,finish}_map32`.
  F3/F4 need a variant and the extra-header helper moved or shared; this slice should not fork them.
- `json_extractor::extract_typed_value` Int32/Int64 policy is the coercion SSOT (F2). Confirm that slice treats JSON as
  canonical.
- `columine-event-processor/src/lib.rs:529-536` does not pass a diagnostic into `extract_msgpack_events`; even with F3
  fixed, EP still maps leftovers to `INVALID_JSON` (`:439-446`). EP slice owns that fold.
- `lib.rs::append_cell` / `ColumnValue` are the test carrier F5 wants production code to stop using.
- `EventColumns::add_event` (`columine-arrow`) already copies `&[u8]`; no change needed there once the scanner stops
  pre-owning.

## Non-findings (checked, clean)

- **DEP-BLOAT:** `Cargo.toml` depends only on `columine-arrow`. No `rmp`/`rmpv`/`serde`. The hand-rolled `Reader` is
  in-process, hot, and needs `Option`/span error typing; a `msgpack` CLI is not a substitute. Keep it. Dev-dep
  `proptest` does not leak into the lib.
- **Zero-copy where it counts:** `read_string` / `read_bin` / extra key-value spans return `&'a [u8]` into `input`. The
  waste is the subsequent own (F5), not the reader.
- **ISO-8601 SSOT:** both files re-export/call `json_scanner::parse_iso8601_to_micros`. Do not duplicate the parser.
- No `unsafe`. No production `unwrap`/`expect` (test helpers only). No `cfg(target_os)`. No 5k-line god file (production
  bodies ~290 / ~378 lines).
- `begin_row` happens after a successful map header (`msgpack_extractor.rs:55-62`); invalid headers do not leave a
  half-row. `abandon_row` on error is correct.
- `read_bin` unwraps 0xc4/c5/c6 payload (not the wrapper token); `bin_unwrap_pin` (`:616-633`) is a real pin.
- Nil skip after a successful peek of `0xc0` is infallible given `skip_value`'s 0xc0 arm; ignoring that `Option` is not
  an operational swallow.
- Extra `0xdf` tests assert typed `ColumnValue::Binary` bytes, not rendered text.
- Capacity: exactly-full batch is legal; one past is `TooManyEvents` (`:24-29`, `:390-421`). Matches the JSON comment.
- Profile trap (§4.1): no benches in this slice; no performance claim made.
