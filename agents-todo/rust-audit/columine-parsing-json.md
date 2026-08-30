# columine-parsing/json

Scope: `packages/columine/crates/columine-parsing/src/lib.rs` (382), `json_parser.rs` (684), `json_scanner.rs` (345),
`json_extractor.rs` (1304), `scan.rs` (136). Doctrine: BYPRODUCT-ENGINEERING.md, PERFORMANCE-HANDBOOK §4.1 / §7.2 / §7.7
/ §7.8 / §7.10bb, `docs/handbook/04-mechanisms.md`, `docs/handbook/05-memory-toolkit.md`. Regime: HOT ingest
(`[profile.wasm-release.package.columine-parsing] opt-level = 3`).

## Summary

- Intended pipeline is one lexer, two alternative consumers: `scan.rs` classifiers → `JsonParser` → either
  `parse_json_events` (`EventColumns`) or `extract_json_events` (`DynamicColumns`). They do not both run on one call.
- The scan is not a byproduct the extractor consumes. `skip_value` / `skip_value_from` re-lex nested bytes into owned
  `Token`s that are thrown away; the scanner then `to_vec()`s the same span.
- Every string and number becomes a heap `String` in `Token`, then (extractor) a `ColumnValue`, then a copy into the
  column. Unescaped strings are already in the input.
- `peek_token` clones the peeked `Token` (field-name `String`) on every `is_object_end` / `is_array_end`.
- `extract_json_events` returns `TooManyEvents` with `ExtractionDiagnostic` still `NONE`; EP copies that empty
  diagnostic into the ABI header.
- `append()` maps `ParseError::OutOfMemory` to `InvalidFieldType`, undoing the OOM path `extract_typed_value` thinks it
  has.
- Extractor/scanner never reject trailing non-whitespace after the top-level array; `JsonParser` tests pin that as
  `InvalidJson`.
- Crate deps are clean (`columine-arrow` + `proptest` dev). Timestamp parsing is already one function.

## Findings

### F1 — HIGH — COPIES — Lexer materializes owned `String` for every string and number; extractor copies again through `ColumnValue`

Evidence: `json_parser.rs:16-27`, `json_parser.rs:292-308`, `json_parser.rs:372-411`, `json_extractor.rs:476-478`,
`lib.rs:41-68`

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
        let run_end = crate::scan::find_string_special(self.input, self.cursor);
        value.extend_from_slice(&self.input[self.cursor..run_end]);
        ...
        b'"' => return String::from_utf8(value).map_err(|_| ParserError::InvalidJson),
```

```
        std::str::from_utf8(&self.input[start..self.cursor])
            .map(str::to_owned)
            .map_err(|_| ParserError::InvalidNumber)
```

```
        ArrowType::Utf8 => match token {
            Token::String(value) => Some(ColumnValue::Utf8(value)),
```

```
        Some(ColumnValue::Utf8(s)) => columns.append_utf8(column, s.as_bytes()),
```

Problem: HOT path. Unescaped strings are a contiguous UTF-8 run in `input`. Numbers are a contiguous ASCII span already
validated by `parse_number`. Both are copied into a `String`, wrapped in `Token`, moved into `ColumnValue` (whose own
comment says this carrier is "useful for tests"), then copied a second time into the Arrow buffer. `expect_int64` /
`extract_typed_value` then re-parse the number `String` (L0 evaporating work). The binary-column path already writes in
place (`reserve_binary_value` + `MsgpackValueWriter`); Utf8/Int/Bool do not. Fix: `Token` holds spans (`start/end`
already exist on `SpannedToken`). Fast-path unescaped strings: `append_utf8` from `&input[start+1..end-1]`. Parse
i64/f64 from `&input[start..end]` with no `String`. Keep a decode scratch only for escaped strings. Stop routing
production appends through `ColumnValue`; keep `ColumnValue` as the test read-back type. Cost/Risk: Every `Token` match
site in parser/scanner/extractor. Tests that compare `Token::String("...")` still pass if `expect_string` returns
`String` at the API edge.

### F2 — HIGH — COPIES — `peek_token` clones the peeked `Token` on every end-check

Evidence: `json_parser.rs:414-422`, `json_parser.rs:462-466`, `json_extractor.rs:346-348`

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

```
    while !parser.is_object_end() {
        let name = parser
            .expect_field_name()
```

Problem: HOT. `is_object_end` / `is_array_end` run once per field. When the next token is a field name, `peek_token`
clones `Token::String(...)` — a second heap copy of the key that `expect_field_name` then moves. The
`expect("peeked was just populated")` is an invariant (acceptable); the clone is not. Fix: `peek_token` returns `&Token`
(or `peek_kind() -> TokenKind` without payload). `is_object_end` matches the peeked discriminant only. Cost/Risk:
`peek_token` callers (parser helpers, scanner, extractor, tests). No ABI change.

### F3 — HIGH — COPIES — `skip_value` re-lexes nested bytes into discarded owned tokens; scanner then copies the same span

Evidence: `json_parser.rs:468-501`, `json_scanner.rs:69-73`

```
    pub fn skip_value(&mut self) -> Result<(), ParserError> {
        let first = self.next_spanned()?;
        self.skip_value_from(first).map(|_| ())
    }
    pub(crate) fn skip_value_from(&mut self, first: SpannedToken) -> Result<usize, ParserError> {
        ...
        while depth != 0 {
            let token = self.next_spanned()?;
            end = token.end;
            match token.token {
                Token::ObjectBegin | Token::ArrayBegin => depth += 1,
                Token::ObjectEnd | Token::ArrayEnd => depth -= 1,
                _ => {}
            }
        }
```

```
            "value" => {
                let token = parser.next_spanned().map_err(json_error)?;
                let start = token.start;
                let end = parser.skip_value_from(token).map_err(json_error)?;
                value = Some(parser.input()[start..end].to_vec());
            }
```

Problem: HOT, Byproduct L0 / handbook §7.8. Nested keys and strings inside a skipped `value` (or an undeclared field)
still go through `parse_string` / `parse_number` and allocate `Token` payloads that are immediately dropped. The scanner
already has the byte interval `(start, end)` and then allocates another `Vec<u8>` of those bytes before `add_event`
copies them again into `EventColumns`. The extractor's undeclared-without-fallback path (`json_extractor.rs:420-423`)
pays the same tokenize-to-discard cost. Fix: A skip that only advances the cursor and depth using
`scan::find_string_special` / `skip_whitespace` — no `Token` payload. For `value`, pass `&input[start..end]` into
`add_event` (or append the span directly). The span is the byproduct of the skip; do not rebuild it. Cost/Risk: Skip
must still reject malformed nested JSON (the current tokenizer is the validator). A span-skip has to enforce the same
grammar or the capture can include invalid bytes. Differential: existing
`parse_json_events_nested_value_preserved_as_raw_json`.

### F4 — HIGH — STRUCTURE — `TooManyEvents` after a full batch leaves `ExtractionDiagnostic` at `NONE`

Evidence: `json_extractor.rs:261`, `json_extractor.rs:283-286`, `json_extractor.rs:302-311`

```
    *diagnostic = ExtractionDiagnostic::default();
    ...
        if count >= columns.capacity as usize && !parser.is_array_end() {
            return Err(ExtractionError::TooManyEvents);
        }
```

```
    if !columns.begin_row() {
        diagnostic.set(
            diagnostic_stage::COLUMN,
            diagnostic_detail::TOO_MANY_EVENTS,
            ...
        );
        return Err(ExtractionError::TooManyEvents);
    }
```

Problem: Two `TooManyEvents` sites; only `begin_row` fills the diagnostic. The post-success capacity check returns with
the diagnostic still default (`stage=0`, `detail=0`, `field_index=NO_FIELD`). EP (`columine-event-processor`
`create_log_entry_dynamic`) copies `extraction_diagnostic` into the result header when `wiring.diagnostics` is on, so
this path publishes an empty ABI diagnostic for a real overflow. `complete()` knows
`TooManyEvents → COLUMN / TOO_MANY_EVENTS` but is not called here. The same hole exists for `next_token()` `InvalidJson`
at `json_extractor.rs:289-291`. No json-extractor test covers `TooManyEvents` (msgpack extractor does). Fix:
`diagnostic.set(COLUMN, TOO_MANY_EVENTS, ...)` on the capacity check (and `JSON/INVALID_JSON` on the trailing
`next_token` failure). Add a json test that fills capacity+1 and asserts `stage`/`detail`. Cost/Risk: EP diagnostic
consumers. Msgpack extractor has the same capacity check without a diagnostic object — cross-slice.

### F5 — HIGH — STRUCTURE — `append()` maps `OutOfMemory` to `InvalidFieldType`

Evidence: `json_extractor.rs:681-689`, `json_extractor.rs:612-628`, `json_extractor.rs:673-678`

```
fn append(...) -> Result<(), ExtractionError> {
    crate::append_cell(columns, column, value).map_err(|error| match error {
        ParseError::BufferOverflow => ExtractionError::BufferOverflow,
        _ => ExtractionError::InvalidFieldType,
    })
}
```

```
            let parse_error = match err {
                ExtractionError::BufferOverflow => ParseError::BufferOverflow,
                ExtractionError::OutOfMemory => ParseError::OutOfMemory,
                _ => ParseError::InvalidFieldType,
            };
```

```
        columine_arrow::VariableValueError::OutOfMemory => ExtractionError::OutOfMemory,
```

Problem: Utf8/Int/Bool/Null/presence appends go through `append()`. Any `ParseError` other than `BufferOverflow` —
including `OutOfMemory` — becomes `InvalidFieldType`. `extract_typed_value`'s caller-side mapping is then unreachable
for OOM, because `append()` already erased it. Binary/variable-value writes preserve OOM via `variable_write_error`. EP
maps `ExtractionError::OutOfMemory` to `ResultCode::OutOfMemory` and everything else to `ParseError`, so a utf8-column
OOM is reported as a type error. Fix: Match `ParseError::OutOfMemory` (and `TooManyEvents` if it can appear) explicitly
in `append()`, same as `variable_write_error`. Cost/Risk: One function. Needs an oracle that forces utf8 append OOM if
arrow actually returns it — confirm with the arrow slice.

### F6 — MEDIUM — STRUCTURE — Extractor and scanner accept trailing non-whitespace after the top-level array

Evidence: `json_extractor.rs:275-292`, `json_scanner.rs:30-39`, `json_parser.rs:199-206`, `json_parser.rs:621`

```
    while !parser.is_array_end() {
        ...
    }
    parser
        .next_token()
        .map_err(|_| ExtractionError::InvalidJson)?;
    Ok(count)
```

```
                Top::Done => {
                    if self.cursor >= self.input.len() {
                        Err(ParserError::EndOfInput)
                    } else {
                        Err(ParserError::InvalidJson)
                    }
                }
```

Parser unit test lists `br#"[1] x"#` as `InvalidJson`. Production entry points consume `ArrayEnd` and return `Ok`
without a further token (or EOF) check, so `[{...}]garbage` is accepted and the suffix is dropped. Fix: After
`ArrayEnd`, `skip_whitespace` and require `cursor == input.len()` (or one more `next_token` that must be `EndOfInput`).
Same in `parse_json_events`. Cost/Risk: Any caller that concatenates documents in one buffer would start failing — that
is the point.

### F7 — MEDIUM — DUPLICATION — `JsonScannerError` restates `ParseError`; `json_error` discards the parser variant

Evidence: `json_scanner.rs:8-27`, `json_scanner.rs:112-114`, `lib.rs:16-19`

```
pub enum JsonScannerError {
    InvalidJson,
    MissingField,
    InvalidFieldType,
    TooManyEvents,
    BufferOverflow,
}
impl From<JsonScannerError> for ParseError { ... identical variants ... }
fn json_error(_: ParserError) -> JsonScannerError {
    JsonScannerError::InvalidJson
}
```

Problem: Greenfield duplicate enum. `ParserError::{UnexpectedToken, EndOfInput, InvalidNumber}` all collapse to
`InvalidJson`, so the scanner cannot tell a bad number from truncated input. `ExtractionError` is a third parallel
vocabulary. Fix: Scanner returns `ParseError` (already the crate's public error). Map only at the EP `ResultCode`
boundary. Keep `ParserError` as the lexer-local type. Cost/Risk: `parse_json_events` signature and its tests. EP already
maps scanner errors to `ResultCode::ParseError` wholesale.

### F8 — MEDIUM — TESTS — Oracles that cannot go red, or that assert only a count

Evidence: `json_parser.rs:524-531`, `json_extractor.rs:1144-1155`, `json_extractor.rs:1157-1178`,
`json_extractor.rs:1098-1121`, `json_extractor.rs:1214-1221`

```
    fn backend_name_reflects_target() {
        assert_eq!(backend_name(), "scalar");
    }
    fn target_info_reports_architecture() {
        assert!(!target_arch().is_empty());
        let _ = target_is_wasm();
    }
```

```
        assert_eq!(extract_json_events(...).unwrap(), 2);  // multiple_events: count only
        ...
        assert!(!c.is_null(3, 0));  // does_not_silently_drop: presence only, not bytes
        assert!(parse_timestamp_to_micros("2024-01-15T10:30:00.123Z").is_some());
```

Problem: Handbook §7.10bb / §4.2b. `backend_name()` is the literal `"scalar"`; the test cannot fail unless that constant
is edited. `target_arch()` is never empty. `extract_json_events_multiple_events` / `with_undeclared_fields` would stay
green if typed cells were wrong. `does_not_silently_drop_undeclared_fields` would stay green if `$extra` were any
non-null blob. No json test for `TooManyEvents` (F4). Fix: Delete the backend/arch tests or pin a real behavior (e.g.
`scan` vs a known input). Assert cell values (and `$extra` bytes) on the multi-event tests, matching
`json_extractor_routes_declared_and_undeclared_fields`. Add `TooManyEvents` + diagnostic assertions. Pin timestamp
micros, not `is_some()`. Cost/Risk: Test-only.

### F9 — LOW — STRUCTURE — Dead public aliases and a no-op free

Evidence: `json_parser.rs:36-46`, `json_parser.rs:85-87`, `lib.rs:193-196`

```
pub const BACKEND_NAME: &str = "scalar";
pub fn backend_name() -> &'static str { BACKEND_NAME }
pub fn target_arch() -> &'static str { std::env::consts::ARCH }
pub fn target_is_wasm() -> bool { cfg!(target_arch = "wasm32") || cfg!(target_arch = "wasm64") }
pub fn init(input: &'a [u8]) -> Self { Self::new(input) }
pub fn free_extraction_config(config: ExtractionConfig) { drop(config); }
```

Problem: Grep of the repo shows no callers outside this crate (and the tests in F8). Greenfield dead surface. Fix:
Delete `init`, `backend_name`/`BACKEND_NAME`, `target_arch`, `target_is_wasm`, `free_extraction_config` and the
re-export in `json_extractor.rs:9`. Cost/Risk: None if the grep holds; if a WASM export table names them, that table is
the other slice.

### F10 — LOW — SSOT — `field_entries` stores unused owned names next to `field_map`

Evidence: `lib.rs:99-104`, `lib.rs:158-176`, `json_extractor.rs:434-437`

```
    pub(crate) field_entries: Vec<(usize, ArrowType, String)>,
    pub(crate) field_map: HashMap<String, FieldLookup>,
```

```
    for (column, _, _) in &config.field_entries {
        if !columns.columns_seen[*column] {
            append(columns, *column, None)?;
```

Problem: Config-time, not per-token. Each schema name is `to_owned` into `field_map` and again into `field_entries`; the
`String` in `field_entries` is never read. Presence fill only needs column indices. Fix:
`field_entries: Vec<(usize, ArrowType)>` or iterate `0..n` / `field_map` values. One owned name per field, in the map
only. Cost/Risk: `ExtractionConfig` layout; msgpack extractor walks the same `field_entries`.

## Cross-slice questions

- `columine-parsing/src/msgpack_extractor.rs`: `extract_typed_value` is a second typed-append ladder over the same
  `ArrowType` match (json at `json_extractor.rs:464`). Did the Int32-strict / Int64-string-timestamp rules drift?
  Msgpack has a `TooManyEvents` test; json does not. Msgpack `extract_msgpack_events` has no `ExtractionDiagnostic` at
  all — EP synthesizes one. Who owns that ABI?
- `columine-arrow`: can `DynamicColumns::append_utf8` / `append_int64` return `ParseError::OutOfMemory`? F5 is a live
  mis-map only if that variant is reachable.
- `packages/columine/src`: comments claim `lib.ts` `DIAGNOSTIC_STAGES` / `DIAGNOSTIC_DETAILS` / `JSON_VALUE_TYPES`. Repo
  grep finds those names only in the rust comments. Is TS decoding the bytes un-named, or is the comment stale?
- `columine-event-processor/src/lib.rs`: `parse_json_events` vs `extract_json_events` are chosen by schema presence;
  this slice does not double-scan in one call. Confirm no other caller invokes both on the same buffer.

## Non-findings (checked, clean)

- Three modules are not three JSON parsers. `scan.rs` is the byte classifier used by `JsonParser::parse_string` /
  `skip_whitespace`. `json_scanner` and `json_extractor` are alternative consumers of one `JsonParser`.
- `parse_timestamp_to_micros` delegates to `parse_iso8601_to_micros`; msgpack scanner/extractor import that one
  function. Timestamp grammar is SSOT.
- `Cargo.toml`: only `columine-arrow` plus `proptest` dev-dep. No serde_json, no extra parser/crypto/hash crates.
  HashMap for schema name lookup is load-bearing (O(1) per field, config-sized).
- Scalar `scan` kernels vs simd128: measured ~23% slower v128 at this crate's opt-level 3; re-measure condition is
  documented. Not a "just add SIMD" miss.
- No `unsafe`. Production `expect` is only the peek invariant. `unwrap` is test-only.
- `scan.rs` tests pin form-feed exclusion, past-end, and a naive-reference proptest (substitution can go red).
- Binary / `$extra` encoding writes into a caller workspace or a column reservation (the pattern Utf8 should copy).
  `columns.abandon_row` on extractor error is correct.
- `ExtractionConfig` presence decoding (`event_value_present.` + `%HH`) lives once in `lib.rs` and is shared.
