# lmao-macros

Scope: `packages/lmao/crates/lmao-macros/Cargo.toml` (19), `src/lib.rs` (396; the only source file — assignment said “6
files / 396 lines”; that count is the crate, not six modules), `tests/trybuild.rs` (9),
`tests/compile_fail/empty_schema.rs` (5) + `.stderr` (5), `tests/compile_fail/unknown_kind.rs` (7) + `.stderr` (5),
`tests/compile_fail/empty_enum.rs` (7) + `.stderr` (5), `tests/pass/full_dsl.rs` (15). Doctrine:
`BYPRODUCT-ENGINEERING.md`, `docs/handbook/04-mechanisms.md`, `05-memory-toolkit.md`, `02-measurement.md` §4.1. Targeted
greps only outside this crate (TS `S` builders, `lmao-core` columns/context/tuning, `lmao-arrow` dict heuristic).

## Summary

- `syn` `features = ["full"]` is compile-time bloat: the only `full`-gated type is `syn::Expr`, used as an opaque token
  forwarder in `span!`.
- `category` and `text` parse as distinct kinds then expand to identical `StrColumn` fields with no leftover
  discriminant; Arrow flush already needs that bit (`01a` text heuristic).
- `define_log_schema!` emits a second `SpanBuffer` lifecycle (`start`/`finish_ok`/`ratchet`) beside `TraceContext::span`
  / `span!` — two sources for one span.
- `span!` has zero tests; the only `pass` fixture cannot go red if generated writers/`start`/`finish_ok` are deleted.
- Magic `64` is hardcoded in both generated capacity paths; `lmao-core` publishes `MIN_CAPACITY`/`MAX_CAPACITY` but no
  default.
- Enum dictionaries emit unscoped `FIELD_VALUES` consts; generated `set_*` is `debug_assert` only and `get_*` indexes
  the dict unchecked.
- Generated `start`/`finish_ok` `Mutex::lock().unwrap()` on an operational poison path.
- `quote` / `proc-macro2` / `trybuild` earn their weight. Compile-time `to_string`/`format!` is once-per-schema, not a
  copies finding.

## Findings

### F1 — HIGH — SSOT — `category` vs `text` is parsed then discarded

Evidence: `packages/lmao/crates/lmao-macros/src/lib.rs:216-225`

```
FieldKind::Category => (
    quote!(::lmao_core::StrColumn),
    quote!(impl Into<::lmao_core::SharedStr>),
    "category string column — raw slot writes, dictionary at flush (`01a`)",
),
FieldKind::Text => (
    quote!(::lmao_core::StrColumn),
    quote!(impl Into<::lmao_core::SharedStr>),
    "text string column — raw slot writes, 2-pass encode at flush (`01a`)",
),
```

The expand at `lib.rs:262-325` emits struct fields, writers, and `attribute_bytes` only — no kind table, no marker type,
no `const` discriminant. Neighbor (not owned): `lmao-arrow/src/dict.rs:464-465` already encodes the `01a` split (`text`
dictionary-encodes only if savings >128; `category`/`enum` always). Problem: Byproduct L0 — the parse paid for the kind
and threw it away. After expansion the two fields are indistinguishable, so flush cannot apply the heuristic the docs
claim. Fix: Keep the kind in the generated artifact. Decision: emit `pub const FIELD_META: &[(&str, FieldStrat)]` (or
newtype `Category(StrColumn)` / `Text(StrColumn)`) from the same `FieldKind` match; `lmao-arrow` reads that, not column
rustc type. `FieldKind` in `lib.rs:49-56` is the single source. Cost/Risk: generated public surface grows; `lmao-arrow`
`SpanSource` (currently “implement later”, `source.rs:5-7`) must consume the meta. No TS change.

### F2 — HIGH — DEP-BLOAT — `syn` `full` exists only to parse `Expr` that is never inspected

Evidence: `packages/lmao/crates/lmao-macros/Cargo.toml:13`

```
syn = { version = "2", features = ["full"] }
```

`packages/lmao/crates/lmao-macros/src/lib.rs:42-47,345-393` — every syn item actually named:

- `derive`+`parsing` subset: `Parse`, `ParseStream`, `Punctuated`, `Ident`, `LitStr`, `Token`, `Visibility`, `braced!`,
  `bracketed!`, `Error`, `syn::parse`
- `full` only: `syn::Expr` for `trace` / `parent` / `body` in `span!` (`lib.rs:346-363`), immediately re-emitted as
  `#trace` / `#p` / `#body` (`lib.rs:389-393`) with no match on the tree `features = ["full"]` does not set
  `default-features = false`, so the crate also builds default `derive`+`clone-impls` on top of `full`. Problem: `full`
  is the complete Rust AST. A 396-line proc-macro that only forwards tokens does not need it. Compile-time regime
  (proc-macro crate build, every consumer compile) — this is the cost that matters here, not a runtime hot loop. Fix:
  Rewrite `span!` as `macro_rules!` in `lmao-core` (two rules: `($trace, $name, $body)` and
  `($trace, $parent, $name, $body)`), which is the whole expansion, then drop `full`. Remaining syn for
  `define_log_schema!`:

```
syn = { version = "2", default-features = false, features = ["derive", "parsing", "printing", "clone-impls", "proc-macro"] }
```

If `span!` must stay a proc-macro, parse with `ParseStream` token-trees until comma instead of `syn::Expr` and use the
same feature list without `full`. Do not keep `full` “just in case”. `quote` and `proc-macro2` stay — load-bearing
(`quote!`/`format_ident!`, `Span::call_site`). `trybuild` is a dev-dep and does not ship. Cost/Risk: `span!` moves to
`lmao-core` (or stays here as `macro_rules!` re-export). Hygiene of `file!()`/`line!()` must remain at the call site —
`macro_rules!` does that. Consumer compile of `lmao-macros` shrinks.

### F3 — HIGH — DUPLICATION — generated schema buffer is a second span lifecycle

Evidence: `packages/lmao/crates/lmao-macros/src/lib.rs:284-316` (generated `start`/`finish_ok` allocate
`SpanBuffer::start_dynamic`, lock a per-schema ratchet, `end_ok`, `record_span`) vs `lib.rs:389-393` (`span!` forwards
to `(#trace).span(#name, #parent_expr, 64, …)` + `set_callsite`). Neighbor confirmation (not owned):
`lmao-core/src/context.rs:74-87` already owns span start/finish; `lmao-core/examples/jcode_tracer.rs:57-89` runs
**both** `ToolCallSchema::start` and `trace.span_with_retry` for one tool-call and comments the fusion as “the next
macro iteration”. Problem: two sources for the same span/log shape. `span!` does **not** reimplement the log shape — it
is a callsite injector over `TraceContext::span`, which runtime cannot do (`file!()`/`line!()`). `define_log_schema!`
**does** reimplement construction/completion/capacity around a second `SpanBuffer`. Attribute columns should attach to
the buffer `TraceContext::span` already creates. Fix: generate only typed column fields + `tag_*`/`set_*`/`get_*` (and
the per-schema `OnceLock` ratchet) on a wrapper that borrows or owns the **same** `SpanBuffer` the context finishes.
Delete generated `start`/`finish_ok` once `SpanContext` is parameterized by schema. `span!` stays the callsite injector;
it should take ratchet capacity, not a second literal `64`. Cost/Risk: `jcode_tracer.rs` and
`lmao-core/benches/hot_path.rs:214-222` call `Schema::start` today — they move with the cutover. No shim.

### F4 — HIGH — TESTS — `span!` untested; pass fixture cannot go red for generated API

Evidence: `packages/lmao/crates/lmao-macros/tests/trybuild.rs:4-8`

```
fn compile_fail_cases() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/*.rs");
    t.pass("tests/pass/*.rs");
}
```

`tests/pass/full_dsl.rs:12-15`:

```
fn main() {
    // Dictionary is a compile-time const.
    assert_eq!(METHOD_VALUES, &["GET", "POST"]);
}
```

No `tests/pass` (or compile_fail) file mentions `span!`. Deleting `tag_*`/`set_*`/`start`/`finish_ok`/`attribute_bytes`
from the expander still compiles `full_dsl.rs`. The `impl` body is type-checked if present; its **absence** is not.
Problem: PERFORMANCE-HANDBOOK §7.10bb — a guard that cannot go red is not a guard. The three `compile_fail` stderr pins
**can** go red (see Non-findings). The pass fixture and `span!` cannot. Fix: (1) `tests/pass/full_dsl.rs` must construct
`FullSchema::start`, call one writer per kind (including `category` vs `text` if F1 lands), `get_*`, `finish_ok`, and
`attribute_bytes`. (2) Add `tests/pass/span_callsite.rs` that invokes `span!` both arities. (3) Add `compile_fail` for a
non-callable `span!` body and a trailing-junk input if those stay as parse errors. Cost/Risk: pass tests need
`lmao-core` (already a dev-dep). trybuild will compile a `TraceContext` stub or the real type.

### F5 — MEDIUM — SSOT — initial capacity `64` restated, not named

Evidence: `packages/lmao/crates/lmao-macros/src/lib.rs:280` `CapacityRatchet::new(64)` and `lib.rs:390`
`(#trace).span(#name, #parent_expr, 64, …)`. Neighbor: `lmao-core/src/tuning.rs:15-16` publishes `MIN_CAPACITY = 8` and
`MAX_CAPACITY = 1024`; `CapacityRatchet::new` takes an un-named initial. No `DEFAULT_CAPACITY`. Problem: two copies in
this crate already disagree in **behavior** (ratchet learns; `span!` never does) while sharing the same literal. A third
copy lives in every caller (`jcode_tracer.rs:74`, benches). Fix: `lmao-core` owns
`pub const DEFAULT_CAPACITY: usize = 64;` next to MIN/MAX. Both quote sites use `::lmao_core::DEFAULT_CAPACITY`. `span!`
should read the schema ratchet when a schema exists (F3); until then the named const is the single source for the cold
start. Cost/Risk: one new public const; all `64` capacity call sites in core/macros move.

### F6 — MEDIUM — SSOT — enum dictionaries are unscoped `FIELD_VALUES` consts

Evidence: `packages/lmao/crates/lmao-macros/src/lib.rs:164-173`

```
let dict_name = format_ident!(
    "{}_VALUES",
    fname.to_string().to_uppercase(),
    span = Span::call_site()
);
...
#vis const #dict_name: &[&str] = &[#(#lits),*];
```

Pinned as the contract by `tests/pass/full_dsl.rs:14` (`METHOD_VALUES`, not `FullSchema::METHOD_VALUES`). Same pattern
in `lmao-core/examples/jcode_tracer.rs:141` (`OUTCOME_VALUES`). Problem: two schemas in one module with an enum field of
the same name fail to compile (`duplicate definition`). The dictionary is schema data living at module scope.
`Span::call_site()` also detaches the ident from the field span. Fix: emit
`impl #name { pub const #dict_name: &[&str] = … }` (or `#name_#dict_name`). Update `full_dsl.rs` and the example. Use
the field ident’s span. Cost/Risk: public const path changes; the pass test and example are the only in-tree readers
found.

### F7 — MEDIUM — STRUCTURE — generated API panics on mutex poison

Evidence: `packages/lmao/crates/lmao-macros/src/lib.rs:291` and `lib.rs:312-315`

```
let capacity = Self::ratchet().lock().unwrap().capacity();
...
Self::ratchet()
    .lock()
    .unwrap()
    .record_span(self.span.write_index().saturating_sub(2) as u64);
```

Problem: poison is an operational failure (another thread panicked in the ratchet). Doctrine: `Result` for operational
failure; panic only for invariants. This is generated into every schema’s `start`/`finish_ok`. Fix:
`lock().unwrap_or_else(|p| p.into_inner())` if the policy is “recover the last capacity”, or return `Result` from
`start`/`finish_ok`. Decision: recover via `into_inner` — capacity is a hint, not a safety invariant, and changing the
generated signature is a larger cut. Cost/Risk: panic-vs-continue policy must be one sentence in the crate docs. No
caller change if `into_inner`.

### F8 — MEDIUM — STRUCTURE — enum `get_*` indexes the dictionary unchecked

Evidence: `packages/lmao/crates/lmao-macros/src/lib.rs:181-190`

```
pub fn set_fn(&mut self, row: usize, index: u16) -> &mut Self {
    debug_assert!(index < #n);
    ...
}
pub fn get_fn(&self, row: usize) -> Option<&'static str> {
    self.#fname.get(row).map(|i| #dict_name[i as usize])
}
```

Problem: release `set_*` will store any `u16`; `get_*` then does `#dict_name[i as usize]` and panics on OOB. That is an
operational input error, not an invariant of the column type (`EnumColumn = NumColumn<u16>`). Fix: `set_*` returns
`Result` (or clamps with a debug-only assert **and** `get_*` uses `#dict_name.get(i as usize).copied()`). Decision:
checked get (`Option<&'static str>` already) so a bad index is `None`, not a panic; keep `debug_assert` on set as a
programmer tripwire. Cost/Risk: `None` now means “null row” **or** “corrupt index”. If those must differ, return
`Result`. Callers of `get_*` already see `Option`.

### F9 — LOW — SSOT — field-kind list restated five times

Evidence:

- enum: `lib.rs:49-56`
- parse match: `lib.rs:81-86`
- error string: `lib.rs:108-111`
- crate docs: `lib.rs:22-28`
- `tests/compile_fail/unknown_kind.stderr:1` Problem: adding `binary`/`unknown` (already named in the error as “not
  supported yet”) requires five edits; the stderr pin only catches the error string, not the match. Fix: one
  `&[(&str, FieldKind)]` or a macro that drives the parse match **and** the error list. Docs and stderr derive from that
  list (or the stderr test prints the function that formats the error). Cost/Risk: small; `unknown_kind.stderr` rewrites
  once.

### F10 — LOW — STRUCTURE — `define_log_schema` is 183 lines with an obvious seam

Evidence: `packages/lmao/crates/lmao-macros/src/lib.rs:145-328` (`pub fn define_log_schema`). Enum arm `163-196` vs
scalar arm `199-260` duplicate `tag_`/`set_`/`get_`/`col_fields`/`col_inits`/`bytes_terms` pushes. Fix: one
`fn expand_field(f: &Field) -> FieldTokens` returning those six vecs’ items; `define_log_schema` only parses and
`quote!`s the struct. Not a 5k-line god file; this is the only expand function. Cost/Risk: none.

## Cross-slice questions

- **LmaoCore** (`packages/lmao/crates/lmao-core/src/columns.rs:143-147`, `context.rs:74-87`, `tuning.rs:15-16`): should
  `DEFAULT_CAPACITY` live next to MIN/MAX? Is `EnumColumn = NumColumn<u16>` the intended width vs TS `S.enum` =
  Uint8Array / 1 byte (`packages/lmao/src/lib/schema/builder.ts:125-127`)? Rust `uint64` user-field kind has no
  `S.uint64()` on the TS builder (`builder.ts:79-198` exposes number/boolean/enum/category/text/binary/unknown/object;
  `bigUint64` is arrow-builder / `systemSchema.uint64_value`). Empty schema is legal in TS (`defineLogSchema({})` in
  `timestamps.bench.ts`) and rejected here (`lib.rs:128-132`) — which is SSOT?
- **ColArrow** (`packages/lmao/crates/lmao-arrow/src/dict.rs:464-465`, `source.rs:3-7`): when macro-generated buffers
  implement `SpanSource`, what typed handle will carry the category/text/enum discriminant F1 says this crate must emit?
- **LmaoCore example/bench** (`examples/jcode_tracer.rs`, `benches/hot_path.rs`): they are the only in-tree
  `define_log_schema!` consumers; F3/F6 change their call sites. `span!` is unused in-tree.

## Non-findings (checked, clean)

- **`span!` does not duplicate the runtime log shape.** It only injects `set_callsite(file!(), line!())` and forwards to
  `TraceContext::span`. That injection cannot be a runtime helper.
- **trybuild `compile_fail` stderr pins can go red.** `empty_schema.stderr`, `unknown_kind.stderr`, `empty_enum.stderr`
  match the `syn::Error` strings at `lib.rs:131`, `108-111`, `97`. Removing the check makes the fixture compile and
  trybuild fails; changing the message mismatches stderr. §7.10bb holds for those three.
- **`quote` / `proc-macro2`**: load-bearing for a proc-macro. Not shell-out candidates.
- **`trybuild`**: dev-dep only; does not leak into the shipped proc-macro dylib. `Cargo.lock` has a single
  `syn 2.0.118`.
- **COPIES / regime:** `Ident::to_string`, `LitStr::value` → `Vec<String>`, `format!` for diagnostics, `to_uppercase`
  for dict names run once per schema at **compile** of the consumer. Not a runtime finding (§4.1: name the regime
  first). Generated `tag_*`/`set_*` are `#[inline]` field stores, not `to_vec`/`clone` on the hot path.
- **unsafe / TODO / FIXME / `cfg(target_os)`**: none in this crate. `unreachable!()` at `lib.rs:226` is after `continue`
  on `FieldKind::Enum` — invariant.
- **God file:** 396 lines, one module. Below the 5k–10k bar.
- **Dead `pub` surface:** both proc-macros are the crate’s product. `span!` is unused in-tree but is documented public
  API, not dead code inside the crate.
