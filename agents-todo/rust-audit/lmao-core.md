# lmao-core

Scope: `packages/lmao/crates/lmao-core/Cargo.toml` (26), `src/lib.rs` (34), `src/entry_type.rs` (82),
`src/packed_header.rs` (122), `src/identity.rs` (124), `src/clock.rs` (191), `src/columns.rs` (217), `src/buffer.rs`
(289), `src/context.rs` (310), `src/result.rs` (102), `src/tuning.rs` (95) — 10 files, 1566 lines. Also read (TESTS axis
/ benches / example): `tests/alloc_gate.rs` (142), `tests/properties.rs` (238), `benches/overhead.rs` (49),
`benches/hot_path.rs` (273), `examples/jcode_tracer.rs` (143). Doctrine: BYPRODUCT-ENGINEERING.md, PERFORMANCE-HANDBOOK
§4.1/§4.2/§4.2b, §7.10bb, §7.1–7.2, §7.12. Targeted greps: `packages/lmao/src/lib/schema/systemSchema.ts`,
`packages/lmao/src/lib/capacityTuning.ts`, `packages/lmao/src/lib/types.ts`,
`packages/lmao/src/lib/physicalLayoutPlan.ts`, `packages/lmao/crates/lmao-arrow/src/convert.rs`,
`packages/lmao/crates/lmao-arena/src/raw.rs`, `packages/lmao/Cargo.lock` `lmao-core` stanza, `AGENTS.md` entry-type
section.

## Summary

- HIGH SSOT: `EntryType` 1..=24 is a hand restatement of TS `ENTRY_TYPE_NAMES` / `ENTRY_TYPE_*`; AGENTS.md already
  forbids this. Numbers currently agree; no generator exists.
- HIGH SSOT: capacity ratchet is a second copy of `capacityTuning.ts` and has already diverged (`MIN_SPANS_SAMPLE` 16 vs
  `MIN_SPANS_FOR_TUNING` 10).
- HIGH TESTS: `overhead.rs` documents 20% throughput / 25% RSS gates and benches only `SpanBuffer::start_dynamic`. The
  claimed cell does not exist; it cannot go red.
- MEDIUM SSOT: packed-header high-24 lane is `VocabularyId` as-is here vs TS `denseIndex+1`.
- MEDIUM TESTS: `hot_path.rs` claims a pooled cell that is absent; `tag_write_f64_proxy` does not call `NumColumn`;
  append cells pass `message: None`.
- MEDIUM COPIES: `SpanContext::start` forces `SharedStr::Owned(name.into())` on every span-start write.
- Warmed `append_dynamic` / `SpanContext::log(Static)` write path has no `String`/`Vec`/`format!`.
  Zero-alloc-after-warmup holds for that lane only.
- Shipped crate has zero `[dependencies]` (lockfile lists only criterion / lmao-macros / proptest / tokio).
- Intra-crate `8..=1024` in `buffer.rs` restates `MIN_CAPACITY`/`MAX_CAPACITY` instead of using them.
- `dyn Clock` + double `append_target` walk sit on every event; regime is hot, cost is a vtable / pointer chase under a
  syscall-dominated clock.

## Findings

### F1 — HIGH — SSOT — EntryType 1..=24 is a hand copy of the TS table

Evidence: `packages/lmao/crates/lmao-core/src/entry_type.rs:1-38`

```
//! The 24 entry types, aligned exactly with the TypeScript runtime mapping.
#[repr(u8)]
pub enum EntryType {
    SpanStart = 1,
    SpanOk = 2,
    SpanErr = 3,
    SpanException = 4,
    SpanRetry = 5,
    Trace = 6,
    // ... through BufferCapacity = 24
}
pub const COUNT: usize = 24;
```

`packages/lmao/src/lib/schema/systemSchema.ts:314-340` (`ENTRY_TYPE_NAMES` with unused index 0, kebab-case
`'span-start'` … `'buffer-capacity'`). `AGENTS.md:788-791`: "`ENTRY_TYPE_NAMES` in `schema/systemSchema.ts` is
authoritative — index it rather than restating these numbers elsewhere." Tests only pin 1..=4 (`entry_type.rs:75-81`);
`from_u8` roundtrip (`:66-72`) would still pass if two mid-range discriminants swapped. Problem: one ABI, three (plus)
restatements. Discriminants currently match TS, so this is not yet a live numeric bug, but the copy is exactly what
AGENTS.md bans. `lmao-arena` and `lmao-arrow` restate it again (cross-slice). Fix: generate `EntryType` from
`ENTRY_TYPE_NAMES` (skip index 0). Generation direction: **TS `systemSchema.ts` → Rust `EntryType`** per AGENTS.md.
Decision I would take instead: lift the 24 kebab names into a single machine table under `specs/lmao/` and generate TS
constants, this enum, and `lmao-arrow::ENTRY_TYPE_NAMES` from it, so TS is an output not an authority. Delete the hand
enum. Cost/Risk: every match on `EntryType`, WASM/TS ABI, Arrow dictionary keys (`entry_type - 1` in lmao-arrow). One
generator, then delete copies.

### F2 — HIGH — SSOT — Capacity ratchet copied from TS; sample threshold already diverged

Evidence: `packages/lmao/crates/lmao-core/src/tuning.rs:8-21,50-58`

```
//! grow  ×2 when utilization > 1.5
//! shrink ÷2 when utilization < 0.5
//! bounded [MIN_CAPACITY = 8, MAX_CAPACITY = 1024]
pub const MIN_CAPACITY: usize = 8;
pub const MAX_CAPACITY: usize = 1024;
const GROW_THRESHOLD: f64 = 1.5;
const SHRINK_THRESHOLD: f64 = 0.5;
const MIN_SPANS_SAMPLE: u64 = 16;
...
if self.spans_created < MIN_SPANS_SAMPLE { return; }
let usable = (self.capacity - 2) as f64;
```

`packages/lmao/src/lib/capacityTuning.ts:30-43,84-89`:

```
const MIN_SPANS_FOR_TUNING = 10;
const GROW_THRESHOLD = 1.5;
const SHRINK_THRESHOLD = 0.5;
const MIN_CAPACITY = 8;
const MAX_CAPACITY = 1024;
if (stats.spansCreated < MIN_SPANS_FOR_TUNING) return;
const usableRowsPerSpan = stats.capacity - 2;
```

Problem: same utilization formula, same grow/shrink/bounds, two sources. **Live divergence:** Rust waits 16 finished
spans before tuning, TS waits 10. Spec `01b2` does not name either constant (grep: no `MIN_SPANS`). Copies no longer
agree. Fix: one table (spec 01b2, machine-readable) generates both. Pick 10 or 16 explicitly; I would take **10** (TS
shipped behavior, smaller sample = earlier reaction) unless a measured reason for 16 exists in-tree — I found none.
Cost/Risk: any host that assumed identical JS/Rust overflow behavior. `properties.rs` ratchet tests stay valid either
way.

### F3 — HIGH — TESTS — overhead.rs cannot measure the gates it claims

Evidence: `packages/lmao/crates/lmao-core/benches/overhead.rs:1-8,26-45`

```
//! Placeholder bench harness for the overhead gates
//! - enabling tracing must not cut median throughput by >20% at a 10^6-event run
//! - peak RSS increase ≤25%
//! Once the tracer facade exists, add a traced-vs-untraced pair ...
//! For now this benches buffer creation so the harness wiring is proven.
c.bench_function("span_start_cap64", |b| {
    b.iter(|| {
        black_box(SpanBuffer::start_dynamic(
            identity.clone(), 64, "span".into(), &anchor, &clock,
        ))
    })
});
```

Problem: §4.2b — a cell that does not run the claimed work measures nothing about it. The 20%/25% gates have no
traced-vs-untraced pair, no 10^6-event run, no RSS sample. Substitution test (§7.10bb): delete tracing entirely, this
function still times buffer construction and stays green. The cell _does_ allocate (three `vec!` in `start_with_header`)
so it is not a folded no-op; it is the wrong kernel. Fix: replace with a same-binary traced vs untraced pair over a
10^6-event workload, oracle on event count (not `is_ok` / empty), plus an RSS delta. Until that exists, delete the gate
comments so they cannot be quoted as measured. Cost/Risk: none to production code. Any published "overhead" number from
this binary is invalid.

### F4 — MEDIUM — SSOT — Packed header high-24: VocabularyId vs TS denseIndex+1

Evidence: `packages/lmao/crates/lmao-core/src/packed_header.rs:1-10,88-96`

```
//! Packed native row headers: low 8 bits are [`EntryType`], high 24 bits are a
//! manifest-global vocabulary ID.
pub const MAX_VOCABULARY_ID: u32 = 0x00ff_ffff;
...
Ok((vocabulary_id.get() << VOCABULARY_SHIFT) | entry_type.as_u8() as u32)
```

`packages/lmao/src/lib/types.ts:121-122`: "Packed row headers: low 8 bits entry type, high 24 bits dense message index
plus one." `packages/lmao/src/lib/physicalLayoutPlan.ts:388-389`:
`headers[0] = (((name + 1) << 8) | ENTRY_TYPE_SPAN_START)`. Problem: same 8+24 word, two semantics. Rust stores a
nonzero global `VocabularyId` directly (0 = dynamic). TS packed stores `denseIndex + 1`. 0-as-sentinel agrees; the
payload does not, unless every caller happens to pass a 1-based id. `MAX_VOCABULARY_ID = 0x00ff_ffff` is also restated
in `packages/lmao/src/lib/vocabularyRegistry.ts:6`. `supports_static_vocabulary` (`packed_header.rs:101-110`:
SpanStart|Trace|Debug|Info|Warn|Error) is restated in TS `convertToArrow.ts` as `SPAN_START` or `TRACE..=ERROR`. Fix:
document the native encoding next to the intentional JS-layout deviation in `columns.rs:14-18`, or pack `denseIndex+1`
here too. Single `MAX_VOCABULARY_ID` / static-kind table shared with TS. I would keep global IDs (no local dense table
on the hot path) and make TS packed resolve through the same id space at the ABI boundary. Cost/Risk: `lmao-arrow`
`split_packed_header` already follows Rust (`header >> 8` as id). Changing either side without the other corrupts
messages.

### F5 — MEDIUM — TESTS — hot_path.rs cells miss the production kernel

Evidence: `packages/lmao/crates/lmao-core/benches/hot_path.rs:7-11,100-114,125-137`

```
//! - "Warm: Trace with tags" -> tag_write_f64_proxy ...
//! - "Memory reuse (100 traces)" -> pooled variant (Vec allocations reused)
fn bench_append_only(...) {
    s.append_dynamic(EntryType::Info, None, 0, &anchor, &fixed); // ×1000
}
fn bench_tag_write_proxy(...) {
    let mut bitmap = [0u8; 8];
    let mut values = vec![0f64; 64];
    bitmap[r >> 3] |= 1 << (r & 7);
    values[r] = black_box(42.5);
}
```

`criterion_group!` (`:140-151`) has no pooled function. `bench_schema_tag_write` (`:211-243`) _does_ hit generated
columns; the proxy does not. Problem: §4.2 / §7.10bb. (1) Comment claims a pooled warm analogue that is not in the
binary. (2) `tag_write_f64_proxy` substitution: break `NumColumn::set`, this cell stays green. (3) `append_1000` /
`span_plus_50_logs` pass `message: None`, so they do not exercise the `StrColumn` first-touch or `SharedStr::Static`
store that `alloc_gate.rs:116-135` treats as the real hot path. `bench_dictionary_build` (`:260-272`) times a
`HashMap<&str,u32>` that is not in this crate. Fix: delete the proxy (superseded by `schema_tag_write_*`). Add the
pooled cell or delete the comment. Point append cells at `append_dynamic(..., Some(SharedStr::Static("…")), ...)`. Move
dict-build to `lmao-arrow` or drop it. Cost/Risk: bench-only. `alloc_gate.rs` remains the alloc oracle.

### F6 — MEDIUM — COPIES — SpanContext::start Arc-allocates the span name on every start write

Evidence: `packages/lmao/crates/lmao-core/src/context.rs:132-144`

```
pub fn start(..., name: &str) -> Self {
    let buf = SpanBuffer::start_dynamic(
        identity, capacity,
        SharedStr::Owned(name.into()),
        &trace.anchor, trace.clock(),
    );
```

`columns.rs:41-45` already has `From<&'static str> for SharedStr` (`Static`, zero alloc). `TraceContext::span`
(`context.rs:74-80`) takes `name: &str` and always goes through `start`. Problem: regime = once per span, not per log.
Still a write-path heap alloc on the public executor. `name.into()` as `&str` → `Arc<str>` copies bytes even when the
caller has a `'static` op name. Warmed _log_ path (`context.rs:156-171`, `SharedStr::Static(template)`) does not
allocate — verified by reading `append_dynamic`/`write_row`. Fix: `start`/`span` take `impl Into<SharedStr>` (or
`&'static str` + an `Owned` overload). Macros pass `file!()`-class statics as `Static`. Cost/Risk: all
`span`/`child`/`span_with_retry` call sites, the example, context tests. Greenfield: change the signature, no shim.

### F7 — MEDIUM — DUPLICATION — buffer.rs restates capacity bounds as literals

Evidence: `packages/lmao/crates/lmao-core/src/buffer.rs:97`

```
debug_assert!(capacity.is_power_of_two() && (8..=1024).contains(&capacity));
```

vs `tuning.rs:15-16`: `MIN_CAPACITY = 8`, `MAX_CAPACITY = 1024`. Problem: intra-crate second copy of the same closed
interval. Release builds do not even check (`debug_assert`), so a generated buffer with cap 4 or 2048 writes past the
intended ratchet range with no signal. If F2 changes 1024, this literal will rot. Fix:
`use crate::tuning::{MAX_CAPACITY, MIN_CAPACITY}` and assert (or `debug_assert`) with those symbols. Decision: keep
`debug_assert` only if macros are the sole caller and are proven to pass ratchet capacity; otherwise fail at admission
(`Result` / panic-as-invariant). Cost/Risk: `SpanBuffer::start_*` callers. Tiny.

### F8 — LOW — COPIES — per-event `dyn Clock` and double overflow walk

Evidence: `packages/lmao/crates/lmao-core/src/buffer.rs:169-175,197-216,228-243`

```
let row = self.append_header(pack_dynamic(entry_type), anchor, clock);
let target = self.append_target(); // second walk
...
fn append_header(...) {
    let target = self.append_target(); // first walk
    ...
}
fn write_row(..., clock: &dyn Clock) {
    self.timestamps[row] = anchor.timestamp(clock); // vtable
}
```

`clock.rs:74-77`: `timestamp` takes `&dyn Clock`. Problem: regime = every log/span event. Two overflow-chain walks per
`append_dynamic` even when `overflow` is `None` (one `is_some`). One vtable call per stamp. Under `SystemClock` the
`Instant::now` syscall dominates (~comment at `clock.rs:81-84`); under `FixedClock` the vtable is a larger fraction. Not
a `String`/`Vec`/`format!` on the write path. Fix: `append_dynamic` keep the `&mut SpanBuffer` from the first
`append_target`. Monomorphize `write_row`/`timestamp` over `C: Clock` at the `SpanContext` boundary; keep `dyn Clock`
only inside `TraceContext` storage. Cost/Risk: public `append_*` signatures grow a type param or an inner generic. Worth
it only after a same-binary A/B against `FixedClock` (§4.1: measure under bench profile, not opt-z).

### F9 — LOW — TESTS — several guards cannot fail on the thing they name

Evidence:

- `entry_type.rs:75-81` pins only discriminants 1..=4; 5..=24 unpinned.
- `packed_header.rs` has no `#[cfg(test)]` module (file ends at line 122).
- `tests/properties.rs:116-129` asserts `num.allocated_bytes()` stability after first touch and never asserts the same
  for `s` (`StrColumn`) after `s.set`.
- `tests/alloc_gate.rs:67-69` first gate uses `append_dynamic(..., None, ...)`; a new alloc inside `StrColumn::set`
  would not turn it red. The second test (`:116-135`) does cover Static + tag columns. Problem: §7.10bb / §4.2b oracles.
  Substitution of a swapped `OpErrors`/`OpInvocations` discriminant, a broken `pack_static` shift, or a `StrColumn`
  realloc all stay green in the named tests. Fix: table-driven equality of all 24 `as_u8()` values against the generated
  SSOT; pack/unpack tests for 0 / 1 / `MAX_VOCABULARY_ID` / rejected kinds; assert `s.allocated_bytes()` is constant
  after first touch; keep the second alloc_gate as the hot-path oracle (or drop the first). Cost/Risk: tests only.

### F10 — LOW — DEP-BLOAT — tokio pulled with default features for one example

Evidence: `packages/lmao/crates/lmao-core/Cargo.toml:9-15`

```
[dependencies]

[dev-dependencies]
proptest = { workspace = true }
criterion = { workspace = true }
lmao-macros = { path = "../lmao-macros" }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time", "sync"] }
```

`packages/lmao/Cargo.lock:1729-1737`: `lmao-core` depends on criterion, lmao-macros, proptest, tokio only. Problem:
shipped lib is zero-dep — correct, and those four dev-deps earn their weight (property tests, benches, schema
bench/example, async identity demo). Tokio is load-bearing for `jcode_tracer.rs`; do not shell out. Missing
`default-features = false` still pulls the default extra crates into the example graph (net/fs/io-util depending on
tokio version). Fix:
`tokio = { version = "1", default-features = false, features = ["rt-multi-thread", "macros", "time", "sync"] }`.
Cost/Risk: example-only compile. Confirm the example still builds with that set.

## Cross-slice questions

- `packages/lmao/crates/lmao-arena/src/raw.rs:86-89` restates `ENTRY_TYPE_SPAN_START..=EXCEPTION` as `u8` constants.
  Should that crate depend on `lmao-core::EntryType` or is the WASM arena forbidden from that dep? (arena slice)
- `packages/lmao/crates/lmao-arrow/src/convert.rs:21-46` restates kebab `ENTRY_TYPE_NAMES` as `[&str; 24]` (no unused
  index 0) and indexes with `entry_type - 1` (`:253`). TS indexes by discriminant. Confirm which dictionary key space
  Arrow flush must emit. (arrow slice)
- `lmao-arrow` `split_packed_header` treats high-24 as a global vocabulary id (Rust encoding). TS packed writes
  `denseIndex+1`. Which word is the interop ABI? (arrow / TS layout slices)
- `packages/lmao/crates/lmao-timestamp-proof/src/layout.rs:9-12` restates 1..=4 again. (timestamp-proof slice)
- TS system columns `error_code`, `retry_attempt`, `retry_delay_ms`, `exception_stack`, `ff_value`, `uint64_value`
  (`systemSchema.ts:54-63,80-144`) do not exist on `lmao-core::SpanBuffer` (only `timestamps`, `headers`,
  `line_numbers`, `messages`). Does `lmao-macros` generate them? This crate has no snake_case column-name table. (macros
  slice)
- `lmao-arena` `MAX_CAPACITY = 512` vs this crate’s 1024 is documented as intentional in arena. Not a bug here; do not
  “unify” without reading that comment.

## Non-findings (checked, clean)

- **Zero-alloc warmed log write:** `write_row` / `SpanContext::log` with `SharedStr::Static` /
  `append_dynamic(..., None|Static, ...)` contain no `String`/`Vec`/`format!`/`to_owned`. Overflow chaining allocates by
  contract (`buffer.rs:16-18`, `alloc_gate.rs:37-38`). `start_with_header`’s three `vec![0; capacity]` are warmup, not a
  hot-path finding.
- **DEP-BLOAT (shipped):** `[dependencies]` empty; lockfile matches. No git2/openssl/sqlite/napi/rand. `Entropy` is a
  seam — omitting `getrandom` is correct. `criterion`/`proptest`/`lmao-macros` belong in dev-deps.
- **Column-name string table:** this crate does not restate TS `SYSTEM_SCHEMA_FIELD_NAMES` /
  `RESERVED_SYSTEM_COLUMN_NAMES`. No SSOT copy of `timestamp`/`entry_type`/… as strings here.
- **Entry-type numeric values 1..=24** currently match TS (read both tables). Divergence is the missing generator (F1),
  not a wrong number today.
- **`supports_static_vocabulary` set** matches TS `SPAN_START | TRACE..=ERROR` (read both).
- **§4.1 profile trap:** benches use `harness = false` + criterion (`Cargo.toml:20-26`); `cargo bench` is the bench
  profile, not `[profile.release]` opt-z. No standalone `--release` probe in this crate.
- **STRUCTURE size:** largest file 310 lines (`context.rs`); no function over ~100 lines; no god file.
- **`unsafe`:** `EntryType::from_u8` transmute has a SAFETY comment citing contiguous `repr(u8)` 1..=24
  (`entry_type.rs:51-54`). `alloc_gate` `GlobalAlloc` is test-only.
- **Panics:** `CapacityRatchet::new` / `CoarseClock::new` `assert!` are constructor invariants. `pack_static` in
  `start_static` `expect`s a type that `supports_static_vocabulary` already admits. Operational failures use `Result`
  (`TraceId::new`, `VocabularyId`, `append_static`).
- **`format!`:** only `TraceId::generate` (once per trace, `identity.rs:57`) and error `Display` impls — not the event
  write path.
- **False-sharing / SoA:** eager columns are parallel `Vec`s (SoA). `CoarseClock`’s two atomics are unpadded; opt-in,
  not production default — not raised.
- **jcode_tracer.rs:** `format!` for session/tool names is example setup, not the crate hot path.
