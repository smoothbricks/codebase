# lmao-timestamp-proof

Scope: `packages/lmao/crates/lmao-timestamp-proof/src/lib.rs` (234),
`packages/lmao/crates/lmao-timestamp-proof/src/layout.rs` (104), `packages/lmao/crates/lmao-timestamp-proof/Cargo.toml`
(26), `packages/lmao/crates/lmao-timestamp-proof/build.rs` (33). Targeted greps/reads (duplication only, not a full
audit of those slices): `packages/lmao/crates/lmao-arena/src/raw.rs`, `packages/lmao/crates/lmao-arena/src/lib.rs`,
`packages/lmao/crates/lmao-core/src/clock.rs`, `packages/lmao/crates/lmao-wasm/src/lib.rs`,
`packages/lmao/src/lib/schema/systemSchema.ts`, `packages/lmao/src/lib/traceRoot.{es,node,ts}.ts`,
`packages/lmao/src/lib/wasm/wasmTraceRoot.ts`, `specs/lmao/01b3_high_precision_timestamps.md`, `packages/lmao/justfile`,
`packages/lmao/Cargo.toml`, `packages/lmao/Cargo.lock`. Src is two files / 338 lines (task said three).

## Summary

- Timestamp math is a third copy of 01b3/arena/TS; the NAPI arm already disagrees with the WASM arm and with production
  Node.
- Span-start/end/log writes and `ENTRY_TYPE` 1–4 are restated from `lmao-arena::raw`; write-index placement has already
  diverged.
- Not a `lmao-core` module (wrong compile graph), but it does not earn a crate: NAPI was rejected by 01b3, the named
  harness file is not in tree, `proptest` is unused.
- WASM `buf_at` is unchecked `from_raw_parts_mut` with no SAFETY invariant.
- NAPI stamps lock a process-global `Mutex` and linear-search 64 anchors on every proof stamp.
- Tests cover only the synthetic layout; formula, WASM, and NAPI are untested.

## Findings

### F1 — HIGH — SSOT — WASM and NAPI timestamp formulas are copies, and they already disagree

Evidence: `packages/lmao/crates/lmao-timestamp-proof/src/lib.rs:41-54`

```
        let wall_clock = i64::from_le_bytes(
            root[TRACE_ROOT_WALL_CLOCK_OFFSET..TRACE_ROOT_WALL_CLOCK_OFFSET + 8]
                .try_into()
                .expect("trace root wall clock"),
        );
        let monotonic_ms = f64::from_le_bytes(
            root[TRACE_ROOT_MONOTONIC_OFFSET..TRACE_ROOT_MONOTONIC_OFFSET + 8]
                .try_into()
                .expect("trace root monotonic"),
        );
        let elapsed_ms = unsafe { performance_now() } - monotonic_ms;
        wall_clock + (elapsed_ms * 1_000_000.0) as i64
```

Evidence: `packages/lmao/crates/lmao-timestamp-proof/src/lib.rs:133-169`

```
    fn wall_clock_nanos() -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0)
    }
    // ...
        Ok(anchor.start_wall_clock_nanos + anchor.started.elapsed().as_nanos() as i64)
```

Problem: The WASM arm restates `lmao-arena::raw::timestamp_nanos` (`raw.rs:654-658`:
`wall + ((current_ms - mono) * 1_000_000.0) as i64`) and the 01b3 WASM layout (wall i64 at 0, monotonic f64 ms at 8).
The NAPI arm does **not** restate that: it captures `SystemTime::as_nanos()` (ns wall, not `Date.now()*1e6`) plus
`Instant::elapsed` (ns monotonic, not `performance.now()` ms). Production Node (`traceRoot.node.ts:40-44`) is
`Date.now()*1e6` plus `process.hrtime.bigint()` with a +1n uniqueness bump. Production ES (`traceRoot.es.ts:42-44`) is
`floor(deltaMs*1000)*1000n` with a +1000n bump. Spec 01b3:23-25 says native acceleration is WASM, not NAPI. Two
artifacts from this crate already cannot agree, and neither NAPI stamp matches the production Node stamp the proof would
need to certify. Regime: proof kernel (every stamp), not process startup. Fix: Delete this crate's formula. WASM proof
subject is `lmao-arena::raw::timestamp_nanos` via `lmao-wasm`. If a Node native arm is kept, it must be
`BigInt(Date.now())*1e6 + (hrtime_now - hrtime_anchor)` as in `traceRoot.node.ts`, not `SystemTime`+`Instant`. Do not
fold into `lmao-core::clock::TraceAnchor` (nanos monotonic, different ABI). Cost/Risk: Proof numbers change if anyone
was comparing `.wasm` vs `.node`. Production TS/arena unchanged if this crate stays proof-only.

### F2 — HIGH — DUPLICATION — Span lifecycle writes and entry-type bytes are a second Rust copy; write-index already diverged

Evidence: `packages/lmao/crates/lmao-timestamp-proof/src/layout.rs:9-12,32-59`

```
pub const ENTRY_TYPE_SPAN_START: u8 = 1;
pub const ENTRY_TYPE_SPAN_OK: u8 = 2;
pub const ENTRY_TYPE_SPAN_ERR: u8 = 3;
pub const ENTRY_TYPE_SPAN_EXCEPTION: u8 = 4;
pub fn write_span_start(buf: &mut [u8], capacity: u32, timestamp_nanos: i64) {
    write_timestamp(buf, 0, timestamp_nanos);
    write_entry_type(buf, capacity, 0, ENTRY_TYPE_SPAN_START);
    write_timestamp(buf, 1, 0);
    write_entry_type(buf, capacity, 1, ENTRY_TYPE_SPAN_EXCEPTION);
    let off = write_index_offset(capacity);
    buf[off..off + 4].copy_from_slice(&2u32.to_le_bytes());
}
pub fn buffer_len(capacity: u32) -> usize {
    capacity as usize * 9 + 4
}
```

Problem: Same four bytes live in `packages/lmao/src/lib/schema/systemSchema.ts:219-228` (SSOT for the TS schema / spec
01h) and `lmao-arena/src/raw.rs:86-89`. Values currently agree. The write sequence (row 0 start, row 1 pre-armed
exception at ts 0, write_index=2; end overwrites row 1; log appends at the index) is the same algorithm as
`raw.rs:765-811`. Divergence that already happened: production keeps write_index in identity (`raw.rs:779`,
`identity_ptr + ID_WRITE_INDEX`) and sizes span_system as `capacity * 9` (`lmao-arena/src/lib.rs:114`); this crate tacks
a u32 write-index onto the system buffer (`capacity*9+4`) and drops identity. A proof of that layout does not exercise
the production write path. `write_log_entry` also has no `row < capacity` check (`layout.rs:48-54`). Fix: Single source
for 1–4 is `systemSchema.ts` / spec 01h; Rust should have one const table in `lmao-arena::raw` and this crate should not
restate it. Delete `layout.rs` writes; the proof subject is `raw::{span_start,span_end,write_log_entry}`. If a stripped
buffer is required, keep write-index where production keeps it. Cost/Risk: Proof ABI
(`span_start(system, capacity, trace_root)` vs production `span_start(system, identity, trace_root, capacity)`) changes;
justfile artifact consumers must move with it.

### F3 — HIGH — DEP-BLOAT — Separate crate is not a `lmao-core` module, but NAPI + unused `proptest` do not earn the compile unit

Evidence: `packages/lmao/crates/lmao-timestamp-proof/Cargo.toml:10-26`

```
crate-type = ["cdylib", "rlib"]
napi = ["dep:napi", "dep:napi-derive", "dep:napi-build"]
napi = { version = "2", default-features = false, features = ["napi6"], optional = true }
napi-derive = { version = "2", optional = true }
napi-build = { version = "2", optional = true }
proptest = { workspace = true }
```

Evidence: `packages/lmao/crates/lmao-timestamp-proof/src/lib.rs:2-6`

```
//! This crate implements the proof machinery (measuring span-timestamp accuracy
//! for the proof harness `proofs/timestamp-accuracy.proof.ts`), not runtime code.
```

Problem: It must not become a module of `lmao-core`: core is std, `Clock`/`TraceAnchor` are in-process nanos
(`clock.rs:16-78`), and this crate is `no_std` wasm32 + `cdylib` + optional napi-rs (`lib.rs:1`, `Cargo.toml:10-16`,
`build.rs:13-31` shared-memory link args). That compile graph is the only honest argument for a crate. It fails the
weight test anyway. (1) 01b3:23-25 already measured NAPI slower than WASM and removed the addon from the runtime
package; this crate reintroduces napi 2.16.17 + napi-derive/syn + napi-sys + ctor + once_cell (`Cargo.lock`
`lmao-timestamp-proof` / `napi` entries) for `span_start.node`. Node can instantiate the WASM module. (2) `proptest` is
a dev-dep with zero uses in this crate. (3) `proofs/timestamp-accuracy.proof.ts` is not in the repo (grep/`glob` over
the tree: only this crate doc and `packages/lmao/justfile:33-49`). Workspace still compiles this member on every
`--workspace` check. Fix: Delete the crate and the `timestamp-proof-artifacts` recipe. Prove against `lmao-wasm`
(already exports `init_trace_root`/`span_start`/`span_end_*`/`write_log_entry`) and
`traceRoot.node.ts`/`traceRoot.es.ts`. If a shared-memory stripped wasm is still required, gate it as a `lmao-wasm`
binary, not a new package. Do not add napi-rs anywhere. Cost/Risk: justfile + workspace member list. No runtime caller
depends on this crate (only the justfile copies artifacts).

### F4 — MEDIUM — STRUCTURE — WASM memory views are `unsafe` with no stated invariant

Evidence: `packages/lmao/crates/lmao-timestamp-proof/src/lib.rs:35-38,67-70`

```
    unsafe fn buf_at<'a>(offset: u32, len: usize) -> &'a mut [u8] {
        unsafe { core::slice::from_raw_parts_mut(offset as usize as *mut u8, len) }
    }
    pub unsafe extern "C" fn span_start(system_ptr: u32, capacity: u32, trace_root_ptr: u32) {
        let ts = unsafe { timestamp_nanos(trace_root_ptr) };
        let buf = unsafe { buf_at(system_ptr, layout::buffer_len(capacity)) };
        layout::write_span_start(buf, capacity, ts);
    }
```

Problem: `buf_at` does not document that `offset..offset+len` must be in host-owned imported linear memory, that `len`
is `buffer_len(capacity)` or 16, or that aliasing with another live `buf_at` is forbidden. A bad `system_ptr`/`capacity`
is UB, not a panic. Host imports (`performanceNow`/`dateNow`, lines 24-30) are also uncommented. `lmao-wasm` at least
names the offset-0 launder (`lmao-wasm/src/lib.rs:63-71`); this crate does not. Fix: If the crate survives, a SAFETY
block on `buf_at` citing imported-memory ownership, in-bounds `offset+len`, and exclusive aliasing; exports cite that
comment. Prefer the `lmao-arena::Mem` path so the proof cannot invent a second memory model. Cost/Risk: Comment-only
unless folded into arena.

### F5 — MEDIUM — STRUCTURE — NAPI proof kernel locks a global `Mutex` and scans anchors per stamp

Evidence: `packages/lmao/crates/lmao-timestamp-proof/src/lib.rs:124-169`

```
    const MAX_TRACE_ROOTS: usize = 64;
    static ANCHORS: Mutex<Vec<TraceRootAnchor>> = Mutex::new(Vec::new());
    fn current_timestamp(key: usize) -> Result<i64> {
        let anchors = ANCHORS.lock().expect("anchor table");
        let anchor = anchors
            .iter()
            .find(|a| a.key == key)
            .ok_or_else(|| Error::from_reason("unknown trace root (initTraceRoot first)"))?;
        Ok(anchor.start_wall_clock_nanos + anchor.started.elapsed().as_nanos() as i64)
    }
```

Problem: WASM stores the 16-byte anchor in the host buffer (zero locks). NAPI keys anchors by `ArrayBuffer` data pointer
and pays `Mutex` + linear search on every `spanStart`/`spanEnd*`/`writeLogEntry`. This crate's job is timestamp-accuracy
proof; the lock is inside the kernel under test (regime: every stamp), so NAPI vs WASM numbers cannot be attributed to
the clock. `lock().expect("anchor table")` treats poison as a panic. `wall_clock_nanos` maps pre-epoch `SystemTime` to 0
(`lib.rs:137-138`) — operational failure swallowed, same shape as `lmao-core` `SystemClock` (`clock.rs:45-48`). Fix:
Delete the NAPI arm (F3). If it stays, put wall+monotonic into the 16-byte `ArrayBuffer` like WASM and drop `ANCHORS`.
Cost/Risk: NAPI ABI: `initTraceRoot` would write the buffer instead of ignoring its bytes (`lib.rs:173-175`).

### F6 — LOW — TESTS — Tests pin layout bytes, not the timestamp contract; FFI arms never run

Evidence: `packages/lmao/crates/lmao-timestamp-proof/src/layout.rs:71-103`

```
    fn span_start_arms_exception_row() {
        let mut buf = vec![0u8; buffer_len(16)];
        write_span_start(&mut buf, 16, 1_234_567_890);
        assert_eq!(read_ts(&buf, 0), 1_234_567_890);
        assert_eq!(buf[16 * 8], ENTRY_TYPE_SPAN_START);
        // ...
    }
    fn log_entries_append_from_persisted_index() {
        assert_eq!(write_log_entry(&mut buf, 8, 7, 100), 2);
```

Problem: Three tests assert typed row/index/entry-type values — they can go red, and they do not assert rendered
strings. They cannot go red if F1's formula is wrong: timestamps are literals supplied by the test. Default `cargo test`
is native without `napi` and not wasm32, so `mod wasm` / `mod node` are compiled out (`lib.rs:16,110-111`). `proptest`
is declared and unused. No test that WASM `dateNow()*1e6+(performanceNow-anchor)*1e6` matches NAPI `SystemTime+Instant`
(it would fail). Log test uses entry types 7 and 8 as opaque bytes, not `ENTRY_TYPE_*` names. Fix: Do not add a test
suite to this crate. Point the accuracy proof at production subjects (`timestamp.test.ts`, arena properties). Delete
`proptest` from this manifest. Cost/Risk: None if the crate is deleted.

## Cross-slice questions

- `lmao-arena` (`raw.rs` timestamp + span writes; `lib.rs` `capacity*9` span_system): should this proof disappear and
  use those functions, or does Col/LmaoWasm own a reason the proof ABI must omit identity?
- `lmao-core` (`clock.rs` `TraceAnchor::timestamp`): NAPI Instant math is closer to core than to 01b3 Node. Core should
  stay the sim/native clock; this crate should not import it.
- `lmao-wasm` (`lib.rs:328-382`): already exports the same C names with an extra `identity_ptr`. If the proof needs a
  wasm artifact, that crate is the one.
- `packages/lmao/src/lib/schema/systemSchema.ts`: SSOT for `ENTRY_TYPE_*`. Rust copies in arena + this crate.
- `proofs/timestamp-accuracy.proof.ts`: named as the consumer, absent from the tree. Orchestrator: is it elsewhere, or
  dead?

## Non-findings (checked, clean)

- `napi` `default-features = false` + optional feature so the wasm32 `core`/`panic_abort` build does not link napi-rs —
  correct given the crate exists.
- `to_le_bytes` / `copy_from_slice` of 8- and 4-byte slots is the unaligned LE write, not a hot-path clone. Regime is
  proof-stamp, not a runtime inner loop; not a COPIES finding.
- No `HashMap`, `String` keys, `Vec<Vec<_>>`, `format!` in the stamp path, no god file, no function over ~100 lines.
- Layout tests assert typed values (`i64` timestamps, `u8` entry types, `u32` write index), not rendered strings.
- WASM `panic_handler` → `unreachable` is the right `no_std` trap.
- `crate-type = ["cdylib", "rlib"]` is required for the artifacts this crate currently builds.
- Intra-crate WASM vs NAPI wrappers are FFI surface, not a second layout implementation (layout is shared; clocks are
  not — F1).
