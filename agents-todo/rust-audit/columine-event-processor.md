# columine-event-processor

Scope: `packages/columine/crates/columine-event-processor/src/lib.rs` (993), `compact.rs` (838), `bloom.rs` (239),
`checkpoint.rs` (231), `Cargo.toml` (19). Doctrine: BYPRODUCT-ENGINEERING.md; PERFORMANCE-HANDBOOK §4.1,
§7.2/§7.6/§7.7/§7.8/§7.10bb, §7.12. Targeted greps/reads for duplication: `columine-vm/src/intern.rs`,
`columine-vm/src/minroar.rs`, `columine-arrow/src/{ipc.rs,schema.rs,columns.rs}`,
`columine-parsing/src/json_extractor.rs`, `columine/src/parse-backend.ts`.

## Summary

- Bloom is the textbook Byproduct L0 anti-pattern: a probe that yields one evaporating bit, then is treated as exact
  membership for DISCARD/LATEST. Replace it with the intern/hash table already in this repo; do not route string IDs
  through minroar.
- Dedup never mutates columns: `duplicates_filtered` / CollisionPolicy are header counters. Arrow IPC still contains
  every row, including "discarded" ones.
- CPB1 wire constants, ResultCode, and ArrowType tags are hand-restated in `parse-backend.ts` (currently agree).
- Compact decode is allocation-free and borrows; `EventProcessor::compact` then does a whole-batch CPB1→IPC rewrite
  (O(data), not O(overlap)) and size-preflights twice.
- Diagnostic stage `4` is both `COLUMN` (json_extractor) and `COMPACT_DIAGNOSTIC_STAGE`; `compact_detail` and
  `diagnostic_detail` share the same header byte with different meanings.
- Bloom tests cannot go red on an always-true `maybe_contains` or a 0.5% FPR; the EP dedup test never asserts IPC row
  count.
- Msgpack `BufferOverflow` always retries workspace growth (comment claims column-limit overflows surface immediately).
- Crate deps are path-only plus test-only arrow/proptest: no DEP-BLOAT in this manifest.
- Dead: `should_grow`/`fill_ratio`/`reset`, `SCHEMA_MESSAGE_MISMATCH`, `DeserializeError::OutOfMemory`.

## Findings

### F1 — HIGH — COPIES — Bloom is L0 evaporating membership; replace, do not keep

Evidence: `packages/columine/crates/columine-event-processor/src/bloom.rs:64-90`

```
    pub fn add(&mut self, key: &[u8]) {
        let h1 = hash_fnv1a(key);
        let h2 = hash_murmur3_seed(key);
        let bit_count = (self.bits.len() * 8) as u64;
        for i in 0..self.hash_count {
            let combined = h1.wrapping_add(u64::from(i).wrapping_mul(h2));
            let bit_idx = combined % bit_count;
            self.bits[(bit_idx / 8) as usize] |= 1u8 << (bit_idx % 8);
        }
        self.total_added += 1;
    }
    pub fn maybe_contains(&self, key: &[u8]) -> bool {
        // ... same two hashes, same % bit_count, returns one bit ...
        true
    }
```

`should_process` then treats that bit as exact (`bloom.rs:156-165`): a probable hit increments `duplicates_detected`
and, under `Discard`, refuses the event. Problem: Regime is per-event on the consumer wiring path (`lib.rs:464-474`),
not startup. Byproduct L0: a probe that yields one evaporating bit is strictly poorer than membership+position.
Consequences that are not theoretical:

1. False positives are unique-event drops under `Discard` and false "replace" counts under `Latest`.
2. The probe leaves no row index, so `Latest` cannot replace (see F2).
3. `should_process` re-hashes on miss (`maybe_contains` then `add`) — the same FNV+Murmur walk twice.
4. `bit_idx = combined % bit_count` is a runtime division on a non-power-of-two length (`sizing_formula_pinned` pins
   1798 bytes → 14384 bits). Handbook §7.6.
5. `should_grow` exists (`bloom.rs:104-107`) and is never called; the bit array is frozen at init capacity while
   `total_added` is unbounded. Checkpoint restore copies those bits (`checkpoint.rs:123-133`). Fill past the 0.1% sizing
   and FPR is a live correctness drift, not a monitoring number. minroar (`columine-vm/src/minroar.rs`) is the wrong
   replacement: it is a u32 roaring bitmap. Hashing event-id bytes down to a u32 to feed it is another evaporating hash.
   The structure that already yields membership+position on variable-length keys is `StringIntern`
   (`columine-vm/src/intern.rs:19-84`: FNV-1a, open addressing, content-verify, stable index). Fix: Delete
   `BloomFilter`. Make `DedupState` an intern/hash table of event-id bytes → first-seen (or last-seen) row ordinal.
   Probe returns membership+index; `Discard`/`Latest` become total functions on that index. Size the table from
   `capacity` with a closed-form power-of-two (`intern.rs` already does `next_power_of_2` + 50% load). Drop the f64
   sizing (`bloom.rs:42-54`) — it exists only to pin checkpoint geometry of the thing being deleted. New checkpoint
   layout keyed by interned ids, not bloom bits. Cost/Risk: `CHECKPOINT_MAGIC` blob is the cross-session contract
   (`checkpoint.rs:1-6`). Every persisted CHKP becomes unreadable; bump `CHECKPOINT_VERSION` and delete the v1 decoder
   (greenfield). Consumer wasm that currently checkpoints bloom bits must move with it. `columine-ep-wasm` wires
   `CollisionPolicy::Latest` even with `dedup: false` — unused after the table only exists when `wiring.dedup`.

### F2 — HIGH — STRUCTURE — Dedup never filters or replaces; IPC still has every row

Evidence: `packages/columine/crates/columine-event-processor/src/lib.rs:461-494`

```
        // Dedup: read event ids from column 0 when consumer wiring is enabled.
        let mut processed = 0u32;
        let mut duplicates = 0u32;
        if let Some(dedup) = self.dedup_state.as_mut() {
            let col0 = &self.dynamic_columns.columns[0];
            for row in 0..self.dynamic_columns.count {
                let event_id = col0
                    .read_variable(row)
                    .unwrap_or_else(|| columine_types::die!("id column is not variable-width"));
                if dedup.should_process(event_id) {
                    processed += 1;
                } else {
                    duplicates += 1;
                }
            }
        } else {
            processed = self.dynamic_columns.count;
        }

        match write_arrow_ipc_from_dynamic_columns(
            &self.dynamic_columns, ...
```

`CollisionPolicy` claims the opposite (`bloom.rs:11-15`, `156-157`): "Keep the latest event (replace)" / "Discard the
new event". Header field is `duplicates_filtered` (`lib.rs:64-66, 598-605`). Crate docs: "parse → columns → (dedup) →
Arrow IPC". Problem: Dedup is a counter over column 0. `write_arrow_ipc_from_dynamic_columns` emits `dynamic_columns` in
full. A DISCARD batch of `[dup, dup, uniq]` reports `processed=2, dupes=1` and an IPC record batch of 3 rows. LATEST
cannot replace because nothing in this crate records the prior row. `die!` if column 0 is not variable-width is an
invariant panic on a schema the caller chose — operational schema mismatch should be `ResultCode::SchemaMismatch`. Fix:
After F1, compact the column set (or a keep-bitmap) before the IPC write: DISCARD drops the later row; LATEST overwrites
the earlier row in place (the intern index IS the slot). Until that exists, rename the header field and the policy
comments to "duplicates_detected" and delete `Latest`/`Discard` — counting is not a collision policy. Do not leave both.
Cost/Risk: Any consumer that already treats IPC rows as the filtered set will change. The in-crate test
`dedup_and_checkpoint_through_ep` (`lib.rs:869-908`) only asserts header counters and bloom `maybe_contains`; it will
need an IPC row-count oracle or it keeps pinning the bug.

### F3 — MEDIUM — SSOT — CPB1 / ResultCode / ArrowType restated in TypeScript

Evidence: `packages/columine/crates/columine-event-processor/src/compact.rs:14-18` vs
`packages/columine/src/parse-backend.ts:90-129`

```
pub const COMPACT_BATCH_MAGIC: u32 = 0x3142_5043; // "CPB1"
pub const COMPACT_ABI_VERSION: u16 = 1;
pub const COMPACT_HEADER_SIZE: usize = 16;
pub const COMPACT_DESCRIPTOR_SIZE: usize = 32;
```

```
const WASM_OUTPUT_HEADER_SIZE = 32;
const MAX_EVENTS_PER_BATCH = 65_536;
const COMPACT_MAGIC = 0x3142_5043;
const COMPACT_VERSION = 1;
const COMPACT_HEADER_SIZE = 16;
const COMPACT_DESCRIPTOR_SIZE = 32;
const COMPACT_KIND_TAG = { null: 0, u32: 1, f64: 2, binary: 3, utf8: 4, bool: 5, i64: 6 };
const COMPACT_STATUS_CODE = { 1: 'INVALID_HANDLE', 2: 'PARSE_ERROR', ... 7: 'SCHEMA_MISMATCH' };
```

Rust SSOT for the same numbers: `ResultCode` (`lib.rs:53-62`), `RESULT_HEADER_SIZE` (`lib.rs:67`), `ArrowType`
(`columine-arrow/src/schema.rs:16-26`), `MAX_EVENTS_PER_BATCH` (`columine-arrow/src/columns.rs:16`). Currently the
copies agree (checked, not inferred). Problem: Two pack/parse implementations of one wire. `parse-backend.ts:402-442`
`validateVariableColumn` is the same loop as `compact.rs:570-623` `validate_variable` (monotonic offsets, null rows
empty, utf8). Divergence is a silent pack/reject mismatch, not a compile error. Fix: Rust constants remain the SSOT
(they are the fail-closed wasm decoder). Generate the TS numeric vocab from those constants, or import a single
generated JSON table both sides include. Keep the wasm re-validation (trust boundary); delete the duplicated _numbers_
and the duplicated offset/utf8 walk from TS if the wasm path is the only encoder consumers hit. Decision: generate the
numbers; keep one structural validator — the Rust one. Cost/Risk: `parse-backend.ts` and `compact-backend.test.ts`
(other slice). A generator has to land in both packages.

### F4 — MEDIUM — SSOT — Diagnostic stage 4 and detail bytes mean two things

Evidence: `packages/columine/crates/columine-event-processor/src/compact.rs:18-32` and
`packages/columine/crates/columine-parsing/src/json_extractor.rs:26-45`

```
pub const COMPACT_DIAGNOSTIC_STAGE: u8 = 4;
pub mod compact_detail {
    pub const BAD_HEADER: u8 = 1;
    ...
    pub const SCHEMA_MESSAGE_MISMATCH: u8 = 12;
}
```

```
pub mod diagnostic_stage { ... pub const COLUMN: u8 = 4; pub const SCHEMA: u8 = 5; }
pub mod diagnostic_detail { pub const NONE: u8 = 0; pub const INVALID_JSON: u8 = 1; ... }
```

Both write `ResultDiagnostic` (`lib.rs:78-86`) into the same 12 reserved header bytes (`lib.rs:609-618`). Compact encode
failures also use stage 4 (`lib.rs:633-641`). TS compact tests already expect `stage: 4`
(`packages/columine/src/__tests__/compact-backend.test.ts:444-447`). Problem: One byte, two vocabularies. A decoder
keyed on `DIAGNOSTIC_STAGES` (json_extractor comments name `lib.ts`) maps compact `BAD_HEADER` (detail 1) to
`COLUMN`/`INVALID_JSON`. `SCHEMA_MESSAGE_MISMATCH = 12` is never assigned in this crate (dead ABI slot). Fix: Give
compact its own stage (6, after SCHEMA) in the _single_ `diagnostic_stage` module in `columine-parsing`, and put compact
details in that same module under a distinct range — or namespace details by stage in the type so a compact detail
cannot be constructed with a parse stage. Delete `SCHEMA_MESSAGE_MISMATCH` until a check exists. Do not keep a second
`compact_detail` module. Cost/Risk: TS compact diagnostic fixture (`stage: 4`) and any consumer `lib.ts` vocabulary.
Cross-slice `columine-parsing`.

### F5 — MEDIUM — COPIES — Compact validates O(data) then rewrites O(data) into Arrow IPC

Evidence: `packages/columine/crates/columine-event-processor/src/lib.rs:253-305` and `compact.rs:570-623`, `384-430`
`CompactBatchView::parse` walks every variable column (`validate_variable`: every offset, every utf8 row).
`EventProcessor::compact` then:

1. `required_arrow_ipc_len(|index| view.column(...))` (`lib.rs:265-267`)
2. `write_arrow_ipc_from_borrowed_columns(...)` which, in `columine-arrow/src/ipc.rs:171`, calls
   `required_arrow_ipc_len` again, then copies each borrowed buffer into an IPC body (`ipc.rs:198-218`). `column()`
   re-runs `decode_descriptor` (`compact.rs:384-390`) on bytes `parse` already accepted. `null_count` walks the validity
   bitmap a second time (`compact.rs:433-444`). Problem: Regime is once per `ep_compact` batch (not per-row). The decode
   side is the right shape: allocation-free borrow, test-pinned (`compact.rs:729-741`
   `valid_nullable_utf8_is_borrowed_without_repacking`). The encode side is Byproduct endgame / §7.8 inverted: pay to
   stream the batch for validation, throw the byproduct away, stream it again to memcpy into a different frame. That is
   whole-data rewrite, not range adoption O(overlap). Overlap checking itself (`checked_range`, `compact.rs:521-532`) is
   O(buffers²) over ≤ `MAX_SCHEMA_FIELDS * 3` — fine. The cost is the second full copy into IPC. Fix: Fuse: validation's
   last pass is the IPC body write (write-combining into the caller buffer whose size was the closed form of the first
   descriptor walk). Store decoded `Descriptor`s in the view so `column()`/`null_count` do not re-parse. Drop the extra
   `required_arrow_ipc_len` in this crate and take the writer's preflight, _or_ have the writer skip its preflight when
   the caller already proved size (ColArrow change). True range adoption — IPC body = CPB1 data ranges, headers only —
   lives in `columine-arrow` and is only legal if IPC buffer layout ≡ CPB1 data layout; [INFERENCE] it does not, so the
   fused copy stays until that is measured. Cost/Risk: `columine-arrow` IPC writer (other slice). Compact's public
   `CompactBatchView` stays `Copy` only if descriptors are a fixed `[Descriptor; MAX_SCHEMA_FIELDS]` beside the borrow,
   not a `Vec`.

### F6 — MEDIUM — TESTS — Bloom and EP dedup tests cannot go red on the rejected lever

Evidence: `packages/columine/crates/columine-event-processor/src/bloom.rs:173-201` and `lib.rs:869-908`

```
    fn basic_operations() {
        ...
        assert!(filter.maybe_contains(b"event-001"));
        assert!(filter.maybe_contains(b"event-002"));
        assert!(filter.maybe_contains(b"event-003"));
    }
    fn false_positive_rate() {
        ...
        assert!((f64::from(false_positives) / 10_000.0) < 0.01);
    }
```

`basic_operations` never asserts an unseen key is absent. `maybe_contains = true` always still passes.
`false_positive_rate` accepts 1% against a documented 0.1% target — a 0.5% implementation ships. Both tests
`format!(...)` per key (`bloom.rs:189-194`); the fixture allocates, so they cannot speak to probe cost (Handbook §4.9 /
L8). `dedup_and_checkpoint_through_ep` asserts `processed == 2`, `dupes == 1`, then `maybe_contains` on the restored
bloom — never IPC row count, so F2 cannot go red (Handbook §7.10bb). Fix: `basic_operations`:
`assert!(!filter.maybe_contains(b"event-absent"))`. Pin FPR at the claimed 0.1% with a bound that a 1% filter fails
(e.g. `< 0.002` on that 10k/10k draw, or an exact bit-count oracle on a frozen fixture). Dedup test: parse the IPC
stream (the compact null-rows test already does this at `lib.rs:730-741`) and assert batch rows == processed under
Discard. Pre-intern the 20k keys so the FPR cell is not a malloc benchmark. Cost/Risk: Test-only. The FPR bound
tightening may flake if left statistical; prefer a pinned bit-image over a random draw.

### F7 — MEDIUM — STRUCTURE — Msgpack BufferOverflow always retries growth

Evidence: `packages/columine/crates/columine-event-processor/src/lib.rs:543-553`

```
                Err(json_extractor::ExtractionError::BufferOverflow) => {
                    let fallback = format == InputFormat::Json;
                    // Retry only workspace overflow; a column-limit overflow
                    // surfaces immediately.
                    let workspace_overflow = !fallback
                        || (diagnostic.stage == diagnostic_stage::MSGPACK
                            && diagnostic.detail == diagnostic_detail::BUFFER_OVERFLOW);
                    let may_grow = workspace_overflow && (fallback || self.wiring.msgpack_growth);
```

Problem: For JSON, `workspace_overflow` is true only on MSGPACK+BUFFER_OVERFLOW (the `$extra` map path in
`json_extractor.rs:374-382`) — that matches the comment. For msgpack, `fallback` is false, so `workspace_overflow` is
unconditionally true. Every `BufferOverflow`, including column-limit, doubles `work_buffer` and re-extracts the whole
input until `MAX_WORK_BUFFER_SIZE` (`lib.rs:212-225`, L4 growth under load). Regime: overflow path, not the happy
per-event loop; still a live mismatch with the comment and a re-parse storm on a column failure. Fix: Thread a distinct
error (workspace vs column) out of `msgpack_extractor` and switch on that. Do not encode it as `!fallback`. JSON path
can keep the diagnostic match until the extractor returns the same enum. Cost/Risk: `columine-parsing` msgpack extractor
(other slice). `msgpack_growth_is_wiring_dependent` (`lib.rs:944-991`) still passes if workspace overflow remains the
retried arm.

### F8 — LOW — STRUCTURE — Dead growth/reset API and unused error/detail slots

Evidence:

- `bloom.rs:92-107` — `reset`, `fill_ratio`, `should_grow` have zero callers in `packages/` (grep).
- `compact.rs:32` — `SCHEMA_MESSAGE_MISMATCH` never constructed.
- `checkpoint.rs:74-80` — `DeserializeError::OutOfMemory` is never returned (`deserialize` has no such arm). Problem:
  `should_grow` in particular is the abandoned L4 path that F1 names; leaving it suggests the filter grows when it does
  not. `OutOfMemory` on deserialize is an error value that cannot happen — callers matching exhaustively handle a
  fiction. Fix: Delete all five. Checkpoint deserialize already fails closed on geometry mismatch; no new error needed.
  Cost/Risk: None if grep stays empty; wasm bindings do not export these.

### F9 — LOW — DUPLICATION — Test schema builder recopies ArrowType→DataType

Evidence: `packages/columine/crates/columine-event-processor/src/lib.rs:680-704` is the same match + `StreamWriter` +
`truncate(...- 8)` as `packages/columine/crates/columine-arrow/src/ipc.rs:406-429` (`schema_bytes`).
`compact.rs:677-686` is a third, narrower copy. Problem: Adding an `ArrowType` variant requires touching test helpers in
two crates or IPC bytes silently go stale. Test-only, once-per-test. Fix: One `DynamicSchemaConfig` test helper in
`columine-arrow`, used here. Cost/Risk: ColArrow test module becomes the SSOT; this crate's tests import it.

## Cross-slice questions

- `columine-arrow` (`ipc.rs` `write_arrow_ipc_from_borrowed_columns` / `DynamicBodyBuilder`): does the IPC body memcpy
  every CPB1 buffer, and is there any layout in which those ranges can be adopted rather than copied? F5's fused-write
  fix lands there.
- `columine-parsing` msgpack extractor: is `ExtractionError::BufferOverflow` used for both workspace and column limits?
  F7 cannot be closed without that enum split.
- `columine-vm` `intern.rs` / `minroar.rs`: confirm intern (not roaring) is the accepted DedupState replacement. minroar
  is u32-only.
- `packages/columine/src/parse-backend.ts` (and any consumer `lib.ts` named by json_extractor): who owns CPB1 numeric
  ABI and `DIAGNOSTIC_STAGES`? F3/F4.
- Checkpoint comment (`checkpoint.rs:3-4`) says TypeScript persists CHKP blobs. Grep of `packages/columine/src` found no
  `CHKP`/`0x43484B50` codec. Is the consumer outside this repo, or is the comment stale?
- `columine-ep-wasm`: `restore()` (`lib.rs:570-577`) overwrites `dedup_state` even when `wiring.dedup` is false. Does
  the columine artifact export `ep_restore`?

## Non-findings (checked, clean)

- DEP-BLOAT: manifest is `columine-arrow` / `columine-parsing` / `columine-types` plus dev `proptest`/`arrow-*`.
  `columine-types` is load-bearing (`die!`). No crypto/tls/git2/napi/sqlite. Keep.
- No `unsafe` in this crate. Operational failures return `ResultCode` / `Option` / `DeserializeError`; `die!` is used
  for "base path without columns", compact IPC > u32, and non-variable id column (the last one should become F2's schema
  error).
- `CompactBatchView` is a validated borrow; `buffer()` fail-open to `&[]` is only reachable after `parse` proved the
  ranges. Overlap checker is O(fields), not O(data).
- Checkpoint layout is pinned (`checkpoint.rs:172-190`) including the unpadded `bloom_offset = 36`. `hash_count` is
  written and not read back because `BloomFilter::new(capacity)` recomputes it; consistent with the pinned formula, not
  a restore bug _until_ F1 deletes the formula.
- `CollisionPolicy::from_u8` is closed (0/1, else None). `InputFormat::ArrowPassthrough` is rejected on both pipelines.
- Geometric workspace growth (`lib.rs:212-225`) is an overflow retry, not the per-event path — noted under F7, not a
  hot-loop alloc finding (§4.1 regime).
- `read_u32` duplicated between `compact.rs` and `checkpoint.rs` is four lines; not worth a third helper crate.
- Compact tests assert typed `ResultCode` + `compact_detail` and borrowed slice identity, not rendered strings.
  `result_header_layout_pinned` pins bytes because the header _is_ the ABI.
