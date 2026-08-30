# columine-vm/minroar+bitmaps

Scope: `packages/columine/crates/columine-vm/src/minroar.rs` (724),
`packages/columine/crates/columine-vm/src/bitmap_ops.rs` (598),
`packages/columine/crates/columine-vm/src/hashset_ops.rs` (303). Doctrine: BYPRODUCT-ENGINEERING.md,
PERFORMANCE-HANDBOOK §4.1 / §7.1–§7.13 (04-mechanisms.md, 05-memory-toolkit.md). Neighbor reads (not audited):
`columine-vm/Cargo.toml`, `packages/columine/Cargo.toml`, `Cargo.lock` roaring entries, `tests/bitmap.rs` oracle block,
`vm.rs` SetDelete undo / `vm_set_contains`, `hash_table.rs` `find_insert` sentinels, `columine-types` `ErrorCode` /
`BITMAP_*`.

## Summary

- HIGH: bitmap add/remove append undo and mutate TTL _before_ `bitmap_store`; a payload-capacity refusal leaves
  eviction-index + undo log out of sync with slot bytes.
- HIGH: `single_set_remove` writes `aux: 0` on TTL slots; `batch_set_remove` / `batch_bitmap_remove` capture
  `latest_eviction_ts` — live SSOT split; undo restores `f64::from_bits(0)`.
- HIGH: `contains_serialized` / `cardinality_serialized` / `bitmap_select` / intersect helpers heap-deserialize the
  whole bitmap per call (L7); cardinality already sits in the portable header.
- HIGH: `Container::to_words` allocates an 8 KiB `Box<[u64; 1024]>` per container on every algebra/intersect;
  `Bit*Assign` rebuilds via `*self = algebra(...)`.
- MEDIUM: `BitmapEnv.last_error` is an untyped `u32` magic table (1/2/5/6/60/61/71/72/75/80/102) that collides with
  `ErrorCode`; `batch_bitmap_remove` swallows load/store failures.
- MEDIUM: SetInsert/SetDelete `MutationRecord` pairs are restated six times across hashset_ops and bitmap_ops (the TTL
  field is what already diverged).
- MEDIUM: `iter_lows` boxes a `dyn Iterator` per container; `bitmap_select` is `iter().nth(rank)`.
- MEDIUM: `set_algebra` empty-slice identities copy the survivor without the deserialize validation the non-empty path
  performs.
- LOW: `MiniRoaring::insert` at a full array returns true for duplicates and promotes to bitset (rawr quirk; VM path
  short-circuits via `contains`).
- LOW: `serialize_into` is typed `Result` but never returns `Err`; `bitmap_store` still copies through `store_temp`.
- roaring crate is dev-only; MiniRoaring is the shipped data plane; portable cookies/layout match RoaringFormatSpec.

## Findings

### F1 — HIGH — STRUCTURE — Undo/TTL commit before `bitmap_store`; capacity refusal desyncs side tables

Evidence: `packages/columine/crates/columine-vm/src/bitmap_ops.rs:244-293` and `:330-372` plus store `:126-129`

```
        if hooks.undo_enabled() {
            meta.set_size(state, cardinality);
            hooks.append_mutation(delta_mode, state, MutationRecord { op: MutationOp::SetInsert, ... }, ...);
        }
        cardinality += 1;
        ...
        if meta.has_ttl() {
            let ttl_result = hooks.insert_with_ttl(state, meta, elem, ts);
            ...
        }
    }
    let store_result = bitmap_store(env, state, storage, &mut bitmap);
    if store_result != ErrorCode::Ok {
        ...
        meta.set_size(state, original_size);
        return store_result;
    }
```

```
        if hooks.undo_enabled() { ... hooks.append_mutation(... SetDelete ...); }
        cardinality -= 1;
        ...
        if meta.has_ttl() {
            hooks.remove_ttl_entries_for_key(state, meta, elem);
        }
    ...
    if bitmap_store(env, state, storage, &mut bitmap) != ErrorCode::Ok {
        return;
    }
```

```
    let serialized_size_needed = bitmap.serialized_size();
    if serialized_size_needed > storage.payload_capacity as usize {
        env.last_error = 60;
        return ErrorCode::CapacityExceeded;
    }
```

Problem: Element cardinality (`meta.capacity`) and payload bytes (`bitmap_payload_capacity`) are independent. Sparse
keys (many containers) can pass the per-element cap check and still fail `bitmap_store`. Undo records and TTL
inserts/removes already ran. On failure only `meta.size` is reverted (add) or nothing is reverted (remove — `()` return,
silent). Slot bytes stay at the old image. Rollback of a SetInsert that was never stored tries to delete a missing key;
rollback of a SetDelete that never left the image re-inserts a still-present key and restores TTL onto a live member.
Fix: Treat in-memory bitmap + undo + TTL as one transaction: buffer mutation records, run TTL after a successful store,
or snapshot TTL/undo and revert them on `bitmap_store != Ok`. Same commit point for add and remove. Make
`batch_bitmap_remove` return `ErrorCode`. Cost/Risk: `vm.rs` undo apply (ColVmCore) and any host that reads
`vm_get_rbmp_last_error` after a mixed TTL batch. Tests in `tests/bitmap.rs` do not exercise TTL+undo+capacity together.

### F2 — HIGH — DUPLICATION — `single_set_remove` drops TTL `aux`; batch paths do not

Evidence: `packages/columine/crates/columine-vm/src/hashset_ops.rs:145-172` vs `:278-296` and `bitmap_ops.rs:330-347`

```
            let prev_ts_bits: u64 = if meta.has_ttl() {
                hooks
                    .latest_eviction_ts(state, meta, elem)
                    .map(f64::to_bits)
                    .unwrap_or(0)
            } else {
                0
            };
            hooks.append_mutation(..., MutationRecord { op: MutationOp::SetDelete, ..., aux: prev_ts_bits }, ...);
```

```
    if hooks.undo_enabled() {
        hooks.append_mutation(
            ...
            MutationRecord { op: MutationOp::SetDelete, ..., aux: 0, },
            MutationRecord { op: MutationOp::SetInsert, ..., aux: 0, },
        );
    }
```

Problem: Two implementations of set-remove undo already disagree. Per-element HASHSET dispatch (`single_set_remove`)
never captures the eviction timestamp. Neighbor `vm.rs:1020-1026` (ColVmCore) restores TTL with
`restore_ttl_entry(..., f64::from_bits(entry.aux))` on `SetDelete`. Bitmap single-remove delegates to
`batch_bitmap_remove`, which _does_ capture `prev_ts_bits` — so HASHSET-typed slots are wrong and BITMAP-typed slots are
not. This is the copies-diverged live bug, not a style note. Fix: One helper
`fn set_delete_records(slot, key, prev_ts_bits) -> (MutationRecord, MutationRecord)` used by `batch_set_remove`,
`single_set_remove`, `batch_bitmap_remove`. `single_set_remove` must call `latest_eviction_ts` the same way the batch
path does. Delete the six in-line struct literals. Cost/Risk: Undo of per-element HASHSET remove on TTL slots. Bitmap
path already correct. No ABI change if `aux` stays u64 timestamp bits.

### F3 — HIGH — COPIES — Serialized probes heap-deserialize the whole bitmap (L7 / evaporating header)

Evidence: `packages/columine/crates/columine-vm/src/bitmap_ops.rs:469-540` and `:160-163`

```
pub fn contains_serialized(data: &[u8], value: u32) -> bool {
    if data.is_empty() { return false; }
    RoaringBitmap::deserialize_from(data)
        .map(|bm| bm.contains(value))
        .unwrap_or(false)
}
pub fn cardinality_serialized(data: &[u8]) -> u32 {
    ...
    RoaringBitmap::deserialize_from(data)
        .map(|bm| u32::try_from(bm.len()).unwrap_or(u32::MAX))
        .unwrap_or(0)
}
pub fn bitmap_select(...) -> Option<u32> {
    let data = storage.serialized_data(state)?;
    let bm = RoaringBitmap::deserialize_from(data).ok()?;
    bm.iter().nth(rank as usize)
}
```

Problem: Regime is per-probe / per-export, not startup. Neighbor `vm.rs:4924-4931` (`vm_set_contains`) and
`columine-wasm` `vm_rbmp_contains_serialized` / `vm_rbmp_cardinality_serialized` / `vm_rbmp_intersect_*` hit these on
the query path. `deserialize_from` allocates `Vec` keys, `Vec` containers, and for bitsets an 8 KiB `Box<[u64; 1024]>`
each, then throws the structure away. Cardinality is already in the descriptive header (`card-1` per container,
`minroar.rs:576-578`). Contains only needs a binary search on the key plane plus an in-place probe of that container's
payload. `bitmap_select` via `nth` also walks every preceding low after the parse. Fix: Add
`MiniRoaring::{contains_bytes, len_bytes, select_bytes, intersect_len_bytes}` that walk the portable layout with
`Reader` and never build `Container`. `cardinality_*` sums header cards; `contains` searches keys then probes
array/bitset/run bytes in the slice. Keep `deserialize_from` for mutate/store only. Cost/Risk: All `vm_rbmp_*` exports
and `vm_set_contains`. Behavior of invalid bytes (contains/cardinality treat as empty/false; `cardinality_validated`
stays the fail-closed variant) must stay as documented.

### F4 — HIGH — COPIES — Algebra always materializes 8 KiB word arrays and rebuilds a new `MiniRoaring`

Evidence: `packages/columine/crates/columine-vm/src/minroar.rs:188-207`, `:410-452`, `:394-407`, `:702-722`

```
    fn to_words(&self) -> Box<[u64; 1024]> {
        let mut words = Box::new([0u64; 1024]);
        match self {
            Container::Array(v) => { ... }
            Container::Bitset(w, _) => words.copy_from_slice(&w[..]),
            Container::Run(runs) => { for &(start, len) in runs { for x in ... { words[...] |= ... } } }
        }
        words
    }
    fn algebra(...) -> Self {
        ...
                let a = self.containers[i].to_words();
                let b = other.containers[j].to_words();
                let mut out = Box::new([0u64; 1024]);
                for k in 0..1024 { out[k] = op(a[k], b[k]); }
                if let Some(c) = Container::from_words(out) { ... }
        ...
    }
    fn bitand_assign(&mut self, rhs: &MiniRoaring) {
        *self = self.algebra(rhs, |a, b| a & b, false, false);
    }
```

Problem: Regime is wasm `set_algebra` / `batch_bitmap_algebra` / `intersect_count_serialized` — production, not boot.
Every matching key pair: two 8 KiB heap bitsets + one output box, even for two sorted arrays of a handful of u16s.
`from_words` then rebuilds an `Array` `Vec` when card ≤ 4096 (a third materialization). `Bit*Assign` cannot reuse
`self.keys`/`self.containers`; AND of a large left and a small right still allocates a full new bitmap. `is_disjoint` is
`intersection_len == 0` with no early-out (`minroar.rs:390-392`), so a miss still pays every matching container. Fix:
Galloping merge for Array/Array and Array/Run (roaring's default). Bitset/Bitset can AND in place on the left
`Box<[u64; 1024]>` with a scratch `[u64; 1024]` parked in `BitmapEnv` (closed-form 8 KiB, once). Early-exit
`is_disjoint` on first nonempty AND. In-place `bitand_assign` drop-left-only keys without cloning kept containers.
Cost/Risk: Algebra results must remain equal to the roaring oracle in `tests/bitmap.rs` `minroar_ops_match_roaring`. Run
containers after algebra are currently only restored by `optimize()` on store — keep that.

### F5 — MEDIUM — STRUCTURE — `last_error` is a parallel untyped error plane; remove swallows operational failure

Evidence: `packages/columine/crates/columine-vm/src/bitmap_ops.rs:74-76`, `:108-110`, `:179-185`, `:314-316`, `:370-372`

```
    pub last_error: u32,
...
            env.last_error = 102; // error.InvalidFormat lane
...
        if env.last_error == 0 {
            env.last_error = 1;
        }
        return ErrorCode::InvalidState;
...
    let Some(mut bitmap) = bitmap_load(env, state, storage) else {
        return;
    };
...
    if bitmap_store(env, state, storage, &mut bitmap) != ErrorCode::Ok {
        return;
    }
```

Problem: `ErrorCode` in `columine-types` is `0..=8` (`CapacityExceeded=1`, `InvalidProgram=2`, `NeedsGrowth=5`,
`ArenaOverflow=6`, `InvalidState=4`). `last_error` reuses 1/2/5/6 as _breadcrumb_ lanes and adds 60/61/71/72/75/80/102
with no enum. `102` is not `ErrorCode` at all. Hosts reading `vm_get_rbmp_last_error` cannot distinguish "load failed,
stuffed 1" from `CapacityExceeded`. `bitmap_load` oversize-len returns `None` without setting `last_error`.
`batch_bitmap_remove` returns `()` and drops both load and store failures (operational, not invariant). Fix:
`enum BitmapDiag { ... }` with explicit discriminants if the wasm ABI pins 60/102; otherwise map every failure onto
`ErrorCode` and delete `last_error`. `batch_bitmap_remove` must return `ErrorCode` like add. Do not reuse `ErrorCode`
numeric values for a second meaning. Cost/Risk: `vm_get_rbmp_last_error` ABI. If a host already keys on 102/60, the enum
must keep those numbers and document them in `columine-types`, not in comments.

### F6 — MEDIUM — DUPLICATION — Set mutation records copy-pasted six times

Evidence: `hashset_ops.rs:66-83`, `:155-172`, `:213-231`, `:278-296`; `bitmap_ops.rs:246-263`, `:338-355`

Problem: Same two `MutationRecord` values (SetInsert↔SetDelete, `prev_value: 0`) appear at every insert/remove site. F2
is the proof the copies are already live-divergent. Hashset insert/remove _policy_ (TTL refresh, change flags, capacity)
is a second implementation of the bitmap loop, which is expected for two storage backends; the undo _record_ is not
storage-specific and should not have been forked. Fix: After F2's helper, the six sites become one call. Do not try to
unify FlatTable insert with MiniRoaring insert — different data structures, same undo DTO. Cost/Risk: None beyond F2.

### F7 — MEDIUM — COPIES — `iter_lows` heap-boxes a trait object; `select` is `nth`

Evidence: `packages/columine/crates/columine-vm/src/minroar.rs:291-310` and `bitmap_ops.rs:160-163`, `:504-511`

```
    fn iter_lows(&self) -> Box<dyn Iterator<Item = u16> + '_> {
        match self {
            Container::Array(v) => Box::new(v.iter().copied()),
            Container::Bitset(words, _) => Box::new(words.iter().enumerate().flat_map(...)),
            Container::Run(runs) => Box::new(runs.iter().flat_map(|&(start, len)| {
                (u32::from(start)..=u32::from(start) + u32::from(len)).map(|x| x as u16)
            })),
        }
    }
```

Problem: Regime: `extract_serialized` (wasm export) and `bitmap_select` (neighbor `vm.rs:5005-5006`). One `Box<dyn>` per
container per walk, plus virtual calls in the element loop (§7.1). `nth(rank)` is O(rank) after a full deserialize (F3).
`run_optimize` (`minroar.rs:271`) additionally `collect()`s every low into a `Vec<u16>` after already counting runs —
evaporating the count (L0), once per store so not F-HIGH. Fix: A concrete `enum LowsIter<'a>` (array slice iter / bitset
tz walk / run range). `select`: walk container cardinalities from the header (closed-form), then index the winning
container. `run_optimize` can emit runs in the same pass that counted them. Cost/Risk: Iterator type leaks only inside
`minroar.rs` today (`iter()` is `impl Iterator`). Tests compare collected `Vec<u32>` values.

### F8 — MEDIUM — STRUCTURE — `set_algebra` empty-slice identities skip format validation

Evidence: `packages/columine/crates/columine-vm/src/bitmap_ops.rs:553-574` vs `:576-582`

```
    if left.is_empty() {
        return match op {
            BitmapAlgebraOp::And | BitmapAlgebraOp::AndNot => ErrorCode::Ok,
            BitmapAlgebraOp::Or | BitmapAlgebraOp::Xor => {
                env.algebra_result.extend_from_slice(right);
                ErrorCode::Ok
            }
        };
    }
    ...
    let Ok(l) = RoaringBitmap::deserialize_from(left) else {
        env.last_error = 71;
        return ErrorCode::InvalidState;
    };
```

Problem: Zero-length is treated as the empty set (correct identity). The surviving non-empty buffer is copied as-is. The
non-empty/non-empty path refuses `InvalidFormat`. A caller that passes `left_len=0` and a garbage `right` gets `Ok` and
a poisoned `algebra_result`. `batch_bitmap_algebra` empty-source AND similarly zeros the target without loading it
(`bitmap_ops.rs:401-418`) — that one is an identity on the target slot and is less wrong. Fix: Empty-slice identities
that _copy_ a side must still `deserialize_from` (or a bytes-validate) that side, or document and test that the wasm
layer never passes untrusted bytes into this shortcut. Do not copy invalid portable buffers into `algebra_result`.
Cost/Risk: `vm_rbmp_and/or/xor/andnot` in columine-wasm. Tests `set_algebra_empty_identities_copy_survivor` assert byte
equality of a _valid_ survivor — keep that, add an invalid-right case that must return `InvalidState`.

### F9 — LOW — STRUCTURE — `MiniRoaring::insert` lies at array-full duplicates

Evidence: `packages/columine/crates/columine-vm/src/minroar.rs:67-88`

```
            Container::Array(v) => {
                if v.len() >= ARRAY_MAX_CARDINALITY {
                    let mut words = Box::new([0u64; 1024]);
                    ...
                    if *w & bit == 0 { *w |= bit; card += 1; }
                    *self = Container::Bitset(words, card);
                    // rawr returns true unconditionally on this path.
                    return true;
                }
```

Problem: A duplicate insert when `len == 4096` returns `true` and promotes array→bitset.
`roaring::RoaringBitmap::insert` returns false and keeps the array. VM `batch_bitmap_add` guards with `already_present`
then `!already_present && bitmap.insert` (`bitmap_ops.rs:206-223`), so the opcode path never observes it. Oracle
`minroar_ops_match_roaring` does not assert insert's `bool` at this boundary. Fix: Return whether the bit was newly set
(the `card += 1` branch). Do not promote on a duplicate. Delete the rawr-compat lie; this repo's control arm is the
roaring crate, not rawr (BYPRODUCT L9). Cost/Risk: None on the VM path. Cross-compat bytes at exactly 4096 unique lows
stay 8192 B either way (array of 4096 vs bitset).

### F10 — LOW — COPIES — `serialize_into` is an infallible `Result`; `bitmap_store` copies a closed-form payload twice

Evidence: `packages/columine/crates/columine-vm/src/minroar.rs:483-551` (only `Ok(())` returns) and
`bitmap_ops.rs:132-148`, `:591-596`

```
    env.store_temp.clear();
    env.store_temp.reserve(serialized_size_needed);
    if bitmap.serialize_into(&mut env.store_temp).is_err() {
        env.last_error = 61;
        return ErrorCode::InvalidState;
    }
    ...
    state[payload..payload + serialized_size as usize].copy_from_slice(&env.store_temp);
```

Problem: Regime is once per store/algebra, not a probe loop — hence LOW. `serialize_into` never constructs `Err`.
`last_error = 61` and `75` are dead. Size is closed-form (`serialized_size`) so the temp buffer is not needed for an
unknown-length write; the comment's two-phase commit is protecting a failure that cannot happen. `set_algebra` does not
even `reserve(serialized_size())` before `serialize_into`. Fix: `serialize_into(&self, out: &mut [u8]) -> usize` writing
into the already-checked payload slice, or keep the Vec sink but return `()`. Use `serialized_size` to reserve in
`set_algebra`. Drop lanes 61/75. Cost/Risk: Cosmetic on the wasm size path; deleting `store_temp` is a
behavior-preserving copy removal.

## Cross-slice questions

- ColVmCore (`packages/columine/crates/columine-vm/src/vm.rs`): confirm `apply` of `FlatUndoOp::SetDelete` at ~1020-1026
  is the only TTL restore from `entry.aux`. F2 is a bug iff that is the restore. Same file's `vm_set_contains`
  ~4924-4931 is the regime proof for F3.
- ColWasm (`packages/columine/crates/columine-wasm/src/lib.rs`): `vm_get_rbmp_last_error` / `vm_rbmp_*` — is 102/60 a
  pinned host ABI, or can F5 collapse onto `ErrorCode`?
- ColTypes (`columine-types` `ErrorCode` in both `types.rs` and `opcodes.rs`): last_error 102 has no variant; who owns
  the diagnostic table?
- tests/bitmap.rs (unowned): oracle never compares `insert()`'s bool at 4096 — needed to lock F9.

## Non-findings (checked, clean)

- **Control arm / dep-bloat.** `columine-vm/Cargo.toml` `[dependencies]` is only `columine-types`.
  `roaring = { workspace = true }` is under `[dev-dependencies]` with an explicit "must never re-enter the shipped
  artifact (35K wasm)" comment. Workspace `packages/columine/Cargo.toml` lists roaring under `[workspace.dependencies]`
  (`default-features = false`, `features = ["std"]`); `Cargo.lock` has a single `roaring 0.11.4` edge, from
  `columine-vm` alongside `proptest`. No other crate in `packages/` lists it. MiniRoaring is the data plane
  (`bitmap_ops.rs:18` `use crate::minroar::MiniRoaring as RoaringBitmap`); roaring is test-only. In-process typed oracle
  earns its weight — a `roaring` CLI is not a thing, and shell-out would not give `Result` equality. Doctrine L9
  respected.
- **RoaringFormatSpec.** Cookies 12346/12347, `NO_OFFSET_THRESHOLD = 4`, `ARRAY_MAX_CARDINALITY = 4096`, bitset 8192 B /
  1024 u64, header `card-1` as u16, run flag bitset `ceil(n/8)`, offsets omitted iff runs && `n < 4`, run pairs
  `(start, len-1)`, keys strictly increasing: matches the portable spec. Empty serializes as 12346+count 0 (8 B).
  Optimize thresholds (`n_runs*4 < card*2` / `< 8192`) omit the 2-byte run header vs CRoaring; `bitmap_ops.rs:7-8`
  already names that as implementation-defined. Bitsets do not demote on remove. Offsets are skipped on read
  (sequential) rather than random-accessed — compatible, not a second format.
- **SSOT for payload capacity.** `bitmap_payload_capacity` is the one formula; `slot_growth.rs` / `state_init.rs`
  consume it. Cookies are not restated as code constants outside `minroar.rs` (test fixture `12346u32` is a spec pin,
  correctly independent of the impl consts).
- **Two set backends, one bitmap.** HASHSET-on-BITMAP delegates through `VmHooks::batch_bitmap_*`
  (`hashset_ops.rs:32-35`, `:132-134`, `:196-200`, `:267-270`) — not a second MiniRoaring. `hash_table::find_insert`
  rejects `EMPTY_KEY`/`TOMBSTONE`; bitmap add skips the same sentinels. Divergence that matters is F2, not membership.
- **No unsafe, no operational unwrap/expect, no `cfg(target_os)`** in the three files. `die!` on missing TTL column is a
  programmer-error invariant. Files are 724/598/303 — not god files. `batch_bitmap_add` is ~132 lines (over the 100-line
  smell; folded into F1 rather than a separate finding).
- **Tests as evidence (neighbor `tests/bitmap.rs`).** `minroar_roaring_cross_compat` / `minroar_ops_match_roaring`
  compare typed `len`/`contains`/`iter` against the roaring crate, both serialize directions, including `optimize()`.
  `roaring_format_spec_fixture_parses` is a hand-built spec buffer, not a rendered-string assertion. Slot algebra is
  checked against `BTreeSet`. Gap: F9's insert-bool at 4096 is untested.
