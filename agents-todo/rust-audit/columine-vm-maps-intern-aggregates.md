# columine-vm/maps+intern+aggregates

Scope: `packages/columine/crates/columine-vm/src/hashmap_ops.rs` (530), `hash_table.rs` (335), `struct_map.rs` (558),
`nested.rs` (543), `intern.rs` (257), `bytes.rs` (97), `aggregates.rs` (468). Targeted greps/reads:
`columine-types/src/types.rs` (`hash_key`, `AggType`, `EMPTY_KEY`, `next_power_of_2`), `columine-types/src/opcodes.rs`
(`AggType` restated), `packages/columine/src/types.ts` (`AggType`, `ComparisonType`), `vm.rs` (hashmap TTL dispatch
reachability), `hashset_ops.rs` (TTL pattern hashmap_ops is missing), `Cargo.toml`, `tests/aggregates.rs`.

## Summary

- CRITICAL: `Strategy::Last`/`First` HASHMAP upserts with TTL never read the timestamp column (`new_cmp` forced to 0);
  single upsert/remove never touch the eviction index at all.
- HIGH: `FlatTable`, `StructMapSlot`/`StructMap2Slot`, and `OuterTable` are three independent open-addressing tables
  that copy the same probe loop; not wrappers.
- HIGH: nested MIN/MAX uses raw `<`/`>` so a first NaN sticks; `aggregates::min_profile`/`max_profile` skip NaN. Live
  policy split.
- HIGH: `StringIntern` treats FNV-1a `0xFFFF_FFFF` as vacant, so that hash never dedups; the type is also unused outside
  tests, so interning does not eliminate copies on the VM path.
- MEDIUM: `AGG_*` discriminants restated vs `AggType`; `read_nested_prefix` rewrites invalid bytes to SUM=1.
- MEDIUM: `ENTRY_TIMESTAMPED` AoS layout is dead; live HASHMAP timestamps are a SoA u64 lane beside `FlatTable`.
- MEDIUM: `StructMapSlot` and `StructMap2Slot` duplicate probe + `write_scalar_field`; nested inner growth swallows
  `insert_key`/`upsert_u32` `None`.
- MEDIUM: aggregates sum proptest oracle is a clone of the kernel (cannot go red under substitution).
- LOW: `field_offset` re-walks field types per write; `Probe` and `cap*7/10` restated; `fill_u32` per-cell copies on
  init.
- Verdict on the asked intern/hash question: intern IDs are sequential, not uniform, so `hash_key` is mixing not
  discarded rehash. Intern grow reuses stored FNV.

## Findings

### F1 — CRITICAL — DUPLICATION — HASHMAP Last/First TTL records 0.0; single/remove skip eviction index

Evidence: `packages/columine/crates/columine-vm/src/hashmap_ops.rs:34-37`, `:188-214`, `:255-261`, `:390-447`,
`:333-385`

```
    pub const fn needs_timestamps(self) -> bool {
        matches!(self, Self::Latest | Self::Max | Self::Min)
    }
    let needs_timestamps = strategy.needs_timestamps();
    ...
        let new_cmp: u64 = if needs_timestamps {
            read_cmp_value(...)
        } else {
            0
        };
            if meta.has_ttl() {
                let ts = if cmp_col.is_some() {
                    cmp_to_f64(new_cmp, cmp_type)
                } else {
                    0.0
                };
                let ttl_result = hooks.insert_with_ttl(state, meta, key, ts);
```

`single_map_upsert` (390-447) never calls `insert_with_ttl`. `batch_map_remove` / `single_map_remove` never call
`remove_ttl_entries_for_key`. Contrast `hashset_ops.rs:239-253` (not owned; pattern only). Problem: "needs timestamps
for Latest/Max/Min compare" was used as the gate for "needs the TTL timestamp". `Strategy::Last` always upserts, so
`new_cmp` is 0; a present `cmp_col` still yields `insert_with_ttl(..., 0.0)`. FOR_EACH / `MAP_UPSERT_LAST_TTL` /
`BatchMapUpsertLast` pass a real ts column that this function then discards. Single-element dispatch never writes the
eviction index. Removes leave stale TTL entries. Live expiry bug, not a comment mismatch. Fix: Read `cmp_col` when
`needs_timestamps || meta.has_ttl()`. Thread that value into `insert_with_ttl` on both batch and single insert/update.
On both remove paths, call `hooks.remove_ttl_entries_for_key` (and capture prev ts for undo, as hashset does). Stop
treating Last's unused cmp lane as a TTL source of zeros. Cost/Risk: `vm.rs` dispatch already passes ts columns for
Last+TTL; once hashmap_ops consumes them, FOR_EACH and batch Last start expiring. Undo/rollback of map+TTL must stay
consistent with the new writes. Tests in `tests/containers.rs` do not cover TTL.

### F2 — HIGH — DUPLICATION — three hash tables, not one table with wrappers

Evidence: `hash_table.rs:202-252` (`FlatTable::find`/`find_insert`); `struct_map.rs:127-180` and `:393-437`
(`StructMapSlot` / `StructMap2Slot`); `nested.rs:235-326` (`OuterTable::resolve`/`lookup`)

```
        let mut pos = hash_key(key, self.cap);
        for _ in 0..self.cap {
            let k = self.key_at(state, pos);
            if k == key { return Some(pos); }
            if k == EMPTY_KEY { return None; }
            pos = (pos + 1) & (self.cap - 1);
        }
```

`struct_map.rs:135-145` is the same loop over `self.capacity`. `nested.rs:245-326` is the same loop plus fused insert.
`hash_table.rs:63-66` and `struct_map.rs:21-24` both define `pub struct Probe { pub pos: u32, pub found: bool }`. Load
factor `cap * 7 / 10` is restated at `hash_table.rs:196-197`, `struct_map.rs:98-99`, `struct_map.rs:368-369`,
`nested.rs:265`. Problem: hashmap_ops is a wrapper around `FlatTable::bind_external` (that part is one table). Nested
outer keys+ptrs is also a headerless u32 map and reimplements probing instead of
`FlatTable::bind_external(keys_off, cap, size_off, ENTRY_U32)`. Struct maps need their own payload (descriptor + rows,
two key lanes) but still copied the probe rather than parameterizing equality. Three (four with intern's FNV table)
probe ABIs to keep in lockstep. Tombstone reuse already differs: `FlatTable::find_insert` returns the first tombstone on
full-table wrap (`hash_table.rs:247-251`); `OuterTable::resolve` returns `None` (`nested.rs:307-309`). Fix: Bind nested
outer as `FlatTable` (SSOT = `hash_table.rs`). Keep struct-map as a separate table but share one probe helper over a
key-eq callback / two-lane adapter. Delete `struct_map::Probe` in favor of `hash_table::Probe`. One
`LOAD_NUM=7, LOAD_DEN=10` const in `hash_table.rs`. Cost/Risk: Probe sequence is observable ABI (comment at
`hash_table.rs:12-14`). Sharing must preserve linear probe, tombstone-skip, first-tombstone reuse, and wrap with
`& (cap-1)`. Nested insert currently fuses alloc; wrap `find_insert` then alloc.

### F3 — HIGH — SSOT — nested MIN/MAX NaN policy diverged from aggregates

Evidence: `aggregates.rs:152-174`; `nested.rs:472-487`

```
fn min_profile(a: f64, b: f64) -> f64 {
    if a.is_nan() { return b; }
    if b.is_nan() { return a; }
    if a < b { a } else { b }
}
        AGG_MIN => {
            let count = bytes::read_u64(state, base + 8);
            let new_val = f64::from_bits(value_bits);
            if count == 0 || new_val < bytes::read_f64(state, base) {
                bytes::write_f64(state, base, new_val);
            }
```

Problem: Nested inners are zeroed (`nested.rs:292`, `458-459`) so the first value always writes, including NaN. The next
finite value does `finite < NaN` → false, so NaN sticks. `min_profile` would replace NaN with the finite. Same split for
MAX (`new_val > existing`). Nested AVG/i64 agg bytes fall through `_ => {}` (`nested.rs:488`) and still set
`SIZE_CHANGED`. Batch kernels have an explicit NaN/tie profile and tests (`tests/aggregates.rs:249-269`); nested does
not. Fix: Call `min_profile`/`max_profile` (pub(crate) them) from `nested_agg_update`. Refuse or implement AVG/i64
nested aggs instead of a silent no-op. SSOT for the FP profile is `aggregates.rs`. Cost/Risk: Any archived nested-agg
state that already stored a leading NaN would change on the next finite update. If nested NaN is specified as "first
write wins", document that as a named exclusion next to the aggregates profile — do not leave two policies.

### F4 — HIGH — STRUCTURE — intern hash sentinel aliases FNV-1a; intern is unused on the VM path

Evidence: `intern.rs:15-26`, `:65-87`, `:92-110`; production callers of `StringIntern::new` / `intern()`: none outside
`intern.rs` tests and `tests/undo_intern.rs`

```
const EMPTY: u32 = 0xFFFF_FFFF;
            let key = self.hash_keys[slot as usize];
            if key == EMPTY {
                return self.insert_new(s, h, slot);
            }
            if key == h { ... content verify ... }
```

`grow_hash` skips `key == EMPTY` (`intern.rs:98-100`), so a live slot whose FNV is `0xFFFF_FFFF` is dropped on rehash.
Problem: Occupancy is encoded as "hash != u32::MAX". A string whose FNV-1a is `0xFFFF_FFFF` inserts, then every later
`intern` of it sees a vacant slot and inserts another copy; growth forgets it. Also: `get` returns `&[u8]` (no clone),
but nothing in `columine-vm` (including `vm.rs`) calls this type. Map keys arrive as already-interned `u32`s. Interning
does not eliminate copies downstream of this crate because there is no downstream. The module comment's "wasm exports in
the bindings layer" has no in-repo consumer. Fix: Store `index.wrapping_add(1)` (0 = vacant) or mix the hash so `EMPTY`
cannot be a stored key. Delete or wire `StringIntern`: if bindings own intern, this module is the SSOT and must be
called; if keys are interned in TS/Arrow, delete this copy. Cost/Risk: Handle values and Arrow `offsets` layout are part
of the intern ABI (`intern.rs:154-163`). Changing occupancy encoding does not change handles. Deleting the module needs
the bindings owner to confirm there is no out-of-tree wasm export.

### F5 — MEDIUM — SSOT — AGG discriminants restated; nested read rewrites invalid bytes to SUM

Evidence: `aggregates.rs:25-32`; `nested.rs:52-57`, `:435-438`; `tests/aggregates.rs:78-80`. Canonical enum:
`columine-types/src/types.rs:220-231` (`AggType::{Sum=1,...,MaxI64=13}`), restated again in `opcodes.rs:121-133` and
`packages/columine/src/types.ts:24-39`.

```
        inner_agg_type_byte: if (1..=13).contains(&agg_byte) {
            agg_byte
        } else {
            1
        },
```

Problem: `aggregates.rs` documents why it cannot go through `AggType::from_u8` (would rewrite invalid bytes).
`read_nested_prefix` then does the rewrite anyway (out-of-range → 1/SUM). Range `1..=13` also admits 6 and 7, which are
not `AggType` variants; those hit nested `_ => {}`. Tests restate `AGG_COUNT=2` etc. Three integer tables plus two
enums. Fix: SSOT = `AggType` in `columine-types`. Aggregates keep raw-byte init for the documented pass-through, but the
named discriminants should be `AggType::Sum as u8` (or associated consts on the enum). Nested prefix must not coerce
invalid → SUM; fail closed (`die!` on invariant, or keep the raw byte and let update no-op without claiming SUM).
Cost/Risk: ColTypes owns the enum. Nested states already written with garbage agg bytes currently behave as SUM;
fail-closed would surface those.

### F6 — MEDIUM — STRUCTURE — ENTRY_TIMESTAMPED is a dead second timestamped-map layout

Evidence: `hash_table.rs:27`, `:40-42`, `:158-175`; `hashmap_ops.rs:3-7`, `:123-126`

```
pub const ENTRY_TIMESTAMPED: u32 = 16; // TimestampedMap
//! `[keys: u32 × cap][values: u32 × cap][cmp/timestamps: u64 × cap]`.
    pub const fn cmp_lane_off(meta: &SlotMetaView) -> u32 {
        meta.offset + meta.capacity * 8
    }
```

`ts_entry_at` / `set_ts_entry_at` / `timestamped_map_byte_size` have no production callers (only `tests/containers.rs`
asserts the byte-size formula). Problem: Two layouts for "map plus timestamp": AoS `{value:u32, pad:u32, ts:f64}` inside
`FlatTable` entries, and the live SoA cmp lane beside an `ENTRY_U32` table. The AoS form is leftover ABI surface. Fix:
Delete `ENTRY_TIMESTAMPED`, `timestamped_map_byte_size`, `ts_entry_at`, `set_ts_entry_at`. HASHMAP timestamps stay the
SoA lane in `hashmap_ops`. Cost/Risk: If a TS/Zig backend still sizes TimestampedMap as 16-byte entries, that backend is
the other copy — confirm with the vm/types slices before delete. In this crate the accessors are unreferenced.

### F7 — MEDIUM — DUPLICATION — StructMapSlot and StructMap2Slot copy probe + scalar write

Evidence: `struct_map.rs:127-180` vs `:393-437`; `:244-285` vs `:487-517`

```
            StructFieldType::UInt32 | StructFieldType::String => {
                let v = bytes::read_u32(col, element_idx * 4);
                bytes::write_u32(state, f_off, v);
            }
```

Map2 collapses Int64|Float64 into one arm (`:505-507`) but is otherwise the same function. `bind` metadata reuse (byte
13 = num_fields, 15 = bitset, 16-17 = row_size) is copied (`:57-83` vs `:329-356`). Problem: Comment at `:304-305`
justifies a separate slot kind for layout (two key lanes, lane-1 sentinels). That does not require a second copy of
find/find_insert/upsert/field_offset/write_scalar_field. `field_offset` (`:189-195`, `:475-481`) re-decodes every
preceding field type on every write (Byproduct L0 / §7.7): bind-time prefix sums are closed form over `num_fields`. Fix:
One probe over a `Key` adapter (`u32` vs `(u32,u32)`). One `write_scalar_field`. At `bind`, fill `[u32; MAX_FIELDS]`
field offsets once. `StructMap2Slot` keeps only keys2_off + pair hash. Cost/Risk: Layout bytes stay as they are. Callers
of `StructMap2Slot::{find,upsert,write_scalar_field}` move to the shared helpers.

### F8 — MEDIUM — STRUCTURE — nested inner growth swallows None; arena sized only for initial inners

Evidence: `nested.rs:94-106`, `:364-382`, `:413-429`

```
    NESTED_PREFIX_SIZE + outer_cap * 4 + outer_cap * 4 + ARENA_HDR_SIZE + outer_cap * per_inner
    let _ = grown.insert_key(state, elem);
    ...
    let was_new = grown.upsert_u32(state, inner_key, value).unwrap_or(false);
```

Problem: `nested_slot_data_size` reserves exactly one inner at `inner_initial_cap` per outer cell (L4 closed form for
the initial shape only). First inner rehash needs another `hashset/hashmap_byte_size(2*cap)` from the same arena;
leftover slack is only the unused 30% load. `insert_key`/`upsert_u32` returning `None` (sentinel key, or 2× still over
load) is treated as success: set `INSERTED`/`UPDATED`, return `Ok`. Sentinel outer keys already return
`CapacityExceeded` via `resolve`; sentinel inner keys do not. Fix: After grow, if insert/upsert is still `None`, return
`CapacityExceeded` (do not `let _ =`). Size the arena for one growth step if growth is part of the contract
(`outer_cap * (per_inner + per_inner_2x)`), or refuse inner growth and surface `NeedsGrowth` to the slot grower. SSOT
for size is `nested_slot_data_size`. Cost/Risk: Larger nested slots, or earlier capacity errors that today silently
"succeed". Slot-growth code (other slice) must agree on who grows inners.

### F9 — MEDIUM — TESTS — sum proptest oracle is a clone of the kernel

Evidence: `tests/aggregates.rs:275-290`, `:307-316`; kernel `aggregates.rs:196-213`

```
fn ref_lane_sum(vals: &[f64]) -> f64 {
    let chunks = vals.len() / 4;
    let mut l = [0.0f64; 4];
    for c in 0..chunks {
        for k in 0..4 { l[k] += vals[c * 4 + k]; }
    }
    let mut r = ((l[0] + l[1]) + l[2]) + l[3];
```

Problem: PERFORMANCE-HANDBOOK §7.10bb: substituting `batch_agg_sum` for `ref_lane_sum` keeps the test green. The
proptest cannot catch a shared lane-order bug. (The min proptest against a sequential `<` fold on NORMAL floats is an
independent oracle; keep that shape.) Test names `batch_agg_sum_f64_simd_reduction` / `masked_agg_count_simd`
(`tests/aggregates.rs:24-53`) assert numeric results of scalar 4-accumulators, not SIMD emission. Fix: Sum oracle =
sequential left-to-right fold (different association than the 4-lane split) plus a separately pinned lane-order vector
for the FP profile, or delete the cloned-lane oracle and keep only the sequential one with an explicit "lane order is
the profile" unit pin (already done for min/NaN at `:249-269`). Rename `*_simd_*` tests. Cost/Risk: Sequential vs 4-lane
sum will disagree on cancellation-prone inputs; that disagreement is the point of the documented profile
(`aggregates.rs:9-16`). Pin the lane result as the profile, not as equality with a clone.

### F10 — LOW — COPIES — field_offset re-walks types on every scalar write

Evidence: `struct_map.rs:189-195`, `:253-255`, `:475-481`, `:496-498`

```
    pub fn field_offset(&self, state: &[u8], field_idx: u8) -> u32 {
        let mut off = self.bitset_bytes;
        for i in 0..field_idx {
            off += struct_field_size(self.field_type(state, i));
        }
```

Regime: per field write on the struct-map upsert path (interpreter hot loop if programs write many fields per row).
`num_fields` is small, but each step re-validates a field-type byte (`field_type` → `from_u8` → `die!` on garbage).
Bind-time offsets are closed form (L4). Fix: compute a `[u32; 256]` or a small boxed slice of prefix offsets in `bind`;
`write_scalar_field` indexes it. Cost/Risk: `StructMapSlot` grows by one pointer/len; bind is once per op, writes are
many.

### F11 — LOW — SSOT — Probe, load factor, intern EMPTY restated

Evidence: `hash_table.rs:63-66` vs `struct_map.rs:21-24`; `hash_table.rs:196-197` vs `struct_map.rs:98-99` vs
`nested.rs:265`; `intern.rs:15` (`EMPTY = 0xFFFF_FFFF`) vs `columine-types` `EMPTY_KEY = u32::MAX`. Problem: Same
values, three spellings. Intern's `EMPTY` happens to equal `EMPTY_KEY` but is a hash occupancy flag, not a key sentinel
— the coincidence is F4's bug. Fix: covered by F2/F4. `intern` should not import `EMPTY_KEY` for hash occupancy.

### F12 — LOW — COPIES — fill_u32 per-cell copy_from_slice; intern buffers grow under load

Evidence: `bytes.rs:51-56`; `intern.rs:45-59`, `:113-123`

```
    for i in 0..count as usize {
        buf[start + i * 4..start + i * 4 + 4].copy_from_slice(&bytes);
    }
```

Regime: `fill_u32` is table init/rehash (once per grow), not probe. `StringIntern::new` zero-fills `cap×32` data plus
two hash vecs; `insert_new` resizes data/offsets on demand (L4 growth). Intern is not on the VM event path today (F4).
`fill_u32` also panics via slice index instead of `oob()` (`bytes.rs:15-19`), unlike `read_u32`/`zero`/`copy`. Fix: For
`EMPTY_KEY == u32::MAX`, `buf[range].fill(0xFF)`. Route OOB through `oob()`. Intern: if kept, take a closed-form data
budget at `new` and fail admission instead of doubling in `insert_new`. Cost/Risk: Init-only; do not spend a hot-path
campaign on it.

## Cross-slice questions

- `packages/columine/crates/columine-vm/src/vm.rs` (ColVmCore): `Opcode::BatchMapUpsertLast` (`:2740-2763`) and FOR_EACH
  `0x22`/`0x25` (`:3605-3657`) pass a timestamp column into `batch_map_upsert`/`single_map_upsert` with
  `Strategy::Last`. Confirm that is the intended TTL path; F1 says hashmap_ops then zeros or ignores it.
- `packages/columine/crates/columine-vm/src/hashset_ops.rs`: single insert calls `insert_with_ttl` (`:239-253`). Is
  hashmap supposed to match, or is HASHMAP TTL only via Latest? Opcodes `BatchMapUpsertLastTtl` / `MAP_UPSERT_LAST_TTL`
  exist either way.
- `columine-types` `AggType` (`types.rs:220-231` and a second copy in `opcodes.rs:121-133`): F5 wants this as SSOT for
  nested/aggregates raw bytes.
- `packages/columine/src/types.ts:23-24,88` still says "Must match Zig AggType/CmpType in vm.zig / hashmap_ops.zig".
  Rust `hashmap_ops::CmpType` and `AggType` are the live contract; TS `ComparisonType` is a hand restatement.
- Bindings/wasm intern exports: F4 found no in-repo `StringIntern` consumer. If another crate owns `intern_*` wasm, that
  crate should be the only caller.

## Non-findings (checked, clean)

- DEP-BLOAT: `columine-vm` depends only on `columine-types`. `roaring`/`proptest` are dev-dependencies with an explicit
  "must never re-enter the shipped artifact" comment. No git2/openssl/napi/sqlite in this crate.
- Byproduct L0 on map keys: intern IDs are dense sequential `0..n`, not uniform hashes. `hash_key` (murmur-style mix
  then `& (cap-1)` in types.rs) is placement mixing, not a discarded second hash of an already-uniform key. Intern
  `grow_hash` re-probes stored FNV and does not rehash bytes.
- `intern::get` returns `&[u8]`; `intern` copies into `data` only on first insert. No post-intern `.to_owned()` in this
  crate (no callers).
- `bytes.rs`: LE copy accessors, single `#[cold] oob()` panic funnel, no `unsafe`. OOB is treated as a programmer
  invariant, which matches the file comment.
- No god files (largest 558). No `unsafe`. `die!`/`assert` in intern/bytes/nested prefix are invariant paths, not
  operational `Result`.
- Aggregates 4-wide loops are independent accumulators (§7.5 ILP), with a documented left-to-right lane fold for FP
  determinism — not evaporating work.
- `FlatTable` itself is a sound offset view (no pointers into state). hashmap_ops using `bind_external` + SoA cmp lane
  is the right wrapper shape; the defect is TTL gating (F1), not the table.
- intern unit tests pin FNV reference vectors, the costarring/liquid collision pair, Arrow offsets, and handle stability
  across growth — those can go red.
