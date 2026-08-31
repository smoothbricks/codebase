# lmao-arena

Scope: `packages/lmao/crates/lmao-arena/src/lib.rs` (284), `packages/lmao/crates/lmao-arena/src/raw.rs` (905),
`packages/lmao/crates/lmao-arena/Cargo.toml` (19), `packages/lmao/crates/lmao-arena/benches/arena.rs` (102). TESTS axis
also read `packages/lmao/crates/lmao-arena/tests/properties.rs` (470). Targeted greps:
`packages/lmao/crates/lmao-wasm/src/lib.rs`, `packages/lmao/crates/lmao-core/src/entry_type.rs`,
`packages/lmao/crates/lmao-core/src/tuning.rs`, `packages/lmao/src/lib/wasm/wasmAllocator.ts`,
`packages/lmao/src/lib/schema/systemSchema.ts`, `packages/lmao/Cargo.lock`.

## Summary

- Dual ABI layout: `repr(C)` structs pin _sizes_; a second offset table actually drives IO; `H_FREELIST_EXACT` is
  invisible to `Header`.
- Every alloc, including freelist reuse, zeros the block with a per-byte `write_u8` loop (warm path).
- Native `VecMem::grow_to` doubles-and-copies the whole region under load and never returns the OOM sentinel the trait
  documents.
- `capacity_to_tier` is `debug_assert`-only; a 1024-row capacity aliases the next size-class freelist.
- TS `SizeClass.Identity = 4` is not a Rust size class; wasm maps unknown `sc` to `Col8B`.
- Public surface is far larger than the only production dependent (`lmao-wasm`).
- Buddy free walks every freelist node (neighbor + double-free); bump is 8-byte, not 64-byte.
- Overlap property measures `block_size`, not the physical extent `effective_block_size`.
- Zero production deps. No `unsafe`. Cascading head-node stats are ABI, not dead.

## Findings

### F1 — HIGH — SSOT — Header layout is two sources; exact-freelist head is only in one

Evidence: `packages/lmao/crates/lmao-arena/src/lib.rs:45-60` + `packages/lmao/crates/lmao-arena/src/raw.rs:46-57` +
`packages/lmao/crates/lmao-arena/src/lib.rs:91-94`

```45:60:packages/lmao/crates/lmao-arena/src/lib.rs
/// Arena header at offset 0; field order and padding are ABI, verified by the
/// const asserts below.
#[repr(C)]
#[derive(Debug)]
pub struct Header {
    pub bump_ptr: u32,
    pub span_id_counter: u32,
    pub alloc_count: u32,
    pub free_count: u32,
    pub freelist_identity: u32,
    _pad0: u32, // align thread_id to 8
    pub thread_id: u64,
    pub freelists: [u32; NUM_FREELISTS],
    pub thread_id_set: u8,
    _reserved: [u8; 47],
}
```

```46:57:packages/lmao/crates/lmao-arena/src/raw.rs
const H_BUMP_PTR: u32 = 0;
const H_SPAN_ID_COUNTER: u32 = 4;
const H_ALLOC_COUNT: u32 = 8;
const H_FREE_COUNT: u32 = 12;
const H_FREELIST_IDENTITY: u32 = 16;
const H_THREAD_ID: u32 = 24;
const H_FREELISTS: u32 = 32; // [u32; 28]
const H_THREAD_ID_SET: u32 = 144;
/// Head of the exact-block freelist. This occupies bytes 148..152 of the
/// header's existing reserved tail; the header remains 192 bytes.
const H_FREELIST_EXACT: u32 = 148;
```

Problem: IO never uses `Header`/`Identity`/`TraceRoot`/`FreeBlock` (no `offset_of!`, no construction). Const asserts
only check `size_of`. Reordering a field keeps size 192 and silently desynchronizes `H_*`. `H_FREELIST_EXACT` lives
inside `_reserved` with no named field, so the struct the crate advertises as the ABI cannot represent the exact-slab
freelist at all. Same pattern for `FB_*` vs `FreeBlock` and `ID_*` vs `Identity`. Fix: Delete the unused structs _or_
make them the only source: named `freelist_exact: u32` in `Header`, then
`const _: () = assert!(offset_of!(Header, bump_ptr) == H_BUMP_PTR as usize)` (and every other field). I would take the
struct+`offset_of!` path and drop the hand-written `H_*` numbers. Cost/Risk: `raw.rs` offset constants and any host that
documents the 192-byte reserved tail. Field order is ABI with TS/wasm linear memory.

### F2 — HIGH — COPIES — Every alloc zeros via a per-byte `write_u8` loop

Evidence: `packages/lmao/crates/lmao-arena/src/raw.rs:299-305` + `packages/lmao/crates/lmao-arena/src/raw.rs:350-357` +
`packages/lmao/crates/lmao-arena/benches/arena.rs:12-22`

```299:305:packages/lmao/crates/lmao-arena/src/raw.rs
fn clear_bytes<M: Mem>(m: &mut M, offset: u32, len: u32) {
    debug_assert_ne!(offset, 0);
    for byte_offset in offset..offset + len {
        m.write_u8(byte_offset, 0);
    }
}
```

```350:357:packages/lmao/crates/lmao-arena/src/raw.rs
pub fn alloc_with_capacity<M: Mem>(m: &mut M, sc: SizeClass, capacity: u32) -> u32 {
    let size = effective_block_size(sc, capacity);
    let offset = alloc_at_tier(m, sc, effective_tier(sc, capacity_to_tier(capacity)));
    if offset != 0 {
        clear_bytes(m, offset, size);
    }
    offset
}
```

Problem: Regime is the **warm freelist-reuse path** (`alloc_free_reuse_span_system_64`), not startup. A 64-row
`SpanSystem` block is 576 bytes → 576 virtual calls per alloc, including reuse. `alloc_exact` / identity alloc do the
same. Zero-on-alloc is load-bearing (`recycled_column_clears_validity_and_value_bytes`); the stupidity is the mechanism.
`Mem` has no `fill`. Fix: Add `Mem::fill(off, len, byte)` (default-loop ok). `VecMem` implements with
`self.0[range].fill(0)`. Wasm backend (other slice) uses a slice/memset over linear memory. Keep the zero-on-alloc
policy. Cost/Risk: `Mem` trait change; `lmao-wasm` `WasmMem` must implement it. Tests that scribble then recycle stay
valid.

### F3 — HIGH — COPIES — Native arena grows under load and cannot return the OOM sentinel

Evidence: `packages/lmao/crates/lmao-arena/src/lib.rs:128-154` + `packages/lmao/crates/lmao-arena/src/raw.rs:179-185`

```128:154:packages/lmao/crates/lmao-arena/src/lib.rs
/// `Vec<u8>`-backed [`raw::Mem`] — the native linear-memory backend. Growth is
/// Vec doubling (bounded below by the requested size); the wasm backend in
/// `lmao-wasm` implements the same trait over `memory.grow` pages.
...
    fn grow_to(&mut self, new_size: u32) -> bool {
        let target = (new_size as usize).max(self.0.len().saturating_mul(2));
        self.0.resize(target, 0);
        true
    }
```

```179:185:packages/lmao/crates/lmao-arena/src/raw.rs
    let size = block_size(sc, tier_to_capacity(tier));
    let aligned = (m.read_u32(H_BUMP_PTR) + 7) & !7u32;
    let new_bump = aligned + size;
    if new_bump > m.size() && !m.grow_to(new_bump) {
        return 0; // OOM sentinel
    }
```

Problem: Byproduct L4/L7: capacities of _blocks_ are closed-form (`block_size`); the _region_ is not reserved. Growth
copies the entire linear image (Vec realloc). `grow_to` always returns `true`; native OOM panics inside `resize` instead
of the documented `false` → alloc `0`. Wasm can fail closed; native cannot. Regime: not per-alloc once the caller
reserved (benches use `1 << 22`); it _is_ the path `arena_grows_on_demand` requires, starting at `HEADER_SIZE`. Fix:
Native `grow_to` resizes to `new_size` (no doubling) and returns `false` if the request cannot be met (try_reserve +
set_len, or a fixed ceiling passed at `Arena::new`). Prefer one reservation at open: caller/host supplies the
closed-form ceiling; bump past it returns 0. Do not grow under load. Cost/Risk: Native-fallback wasm tests
(`VecMem::with_zeroed(1 << 20)`) and any host that relied on silent doubling. Wasm `memory.grow` stays the page
analogue.

### F4 — HIGH — STRUCTURE — Unchecked `capacity_to_tier` aliases another size-class freelist

Evidence: `packages/lmao/crates/lmao-arena/src/lib.rs:96-106` + `packages/lmao/crates/lmao-arena/src/raw.rs:103-106` +
`packages/lmao/crates/lmao-core/src/tuning.rs:15-16`

```96:106:packages/lmao/crates/lmao-arena/src/lib.rs
pub fn capacity_to_tier(capacity: u32) -> usize {
    debug_assert!(capacity.is_power_of_two() && (MIN_CAPACITY..=MAX_CAPACITY).contains(&capacity));
    (capacity.trailing_zeros() - MIN_CAPACITY.trailing_zeros()) as usize
}
```

```103:106:packages/lmao/crates/lmao-arena/src/raw.rs
fn freelist_off(sc: SizeClass, tier: usize) -> u32 {
    H_FREELISTS + 4 * (sc as u32 * NUM_TIERS as u32 + tier as u32)
}
```

Problem: Release builds accept any `u32`. Capacity 1024 (legal in `lmao-core` ratchet, `MAX_CAPACITY = 1024`) yields
tier `10-3 = 7`. `NUM_TIERS` is 7 (valid 0..=6). `freelist_off(SpanSystem, 7)` writes index 7 = `Col1B` tier 0.
Non-power-of-two underflows `trailing_zeros` subtraction into a huge `usize`. Comment at `lib.rs:22-24` forbids matching
the two maxima, then leaves the door open. Fail-closed belongs at this boundary (§7.7), not a debug_assert. Fix: Return
a sentinel / refuse to alloc when `capacity` is not in `{8,16,32,64,128,256,512}`. Match on those seven values (or a
4-bit LUT). Do not compute `trailing_zeros` against caller input. Cost/Risk: Every `alloc_with_capacity` / `write_col_*`
/ wasm export that forwards `capacity`. Current TS default is 64; this is a landmine, not a demonstrated production 1024
path.

### F5 — MEDIUM — SSOT — TS `SizeClass.Identity = 4` is not a Rust size class

Evidence: `packages/lmao/crates/lmao-arena/src/lib.rs:35-43` + `packages/lmao/src/lib/wasm/wasmAllocator.ts:133-138` +
`packages/lmao/crates/lmao-wasm/src/lib.rs:130-136`

```35:43:packages/lmao/crates/lmao-arena/src/lib.rs
#[repr(u8)]
pub enum SizeClass {
    SpanSystem = 0,
    Col1B = 1,
    Col4B = 2,
    Col8B = 3,
}
```

```133:138:packages/lmao/src/lib/wasm/wasmAllocator.ts
export enum SizeClass {
  SpanSystem = 0,
  Col1B = 1,
  Col4B = 2,
  Col8B = 3,
  Identity = 4,
}
```

Problem: Identity is a separate header freelist (`H_FREELIST_IDENTITY`), not size-class 4. wasm `size_class` maps
`_ => Col8B`, so discriminant 4 would read Col8B stats. I found no TS call site passing `SizeClass.Identity` (enum
member only). Dormant, but the copies already disagree. Fix: Rust `SizeClass` is the SSOT (4 variants). Delete
`Identity = 4` from TS. If wasm needs identity-freelist stats, add a dedicated export, not a fake size class. Cost/Risk:
TS `WasmAllocator` stats API; wasm `size_class` catch-all (other slice).

### F6 — MEDIUM — SSOT — Span entry-type discriminants restated beside `lmao-core` and TS

Evidence: `packages/lmao/crates/lmao-arena/src/raw.rs:85-89` +
`packages/lmao/crates/lmao-core/src/entry_type.rs:10-14` + `packages/lmao/src/lib/schema/systemSchema.ts:218-228`

```85:89:packages/lmao/crates/lmao-arena/src/raw.rs
pub const ENTRY_TYPE_SPAN_START: u8 = 1;
pub const ENTRY_TYPE_SPAN_OK: u8 = 2;
pub const ENTRY_TYPE_SPAN_ERR: u8 = 3;
pub const ENTRY_TYPE_SPAN_EXCEPTION: u8 = 4;
```

Problem: Values currently agree (`1..=4`). They are also the first four of `lmao-core::EntryType` and `systemSchema.ts`.
Arena stays dep-free, so a copy is the isolation cost — but the constants are `pub` and will drift independently.
`lmao-timestamp-proof` repeats them again (other slice). Fix: Keep numeric literals private to `raw.rs` (only
`span_start`/`span_end` need them). Do not export a second enum. SSOT for the 24-wide table is `lmao-core::EntryType` /
`systemSchema.ts`. Do not add a `lmao-core` dependency to this crate. Cost/Risk: wasm tests and this crate's properties
that name `raw::ENTRY_TYPE_*`.

### F7 — MEDIUM — STRUCTURE — Public surface is not the wasm minimum

Evidence: `packages/lmao/crates/lmao-wasm/src/lib.rs:24-27` +
`packages/lmao/crates/lmao-arena/src/lib.rs:49-89,181-256` + `packages/lmao/Cargo.lock:1707-1713,1776-1778`

Problem: The only non-dev dependent is `lmao-wasm`. It imports `SizeClass`, `raw::{self, Mem}`, and (native fallback)
`VecMem::with_zeroed`. It never uses `Arena`, `Header`, `Identity`, `TraceRoot`, `FreeBlock`, `Offset`, `block_size`,
`capacity_to_tier`, `tier_to_capacity`, `MIN_CAPACITY`/`MAX_CAPACITY`/`NUM_*`, or `Arena::is_empty`. Those types are
size-assert / test-bench API leaked `pub`. `Arena::new` duplicates `VecMem::with_zeroed` (`lib.rs:134-196`). Fix:
`pub use` only what wasm needs (`Mem`, `VecMem`, `SizeClass`, `raw`). Demote `Arena` to `pub(crate)` or tests. Delete
`is_empty` (always `false` at `lib.rs:237-238`). Route `Arena::new` through `VecMem::with_zeroed` if `Arena` stays.
Cost/Risk: this crate's benches/tests (`Arena`, `block_size`). No wasm change if re-exports remain.

### F8 — MEDIUM — COPIES — Free path is two O(n) freelist walks plus a cloned merge function

Evidence: `packages/lmao/crates/lmao-arena/src/raw.rs:197-235` + `packages/lmao/crates/lmao-arena/src/raw.rs:260-323`

```197:215:packages/lmao/crates/lmao-arena/src/raw.rs
fn free_at_tier<M: Mem>(m: &mut M, offset: u32, sc: SizeClass, tier: usize) {
    if tier + 1 < NUM_TIERS {
        let size = block_size(sc, tier_to_capacity(tier));
        let right = offset + size;
        if find_and_remove_by_offset(m, sc, tier, right) {
            free_at_tier_with_merge(m, offset, sc, tier + 1);
            return;
        }
        ...
    }
    push_to_freelist(m, offset, sc, tier, false);
}
```

```307:323:packages/lmao/crates/lmao-arena/src/raw.rs
fn capacity_block_is_free<M: Mem>(m: &M, offset: u32, sc: SizeClass) -> bool {
    for tier in 0..NUM_TIERS {
        let size = u64::from(block_size(sc, tier_to_capacity(tier)));
        let mut current = freelist_head(m, sc, tier);
        while current != 0 {
            ...
            current = m.read_u32(current + FB_NEXT_PTR);
        }
    }
    false
}
```

Problem: Regime = every `free_with_capacity`. Neighbor merge cannot be XOR-buddy (column sizes are not 2^n because of
the null-bitmap prefix), so address adjacency is right — but locating the neighbor is a linear scan, then double-free
protection scans **all seven tiers** again. `free_at_tier` and `free_at_tier_with_merge` are byte-identical except the
final `is_merge` flag. Cascading 4×u32 head stats are ABI (`get_freelist_*` wasm exports) — leave them; they are not
this finding. Fix: One `free_at_tier(..., is_merge: bool)`. Replace the neighbor scan with a per-tier occupancy
structure whose size is closed-form in `NUM_TIERS` (bitmap / sorted offset). Double-free: a generation in the overlay,
or the same occupancy bit, not a second walk. Cost/Risk: buddy conservation properties; wasm debug stats stay on the
overlay head.

### F9 — MEDIUM — COPIES — Bump alignment is 8 bytes, not the 64-byte line convention

Evidence: `packages/lmao/crates/lmao-arena/src/raw.rs:179-188` + `packages/lmao/crates/lmao-arena/src/lib.rs:8,30`

```179:181:packages/lmao/crates/lmao-arena/src/raw.rs
    // Bump allocate, 8-byte aligned, growing memory as needed.
    let size = block_size(sc, tier_to_capacity(tier));
    let aligned = (m.read_u32(H_BUMP_PTR) + 7) & !7u32;
```

Problem: Header is 192 B = 3 cache lines (good, and first bump at 192 is 64-aligned). After a 72-byte `SpanSystem`/cap-8
block, bump is 264 (264 % 64 = 8). Subsequent identity (128 B) and column blocks straddle extra lines. `alloc_exact`
uses `alignment.max(4)`, so 1-byte requests land 4-aligned. Repo convention is 64 B lines / padding-as-arithmetic. Fix:
Align bump to 64 for the identity/span superblock path (`alloc_identity_block`, `alloc_exact(..., 8)` → 64). Column
classes can stay 8 if packing density wins — measure; do not guess. Header stays 192. Cost/Risk: every offset in linear
memory (TS host, Arrow views). Waste up to 63 B per bump. ABI-visible.

### F10 — MEDIUM — TESTS — Overlap property cannot see the col_1b over-provisioned tail

Evidence: `packages/lmao/crates/lmao-arena/tests/properties.rs:29-45` +
`packages/lmao/crates/lmao-arena/src/raw.rs:325-347`

```29:45:packages/lmao/crates/lmao-arena/tests/properties.rs
    fn allocated_blocks_do_not_overlap(
        allocs in prop::collection::vec((size_class_strategy(), capacity_strategy()), 1..200),
    ) {
        ...
            let size = block_size(sc, cap);
            for &(o, s) in &live {
                prop_assert!(off + size <= o || off >= o + s, "blocks overlap");
            }
            live.push((off, size));
```

Problem: `col_1b` cap 8/16 allocate `effective_block_size` 36 B (`raw.rs:331`) but the property records `block_size`
9/18. Overlap in `[off+block_size, off+effective)` cannot go red (§7.10bb). Buddy tests _do_ use `effective_block_size`;
this one does not. Fix: Record `raw::effective_block_size(sc, cap)` (and still assert `off >= HEADER_SIZE`). Cost/Risk:
this one property. May need a larger arena if physical extents now collide in the generator — they should not if the
allocator is correct.

### F11 — MEDIUM — STRUCTURE — `write_log_entry` / `span_start` do not bound `capacity` or `write_index`

Evidence: `packages/lmao/crates/lmao-arena/src/raw.rs:765-811`

```796:811:packages/lmao/crates/lmao-arena/src/raw.rs
pub fn write_log_entry<M: Mem>(...) -> u32 {
    let idx = m.read_u32(identity_ptr + ID_WRITE_INDEX);
    let ts = timestamp_nanos(m, trace_root_ptr, current_ms);
    m.write_i64(system_ptr + idx * 8, ts);
    m.write_u8(system_ptr + capacity * 8 + idx, entry_type);
    m.write_u32(identity_ptr + ID_WRITE_INDEX, idx + 1);
    idx
}
```

Problem: No `idx < capacity` check. Native panics on OOB slice; wasm corrupts adjacent linear memory. `span_start`
assumes `capacity >= 2`. Operational, not an invariant the type system holds. Bench `write_log_entry_50` uses cap 64 and
50 logs, so it cannot catch overflow. Fix: If `idx >= capacity` or `capacity < 2`, return 0 / no-op (same sentinel
convention as alloc). Fail closed at this boundary. Cost/Risk: wasm `write_log_entry` export; TS host that already
checks `_writeIndex` would be unaffected.

### F12 — LOW — DUPLICATION — Three column writers and two identity-init paths repeat one shape

Evidence: `packages/lmao/crates/lmao-arena/src/raw.rs:816-880` + `packages/lmao/crates/lmao-arena/src/lib.rs:111-118`

```816:835:packages/lmao/crates/lmao-arena/src/raw.rs
pub fn write_col_f64<...>(...) -> u32 {
    let offset = if col_offset == 0 {
        alloc_with_capacity(m, SizeClass::Col8B, capacity)
    } else { col_offset };
    ...
    let null_bitmap_size = (capacity + 7) >> 3;
    m.write_f64(offset + null_bitmap_size + row_idx * 8, value);
    set_valid_bit(m, offset, row_idx);
    offset
}
```

Problem: `write_col_u32` / `write_col_u8` are the same function with a different `SizeClass` and store width.
`(capacity + 7) >> 3` is restated in `block_size` and every column IO. Not hot enough to matter vs F2; it is a second
copy of the layout formula. Fix: `null_bitmap_bytes(capacity)` next to `block_size`. One generic
`write_col<T: Store>(sc, width)`. Cost/Risk: three wasm exports stay as thin wrappers.

## Cross-slice questions

- `lmao-wasm` `size_class` catch-all (`crates/lmao-wasm/src/lib.rs:130-136`) maps discriminant 4 to `Col8B`. If that
  slice owns the mapping, F5's fix lives there.
- `lmao-core` `MAX_CAPACITY = 1024` (`crates/lmao-core/src/tuning.rs:16`) vs this crate's 512. Comment claims
  intentional. Confirm no native path feeds the ratchet into `Arena::alloc` / `write_col_*`.
- `lmao-timestamp-proof/src/layout.rs` restates `ENTRY_TYPE_SPAN_*` (F6). That crate should import
  `lmao-core::EntryType`, not this crate's copies.
- `Mem::fill` (F2) must be implemented on `WasmMem` in `lmao-wasm`.

## Non-findings (checked, clean)

- Production deps: none. Lockfile lists only `criterion` + `proptest` as this crate's deps. No git2/openssl/napi/sqlite
  analogue. Dev-only, does not leak into the wasm artifact.
- No `unsafe` in this crate. `try_into().unwrap()` on a 4/8-byte window is an invariant (slice length), not operational
  failure.
- `MAX_CAPACITY` 512 vs core 1024 is explicitly not-a-bug in `lib.rs:22-24` (the bug is the unchecked conversion, F4).
- Cascading freelist stats (4 u32s copied into the new head) are the TS/wasm debug ABI (`get_freelist_len` etc.). Do not
  delete; they are not evaporating if the host reads them.
- `effective_tier` clamp for `col_1b` cap 8/16 is documented and property-tested (`prop_assume` on split families).
- `reset` leaking live blocks is stated.
- OOM/null sentinel `0` is consistent (header lives at 0).
- Benches: kernels write linear memory and `black_box` a result; `alloc_free_reuse_*` and `identity_alloc_free` are
  warmed. Not a §4.2b empty cell. `tier_churn_split_merge` is cold on the first iter only (criterion amortizes).
  Criterion uses the bench profile (PH-4.1).
- Property tests assert typed offsets/counters/entry types, not rendered strings. Packed-span tests pin `1_002_000_000`
  as a timestamp value (the formula under test).
- `Header` 192 B = 3 lines; `Identity` 128 B; `FreeBlock` 20 B with size assert against `FB_MERGE_COUNT`.
- Functions stay well under 100 lines; `raw.rs` is 905, not a 5k god file.
- `init` is idempotent; clocks are passed in (deterministic).
