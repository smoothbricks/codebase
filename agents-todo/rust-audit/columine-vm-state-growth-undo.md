# columine-vm/state+growth+undo

Scope: `packages/columine/crates/columine-vm/src/state_init.rs` (1602), `slot_growth.rs` (306), `undo_log.rs` (349),
`meta.rs` (164), `hooks.rs` (167). Doctrine: `BYPRODUCT-ENGINEERING.md`, `docs/handbook/04-mechanisms.md`,
`05-memory-toolkit.md`, `02-measurement.md` §4.1. Targeted greps: `SlotMetaOffset`, `AggType`,
`MutationOp`/`FlatUndoOp`, `NEEDS_GROWTH`, `EVICTION_ENTRY_SIZE`, `nested_slot_data_size`, `ProgramHeader`,
`is_array_field_type`, `hashmap_byte_size`. Neighbor confirmation (not audited): `vm.rs` Nested `CapacityExceeded` →
`NEEDS_GROWTH`; `nested.rs` `nested_slot_data_size` / outer+arena overflow.

## Summary

- CRITICAL: `SlotType::Nested` has no growth size/copy path; `slot_data_size` returns 0 and `grow_state` memcpy's 0
  bytes, including for non-grown Nested siblings — live data loss when any slot grows.
- HIGH: Host-level 2× `grow_state` under load (Byproduct L4). SLOT_DEF capacity is already closed-form
  (`next_power_of_2(cap*2)`); growth is unbounded retry, not an admission ceiling.
- HIGH: Hash/bitmap/agg/scalar/array/condition-tree byte formulas are restated in `calculate_state_size`, `init_state`,
  and `slot_data_size`; the last is unused by the first two.
- HIGH: Three independent init-bytecode walkers (`validate_init_code`, `calculate_state_size`, `init_state`) plus
  400-line `init_state`/`grow_state`.
- MEDIUM: Slot-meta layout is a second source of truth: raw `+12/+13/+16/+18/+20` and flag masks `0x10/0x20/0x40` beside
  `SlotMetaOffset` / `SlotTypeFlags`; struct-map overlays TTL field offsets.
- MEDIUM: `EVICTION_ENTRY_SIZE = 16` and magic `1024` eviction-buffer cap restated; not `size_of::<EvictionEntry>()`.
- MEDIUM: Default capacity `1024` and `next_power_of_2(cap*2)` copied through SLOT_ARRAY / STRUCT_MAP / NESTED instead
  of `slot_def_capacity`.
- MEDIUM: Four near-identical open-address rehash loops in `slot_growth.rs`; `MutationOp` restates a subset of
  `FlatUndoOp` without wire discriminants.
- undo_log: no per-entry heap; 24-byte `Copy` records. Offsets-into-arena would dangle after later writes — copies of
  `prev_value`/`aux` are the ABI, not waste.
- DEP-BLOAT: these modules use only `columine-types` + sibling crate modules. Crate dep is `columine-types`; `roaring`
  is a commented dev-dep. Clean.

## Findings

### F1 — CRITICAL — SSOT — Nested slots are size 0 on grow; live data is dropped

Evidence: `packages/columine/crates/columine-vm/src/slot_growth.rs:9-42`

```
/// Primary data size per slot type. STRUCT_MAP, ORDERED_LIST, and NESTED return
/// zero because their sizing is metadata-driven.
        SlotType::StructMap | SlotType::StructMap2 | SlotType::OrderedList | SlotType::Nested => 0,
```

`packages/columine/crates/columine-vm/src/state_init.rs:1192-1215` (grown size) and `:1261-1266`, `:1475-1504` (copy):

```
            SlotType::OrderedList => ordered_list_primary_size_from_meta(old_state, meta_base, cap),
            _ => slot_growth::slot_data_size(
                m.slot_type,
                cap,
                m.has_hashmap_timestamps,
                m.agg_type_byte,
            ),
```

```
            // Non-grown slot: memcpy data as-is (incl. struct-map arena).
            let primary_size = match m.slot_type {
                SlotType::StructMap | SlotType::StructMap2 => { ... }
                SlotType::OrderedList => {
                    ordered_list_primary_size_from_meta(old_state, meta_base, old_cap)
                }
                _ => slot_growth::slot_data_size( ... ),
            };
            if primary_size > 0 {
                bytes::copy(new_state, new_offset, old_state, old_offset, primary_size);
            }
```

StructMap / OrderedList have metadata-driven helpers. Nested does not. Comment at `state_init.rs:1458-1459` names nested
slots as a growth source. Neighbor `vm.rs:4649-4651` (and `:4674-4676`) maps `nested_set_insert` /
`nested_map_upsert_last` `CapacityExceeded` onto `NEEDS_GROWTH`. Neighbor `nested.rs:356-357,374-375` returns that error
when the outer table or inner arena is full. `tests/state_init.rs` has no Nested case.

Problem: `calculate_grown_state_size` adds 0 for Nested; `grow_state` copies 0 bytes both when Nested is the grown slot
and when a sibling HashMap/Set/list grows. The new metadata `offset` points at an empty region. Outer keys, inner
tables, and the nested arena vanish. That is a live correctness bug, not a dead arm.

Fix: Add a Nested arm next to StructMap/OrderedList. Size =
`nested::nested_slot_data_size(new_outer_cap, inner_cap, inner_type, inner_agg)` (or the old arena payload when not
growing Nested). Copy prefix + rehash outer keys/ptrs + copy or compact the arena. Read inner params from the nested
prefix already written at init (`state_init.rs:1058-1078`). Delete the Nested `=> 0` arm or keep it only if every caller
is exhaustive and Nested never hits `_`.

Cost/Risk: `grow_state` / `calculate_grown_state_size` / `slot_data_size`. Must stay in lockstep with `nested.rs`
prefix/arena layout (ColVmMaps). Any program that mixes Nested with a growable HashMap is already one overflow away from
silent Nested wipe. Add a grow oracle: Nested-only overflow, and HashMap overflow with a live Nested sibling.

### F2 — HIGH — COPIES — L4: growth under load instead of an admission ceiling

Evidence: `packages/columine/crates/columine-vm/src/state_init.rs:186-201`, `:1109-1111`, `:1175-1187`

```
    if !is_fixed_size && capacity == 0 {
        capacity = 1024;
    }
    if !is_fixed_size {
        // 2x for load factor
        capacity = next_power_of_2(capacity * 2);
    }
```

```
// When a HashMap or HashSet exceeds 70% load, the VM reports NEEDS_GROWTH so
// the caller can allocate the larger state and retry the batch.
```

```
        let cap = if slot_i == grown_slot_idx {
            next_power_of_2(m.capacity * 2)
        } else {
            m.capacity
        };
```

`slot_growth.rs:49-84` rehashes every live key into a 2× table (open-address probe, per-entry copies of key/value/ts).

Problem: Byproduct L4: sizes are formulas; growth under load is a contract violation that must fail at admission.
SLOT_DEF already carries the cardinality the program admits; init then 2×s it for load factor. That formula is the state
size. `NEEDS_GROWTH` + `grow_state` then 2× again, unbounded, mid-batch, with a full-buffer copy and rehash. Regime:
under load (each overflow batch), not once-per-open. Nested inner 2× inside the arena (`nested.rs:372-377`) is a
different, data-dependent size — that one is not a closed form of SLOT_DEF unless the host also admits an arena ceiling.

Fix: Treat post-load-factor SLOT_DEF capacity as the host ceiling. `CapacityExceeded` stays a hard error; delete
`calculate_grown_state_size` / `grow_state` / `NEEDS_GROWTH_SLOT` (and the wasm grow-and-retry loop). If the product
must accept unknown cardinality, the host passes a ceiling into `calculate_state_size` at open — still one reservation,
still no mid-flight 2×. Keep in-arena nested inner rehash only if the nested arena itself is reserved closed-form at
init (`outer_cap * per_inner`, which `nested_slot_data_size` already is).

Cost/Risk: wasm-backend grow retry, `vm_get_needs_growth_slot`, dispatch tests that assert `NEEDS_GROWTH`. Product
decision: bounded (fail closed) vs unbounded (keep growth, but then F1 must be fixed first). I would fail closed: the
program already declared capacity.

### F3 — HIGH — SSOT — Slot byte-size formulas restated three times

Evidence: `slot_growth.rs:17-44` vs `state_init.rs:435-466` vs `state_init.rs:727-802`. HashMap:

```
        SlotType::HashMap => {
            capacity * 4 + capacity * 4 + if has_timestamps { capacity * 8 } else { 0 }
        }
```

```
                SlotType::HashMap => {
                    size += capacity * 4 + capacity * 4;
                    if !type_flags.no_hashmap_timestamps() {
                        size += capacity * 8;
                    }
                }
```

COUNT size as a raw `2` in two places:

```
            if agg_type_byte == 2 {
                8
            } else {
                16
            }
```

(`slot_growth.rs:23-28`) and `state_init.rs:448-449` (`cap_lo == 2`). Neighbor `hash_table::data_size_no_header` /
`aggregates::agg_slot_byte_size` already exist. Bitmap is the one formula that _does_ go through
`bitmap_ops::bitmap_payload_capacity` in all three — the exception that proves the rule.

Problem: Init sizing does not call `slot_data_size`. Growth sizing does. A change to HashMap timestamps, COUNT width,
Array (`cap*4+cap*8`), or ConditionTree (`CONDITION_TREE_STATE_BYTES + cap*16`) can land in one walker and not the
other. ConditionTree already differs in _shape_: `calculate_state_size` does `align8` before the derived-facts plane
(`:456-458`); `slot_data_size` does not (`:31-32`). `CONDITION_TREE_STATE_BYTES` is 8 so they agree today; the align is
a loaded footgun.

Fix: `calculate_state_size` and `init_state` SlotDef data deltas call `slot_data_size` (and `ttl_side_buffer_size` —
already defined at `:130-139` but inlined again at `:469-476` and `:517-523`). COUNT compares `AggType::Count as u8` or
`aggregates::agg_slot_byte_size`. HashMap uses `hash_table::data_size_no_header`. One table of sizes.

Cost/Risk: `calculate_state_size` / `init_state` / `slot_growth::slot_data_size` / tests in `tests/state_init.rs` that
re-derive the same formulas. Must not change the ConditionTree align without a pin test.

### F4 — HIGH — STRUCTURE — Three bytecode walkers; init_state 438 lines, grow_state 374

Evidence: `validate_init_code` `state_init.rs:237-383`; `calculate_state_size` `:398-599`; `init_state` `:651-1089`;
`grow_state` `:1228-1602`. Capacity default restated outside `slot_def_capacity`, e.g. SLOT_ARRAY `:478-488` vs
`:846-854`; SLOT_STRUCT_MAP `:496-500` vs `:893-897`; SLOT_ORDERED_LIST `:531-535` vs `:958-962`
(`next_power_of_2(capacity)` — **no** `*2`); SLOT_NESTED `:562-566` vs `:1028-1032`. `ttl_side_buffer_size` exists but
SlotDef/StructMap size arms inline it.

Problem: The file header (`:142`) admits "three bytecode walkers". Validate rejects TTL on struct-map (`:317-319`) while
size still adds TTL bytes (`:517-523`) — dead only because both go through `validated_program`. Ordered-list
power-of-two is a silent third policy. `init_state` and `grow_state` each exceed ~100 lines with 12-argument
`write_slot_meta` (`:606-621`). Not a 5k god file, but the seams are already named and not encoded.

Fix: One `for_each_slot_def(program) -> SlotDef` iterator used by validate/size/init. `slot_def_capacity` (or a sibling
that takes the `*2` vs not-`*2` policy as data) is the only capacity function. `write_slot_meta` takes a `SlotMetaWrite`
struct. Split `grow_state` per `SlotType` the way `slot_growth` already split rehash.

Cost/Risk: Entire `state_init.rs` plus `tests/state_init.rs`. Do this after F1/F3 so Nested size is in the iterator, not
a fourth copy.

### F5 — MEDIUM — SSOT — Slot-meta offsets and flag bits hand-restated; struct-map overlays TTL fields

Evidence: `state_init.rs:1127-1142`

```
    let type_flags_byte = old_state[(meta_base + 12) as usize];
    let slot_type = SlotType::from_u8(type_flags_byte & 0x0f).unwrap_or_else(|| {
        columine_types::die!("invariant: state metadata contains an invalid slot type")
    });
        has_ttl: type_flags_byte & 0x10 != 0,
        has_evict_trigger: type_flags_byte & 0x20 != 0,
        has_hashmap_timestamps: slot_type != SlotType::HashMap || (type_flags_byte & 0x40 == 0),
        agg_type_byte: old_state[(meta_base + 13) as usize],
```

`write_slot_meta` `:624-640` uses `meta+0/+4/+8/+24/+28/+32/+36/+40` mixed with `SlotMetaOffset::*`. Struct-map init
`:909-943`:

```
            // Byte 13 (AGG_TYPE) is reused for num_fields; byte 15
            // (TIMESTAMP_FIELD_IDX) for bitset_bytes; bytes 16-17
            // (TTL_SECONDS low half) for row_size; byte 18 for has_timestamps.
            ...
                bytes::write_u32(state, meta_base + 20, data_offset);
```

`SlotMetaOffset` (types.rs): `TYPE_FLAGS=12`, `AGG_TYPE=13`, `TTL_SECONDS=16`, `GRACE_SECONDS=20`. `SlotTypeFlags`:
`HAS_TTL_MASK=0x10`, `NO_HASHMAP_TIMESTAMPS_MASK=0x40`. `SlotMetaView::has_hashmap_timestamp_storage` (`meta.rs:66-68`)
is `HashMap && !no_timestamps` — the **inverse** of `OldSlotMeta.has_hashmap_timestamps` for non-HashMap (true vs
false). Unused for non-HashMap sizes today only because those types ignore the flag.

Problem: Two layouts in one 48-byte record, addressed by magic numbers. `SlotMetaView::cutoff()` on a struct-map would
interpret the arena-header offset as `f32` grace. Illegal states are representable. Flag-mask copies will diverge the
moment a bit moves.

Fix: `read_old_slot_meta` = `SlotMetaView::read` plus typed extras. All numeric offsets go through `SlotMetaOffset` (add
`STRUCT_NUM_FIELDS`, `STRUCT_ROW_SIZE`, `STRUCT_ARENA_HDR` — or a `StructMapMeta` view — instead of overlaying TTL).
`has_hashmap_timestamps` uses `SlotTypeFlags`, same predicate as `SlotMetaView`.

Cost/Risk: `state_init.rs` growth/init, `meta.rs`, every test that pokes `meta+13`. types.rs `SlotMetaOffset` (other
slice).

### F6 — MEDIUM — SSOT — Eviction entry size and 1024-cap restated, not derived

Evidence: `state_init.rs:42`, `:134-137`, `:469-476`, `:825`, `:1554`

```
pub const EVICTION_ENTRY_SIZE: u32 = 16;
...
    let mut size = align8(capacity * EVICTION_ENTRY_SIZE);
    if has_evict_trigger {
        size += align8(1024 * EVICTION_ENTRY_SIZE);
    }
```

```
                    copied_evicted_count = old_evicted_count.min(1024);
```

Neighbor `vm.rs:155` publishes a second `pub const EVICTION_ENTRY_SIZE: u32 = 16`.
`columine_types::types::EvictionEntry` is `repr(C, align(16))` and tests `size_of::<EvictionEntry>() == 16`. TS
`wasm-backend.ts` restates `EVICTION_ENTRY_SIZE = 16` and `SLOT_META_SIZE = 48`.

Problem: The type is the SSOT. Two Rust `pub const`s plus a TS const plus a bare `1024` will drift independently.
`ttl_side_buffer_size` is already the growth formula; init size inlines a slightly different align schedule (`:469-476`
vs `:134-137`). They agree today because `16*n` is 8-aligned.

Fix: `EVICTION_ENTRY_SIZE = size_of::<EvictionEntry>() as u32` in types.rs; this crate and `vm.rs` import it. Name
`EVICTED_BUFFER_CAP: u32 = 1024` next to it. `calculate_state_size` calls `ttl_side_buffer_size`.

Cost/Risk: `state_init.rs`, `vm.rs` (ColVmCore), `wasm-backend.ts`. Layout tests in types.rs already pin 16.

### F7 — MEDIUM — DUPLICATION — Four open-address rehash loops

Evidence: `slot_growth.rs:66-81` (`grow_hash_map`), `:98-107` (`grow_hash_set`), `:137-153` (`grow_struct_map`),
`:184-203` (`grow_struct_map2`). Same skeleton:

```
            let mut pos = hash_key(k, new_cap);
            while bytes::read_u32(new_state, new_offset + pos * 4) != EMPTY_KEY {
                pos = (pos + 1) & (new_cap - 1);
            }
            bytes::write_u32(new_state, new_offset + pos * 4, k);
```

`grow_struct_map2` swaps in `hash_key_pair` and a second key lane; payload copy differs (u32 value / none / row bytes).

Problem: Probe, EMPTY skip, TOMBSTONE skip, wrap-mask copied four times. A load-factor or sentinel change will hit one
family and not another. `#[allow(clippy::too_many_arguments)]` on both struct-map growers (`:114`, `:160`) is the same
missing `GrowArgs` struct.

Fix: One `rehash_keys(old, new, old_off, new_off, old_cap, new_cap, hash, on_place)` where `on_place(old_i, new_pos)`
copies the payload. `new_cap` power-of-two stays an invariant (assert).

Cost/Risk: `slot_growth.rs` only. Existing hashmap/hashset tests; add struct_map/struct_map2 rehash tests (currently
absent in this file).

### F8 — MEDIUM — SSOT — MutationOp is a second undo-op enum without wire values

Evidence: `hooks.rs:25-34`

```
/// The mutation opcodes the container family emits (subset of the undo-log
/// op enum; the undo_log slice completes it).
pub enum MutationOp {
    SetInsert,
    SetDelete,
    MapInsert,
    MapDelete,
    MapUpdate,
}
```

`undo_log.rs:18-34`: `MapInsert = 1`, `MapUpdate = 2`, `MapDelete = 3`, `SetInsert = 4`, `SetDelete = 5`. Neighbor
`vm.rs:841-848` `mutation_op_to_flat` is the bridge. `FlatUndoEntry::read_from` (`undo_log.rs:119-136`) restates every
discriminant in a `match buf[0]` instead of `FlatUndoOp::from_u8`.

Problem: `MutationOp` discriminants are Rust-default 0..4, not the wire 1..5. Safe only because nothing serializes
`MutationOp`. Two names for one op, plus a third table in `read_from`. Adding a container mutation requires hooks +
undo_log + mapper + decode match.

Fix: Delete `MutationOp`. Containers emit `FlatUndoOp`. Or
`#[repr(u8)] enum MutationOp { SetInsert = FlatUndoOp::SetInsert as u8, ... }` with `From<MutationOp> for FlatUndoOp` as
a transmute-checked map. Put `from_u8` on `FlatUndoOp` and use it in `read_from`.

Cost/Risk: `hooks.rs`, `undo_log.rs`, hashmap/hashset/bitmap_ops, `vm.rs` mapper (ColVmCore). Compile-fails on missed
match arms today, so this is duplication debt, not a live decode bug. `opcode_audit.rs` already pins `FlatUndoOp`.

### F9 — MEDIUM — SSOT — Array-field detection is `byte >= 5`, not `StructFieldType`

Evidence: `state_init.rs:88-95`, `:99-108`, grow compaction `:1386-1408`

```
        .any(|&b| b >= 5)
...
        5 | 8 => 4,
        6 | 7 => 8,
        9 => 1,
        _ => columine_types::die!("invariant: arenaElemSize called on a non-array field type"),
```

```
                                if ft_byte < 5 {
                                    continue;
                                }
...
                                let elem_sz = arena_elem_size_strict(ft_byte);
```

Neighbor `types.rs`: `is_array_field_type`, `has_array_fields`, `arena_elem_size` (returns 0 on scalar).
`StructFieldType` array variants are 5..=9. Comment at `:88-89` admits bytes `> 9` count as arrays. Validate rejects
unknown field types (`:325-329`), so init is safe; growth reads the descriptor out of state.

Problem: A new scalar discriminant `10` becomes an array on the growth path and a scalar on `has_array_fields`.
`arena_elem_size_strict` would `die!` on it after the `< 5` skip failed. Two tables, one waiting to disagree.

Fix: Delete `has_array_fields_raw` / `arena_elem_size_strict`. Call `has_array_fields` / `arena_elem_size` and treat `0`
as "not an array" or `die!` on `from_u8` miss. SSOT is `StructFieldType`.

Cost/Risk: `state_init.rs` init + arena compaction. types.rs helpers (other slice) already exist.

### F10 — MEDIUM — SSOT — Program header parsed by hand beside `ProgramHeader`

Evidence: `state_init.rs:160-183`

```
    let magic = u32::from(content[0])
        | (u32::from(content[1]) << 8)
        | (u32::from(content[2]) << 16)
        | (u32::from(content[3]) << 24);
    ...
    let num_slots = content[6];
    let init_len = u16::from(content[10]) | (u16::from(content[11]) << 8);
    ...
        init_code: &content[14..14 + usize::from(init_len)],
```

`ProgramHeader::from_wire_bytes` (opcodes.rs) already decodes magic / `num_slots` / `init_code_len`. `grow_state` /
`calculate_grown_state_size` read `old_state[9]` (`:1179`, `:1233`) instead of `StateHeaderOffset::NUM_SLOTS` (value 9).
`ARENA_HEADER_SIZE = 8` (`state_init.rs:49`) duplicates neighbor `nested::ARENA_HDR_SIZE = 8`.

Problem: Header layout lives in `ProgramHeader`. This walker will miss a field move. `old_state[9]` is the same class of
bug as F5.

Fix: `parse_program` uses `ProgramHeader::from_wire_bytes`. Num-slots reads `StateHeaderOffset::NUM_SLOTS`. One
`ARENA_HEADER_SIZE` in types.rs.

Cost/Risk: `parse_program` only plus two `old_state[9]` sites. opcodes.rs / nested.rs are other slices.

### F11 — LOW — COPIES — `to_vec` of field-type bytes on the growth compaction path

Evidence: `state_init.rs:1376-1377`

```
                        let field_types: Vec<u8> =
                            new_state[new_offset as usize..(new_offset + nf) as usize].to_vec();
```

Same pattern at init `:899`, `:969` (once-per-open; not a finding). Growth loop then indexes `field_types[fi]` per live
key (`:1386-1401`).

Problem: Regime: under load, once per struct-map+arena grow, `nf` bytes (≤255). Not the hot insert path. Still a copy of
bytes that already live at `new_offset`. `struct_field_offset` / `has_array_fields` take `&[u8]`.

Fix: `let field_types = &new_state[new_offset as usize..(new_offset + nf) as usize];` Split the `new_state` borrows or
copy `ft_byte` out before the mutating `bytes::copy`/`write_u32`. Init: copy from `init_code` slice, no `Vec`.

Cost/Risk: Local to arena compaction / struct-map init. Need a split borrow or a `u8` temp so `new_state` can be mutably
written.

### F12 — LOW — STRUCTURE — `grow_state` is `Result` but always `Ok`; `NoVm` panics are fine

Evidence: `state_init.rs:1228-1232`, `:1601` — `Ok(())` is the only return. `hooks.rs:96-98,114,124` — `NoVm`
panics/unreachables documented as programmer errors when `undo_enabled()` is false.

Problem: `Result<(), ErrorCode>` implies operational failure. Out-of-range `grown_slot_idx` silently copies every slot
at the old cap. That hides F1-style misses. `NoVm` panics are invariant, not operational — leave them.

Fix: `grow_state` returns `Err(InvalidState)` if `grown_slot_idx >= num_slots` or if the grown type is
Nested/Aggregate/Scalar/ConditionTree until F1 gives Nested a real arm. Or change the signature to `()`.

Cost/Risk: wasm/C ABI wrappers that map the `Result`. Small.

### F13 — MEDIUM — TESTS — Growth oracles miss Nested and struct-map rehash

Evidence: `slot_growth.rs:208-306` tests hashmap/hashset/timestamps and the bitmap formula; no `grow_struct_map` /
`grow_struct_map2`. `undo_log.rs:294-348` pins serialized bytes and discriminants (correct for a wire ABI; would go red
if `write_to` moved a field). In-crate `tests/state_init.rs` has no Nested string (grep).

Problem: PH §7.10bb: a guard that cannot go red is not a guard. F1 would land green in this slice's tests. Bitmap's "one
formula" test is the pattern to copy.

Fix: (1) Nested sibling survives HashMap grow; (2) Nested outer overflow grow preserves inner membership; (3)
`grow_struct_map` / `grow_struct_map2` preserve keys+rows. Keep the undo layout pin — that one _can_ go red.

Cost/Risk: tests only. `tests/state_init.rs` is outside this slice's files but is the right home for (1)(2).

## Cross-slice questions

- `packages/columine/crates/columine-vm/src/vm.rs` (ColVmCore): Nested `CapacityExceeded` → `NEEDS_GROWTH` at
  `:4649-4651`, `:4674-4676`; second `EVICTION_ENTRY_SIZE` at `:155`; `mutation_op_to_flat` at `:841-848`. F1 depends on
  that signaling staying live.
- `packages/columine/crates/columine-vm/src/nested.rs` (ColVmMaps): `nested_slot_data_size` / `ARENA_HDR_SIZE` must be
  the Nested grow formula. Inner in-arena 2× (`:372-377`) is a separate growth path; host-level grow must not fight it.
- `packages/columine/crates/columine-types/src/types.rs` + `opcodes.rs`: `SlotMetaOffset`, `EvictionEntry`,
  `ProgramHeader`, `AggType`, `has_array_fields` / `arena_elem_size` are the SSOT F5–F10 should call.
  `PROGRAM_HEADER_SIZE` is defined in both types.rs and opcodes.rs.
- `packages/columine/src/types.ts`: `SlotType` has no `NESTED = 9` (Rust does). `AggType` / `StructFieldType` /
  `EVICTION_ENTRY_SIZE` restated. Not this slice; flagging the Nested hole because F1 is unobservable from TS if the
  enum omits it.
- `packages/columine/crates/columine-vm/src/hash_table.rs` / `aggregates.rs`: `data_size_no_header`,
  `agg_slot_byte_size`, and local `AGG_COUNT = 2` should own the formulas F3 currently copies.

## Non-findings (checked, clean)

- **DEP-BLOAT:** `columine-vm` depends on `columine-types` only; `roaring` is a size-budgeted dev-dep for minroar. These
  five files import `core::sync::atomic`, `columine_types`, and sibling modules. No git2/openssl/plist/napi-class
  crates. Feature flags: none in this crate's manifest.
- **undo_log allocation:** `FlatUndoEntry` / `FlatDeltaEntry` are `Copy`, 24/48 bytes. `write_to` fills a caller
  `[u8; 24]`. No per-entry `Vec`/`String`/`Box`. Rollback writes tombstones / prev u32 / u64 bits in place via
  `FlatTable`. Storing arena offsets instead of `prev_value`/`aux` would dangle after later mutations; `StateBytes`
  copies ≤8 bytes into `aux` _because_ the 24-byte ABI cannot hold a live span. Re-probe by key (`tbl.find`) is extra
  work on rollback only — not the insert hot path; not inflated.
- **undo_log tests:** layout pin asserts typed fields _and_ byte slots; substituting a wrong offset goes red.
  Discriminant test includes the 11→12 gap and rejects 0 and 16.
- **meta.rs:** `SlotMetaView` is `Copy`, reads once, writes through `bytes`. `slot_type()` `die!` cites the `read`
  invariant. No growth, no extra deps.
- **hooks.rs:** `MutationRecord` is `Copy`. `NoVm` panics only on services the trait says are programmer errors when
  undo/TTL/bitmap are unwired. `append_mutation` is `unreachable!` behind `undo_enabled() == false`.
- **unsafe:** none in this slice.
- **cfg(target_os):** none.
- **NEEDS_GROWTH_SLOT:** `AtomicU8` Relaxed, documented as the wasm global. Once-per-overflow store, not a hot-loop
  alloc.
- **reset_state:** `state.fill(0)` then `init_state` — once per reset, restores HASHMAP value-side zeros. Correct, not a
  copy finding.
- **SMF_ROW_ABSENT == SMR_ROW_ABSENT == 0x02:** documented as deliberate so rollback shares one test
  (`undo_log.rs:61-63`).
- **Bitmap size:** alloc/copy/reader share `bitmap_payload_capacity`; in-file test pins it (`slot_growth.rs:291-304`).
- **Profile trap (§4.1):** no benches in this slice.
