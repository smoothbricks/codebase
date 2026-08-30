# columine-vm/vm.rs

Scope: `packages/columine/crates/columine-vm/src/vm.rs` (5146), `packages/columine/crates/columine-vm/src/lib.rs` (28).
Doctrine: `BYPRODUCT-ENGINEERING.md`, `docs/handbook/04-mechanisms.md`, `05-memory-toolkit.md`, `02-measurement.md`
§4.1. Targeted SSOT reads: `packages/columine/crates/columine-types/src/opcodes.rs`,
`packages/columine/crates/columine-types/src/types.rs` (`Opcode`/`AggType`/`ProgramHeader`),
`packages/columine/src/types.ts` (`Opcode`), `packages/columine/crates/columine-vm/src/state_init.rs`
(`EVICTION_ENTRY_SIZE`), `packages/columine/crates/columine-vm/src/hash_table.rs` (`FlatTable::find`),
`packages/columine/crates/columine-vm/Cargo.toml`.

Regime for copy/bounds verdicts: `execute_impl` / `execute_element_opcodes` / `execute_batch_aggregates` are the
per-batch / per-element hot path (wasm size-sensitive). Undo overflow snapshots and delta export are once-per-overflow /
once-per-export, not findings.

## Summary

- Opcode numbering is not single-source: `types::Opcode` in the top-level match, raw `0xNN` in the body/agg loops, a
  third length table, a fourth agg-length table; this is the 0x81-class defect class.
- Canonical `columine-types/src/opcodes.rs` `Opcode` is missing nested ops that `vm.rs` actually dispatches; TS `Opcode`
  is missing TTL/scalar/nested bytes the VM executes.
- 5146-line god file; `execute_element_opcodes` is ~1187 lines and is a second copy of the reduce ISA.
- Top-level reduce dispatch indexes operand bytes with `code[pc+N]` after advancing past the opcode, so a truncated
  program panics instead of `INVALID_PROGRAM`.
- Per-opcode `to_vec()` on bitmap algebra and per-row re-parse of FOR_EACH match ids are evaporating work on the hot
  path.
- Probe loops in the read ABI restate `FlatTable::find` / struct-map find instead of calling them.
- Crate prod dep is only `columine-types` (load-bearing). `lib.rs` is a module list.

## Findings

### F1 — HIGH — SSOT — Four opcode tables in one file; body/agg loops restate numbering as raw bytes

Evidence: `packages/columine/crates/columine-vm/src/vm.rs:2682-2690`

```
            let Some(op) = Opcode::from_u8(op_byte) else {
                return INVALID_PROGRAM;
            };

            match op {
                Opcode::Halt => break,

                Opcode::BatchMapUpsertLatest | Opcode::BatchMapUpsertLatestTtl => {
```

`packages/columine/crates/columine-vm/src/vm.rs:3546-3548`

```
            match op_byte {
                // MAP_UPSERT_LATEST (0x20) / MAP_UPSERT_LATEST_TTL (0x24)
                0x20 | 0x24 => {
```

`packages/columine/crates/columine-vm/src/vm.rs:3395-3405`

```
            let op_byte = body[bpc];
            if !is_aggregate_op(op_byte) {
                ...
            match op_byte {
                0x40 | 0x42 | 0x43 => {
```

`packages/columine/crates/columine-vm/src/vm.rs:2450-2466` (`is_aggregate_op` / `agg_op_len` on raw `0x40`…`0x4b`) and
`:2470-2538` (`body_op_len` lengths keyed off `Opcode` again). Problem: The executable consumer of the ISA is this file,
but the numbering lives in four places. Top-level uses `columine_types::types::Opcode`; FOR_EACH pass-1 and pass-2 match
`u8` literals; `body_op_len` restates operand lengths; `agg_op_len` restates aggregate lengths with a `_ => 2` fallback.
`packages/columine/crates/columine-vm/tests/opcode_audit.rs` exists because `0x81` was declared in a registry and
missing from a length/dispatch arm, then skipped as an unknown byte. A rename or renumber of `Opcode` does not fail to
compile the body loop. Fix: One table. Give `Opcode` (the enum `execute_impl` already uses)
`fn encoded_len(code, pc) -> Option<usize>` and decode every arm with `Opcode::from_u8`. Delete
`is_aggregate_op`/`agg_op_len` as independent maps; aggregate-ness is
`matches!(op, Opcode::BatchAggSum..=Opcode::BatchAggMaxI64)` or a method on the enum. SSOT is `types::Opcode` until the
two Rust enums are merged (F2). Cost/Risk: `execute_element_opcodes` and `execute_batch_aggregates` must change
together; `opcode_audit.rs` harvests both `Opcode::` arms and raw hex — update the harvest after the raw arms disappear.

### F2 — HIGH — SSOT — `opcodes.rs` is documented as canonical but is not what the VM dispatches; TS enum is a third, incomplete copy

Evidence: `packages/columine/crates/columine-vm/src/vm.rs:31-34` imports `Opcode` from `columine_types::types`, not
`columine_types::opcodes`. `packages/columine/crates/columine-types/src/opcodes.rs:1-10,15-28,112-115` — module docs
call this the canonical registry; the enum has no
`SlotNested`/`NestedSetInsert`/`NestedMapUpsertLast`/`NestedAggUpdate`.
`packages/columine/crates/columine-types/src/types.rs:450-459,529-531` — those four exist here and `from_u8` decodes
`0x90`/`0x92`/`0x95`. `packages/columine/crates/columine-vm/src/vm.rs:4631-4681` — body dispatch executes
`0x90`/`0x92`/`0x95`. `packages/columine/src/types.ts:226-329` — TS `Opcode` has no `BATCH_MAP_UPSERT_LATEST_TTL`
(`0x24`), `BATCH_MAP_UPSERT_LAST_TTL` (`0x25`), `BATCH_SET_INSERT_TTL` (`0x32`), `BATCH_SCALAR_LATEST` (`0x48`), nested
`0x90`/`0x92`/`0x95`, or `SLOT_NESTED` (`0x1a`); comment at `:229` still says "must match vm.zig Opcode enum". Problem:
Three registries. The VM's executable set is `types::Opcode` plus the raw-byte body match. The file named "canonical" is
a subset (nested ops documented in a comment as implemented, absent from the enum). The TS compiler enum cannot name
several bytes this VM runs. That is live divergence, not hypothetical drift. Fix: Delete one Rust `Opcode`. Keep
`types::Opcode` (it is what `from_u8` + `execute_impl` already use) or move it into `opcodes.rs` and re-export; migrate
every `use`. Generate or audit-lock `packages/columine/src/types.ts` `Opcode` against that enum (the existing fixture in
`abi_registry_fixture.rs` should be the only frozen copy). Decision: `types::Opcode` wins because it is the decode table
the VM already calls. Cost/Risk: ColTypes slice owns both Rust enums and the TS enum. Compiler emission sites that still
write hex will keep compiling until they switch.

### F3 — HIGH — STRUCTURE — 5146-line god file; `execute_element_opcodes` is ~1187 lines

Evidence: `packages/columine/crates/columine-vm/src/vm.rs:1-13` (module docs claim dispatch + undo + TTL + reads in one
file). `packages/columine/crates/columine-vm/src/vm.rs:3519-3521`

```
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn execute_element_opcodes(
```

`execute_impl` is `:2648-3378` (~730 lines). `rollback_entry` is `:959-1106`. `lib.rs:13-28` already names the natural
crates-internal modules (`hashmap_ops`, `struct_map`, `nested`, `undo_log`, `aggregates`, `bitmap_ops`, `state_init`).
Problem: A 5k-line file is itself a finding. The function that carries the per-element ISA cannot be reviewed, and
`#[allow(clippy::too_many_lines)]` is the admission. Wasm/opt-z (§4.1) outlines repeated fragments in a fat match; two
giant matches (F5) make that worse. Fix: Split along the seams that already have section banners, into sibling modules
(not new abstractions):

- `columns.rs` — `col_*` / `batch_col!` (`:43-147`)
- `ttl.rs` — eviction index (`:149-505`)
- `undo.rs` — `UndoState` + rollback + delta export (`:552-795`, `:958-1218`, `:4710-4892`)
- `struct_map_exec.rs` — journaling + single-row upserts (`:1220-2210`)
- `agg_exec.rs` — `exec_agg_*` / `exec_scalar_latest` (`:2212-2444`)
- `dispatch.rs` — length tables + `execute_impl` / FOR_EACH / element loop (`:2446-4708`)
- `reads.rs` — `vm_map_get` and iterators (`:4894-5146`) `vm.rs` keeps `struct Vm` and the two public `execute_batch*`
  entry points. Cost/Risk: wasm binary layout will shift; no behavior change if moves are file-only. Do not split the
  two dispatch matches until they share one opcode table (F1), or the split cements the duplication.

### F4 — HIGH — STRUCTURE — Top-level reduce dispatch panics on truncated operand bytes

Evidence: `packages/columine/crates/columine-vm/src/vm.rs:2678-2696`

```
        let mut pc = 0usize;
        while pc < code.len() {
            let op_byte = code[pc];
            pc += 1;
            ...
                Opcode::BatchMapUpsertLatest | Opcode::BatchMapUpsertLatestTtl => {
                    let (slot, key_col, val_col, ts_col) =
                        (code[pc], code[pc + 1], code[pc + 2], code[pc + 3]);
                    let Some(cmp_type) = CmpType::from_u8(code[pc + 4]) else {
                        return INVALID_PROGRAM;
                    };
                    pc += 5;
```

Contrast FOR_EACH, which does use `get` + `validate_body` (`:3302-3330`). `Opcode::from_u8` docs in `types.rs:467-469`
say unknown bytes become `INVALID_PROGRAM` "rather than panicking on wild input". Problem: The loop only proves one
opcode byte exists. Operand fetches are unchecked `Index`. A reduce section that ends mid-instruction is operationally
malformed program input and panics (wasm trap) instead of `ErrorCode::InvalidProgram`. FOR_EACH bodies cannot hit this
because `validate_body`/`body_op_len` ran first; the top-level path never calls them. Fix: At the top of each iteration,
`let Some(len) = body_op_len(code, pc_at_opcode) else { return INVALID_PROGRAM };` then slice `code[pc..pc+len]` and
decode from that slice (or `get` every operand). Same `INVALID_PROGRAM` as unknown bytes. Do not `unwrap` the slice.
Cost/Risk: One extra length walk per top-level op (tiny vs the op itself). Must not treat `Halt` as needing operands.

### F5 — HIGH — DUPLICATION — Batch dispatch and per-element dispatch are two implementations of one ISA (wasm size)

Evidence: Latest upsert, top-level `:2690-2716` (`Opcode::BatchMapUpsertLatest`, `batch_map_upsert`, `pc += 5`) vs body
`:3547-3579` (`0x20 | 0x24`, `single_map_upsert`, `bpc += 6`). Struct-map last/first/max, top-level `:3109-3184` vs body
`:3949-4083`. Struct-map2, `:3187-3298` vs `:4085-4164`. Nested, body-only `:4631-4701` with no top-level arm (top-level
`_ => return INVALID_PROGRAM` at `:3372-3373`). Problem: Every opcode that can run both as a batch op and inside
FOR_EACH/FLAT_MAP is written twice. That is the wasm size budget: two match trees, two operand decoders, two
growth/error tails. It is also why F1's raw-byte body match can drift from the enum match. Near-identical functions
differing only in "batch column vs one cell" is the assignment's duplication shape. Fix: One decoder per opcode
producing a small operand struct (the struct-map path already has `StructMapUpsertOperands`). One `exec_*` that takes
either a full column view or a single index. `execute_impl` and `execute_element_opcodes` become loops over that table.
Delete the duplicate arms. Cost/Risk: Large mechanical rewrite of `:2687-3374` and `:3546-4705`. Behavior must stay:
IF/probe/list/nested/FLAT_MAP remain body-only unless the compiler is also changed.

### F6 — MEDIUM — SSOT — `EVICTION_ENTRY_SIZE = 16` restated next to state_init and TS

Evidence: `packages/columine/crates/columine-vm/src/vm.rs:152-155`

```
/// `EvictionEntry` layout: `timestamp:f64 @0, key_or_idx:u32 @8, value:u32 @12`
/// (16 bytes; layout pinned in columine-types). Accessors below address entry
/// `i` of the index/buffer starting at `base`.
pub const EVICTION_ENTRY_SIZE: u32 = 16;
```

`packages/columine/crates/columine-vm/src/state_init.rs:42` — second `pub const EVICTION_ENTRY_SIZE: u32 = 16`.
`packages/columine/src/wasm-backend.ts:36` — `const EVICTION_ENTRY_SIZE = 16`. Problem: Comment says the layout is
pinned in columine-types; the constant is not there. Two `pub const`s in the same crate can diverge; TS is a third copy.
Layout math in `state_init` and the accessors here would silently disagree. Fix: One constant in `columine-types` next
to the eviction record layout (or, if types does not own that record, only `state_init::EVICTION_ENTRY_SIZE`). `vm.rs`
imports it. Delete the vm.rs definition. TS reads the same exported value or is fixture-locked. Cost/Risk: Any
`use columine_vm::vm::EVICTION_ENTRY_SIZE` must move. state_init tests already import from `state_init`.

### F7 — MEDIUM — SSOT — Scalar subtype bytes `8..=10` restated against `AggType`

Evidence: `packages/columine/crates/columine-vm/src/vm.rs:2356-2377`

```
    if meta.slot_type() != SlotType::Scalar || !matches!(scalar_type, 8..=10) {
        return ErrorCode::InvalidProgram;
    }
    ...
    // Scalar discriminants are SCALAR_U32 = 8, SCALAR_F64 = 9, and
    // SCALAR_I64 = 10; 6–7 are reserved.
    let matches = |i: usize| type_mask.is_none_or(|(td, id)| td[i] == id);
    match scalar_type {
        8 => {
```

Same `8..=10` check in `validate_body` at `:2563-2565`. `AggType::ScalarU32 = 8` / `ScalarF64 = 9` / `ScalarI64 = 10`
already exist in `columine-types` (`types.rs:226-228` and `opcodes.rs:127-129`). `vm.rs` does not import `AggType`.
Problem: The comment is a handwritten copy of the enum. A new scalar subtype will compile here as `InvalidProgram` or
fall into `unreachable!` (`:2416`) depending on which match is edited. Fix:
`let Some(kind) = AggType::from_u8(scalar_type) else { return InvalidProgram }; match kind { AggType::ScalarU32 => ..., AggType::ScalarF64 => ..., AggType::ScalarI64 => ..., _ => return InvalidProgram }`.
Same in `validate_body`. Cost/Risk: None beyond the two sites.

### F8 — MEDIUM — SSOT — Program content header `14` restated; comment disagrees with `ProgramHeader`

Evidence: `packages/columine/crates/columine-vm/src/vm.rs:2669-2676`

```
        // Content header: magic(4) version(2) numSlots(1) numInputs(1)
        // reserved(2) initLen(2) reduceLen(2) = 14 bytes.
        let init_len = u32::from(bytes::read_u16(content, 10));
        let reduce_len = u32::from(bytes::read_u16(content, 12));
        if PROGRAM_HEADER_SIZE + init_len + reduce_len > program.len() as u32 {
            return INVALID_PROGRAM;
        }
        let code = &content[(14 + init_len) as usize..(14 + init_len + reduce_len) as usize];
```

`packages/columine/crates/columine-types/src/opcodes.rs:211-223` — `ProgramHeader::WIRE_SIZE = 14`, fields
`num_callbacks` at byte 8 and `flags` at byte 9, not "reserved(2)". Offsets 10/12 for init/reduce lengths currently
match. Problem: The 14 and the field offsets are a second decoder beside `ProgramHeader::from_wire_bytes`. The comment
is already wrong. A header version bump that moves `init_code_len` would still compile and slice the wrong reduce body.
Fix: `let header = ProgramHeader::from_wire_bytes(<14 bytes>)`; use `header.init_code_len` / `header.reduce_code_len` /
`WIRE_SIZE`. Delete the literals `10`, `12`, `14`. Cost/Risk: Need a 14-byte copy or `try_into` of
`content[..WIRE_SIZE]`. Magic check immediately above can stay or move into the header decode.

### F9 — MEDIUM — COPIES — Bitmap algebra copies the source payload with `to_vec()` on every opcode

Evidence: `packages/columine/crates/columine-vm/src/vm.rs:2965-2992`

```
                    let source_len = source_storage.serialized_len(state);
                    let source_data = if source_len > 0 {
                        let off = source_storage.payload_offset() as usize;
                        state[off..off + source_len as usize].to_vec()
                    } else {
                        Vec::new()
                    };
                    ...
                    let result = batch_bitmap_algebra(
                        env,
                        &mut hooks,
                        alg_op,
                        state,
                        &target_meta,
                        &source_data,
                    );
```

Scratch variant `:3008` — `let source_data = self.bitmap_env.algebra_result().to_vec();` then the same call. Regime:
once per algebra opcode, payload-sized (not per row, not startup). The copy exists because `state` is mutably borrowed
for the target while the source is also in `state`. Problem: A full serialized-bitmap allocation + copy per
`AND/OR/ANDNOT/XOR`. Scratch already lives in `BitmapEnv.algebra_result`; copying it is evaporating work. Wasm
size/alloc budget pays a heap vector the API does not need if the algebra kernel took a source slice plus a disjoint
target, or copied into `BitmapEnv.store_temp` (already the reusable buffer — `bitmap_ops.rs:71-78`). Fix: Copy source
into `self.bitmap_env.store_temp` (reuse) or split-borrow payload vs slot meta so `batch_bitmap_algebra` takes `&[u8]`
from `state` without owning. Scratch path: pass `algebra_result` by split-borrow / `mem::take` + restore, not
`to_vec()`. Cost/Risk: `batch_bitmap_algebra` signature lives in `bitmap_ops.rs` (other slice). Dummy
`BitmapEnv::default()` at `:2918-2920` is a borrowck workaround, not a heap copy (empty `Vec`); leave it until the
algebra signature no longer needs two envs.

### F10 — MEDIUM — COPIES — FOR_EACH re-parses match-id words for every row

Evidence: `packages/columine/crates/columine-vm/src/vm.rs:3350-3365`

```
                    for ei in 0..batch_len {
                        let val = type_data[ei as usize];
                        let mut matched = false;
                        for mj in 0..match_count {
                            let id_off = match_ids_start + mj * 4;
                            if val == bytes::read_u32(code, id_off as u32) {
                                matched = true;
                                break;
                            }
                        }
                        if !matched {
                            continue;
                        }
                        let elem_result = self
                            .execute_element_opcodes(delta_mode, state, body, cols, ei, ei, 0xFF);
```

Pass 1 already reads the same ids at `:3339-3341`. `match_count` is a `u8` (`:3306`). Problem: L0 evaporating work on
the per-row path. Each row re-does `match_count` unaligned `read_u32`s from bytecode. The id list is immutable for the
op. Fix: After parsing the header, decode ids once into a stack array `[u32; 256]` (or
`code.get(match_ids_start..body_len_offset)` viewed once). Reuse that slice in both passes. Optional: for
`match_count == 1` (the common compiler output), compare against one register and skip the inner loop. Cost/Risk: None.
`match_count` is already bounded by the `u8` in the encoding.

### F11 — MEDIUM — COPIES — `exec_scalar_latest` re-loads the stored timestamp every row

Evidence: `packages/columine/crates/columine-vm/src/vm.rs:2370-2386`

```
    let prev_value = bytes::read_u64(state, data);
    let prev_ts = bytes::read_f64(state, data + 8);
    ...
            for i in 0..batch_len as usize {
                let ts = cmp_vals[i];
                if matches(i) && ts > bytes::read_f64(state, data + 8) && vals[i] != EMPTY_KEY {
                    bytes::write_u32(state, data, vals[i]);
                    bytes::write_f64(state, data + 8, ts);
```

Same reload in the f64/i64 arms (`:2394-2412`). Regime: per-row in `BATCH_SCALAR_LATEST` (top-level and FOR_EACH pass
1). Problem: The running max timestamp is already known after each write. Re-reading it from `state` is a dependent load
(§7.12) on every row. `prev_ts` is captured and then ignored by the loop. Fix: `let mut best_ts = prev_ts;` (and
`best_val`) in registers; compare `ts > best_ts`; write through at the end if changed (or write-through while updating
`best_ts`). Undo journal already diffs `prev_*` vs final (`:2419-2441`). Cost/Risk: Must keep NaN/`==` behavior
identical; current compare is `>` on `f64`.

### F12 — MEDIUM — DUPLICATION — Read ABI reimplements open-addressing already in `hash_table` / `struct_map`

Evidence: `packages/columine/crates/columine-vm/src/vm.rs:4898-5111` — `vm_map_get`, `vm_set_contains` (non-bitmap),
`vm_struct_map_get_row_ptr`, `vm_struct_map2_get_row_ptr` each do `hash_key` / `hash_key_pair` then
`for _ in 0..capacity { read key; EMPTY_KEY stop; match return; pos = (pos+1) & (cap-1) }`.
`packages/columine/crates/columine-vm/src/hash_table.rs:202-217` — `FlatTable::find` is the same loop (and rejects
sentinel keys). Problem: Two probe implementations. `vm_map_get` does not reject `EMPTY_KEY`/`TOMBSTONE` as search keys
(`FlatTable::find` does). Iterator helpers (`vm_map_iter_start` `:4949-4956`) are a third scan of live cells. A
probe-mask or tombstone-policy change has to land in both. Fix: Bind a `FlatTable` / `StructMapSlot` from the exported
`(offset, capacity, ...)` and call `find`. Sentinel policy must be the table's, not a second one. `0xFFFF_FFFF` vs
`u32::MAX` in struct-map vs struct-map2 (`:5035` vs `:5094`) becomes one constant. Cost/Risk: Read ABI is called from
wasm exports (other slice). Bind helpers that do not need a slot index (offset+cap only) already exist as
`FlatTable::bind_external` (`hashmap_ops.rs:114-117`).

### F13 — MEDIUM — DUPLICATION — Struct-map and struct-map2 journal/rollback are copy-paste

Evidence: `packages/columine/crates/columine-vm/src/vm.rs:1258-1278` vs `:1369-1386` (`capture_struct_map_row_prior` /
`capture_struct_map2_row_prior`). `:1283-1367` vs `:1389-1463` (emit upsert journal). `:1108-1163` vs `:1165-1218`
(rollback field/row). `single_struct_map_upsert_last` `:1821-1896` vs `single_struct_map2_upsert_last` `:1899-1971`.
Problem: Near-identical functions differing in one vs two keys and `StructMapSlot` vs `StructMap2Slot`. Field
bitset/cell packing is the same. Wasm compiles both. A journal-flag bug has to be fixed twice (and already has
`#[allow(clippy::too_many_arguments)]` on both emit paths). Fix: One capture/emit/rollback over a tiny key trait or a
`(key, Option<key2>)` packed the way the undo entry already packs `key`/`prev_value` for map2 (`:1167`, `:1407-1408`).
Keep the two public bind types. Cost/Risk: Undo byte layout must stay identical (`key`/`prev_value` roles). Differential
tests for map vs map2 rollback.

### F14 — MEDIUM — STRUCTURE — Delta apply swallows malformed segments; corrupt op panics

Evidence: `packages/columine/crates/columine-vm/src/vm.rs:4826-4841`

```
        if entry_size != FLAT_UNDO_ENTRY_SIZE
            || !(undo_segment.len() as u32).is_multiple_of(entry_size)
        {
            return;
        }
        ...
            rollback_entry(&mut self.bitmap_env, state, &entry);
```

`packages/columine/crates/columine-vm/src/vm.rs:4885-4891`

```
    FlatUndoEntry::read_from(&buf)
        .unwrap_or_else(|| columine_types::die!("corrupt undo-entry op byte in delta segment"))
```

Docs at `:4824-4825` say a corrupt op byte "is a programmer error and panics". Wrong `entry_size` returns without
applying and without an error code. Problem: Two operational failures, two postures: silent no-op vs process-killing
panic. Bindings passing a bad length get a successful-looking empty apply. That is `/dev/null` on the error path. Fix:
Return `ErrorCode` (or `Result<(), ErrorCode>`) from both apply functions. Reject size mismatch and unknown op with
`InvalidProgram`/`InvalidState`. Delete `die!` here; it is not an invariant of bytes that arrived from outside.
Cost/Risk: wasm export signatures if they currently return void. Callers must handle the code.

### F15 — LOW — STRUCTURE — `rollback_entry` is a 147-line match; TTL restore uses `debug_assert` on operational insert failure

Evidence: `packages/columine/crates/columine-vm/src/vm.rs:958-1106` (`rollback_entry`). `:271-276`

```
    let result = insert_with_ttl(state, meta, key, timestamp);
    debug_assert_eq!(result, ErrorCode::Ok);
```

Problem: Capacity failure while restoring TTL during rollback is operational (the comment at `:269-270` says so) but
only asserted in debug. Release continues with a missing TTL entry. Not a hot path. Fix: Surface `ErrorCode` from
`restore_ttl_entry` / `rollback_entry`, or treat failure as overflow-to-shadow (already the recovery for journal
overflow). Do not `debug_assert` operational results. Cost/Risk: Rollback currently returns `()`. Changing that touches
every apply/undo caller.

## Cross-slice questions

- `packages/columine/crates/columine-types/src/opcodes.rs` vs `.../types.rs`: two `Opcode` enums and two `AggType`
  enums. Nested ops `0x1a`/`0x90`/`0x92`/`0x95` exist only in `types.rs`. Who is allowed to delete which? (ColTypes)
- `packages/columine/src/types.ts` `Opcode` missing `0x24`/`0x25`/`0x32`/`0x48`/`0x90`/`0x92`/`0x95`/`0x1a` — does the
  TS compiler emit those bytes via raw hex, or are they Rust-only? (ColTypes / compiler slice)
- `packages/columine/crates/columine-vm/src/state_init.rs:42` — duplicate `EVICTION_ENTRY_SIZE`; should types own the
  eviction record? (ColVmState / state_init owner)
- `packages/columine/crates/columine-vm/src/hash_table.rs` `FlatTable::find` — intended owner of `vm_map_get`?
  (ColVmMaps)
- `packages/columine/crates/columine-vm/src/bitmap_ops.rs` `batch_bitmap_algebra` — can it take a source slice that
  aliases `state` so F9's `to_vec` dies? (bitmap_ops owner)
- Compiler: IF/probe/scatter/list/nested/FLAT_MAP are `INVALID_PROGRAM` at reduce top-level (`vm.rs:3372-3373`) and only
  valid inside FOR_EACH bodies. Confirm emission never places them at top level.

## Non-findings (checked, clean)

- DEP-BLOAT: this crate's prod manifest is only `columine-types`. `roaring`/`proptest` are dev-dependencies with an
  explicit "must never re-enter the shipped artifact" comment. `vm.rs` does not add crates.
- `lib.rs` is a 28-line module list; no duplication, no alloc.
- `unsafe` in `col_u32`/`col_f64`/`col_i64`/`u32s_as_bytes` etc. (`:55-89`) has SAFETY comments citing alignment,
  lifetime, and bit-validity.
- Big-endian is a `compile_error!` (`:46-47`), not a silent wrong-endian path.
- FOR_EACH bodies are length-checked (`validate_body`) before `execute_element_opcodes`; the panic hole is top-level
  only (F4).
- `col_*_exact` vs clamped `col_*` is an intentional split (`:101-106`) with `ColumnUnderrun` on the batch path.
- Dummy `BitmapEnv::default()` in bitmap arms (`:2918-2920`) is an empty `Vec` pair for borrowck, not a per-row heap
  alloc.
- `UndoState` snapshot `to_vec` (`:636`) is first-overflow only; not a finding.
- No tests live in these two files. `opcode_audit.rs` (outside this slice) harvests source text of both dispatch styles;
  it cannot go red on a length mismatch between `body_op_len` and a body arm that still names the same byte (PH §7.10bb)
  — noted, not counted, because the test file is not owned here.
- `execute_impl` unknown/`from_u8` miss returns `INVALID_PROGRAM` (`:2683-2684`); Halt breaks (`:2688`).
