# Struct-map key index

## 1. Scope

A struct-map slot (`STRUCT_MAP`, one key; `STRUCT_MAP2`, two keys) is laid out as
`[field descriptor, padded to 8][key index region][rows]`, then a one-key map's optional comparison-timestamp lane, then
its CSR arena. A row's ordinal is its identity: the row lives at `rows_base + ordinal * row_size`, and the slot's key
index hands out ordinals in `0..capacity`. Which ordinal a key owns, the order keys iterate in, and every byte of the
index region belong to one key-index family, the `KeyIndex` trait of `columine_vm::key_index`: a static type whose
associated functions the VM calls, monomorphized, for every key it reads or writes. No other code reads or writes an
index region: the slot views (`StructMapSlot`, `StructMap2Slot`), every struct-map opcode, sizing, initialization,
growth, validation, capacity admission, logical export and import, row expressions, the stored-row sweep, the checked
readers and the seal reach keys only through it.

`HashIndex` is the family every image with columine's own state header uses: the open-addressed linear-probe table. An
embedder supplies another family by implementing `KeyIndex` and running the VM as `Vm<I>`.

## 2. The family

A family names its state-header magic (`STATE_MAGIC`) and format version (`FORMAT_VERSION`). Initialization writes them,
and every header check compares them: an image whose header names another family is `InvalidState` before any slot is
read. A family sizes a slot (`capacity_for` the declared capacity, `grown_capacity`, `region_bytes` for a capacity and
key lanes, a multiple of 8) and admits rows (`max_rows`: a new key past it is `CapacityExceeded`, answered with
`NEEDS_GROWTH`). It lays out (`init`) and checks (`validate`) an empty or populated region.

Keys are `Key { k1, k2 }`, lane 2 zero for a one-key map. Reads are `find`, `key_at` (a dead or out-of-range ordinal is
absent), and iteration in the family's own order: `first`, `next`, and for two-key maps `first_with_key1` and
`next_with_key1`, the live rows whose lane 1 is a given key. Ordinals never move except by growth and seal, so a
follower found before a row is removed is still live after.

Mutation is planned, then applied. `plan_insert` answers the ordinal a key owns or will own and, for a new key, the
`IndexWrites` its apply may change; `plan_remove` answers the ordinal and writes of a present key. `IndexWrites` holds
at most `INDEX_WRITES_MAX` disjoint absolute `(offset, len)` ranges inline. A journaled writer captures them in the same
frame as the slot's size, change flag and row bytes, taken before any byte moves, so rollback restores the index byte
for byte. The slot's size is the VM's: the views add or subtract one around the family's apply. Planning and applying
allocate nothing beyond the family's scratch.

`grow` copies every key of a region into an empty larger one and names each key's new ordinal; the VM moves the row, its
timestamp cell and, compacted in ordinal order, its arena payloads. `seal` rebuilds a region canonically: its bytes
become a function of its keys and capacity alone, and it names each key's new ordinal (section 6).

Key lanes are declared by the program (section 3). A family that orders keys reads an interned lane's UTF-8 bytes
through the caller's resolver, `InternBytes`; an id it cannot resolve is a refusal value. Each call takes a `KeyCx`: the
resolver and the family's `Scratch`, which one VM owns and lends to each batch.

Sentinel keys are the VM's: `EMPTY_KEY` and `TOMBSTONE` in lane 1 name no row. The views answer them before any family
call (absent for a read or remove, `InvalidKey` for an insert), and each opcode keeps its own rule: one-key upserts,
removes and scatters skip a sentinel key, two-key upserts and removes refuse it with `InvalidKey`. `EMPTY_KEY` stays a
STRING cell's null.

## 3. Key lanes

A lane holds raw `u32` values or interned-string ids. On `SLOT_STRUCT_MAP` and `SLOT_STRUCT_MAP2` records, which carry
no TTL, type-flag bit `0x20` (`SlotTypeFlags::STRUCT_KEY1_INTERNED`) declares lane 1 interned and bit `0x40`
(`SlotTypeFlags::STRUCT_KEY2_INTERNED`) lane 2; a one-key record that sets `0x40` is a malformed program. On every other
record the bits keep their TTL meanings. Initialization stores the lane bits in slot-metadata byte 19
(`SlotMetaOffset::STRUCT_KEY_LANES`), free in the struct overlay, and the slot's `TYPE_FLAGS` byte without them. A
stored byte no declaration writes is a malformed image, and an image whose lane byte differs from the running program's
declaration is refused as a mismatched program. `HashIndex` ignores lane kinds.

## 4. HashIndex

The region is lane 1's `u32` cells, then a two-key map's lane 2 cells, one per ordinal. `EMPTY_KEY` or `TOMBSTONE` in
lane 1 marks a dead cell; lane 2 holds any value. A key's hash picks its home cell, a probe walks forward from it, a
remove leaves a tombstone, and an insert reuses the first tombstone only when the key is absent. Capacity is the
declared capacity doubled and rounded to a power of two, growth doubles it (at least 16), and admission stops at 70 %
load. Iteration is ascending cell order, the order the TypeScript readers walk. Its images are byte-identical to the
pre-seam layout, under the state magic `STATE_MAGIC` and format version 3.

## 5. Two-key prefix removal

`BATCH_STRUCT_MAP2_REMOVE_PREFIX` is opcode `0x8e`, `slot:index, key1_col:index` (`Opcode::BatchStructMap2RemovePrefix`;
TypeScript `Opcode.BATCH_STRUCT_MAP2_REMOVE_PREFIX`). For each element it removes every row of the two-key map whose
lane 1 equals the element's key1, in the map's index order, each row in its own capture of the size, change flag, index
writes and row bitset, and records the slot's removal change flag. A sentinel key1 is `InvalidKey`; a key1 no row holds
removes nothing; a slot that is not a two-key map is `InvalidProgram`. It runs at top level and in element bodies,
reserves no capacity, and reads only its key1 column.

## 6. Seal

`seal_state(state, strings, scratch)` (and `Vm::seal_state`) rebuilds every struct-map slot of a validated image: the
family seals the index, each live row moves to its new ordinal keeping only its bitset and present field cells (and its
timestamp cell), every dead row is zero, and the CSR arena is repacked in new ordinal order with every unused byte zero.
Images holding the same keys and rows seal to identical bytes whatever history produced them. Sealing has no journal:
`Vm::seal_state` is `SpeculationActive` while an undo window is open, as growth is refused. `HashIndex` seals by
reinserting its live keys into an empty region in ascending `(k1, k2)` order. Seal space comes from the VM's
`KeyScratch`; memory that cannot back it is `OutOfMemory` before that slot's first write.

## 7. Entry points

The existing constructors and free functions are the `HashIndex` instantiation: `Vm::new`, `Vm::default`, `init_state`,
`validate_state`, `grow_state`, `bind_slot`, `CheckedStructMap::bind`, `export_slot`, and the rest keep their
signatures. Each has a generic form for another family: `Vm::new_in`, `execute_batch_in` and its delta and live forms,
`init_state_in`, `calculate_state_size_in`, `validate_state_in`, `calculate_grown_state_size_in`, `grow_state_in`,
`bind_slot_in`, `bind_struct_map_in`, `bind_struct_map2_in`, `CheckedStructMap::bind_in` with `row_in`, `iter_start_in`
and `iter_next_in`, `StoredRow::lookup_in`, `export_slot_in`, `import_slot_in` and `rewrite_slot_set_in`. Calls that
read key order or identity take the resolver; `HashIndex` callers pass `NoStrings`. An admitted program carries its
family as a type parameter (`AdmittedProgram<A, I>`): `AdmittedProgram::load` admits a `HashIndex` program, `load_in`
any family's, and the program sizes, initializes and executes images of that family only.

## 8. Behavioral acceptance

The existing HashIndex suites pass unchanged. On HashIndex, tests prove that prefix removal removes exactly the rows of
its key1, at top level and per element, refuses sentinel keys and other slot kinds, and that undo restores and delta
redo reproduces the image byte for byte; that lane-kind bits decode, store in byte 19, are refused on a one-key record's
lane 2 and in malformed images, and leave HashIndex execution unchanged; that permuted insertion histories seal to
identical images; and that a header of another family is refused by every entry point.
