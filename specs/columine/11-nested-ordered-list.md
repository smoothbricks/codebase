# Nested ordered list of struct rows

## 1. Scope <a id="smoo/columine!n/nested-ordered-list-scope"></a>

A nested slot (`SlotType::Nested`, `9`) maps each `u32` outer key to an arena-allocated inner container. This spec
extends `SLOT_NESTED` (`0x1a`) with a fourth inner kind: `SlotType::OrderedList` (`7`), an append-only ordered list of
packed struct rows per outer key, written by the body opcode `NESTED_LIST_APPEND_STRUCT` (`0x93`).

Each admitted event row appends one struct row under its outer key in element order. Readers resolve an outer key in
O(1) expected time and index any row of its list in O(1) time.

## 2. Declaration and program admission <a id="smoo/columine!n/nested-ordered-list-declaration"></a>

`SLOT_NESTED` (`0x1a`) carries its existing header followed by a struct-field descriptor when `inner_type` (low nibble)
is `SlotType::OrderedList` (`7`):

```text
0x1a  slot:index  outer_type_flags:u8  outer_cap:u16le
      inner_type:u8  inner_initial_cap:u16le  inner_agg_type:u8
      num_fields:u8  field_type:u8 × num_fields
```

Program admission (`state_init::calculate_state_size`, `state_init::init_state`, `program_admission::AdmittedProgram`,
and `reducer-bytecode.ts`) enforces:

- `outer_type_flags` names `SlotType::Nested` with no TTL bit;
- `inner_agg_type` is `0`;
- `num_fields >= 1`;
- every `field_type` byte is a scalar `StructFieldType` accepted by ordered-list struct rows: `UInt32` (`0`), `Int64`
  (`1`), `Float64` (`2`), `Bool` (`3`), or interned `String` (`4`). Arena field kinds (`ArrayU32`..=`ArrayBool`, `Vo`)
  and any unknown byte are refused with a typed error (`MalformedProgram` / `InvalidProgram`);
- `inner_initial_cap == 0` defaults to `16`; any nonzero value is kept verbatim as the list's initial row capacity (not
  rounded up to a power of two).

## 3. Image byte layout <a id="smoo/columine!n/nested-ordered-list-layout"></a>

All multi-byte fields are little-endian. A nested slot's data region starts at `slot_offset` (`SlotMetaOffset::OFFSET`):

```text
+0                          NestedPrefix (8 bytes):
                              [0]    inner_type: u8         (7 = OrderedList)
                              [1..3] inner_initial_cap: u16 (declared initial cap, >= 1)
                              [3]    inner_agg_type: u8     (0)
                              [4]    depth: u8              (1)
                              [5]    num_fields: u8         (1..=255)
                              [6..8] row_size: u16          (4-byte padded row width)
+8                          Field descriptor:
                              [0..num_fields]               field_type: u8 × num_fields
                              [num_fields..descriptor_size] zero padding (descriptor_size = align8(num_fields))
+8 + descriptor_size        Outer keys:     u32 × outer_cap (0xffff_ffff empty, 0xffff_fffe tombstone)
+8 + descriptor_size + 4C   Outer pointers: u32 × outer_cap (absolute state byte offset, or 0)
+8 + descriptor_size + 8C   Arena header:   [arena_capacity: u32][arena_used: u32]
+16 + descriptor_size + 8C  Arena data:     8-byte-aligned bump allocations
```

where `C = outer_cap` (power of two). For non-list nested kinds (`HashMap`, `HashSet`, `Aggregate`), `num_fields = 0`,
`row_size = 0`, and `descriptor_size = 0`, so their layout is unchanged.

Each live outer pointer addresses an inner list container inside the arena's `used` bytes:

```text
ptr + 0  count: u32      (0 <= count <= capacity)
ptr + 4  capacity: u32   (>= 1)
ptr + 8  rows:           row_size × capacity bytes
```

Row `i` (`0 <= i < count`) sits at `ptr + 8 + i * row_size` and is byte-identical to a top-level `ORDERED_LIST` struct
row of the same descriptor:

```text
[bitset: ceil(num_fields / 8) bytes][field_0][field_1]...[field_{N-1}][zero pad to 4-byte multiple]
```

Bit `f` (`bitset[f / 8] & (1 << (f % 8))`) is `1` iff field `f` was provided and valid on that append; unset fields and
alignment padding are zero.

Geometry validation (`state_init::validate_state`, `nested::check_inner`) verifies that `inner_initial_cap >= 1`,
`inner_agg_type == 0`, `num_fields >= 1`, every descriptor byte is a valid non-arena `StructFieldType`, `row_size`
equals `compute_struct_row_layout_padded(num_fields, descriptor).row_size`, and every live outer pointer's container
(`8 + capacity * row_size`, with `capacity >= 1` and `count <= capacity`) lies wholly inside
`[arena_data, arena_data + arena_used]`.

## 4. Append opcode and semantics <a id="smoo/columine!n/nested-ordered-list-append"></a>

`NESTED_LIST_APPEND_STRUCT` (`0x93`) is a `FOR_EACH` / `FLAT_MAP` body instruction with operands:

```text
0x93  slot:index  outer_key_col:index  num_vals:u8  [(val_col:index, field_idx:u8) × num_vals]
```

The value tail matches `LIST_APPEND_STRUCT` (`0x85`) and shares its scalar-field validation and row writer:

- `outer_key_col` reads a `u32` lane; each `val_col` reads at the lane declared by `field_idx` (`u32` for `UInt32`,
  `String`, `Bool`; `i64` for `Int64`; `f64` for `Float64`), enforced by `column_uses` and `row_exprs`.
- Per element, in batch element order:
  1. Validate the slot kind, inner kind, `outer_key` column bounds, and every `(val_col, field_idx)` pair against the
     stored descriptor before planning.
  2. Probe `outer_key` in the outer table (sentinel keys `>= TOMBSTONE` refuse with `InvalidKey`).
  3. On first sight of `outer_key`, allocate a new inner list of capacity `inner_initial_cap` in the arena
     (`count = 0`), insert `(outer_key, ptr)` in the outer table, and increment outer `SIZE`.
  4. If the existing inner list is full (`count == capacity`), allocate a new inner list of capacity `2 * capacity` in
     the arena, copy all `count * row_size` bytes of existing rows in order into the new container, and re-point the
     outer entry to it.
  5. Zero the new row's `row_size` bytes, write each valid provided field and set its bit in the presence bitset, and
     increment `count`.
- **Order and split invariance.** Every matching element appends one row under its outer key in element order, including
  repeated outer keys within one batch. Executing a sequence of rows as one batch or one row per batch yields identical
  per-key lists.
- **Refusal atomicity.** Any refusal (`InvalidProgram`, `InvalidKey`, `ColumnUnderrun`, `CapacityExceeded` /
  `NeedsGrowth`, `OutOfMemory`) happens before the plan applies and leaves the outer table, arena header, inner lists,
  change flags, and write sequence unchanged.
- **Undo and delta.** Plans retain the exact overwritten byte ranges (outer key/size/pointer, arena usage, inner list
  header, copied and appended rows, change flags, and write sequence) so undo rollback and delta redo restore or replay
  the exact image bytes.

## 5. Growth and capacity admission <a id="smoo/columine!n/nested-ordered-list-growth"></a>

- **Inner growth.** When an inner list fills (`count == capacity`), it doubles inside the slot's arena, preserving every
  row and its order.
- **Outer / arena growth.** Exhausting the outer table load limit or the slot's arena signals `NeedsGrowth` for the
  slot. `grow_state` doubles `outer_cap` (which doubles the arena capacity), copies the prefix and field descriptor,
  copies `arena_used` bytes verbatim, and relocates every live outer pointer by the arena-data delta.
- **Capacity admission.** `capacity.rs` delegates `NestedListAppendStruct` to `nested::batch_demand` without
  deduplicating repeated outer keys, simulating the exact per-key append count and doubling allocations to decide
  admission before any write.

## 6. Change feed and watch log <a id="smoo/columine!n/nested-ordered-list-feed"></a>

As specified in [change feed](07-change-feed.md) §4 and §7, a `NESTED_LIST_APPEND_STRUCT` write marks
`ChangeFlag::INSERTED`, advances `WRITE_SEQ`, overflows the slot in an armed change feed, and appends one keyless
whole-slot entry with no prior to the watch log.

## 7. Logical export and import <a id="smoo/columine!n/nested-ordered-list-export"></a>

As specified in [logical export](03-logical-export.md) §2 and §3:

- Export validates geometry, exports `field_types` from the slot's stored descriptor, exports outer entries in ascending
  logical-key order, and exports each key's inner rows in stored index order (`0..count`), resolving `String` fields
  through `InternLookup`.
- Import checks that target and logical descriptors match, sizes each inner list to the smallest power-of-two multiple
  of `inner_initial_cap` covering its rows, grows the target slot through `grow_state` as needed, re-interns `String`
  fields, and writes each list in order with change flags clear.
- `rewrite_slot_set` always transfers nested slots through the logical path because outer pointers are absolute.

## 8. Embedder read helper <a id="smoo/columine!n/nested-ordered-list-reader"></a>

`columine_vm::nested::nested_list_lookup(state, slot, outer_key) -> Result<Option<NestedListView>, ErrorCode>` probes
`outer_key` in O(1) expected time, checks the inner container bounds via `check_inner`, and returns:

- `Ok(Some(view))` with `rows_offset`, `count`, `capacity`, `row_size`, `descriptor_offset`, `num_fields`, and
  `bitset_bytes` when `outer_key` is present;
- `Ok(None)` when `outer_key` is absent from a well-formed nested `OrderedList` slot;
- `Err(ErrorCode::InvalidSlot)` when `slot` is out of range;
- `Err(ErrorCode::InvalidProgram)` when `slot` is another valid slot kind or a nested slot whose valid inner kind is not
  `OrderedList`;
- `Err(ErrorCode::InvalidState)` when the image, metadata, prefix, arena bounds, or matched inner list container is
  malformed.
