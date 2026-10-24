# First-since-reset struct-map upsert

## 1. Scope

A unary struct map keeps one row per key. Its upserts differ only in which incoming row replaces the stored one:

| Opcode | Name                                        | A row replaces when                                                             |
| ------ | ------------------------------------------- | ------------------------------------------------------------------------------- |
| `0x80` | `BATCH_STRUCT_MAP_UPSERT_LAST`              | always                                                                          |
| `0x81` | `BATCH_STRUCT_MAP_UPSERT_FIRST`             | the key is absent                                                               |
| `0x82` | `BATCH_STRUCT_MAP_UPSERT_MAX`               | the key or stored comparison is absent, or the incoming one is strictly greater |
| `0x8b` | `BATCH_STRUCT_MAP_UPSERT_FIRST_SINCE_RESET` | the key is absent, the stored reset is true, or the incoming reset is           |

`FIRST_SINCE_RESET` keeps, for each key, the first row since that key's last reset. A row whose reset field is true is
the reset: it replaces the stored row and is itself stored. The next row after it opens the key again, and every later
row with a false reset leaves that opening row in place until the next reset. `FIRST` is the case with no reset rows.

The decision reads only the stored row and the incoming row; it adds no predicate language. A host keeps an interval
open from its first event through a closing event with it: an outage opens at the first `down` after the last `up`, and
the stored row carries the opening `down`'s fields until an `up` resets it.

## 2. Encoding

`0x8b` is followed by the `0x80` row operands,
`slot:index, key_col:index, num_vals:u8, (val_col:index, field_idx:u8) × num_vals, num_arrays:u8, (offsets_col:index, values_col:index, field_idx:u8) × num_arrays`,
and then `reset_field_idx:u8`, the same trailing position `0x82` gives its comparison ordinal. The Rust registry names
it `Opcode::BatchStructMapUpsertFirstSinceReset` and the TypeScript registry
`Opcode.BATCH_STRUCT_MAP_UPSERT_FIRST_SINCE_RESET`; the frozen ABI fixture lists the byte and its dispatch membership.
It executes at top level and as a `FOR_EACH` or `FLAT_MAP` body instruction, and every program walk (body length, column
uses, capacity effects, value-object input links) reads the trailing ordinal.

The reset ordinal names a `BOOL` field the instruction itself maps from a scalar column. Every unary struct-map upsert
maps each field at most once, because a pick lane binds one column and the row writer must store that same column. A
reset ordinal outside the record, naming a field of another kind or a field the instruction does not map, and an
instruction mapping one field twice, are `InvalidProgram` before any row is decided, at top level and in a block. The
incoming reset is the row's `BOOL` cell, a `u32` lane where nonzero is true. A stored row whose reset field is absent
reads it as false.

## 3. Ordered fold

Rows apply in input order, one at a time, against the state the previous row left; the instruction never reduces rows
into a summary first. The decision is not associative over final rows: with `a = down₁`, `b = up`, `c = down₂`,
`(a ⊕ b) ⊕ c` keeps `down₂` while `a ⊕ (b ⊕ c)` keeps `down₁`. Columine therefore defines no merge of independently
reduced rows for this pick, as it defines none for `FIRST`, `LAST` or `MAX`. What it guarantees is batch-partition
parity: the same row sequence gives the same state whether it arrives as one batch or split across batches. A compiler
that lowers several sources into one slot preserves their relative row order, or the result depends on the lowering.

## 4. Mutation and recovery

A row is decided before any bitset clearing, arena write, journal record or change flag. A skipped row mutates nothing.
An accepted row is the `0x80` write: it replaces the whole row, scalar and array fields, through the same undo capture
and delta redo, and stamps the same change flags. The pick is bound to its mapped column before any row is decided: once
for a top-level instruction's row loop, and before each element's decision in a block.

## 5. Behavioral acceptance

Tests drive the outage sequence `down, down, unwatch, up, down, down, unwatch` and keep the first `down` after the `up`;
a single batch `down, down, unwatch, up, down, down` folds to the same row; every sequence of up to five `down`, `up`
and `unwatch` rows gives an ordered model's row and byte-identical state whether run as one batch or one row per batch,
at top level and under `FOR_EACH`; an absent stored reset reads false; a skipped row leaves state, journal and change
flags untouched while an accepted row rolls back exactly; a `FLAT_MAP` body reads the trailing ordinal; an unmapped,
duplicated, out-of-range or non-`BOOL` reset lane is refused without mutation at top level and under `FOR_EACH`; a
truncated instruction is refused at admission and at top-level execution; a reset column shorter than the batch is
`ColumnUnderrun`; and an accepted row's exported undo and redo replay byte-exact.
