# Struct-map upsert-last-if

## 1. Scope

`BATCH_STRUCT_MAP_UPSERT_LAST_IF` is the struct-map record form of `BATCH_MAP_UPSERT_LAST_IF` (`0x2a`): a unary
struct-map upsert-last whose write each element's predicate cell decides. A live row expression
([stored-row sweep](01-stored-row-sweep.md) §2) computes that cell from the incoming row's columns (`incoming`) and the
destination's stored row for the element's key (`_`). A true predicate writes the incoming row, exactly as `0x80` does.
A false one writes nothing, even on an absent key, which stays absent. Columine adds no predicate language: the
expression and its null semantics are the embedder's, and the VM supplies the binding and the typed field bytes.

## 2. Encoding

`0x8f` is followed by the `0x80` row operands,
`slot:index, key_col:index, num_vals:u8, (val_col:index, field_idx:u8) × num_vals, num_arrays:u8, (offsets_col:index, values_col:index, field_idx:u8) × num_arrays`,
then `pred_col:index`, in the trailing position where `0x82` and `0x8b` put their field ordinal. Each index is the
unsigned LEB128 `u32` every operand shares. The Rust registry names it `Opcode::BatchStructMapUpsertLastIf` and the
TypeScript registry `Opcode.BATCH_STRUCT_MAP_UPSERT_LAST_IF`. The frozen ABI fixture lists the byte and its dispatch
membership.

Like `0x2a`, it is a `FOR_EACH` or `FLAT_MAP` body instruction only. At top level, execution refuses it as
`InvalidProgram` before any write, and a bind refuses a live predicate column there, because a top-level read of a live
column would see its placeholder cells. Every program walk reads the trailing operand:

- body length and widening;
- column uses, where the predicate is an element predicate and every other operand a plain read;
- capacity effects, where the destination is charged as `0x80` charges it, an upper bound the predicate can only lower;
- removal order, where the instruction is a producer of its destination ([log-order removal](05-log-order-removal.md));
- companion pairing, where it is a write of its destination;
- value-object input links, through its array fields.

## 3. The predicate at its element

An element first decodes and validates its field mapping as `0x80` does. A removal cut that covers the element's key at
its row then skips it before the predicate is read, so a cut element evaluates nothing. Otherwise the predicate cell is
read at the element:

- **Live column.** The expression evaluates now, through the live-column handle, against the state every earlier element
  of the batch left. `BatchView::stored()` answers `StoredBinding::Row` with the destination's `StoredRow` for the
  element's key, or `StoredBinding::Absent` when the key holds no row. `BatchView::stored_row()` answers that row or
  `None`. `StoredBinding::Unbound` is the binding of ordinary row evaluation and of admission, where no stored row is in
  scope.
- **Bound column.** A column the batch carries, or a bound row expression, is read at the element. A column that does
  not cover the element is `ColumnUnderrun`.

A nonzero cell writes and zero writes nothing. The predicate evaluates at its own row, after the rows before it landed,
so one batch of `n` rows and `n` batches of one row store the same rows. As for `0x8b`
([first-since-reset](02-first-since-reset-upsert.md) §3), the guarantee is batch-partition parity, not a merge of
independently reduced rows.

Field reads are typed. `StoredRow::field_type` names a field's physical type. `StoredRow::field` borrows its stored cell
exactly: an `INT64` field is its eight little-endian bytes, which an embedder compares as `i64` with no `f64` widening
(microsecond times past 2^53 differ by one), and a `STRING` field is its four-byte intern id, compared by id. An unset
field is `None`. A field read through `_` on an absent key is the embedder's null, and short-circuiting it is the
embedder's to do.

## 4. Mutation and recovery

A refused element mutates nothing: no bitset clearing, arena write, journal record or change flag. An admitted element
is the `0x80` row write: the same undo capture and delta redo, and the same `INSERTED` or `UPDATED` change flag.

Capacity admission charges the destination like `0x80`. When its bounds cannot clear the batch, the dry run evaluates
the predicates on a copy of the image and answers exactly. A `NEEDS_GROWTH` refusal therefore leaves the image as it
was, the host grows the named slot and runs the batch again, and an element the predicate refuses never needs capacity.
After warm-up, a bind into retained row storage and the batch's execution allocate nothing: the stored-row view borrows
the image.

## 5. Behavioral acceptance

Tests fold the lease-winner predicate over an interned action and holder and `INT64` microsecond times:

```text
(incoming.action == 'claim' && (_ == null || _.action == 'release' || _.holder == incoming.holder
    || incoming.atMicros > _.untilMicros))
|| (incoming.action != 'claim' && _ != null && _.action != 'release' && _.holder == incoming.holder)
```

They cover its transition matrix, each scenario on its own key and all of them interleaved in one batch:

- a claim on an empty key writes, while a renew or release there leaves the key absent;
- a foreign claim at or before `until` is inert, and one microsecond after `until = 2^53` it takes over;
- the holder's claim, renew and release write, and the holder renews after expiry;
- a foreign renew or release is inert, even after expiry;
- a released lease is acquired by a foreign claim.

Every sequence of up to three rows over the actions, two holders and times straddling 2^53 folds to an ordered model's
leases. It folds to byte-identical sealed images as one batch and as one batch per row. The tests also check:

- each element reads the row the elements before it left;
- a removal cut skips its element before the predicate;
- inert rows journal nothing, and written rows roll back and export a delta that replays byte for byte;
- a growth refusal leaves the image as it was, and its retry folds the model, while predicate-refused rows need no
  growth;
- a warmed bind and batch allocate nothing;
- a bound predicate column gates elements, and a short one is `ColumnUnderrun`;
- the trailing operand parses under `FLAT_MAP`;
- the instruction is refused at top level.
