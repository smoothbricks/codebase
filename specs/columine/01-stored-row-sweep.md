# Stored-row sweep

## 1. Scope

A struct-map sweep evaluates a predicate against the rows the map already stores. It is not an incoming-key remove: a
row is eligible even when the current batch contains no event carrying its key. Expression semantics remain the
embedder's existing `RowExpression` semantics. The VM adds no predicate language and does not copy stored rows into
synthetic batch columns.

The compiler places the sweep after the writes whose resulting rows it examines. One instruction scans one unary
struct-map slot once, in its key index's iteration order ([key index](04-key-index.md)), evaluating every predicate
before any row changes. A sweep with an empty batch changes nothing: there is no new logical event row. With a nonempty
batch, the last input row is the predicate's logical input row. A retention predicate reads its clock from that row's
deterministic event time or the embedder's equivalent logical-time binding, never the wall clock. A compiler that needs
expiry on every reducer wake covers every admitted event type in the predicate entry's match set.

## 2. Encoding and predicate binding

`STRUCT_MAP_SWEEP` is opcode `0x8a`, followed by `slot:index, predicate_col:index`. The Rust registry names it
`Opcode::StructMapSweep` and the TypeScript registry names it `Opcode.STRUCT_MAP_SWEEP`; the frozen ABI fixture includes
the same byte and dispatch membership. Each index is the same unsigned LEB128 `u32` used by the other instruction
operands. The opcode is a reduce-section instruction, not an init declaration or a per-element body instruction.

The predicate column uses the existing row-expression table and live-column binding. A live expression receives the
unchanged input batch and a borrowed `StoredRow` view of one live row through
`BatchView::stored_row() -> Option<&StoredRow>`. The view is absent during ordinary input-row evaluation and expression
admission. `BatchView::stored() -> &StoredBinding` answers the same binding as `Row`, or `Unbound` where no stored row
is in scope; `Absent` is the struct-map upsert-last-if's key with no row
([upsert-last-if](06-struct-map-upsert-last-if.md) §3). `StoredRow::key() -> u32` and `num_fields() -> u8` describe the
row; `field_type(field: u8) -> Result<StructFieldType, ErrorCode>` names a field's physical type;
`field(field: u8) -> Result<Option<&[u8]>, ErrorCode>` borrows a present scalar cell or arena payload. An absent field
is `None` and an invalid field ordinal is `InvalidProgram`. The view borrows the slot's validated geometry and is valid
only during that evaluation. No expression can mutate the stored row or retain a reference beyond the borrow.

A bound predicate does not read a stored row and applies its logical input row's existing cell to every stored row. A
live predicate evaluates once for each live stored row at the instruction's apply point; it sees prior instructions'
writes. Existing event-type match gating is preserved. A zero predicate keeps the row; a nonzero predicate removes it.
Malformed geometry, an invalid slot, unavailable columns or a predicate failure are reported with the existing typed VM
error codes rather than silently treated as a false predicate.

## 3. Mutation and recovery

Removing a row removes its key from the slot's key index, decrements the map's size, and records the slot's removal
change flag. It does not rewrite a kept row or a removed row's bytes, and does not materialize a second map. Each
removal is one capture, under the same sparse `UndoState` authority as other struct-map writes, of the size and the
bytes the index plan names. Undo restores the removed keys, size and change flags; the delta mutation lane retains
physical redo for the same bytes. An idle sweep sets no removal flag.

The instruction borrows the slot geometry once for its scan and holds the selected keys in VM working memory reserved at
the VM's existing batch-admission boundary, not allocated per stored row. Predicate evaluation does not construct row
objects or column arrays. State readers remain immutable borrows and introduce no lock.

## 4. Behavioral acceptance

Tests exercise rows whose keys are absent from the incoming batch, selective removal from mixed eligible and ineligible
rows, deterministic logical-time boundaries, empty batches, absent optional fields, invalid slot/predicate inputs, exact
undo and delta redo, and a repeated idle sweep. A row whose predicate returns zero remains present regardless of its
age. The tests assert state and typed failures, not diagnostic wording.
