# Wide row cells

## 1. Scope

A row-expression entry derives one batch column per row: the program's row-expression table follows its reduce section
and the embedder's `RowExpression` evaluates each entry ([stored-row sweep](01-stored-row-sweep.md) §2). An entry's cell
is a u32 (an intern id, a 0/1 predicate, an exact u32), an f64 or an i64. A computed record field, such as a product of
two prices or a microsecond time past 2^53, reaches an f64 or i64 aggregate, struct-map field or comparison through a
wide cell at full precision, with no host-side evaluation. The expression language stays the embedder's. Columine owns
the lane, the cell storage, the splice, and the refusal of a read at another lane.

## 2. Encoding

An `RXP1` table (`ROW_EXPRESSIONS_MAGIC`, `0x3150_5852`) is unchanged, and every entry in it is u32. An `RXP2` table
(`LANED_ROW_EXPRESSIONS_MAGIC`, `0x3250_5852`) carries `lane:u8` after each entry's target column:

```text
u32   magic "RXP2"
u16   count
index type_col
count × {
  index target_col
  u8    lane
  index match_count       ≥ 1
  u32 × match_count
  u16   expr_len
  expr_len bytes
}
```

The lane byte is a `CmpType`, the input-stride code a comparing upsert's `cmp_type` already names: 0 is u32, 1 is f64
and 2 is i64. Any other byte refuses the bind with `InvalidProgram`. A compiler writes RXP2 only when an entry is wide,
so every program written before stays byte-identical and binds as before.

## 3. Evaluation and storage

`RowExpression::admit(expr, lane, reads)` and `RowExpression::eval(expr, lane, batch, row, strings)`
([row interning](09-row-interning.md)) receive the entry's lane. Admission is the program's, not a batch's:
`RowPlan::of(program, reduce_reads, eval)` parses the table and admits each entry once, and every
`bind_row_columns(program, plan, …)` of that program runs the plan (an `AdmittedProgram` makes its plan at its first
`execute_bound` and keeps it), so no bind parses the table or admits an entry again (a host that binds the same program
for every committed row pays the admission per row without it). An evaluator refuses at admission, with
`InvalidProgram`, an expression that cannot answer the lane, such as a predicate in a wide lane, and names to `reads`
each column the expression reads and the lane it reads it at. It answers each row with a `RowCell` (`U32`, `F64` or
`I64`). The bind stores a cell only in its entry's lane. A cell of another lane, or a value the lane cannot hold,
refuses the bind with `InvalidCellValue` before any column is spliced.

`RowColumns` keeps each entry's cells in 8-byte words, two u32 cells or one wide cell to a word, so a wide column is
aligned for its typed view. The splice hands an entry's column to the reduce section as `batch_len` cells of the lane's
stride in little-endian bytes. A u32 column is byte-identical to the column an RXP1 entry binds, and a wide column has 8
bytes per row. A row outside the entry's match set keeps the zero cell (`0`, `+0.0`, `0`). A later entry reads an
earlier entry's column through `BatchView::column` at that column's stride. The plan refuses with `InvalidProgram` a
read of an earlier entry's column at another lane than that entry's; every other column a plan reads is the host's, and
a bind refuses one that does not hold `batch_len` cells of the lane it is read at with `ColumnUnderrun`, checking each
once. After warm-up a bind allocates nothing: an entry's words grow to the longest batch once and are reused.

## 4. Readers

A host `ColumnInput` carries its raw typed-array view only. It has no duplicate element-type label: the bytecode's
operand and slot declarations are the authority for interpreting those bytes, including the temporal table's i64
microsecond clock. An integer clock is never labeled as a float to satisfy a host wrapper. The reduce section reads a
wide derived column exactly as it reads a host column of the same lane. The operand walk (`column_uses`) names the lane
of every column operand:

- **Fixed by the instruction.** `BATCH_AGG_SUM`, `BATCH_AGG_MIN`, `BATCH_AGG_MAX` and their `_IF` forms read f64 values.
  `BATCH_AGG_SUM_I64`, `BATCH_AGG_MIN_I64` and `BATCH_AGG_MAX_I64` read i64 values. A TTL timestamp, whether a TTL
  slot's timestamp column, `BATCH_MAP_UPSERT_LAST_TTL`'s or `BATCH_SET_INSERT_TTL`'s, reads f64. So do
  `BATCH_SCALAR_LATEST`'s comparison and `NESTED_AGG_UPDATE`'s value. The two-key tuple max and min upserts compare i64
  columns. Keys, hash-map values, set elements, predicates, offsets, scatter route and op columns, and the `FOR_EACH`
  type column read u32.
- **Named by the instruction.** A comparing map upsert's timestamp or comparison column reads at its `cmp_type`.
- **Declared by the slot.** A struct-map, two-key struct-map or struct-list field value reads at its field's lane: INT64
  is i64, FLOAT64 is f64, and UINT32, STRING and BOOL are u32. This covers the struct-map upserts, the guarded upsert, a
  scatter's kind-0 value, a guarded scatter's guards and `LIST_APPEND_STRUCT`. A scalar list's element reads at its
  element kind, and `BATCH_SCALAR_LATEST`'s value at its slot's scalar kind.

A `FLAT_MAP` body's timestamp operands read the parent timestamp column instead of their own, at their own lanes, so the
walk names the parent timestamp column at each of them.

The bind refuses with `InvalidProgram` a program that reads a wide column through an operand of any other lane: a u32
operand would read half a cell, and an f64 operand an i64's bits. A CSR pair's columns and an array or value-object
field are never read as a wide column, and neither is the table's type column. The walk reads only the program's bytes
(a TTL slot's timestamp column is the one its init definition names, which every state laid out for the program
records), so its answer, `ReduceReads`, is derived once per program and handed to every bind; no bind walks the reduce
section. The table's targets and lanes are program bytes too, and which entries bind live is the evaluator's answer at
admission, so every refusal of a derived column's reader is the program's: the plan judges it once, beside the reads,
against the last entry targeting each column (the one spliced), and no bind is reached. A live entry's column refuses
when some operand reads it plainly, the type column refuses when it is live, and a table that does not parse refuses the
plan. A u32 entry is read exactly as before.

## 5. Live entries

The live-column handle answers u32 predicate cells to the `*_IF` family and to the stored-row sweep. A predicate is a
u32 operand, so the check above refuses a wide live entry that the reduce section reads, and one it never reads binds
the zero placeholder like any live column.

## 6. Behavioral acceptance

`columine-vm/tests/wide_row_cells.rs` covers:

- an f64 entry answering `1.5 × n` read by `BATCH_AGG_SUM`, and an i64 entry answering `2^53 + n` read by
  `BATCH_AGG_SUM_I64`, whose sums are exact folded as one batch and as one batch per row;
- the same entries filling a FLOAT64 and an INT64 struct-map field, which store `1.5 × n` and `2^53 + n` exactly under
  both cuts;
- a later i64 entry reading an earlier i64 column through `BatchView::column`, the plan holding its lane;
- an RXP1 table binding the u32 cells an RXP2 u32 entry binds;
- refusals of a cell of another lane than its entry, an f64 column read by the i64 sum, a field of another kind, a u32
  predicate reading a wide column, a wide type column and an unknown lane byte, each leaving every column as the host
  passed it;
- a warmed wide bind that allocates nothing.
