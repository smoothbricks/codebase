# Program extension tables

## 1. Scope

A program's content ends with its reduce section, then its tail. The VM reads one table there, the row-expression table
([stored-row sweep](01-stored-row-sweep.md) §2, [wide row cells](07-wide-row-cells.md) §2). An embedder also compiles
tables of its own into a program: a reducer carries its input binding, the instructions that fill its input columns from
an event schema's columns. Carried in the program, such a table is part of the program's bytes and so of every identity
taken over them: a change to it alone is a program change. The VM never reads an embedder's table, and an embedder never
reads the row table through this framing.

## 2. Encoding

```text
tail := row_table? extension_table*

row_table        an RXP1 or RXP2 table, as 07 §2 encodes it, opening the tail
extension_table := u32 magic, u32 length, length bytes
```

The row table needs no length: its header and entries delimit it, so a tail that opens with `RXP1` or `RXP2` holds the
row table up to the end of its last entry. Every byte after it, or every byte of a tail that opens with any other magic,
is a sequence of whole extension tables. No two extension tables share a magic, and none takes `RXP1` or `RXP2`.
`columine_types::ProgramTail` splits a tail: `row_table` is the row table's exact bytes, and `extension(magic)` answers
one extension table's bytes. A program with no tables, or with only a row table, encodes exactly as before.

## 3. Refusal

A tail that does not split is `InvalidProgram` wherever the VM reads the row table (`bind_row_columns`, program
admission): a row table that runs past the tail, bytes after it that end inside an extension table's header or bytes,
two extension tables of one magic, or an extension table under a row table's magic. Nothing in an extension table's
bytes is validated by columine; its embedder validates its own table.

## 4. Behavioral acceptance

`columine-vm/tests/row_exprs.rs` covers a program whose row table is followed by an extension table binding and reducing
exactly as the same program without it, a tail of extension tables alone binding nothing, and each refusal above.
`columine-types` covers splitting a tail into its row table and two extension tables found by magic.
