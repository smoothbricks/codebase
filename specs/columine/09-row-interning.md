# Row interning

## 1. Scope

A row-expression entry's u32 cell may name a string by its intern id, the handle a host converter writes for a string
field. A row expression that builds a string, such as a composite key joined from two key parts, must answer that
string's id in the same intern table the host's ids and the state's key lanes resolve through, so a key it builds and a
key the host sent are one key. The expression language and the intern table stay the embedder's. Columine owns when the
table may be written: the bind hands the evaluator the table to read and intern into, and the reduce section only reads
it.

## 2. The interner

The interner is the caller's [logical export](03-logical-export.md) `Interner`: `InternLookup::text(id)` names an id's
text, and `Interner::intern(text)` answers the id of `text`, adding it when new. Equal text answers one id, so a refold
of the same log in the same order makes the same ids. A table that cannot take the text answers its refusal as an
`ErrorCode` (`OutOfMemory`, `CapacityExceeded`), never a panic, and the bind refuses with it before any column is
spliced. `NoStrings` is the interner of a state with no string table: it names no text and refuses an intern with
`InvalidProgram`.

## 3. Bind and reduce

`bind_row_columns(program, state, cols, batch_len, eval, strings, rows)` takes the interner as `&mut dyn Interner` and
hands it to `RowExpression::eval(expr, lane, batch, row, strings)` for every bound entry, in table order and row order.
An entry that makes strings answers each id as its u32 cell; a wide lane holds no id. The bind is the only writer:
`AdmittedProgram::execute_bound` takes the interner as `&mut S` for the bind, then hands the reduce section `&S`, so the
key lanes the reduce section resolves can never see the table change under them.

A live entry evaluates during the reduce section, with `NoStrings`: it reads no text and makes none, since a live cell
is a u32 predicate ([wide row cells](07-wide-row-cells.md) §5). An evaluator refuses at admission, with
`InvalidProgram`, a live expression that would read or make text. A resume (`execute_spliced`) re-evaluates only live
entries and so needs no interner; the ids its bound cells hold were made by the bind it resumes.

## 4. Behavioral acceptance

`columine-vm/tests/row_interning.rs` covers:

- an entry that joins the texts of two id columns with `::` and interns the result, whose ids are equal for equal joined
  texts within a batch and across batches, and resolve through the interner to the joined text;
- the derived ids keying a hash map, which holds one entry per distinct joined text whether the host sends one batch or
  one batch per row;
- an interner refusal refusing the bind with its code, leaving every column as the host passed it;
- `NoStrings`, with which a live entry evaluates, naming no text and refusing an intern with `InvalidProgram`.
