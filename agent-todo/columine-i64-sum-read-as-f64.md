# The wasm backend reads an i64 sum slot as f64 bits

`packages/columine` `wasm-backend.ts` `getAggregateValue` reads every aggregate slot's eight bytes as an f64. A slot
written by `SUM_I64` holds an i64 (specs/columine/07-wide-row-cells.md §4, "An i64 sum is exact or refused"), so the
value it answers for such a slot is the float whose bits are the integer's.

Change: read the slot at the lane its aggregate type declares, i64 for `SUM_I64` and the keyed i64 sum, f64 otherwise,
as `NESTED_AGG_UPDATE` does on the write side. Answer an i64 as a `number` while it is a safe integer and a `bigint`
beyond.

Red first: a program with one `SUM_I64` slot over the rows 1, 2, 3 answers 6 through `getAggregateValue`.
