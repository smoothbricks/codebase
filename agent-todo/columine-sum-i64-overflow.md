# columine: an i64 sum wraps on overflow

## Need

`BATCH_AGG_SUM_I64` documents "lossless i64 accumulation" (`packages/columine/crates/columine-types/src/types.rs:1006`),
but `reduce_col_i64` adds with `wrapping_add` in both its two-lane and its scalar path
(`packages/columine/crates/columine-vm/src/aggregates.rs:412-470`). A sum past ±2^63 silently answers a wrong value. A
wrapped sum is never a correct answer, and an embedder that sums amounts or counts cannot tell it happened.

## Spec first

An anchored sentence in `specs/columine/07-wide-row-cells.md` §4, or in an aggregates spec of its own: an i64 sum
refuses on overflow and leaves the slot as it was.

## Red first

In `columine-vm/tests/aggregates.rs`, fenced with that node: `an_overflowing_i64_sum_is_refused`. Drive it through the
VM's execute entry, not the kernel, with an i64 column `[i64::MAX, 1]` summed as one batch and as one batch per row,
with and without a type mask and a predicate (`_IF`). Each run is refused with an error naming the slot, and the slot
keeps its prior value. An in-range sum is unchanged.

## Fix

Checked adds in both paths. The overflow surfaces through the three dispatch sites
(`columine-vm/src/vm.rs:5839,6482,6668`) as an `ErrorCode` (`columine-types/src/types.rs:1194`), before any slot write.
Measured on a wasm fold by hand: 0.36 to 0.45 ns per row. Report before and after on the aggregates bench.
