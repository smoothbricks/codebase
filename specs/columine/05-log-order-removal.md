# Log-order removal

## 1. Contract

A removal erases its key at its row's log position inside the target slot's fold. A batch's rows are its log order, and
a `FLAT_MAP` element takes its parent row's position. Rows at or before a removal of their key contribute nothing to the
slot, rows after it fold into a fresh row, and a row that writes and removes one key composes write-then-erase. One log
therefore folds to the same slot whether it arrives as one batch or as several: under a `last`, `first` or `max` pick,
`[add k, remove k]` leaves `k` absent, `[remove k, add k]` leaves it present, and `max` over `[add 5, remove k, add 3]`
keeps 3, under every batching (the owner's ruling, 2026-10-30).

The removals are `BATCH_STRUCT_MAP_REMOVE` (`0x8c`, one key), `BATCH_STRUCT_MAP2_REMOVE` (`0x86`, an exact pair),
`BATCH_STRUCT_MAP2_REMOVE_PREFIX` (`0x8e`, every pair under one first key), `BATCH_MAP_REMOVE` (`0x23`) and
`BATCH_MAP_REMOVE_IF` (`0x2b`, only the rows whose predicate passes). Set and bitmap removals (`0x31`, `0x35`) and the
stored-row sweep ([stored-row sweep](01-stored-row-sweep.md)) are outside this contract and apply where they run.

## 2. Program order

Every removal of a slot precedes every producer of that slot in the reduce section, `FOR_EACH` and `FLAT_MAP` bodies
included. A producer is an instruction whose capacity effect inserts or rewrites the slot's rows, read from the one
table batch admission reads (`capacity::effects`): every struct-map, two-key and hash-map upsert family, a counts fold,
a guarded upsert's destination, a probe's output slot and a scatter's destinations. A program that removes from a slot
after producing into it is `InvalidProgram`, refused before its first batch writes. The check runs once per program and
is cached with the companion-pairing check. A removal may follow another slot's producer. One `FOR_EACH` body runs row
by row, so a body that removes and then produces folds each row in log order.

## 3. The per-batch cut

A removal keeps its erase, journaled as before, and records a cut: for its slot and key (the key, the pair, or the first
key of a prefix) the latest batch row that removed it. A sentinel key records nothing. A producer skips a row or element
whose position is at or before the cut for its key; on a two-key slot the cut of the pair or of its first key applies.
The removal already erased what earlier batches left, so the skip leaves exactly the rows after it.

Cuts are VM working memory, never image bytes. Each batch run (a dry run and the run after it alike) starts with none.
Batch admission reserves one cut per removal execution from the row-count bound before the first write, and a sealed
batch never grows them. Only the start of a batch run forgets cuts: a reservation made inside one (a container call's
own scratch in a dry run) keeps the cuts already recorded. A producer reads its slot's cuts ordered by key, by binary
search; the cuts recorded since the last read are merged first, a short tail by binary insertion and a long one by
sorting.

## 4. Reads of a removal target

A read of a slot that a later row of the batch removes from (a probe's source, a guarded upsert's companion, a guarded
scatter's stored transaction) sees the slot as the removals left it, not as it stood at the reading row. A guarded
scatter element at or before a removal of its guarded row writes nothing to that row and treats it as absent.

## 5. Behavioral acceptance

Tests fold each removal kind with each producer pick it serves, at top level, in one body, in bodies gated by event type
and under `FLAT_MAP`, as one batch and as one batch per row, and require byte-identical sealed images and the outcomes
above. A rolled-back batch and a capacity-refused batch leave the image byte for byte, a warmed batch records and reads
its cuts without allocating, and a removal after its slot's producer refuses the program before any write.
