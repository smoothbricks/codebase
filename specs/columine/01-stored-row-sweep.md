# Stored-row sweep

## 1. Scope

A struct-map expiry evaluates the values the map stores, not only incoming values. One batch folds exactly as its rows
one batch per row, in log order. Before each row, TTL evicts against that row's event clock. After all that row's
writes, stored-row expiry runs, including when the row is inactive or its type writes no slot. The clock is the event's
own i64 timestamp in nanoseconds, never a batch maximum, a slot's custom age column, or the wall clock. A batch crosses
the embedder boundary once; the VM borrows its original columns throughout the row fold.

The embedder classifies stored predicates. A time-independent predicate is evaluated only on changed stored keys. A
time-independent guard conjoined with a monotone field deadline schedules a hidden HASHMAP companion identified by its
owner and the role `expiry`. Its comparison lane and the existing TTL expiry index hold exact i64 nanoseconds. After
each row's writes, the next-deadline minimum is compared once with that row's clock; only due entries are drained. An
expiry remains an erasure if a later row has an earlier timestamp. Other time-dependent predicates are refused by the
embedder's compiler: they never fall back to a full stored-row scan per row.

`Vm::execute_batch*` executes logical event rows. A TTL reducer must bind its event clock explicitly; a missing binding
is `InvalidProgram`, never inferred from an age column. `Vm::execute_mutations_in` is the separate entry for
already-explicit non-row writers, including RETE relation and closure programs compiled from the same slot declarations.
It executes the same mutation kernels, without advancing time, running expiry or clearing the event batch's evicted
buffer. Such a program carries no `TMP1` table or sweep instruction. It is a transition of its own, not a fake event.
`Vm::execute_mutations_delta_in` is its paired-recording counterpart, following the event batch API's existing `_delta`
convention; both retain the same non-row semantics and take an optional borrowed live-column binding. Unlike the batch
API's separate live wrappers, the mutation pair exposes that existing Option directly; no unused public live wrappers
are added. The generic WASM ABI exposes the same distinction as `vm_execute_batch[_delta]` and
`vm_execute_mutations[_delta]`; `ColumineBackend.executeBatch[Delta]` and `executeMutations[Delta]` forward explicitly
to their matching entry. Neither chooses a lane from missing metadata. Algebra-only and explicit-eviction writers use
the mutation entry, preserving even unordered f64 expiry values until an explicit eviction.

The existing evicted buffer is cleared once per nonempty logical event batch and appends each row's evictions in row
order. The batch's capacity admission bounds its room; overflow refuses rather than dropping an observation. A scalar
eviction alpha binds the first buffered observation. No second queue or per-row embedder callback is introduced. Empty
and refire invocations preserve current observations and perform no temporal transition.

## 2. Encoding and predicate binding

`STRUCT_MAP_SWEEP` is opcode `0x8a`, followed by `slot:index, predicate_col:index`. The Rust registry names it
`Opcode::StructMapSweep` and the TypeScript registry names it `Opcode.STRUCT_MAP_SWEEP`; the frozen ABI fixture includes
the same byte and dispatch membership. Each index is the same unsigned LEB128 `u32` used by the other instruction
operands. The opcode is a reduce-section instruction, not an init declaration or a per-element body instruction.

### Integer deadline index

On a TTL HASHMAP slot, flag `0x80` declares `integer_deadline`: both its comparison lane and each expiry entry's first
eight bytes are little-endian i64 nanoseconds. Other TTL slots retain their existing f64 timestamp lane. The existing
TTL tail remains present; an expiry companion declares zero TTL and grace. `TtlMutation::UpsertI64` supplies an exact
integer timestamp, and a mutation of the wrong lane is `InvalidCellValue` before any write. Remove-only plans select the
lane from the slot flag. Integer and floating timestamps share the existing retained placement planner, captures and
index geometry; there is no second schedule. Raw integer bytes are never interpreted through f64.

### Temporal program table

The `TMP1` extension (`0x31504d54`) carries `clock_col:u32le, count:u32le`, then `count` fixed 24-byte entries:
`slot:u32le, predicate_col:u32le, companion:u32le, field:u8, field_unit:u8, reserved:u16le=0, age_nanos:i64le`. The
clock column is always i64 nanoseconds. `companion=0xffffffff` means a time-independent predicate; otherwise it names
the owner's integer-deadline HASHMAP. `field_unit` is the deadline field's time-unit code, the one the Timestamp plane's
`type_param` uses: `0` seconds, `1` milliseconds, `2` microseconds, `3` nanoseconds; the field's value is scaled to
nanoseconds, and any other code is `InvalidProgram`. A floating TTL slot's age, TTL and grace stay seconds and are
compared with the row clock's nanoseconds divided by 10^9. The predicate is the static guard for an indexed expiry, or
the whole static predicate. Constants must convert exactly to an i64 age; checked field scaling and deadline addition
refuse `InvalidCellValue` on nonintegral, nonfinite or overflowing values. `STRUCT_MAP_SWEEP` names the table's
slot/predicate pair, once at the reduce section's top level, after its writes. The table, not a second host clock
argument, binds the row clock.

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

A live predicate evaluates against a borrowed changed stored row after that logical row's writes; a zero predicate keeps
the row (and removes an indexed schedule), a nonzero static predicate erases it, and a nonzero deadline guard refreshes
the key's schedule. A removed key removes its schedule. Predicate match gating includes every admitted row. Malformed
geometry, invalid slots, unavailable columns and predicate failures use existing typed VM errors.

## 3. Mutation and recovery

Removing a row removes its key from the slot's key index, decrements the map's size, and records the slot's removal
change flag. It does not rewrite a kept row or a removed row's bytes, and does not materialize a second map. Each
removal is one capture, under the same sparse `UndoState` authority as other struct-map writes, of the size and the
bytes the index plan names. Undo restores the removed keys, size and change flags; the delta mutation lane retains
physical redo for the same bytes. An idle sweep sets no removal flag.

The VM reuses the changed-key feed's mutation authority and reserves changed-key storage at batch admission. It sorts
and deduplicates each row's changed keys, never scanning every stored row to discover changes. Predicate evaluation
constructs no row objects or column arrays; state readers stay immutable borrows and introduce no lock. Index planning,
growth, exact rollback, paired delta redo and logical export/import preserve each owner's schedule.

## 4. Behavioral acceptance

Owner-boundary property tests compare a whole batch with singleton rows, including nonmonotone event times, repeated
keep-last writes, inactive and irrelevant rows, changed guards, absent optional deadline fields and inclusive deadlines.
They assert the no-expiry path's one minimum comparison per row without visiting stored rows, exact rollback and delta
continuation, and typed refusal before mutation for invalid lane, constant conversion and deadline overflow.
