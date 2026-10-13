# lmao: performance gates the specs state and the code does not enforce yet

The lmao specs state their performance contract as design: zero-copy Arrow conversion with measured copies, per-lane
shape and allocation gates for the span writer, and zero-allocation warmed writes. These items are what the code still
needs for the contract to be enforced rather than asserted. They are independent.

## Arrow conversion borrows by default

- Spec: `specs/lmao/01f_arrow_table_structure.md:16-33`: every conversion wraps TypedArray references as bounded
  `subarray()` views; a copy needs a measurement against the borrow; overflow chains are wrapped chunk by chunk, and the
  one bulk concatenation is for a consumer that requires one contiguous region.
- Code: `packages/lmao/src/lib/convertToArrow.ts:596-636` borrows chunks only when a caller passes `borrowChunks`, which
  defaults to `false`; otherwise overflow chains are concatenated (`concatenateFloat64Arrays`, `concatenateNullBitmaps`,
  `concatenateUint8Arrays`, `convertToArrow.ts:57-59`).
- Work: make chunk wrapping the default for JS-heap buffers and the concatenation the opt-in of a consumer that declares
  it needs one contiguous region; charge the copied bytes to the flush. WASM-backed columns stay copied: linear-memory
  growth detaches borrowed views (`specs/lmao/01q_wasm_memory_architecture.md` "Lifecycle and Asynchronous Ownership").
- Acceptance: a Mitata flush benchmark per column type shows borrow at least as fast as copy on the JS-heap lane; the
  allocation census of a JS-heap flush over an overflow chain counts zero payload copies unless the consumer requested
  contiguity.

## Span-writer shape rules and per-lane census

- Spec: `specs/lmao/05_span_writer_lanes.md:203-224`: Tier-1 shape rules (`SHAPE-FMTROW`, `SHAPE-CAP0`/`SHAPE-SEED`,
  `SHAPE-RMWROW`, `SHAPE-SPILL`), borrow/scalar boundary signatures, and a Tier-2 census per lane: `calls ≤ 2` per
  per-element symbol, `grow = 0`, the same symbols and counts on every native-row-store lane, and ABBA floors for
  JS-heap lanes.
- Code: `packages/lmao/crates/lmao-core/tests/thread_buffer_alloc.rs` proves zero allocations after warmup on the native
  row store. No check enforces the four shape rules, the boundary signatures, the per-symbol call/grow counts, or the
  cross-lane equality; `packages/lmao/benchmarks/abba-lanes.ts` measures floors but gates nothing. No `ABI-TWIN` check
  exists.
- Work: a lint or test that rejects each shape on row paths (`lmao-core`, `lmao-wasm`); a census harness that records
  per-symbol call and grow counts for the wasm and `thread_ffi` (`lmao-ffi-dylib`) lanes and fails on a mismatch; the
  ABBA floors as a gate with recorded thresholds; the `ABI-TWIN` entry-set comparison.
- Acceptance: introducing a `format!` on a row path, a per-row shared atomic, or a growable vector in the arena fails
  the gate; the census for wasm and `thread_ffi` reports identical symbols and counts with `calls ≤ 2`, `grow = 0`; a
  member added to one binding only fails `ABI-TWIN`.

## Zero-allocation warmed writes on the JavaScript lanes

- Spec: `specs/lmao/00_package_architecture.md:325`, `01_trace_logging_system.md:17` and `:26-34`: warmed fixed-capacity
  writes allocate nothing, with monomorphic stores and one predictable capacity branch, proved by the Mitata protocol of
  01b1.
- Code: the Rust census (`lmao-core/tests/alloc_gate.rs`, `thread_buffer_alloc.rs`) covers the native row store. No test
  measures heap allocation of a warmed write on the JS-heap or WASM-backed JavaScript lanes.
- Work: a Mitata scenario per JavaScript lane that warms a span, then counts heap growth across N in-capacity writes,
  and fails on any growth.
- Acceptance: the scenario reports zero new heap allocations for warmed in-capacity writes on each lane and fails when a
  write path allocates (for example a template string built per write).

## One measured lease per request flush

- Spec: `specs/lmao/00_package_architecture.md:371-374`: each request's flush produces one measured `ArrowLease`, and
  async Arrow/WASM consumption pins its storage through that lease.
- Code: `packages/lmao/src/lib/flushScheduler.ts:383-415` leases each root buffer of a tick separately, then merges
  every root's table into one `combinedTable` for the flush handler, so the handler receives one table spanning several
  requests and no per-request lease.
- Work: hand the flush handler one leased result per request root, or keep request boundaries and their leases inside
  the coalesced result.
- Acceptance: two concurrent requests flushed in one tick reach the handler as two leased results, and releasing one
  does not release the other's storage.
