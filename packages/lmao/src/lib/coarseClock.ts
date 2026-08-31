/**
 * Coarse row-stamp policy — the one process-wide coarsening constant.
 *
 * Every lane that stamps log rows (js-heap `traceRoot.node`/`traceRoot.es`, the
 * WASM thread lane, `containium-trace` on the Rust side) rides the same
 * bounded-staleness cache at the same cadence. Two coarse clocks with different
 * refresh policies in one process would produce an inconsistency nobody can
 * reproduce, so the number lives here and is imported, never redeclared.
 *
 * @module coarseClock
 */

//#region smoo/lmao!n/trace-root-timestamps #coarse-clock

/**
 * Log rows between forced clock reads.
 *
 * WHY coarsen: the frozen-clock decomposition of the js-heap row stamp
 * (`benchmarks/_stampDecompose.ts`) prices today's exact stamp at 31.6 ns/row,
 * of which the `process.hrtime.bigint()` read plus its BigInt cell is 14.8, the
 * `<=` monotonic guard 5.5, and the epoch-offset `+` 5.0 — 25.3 ns of BigInt
 * work around a clock read, against a 7.3 ns `BigInt64Array` store that every
 * variant pays. The same shape holds in Rust, where `lmao-core`'s `CoarseClock`
 * doc puts `Instant::now()` at ~80% of per-event cost.
 *
 * WHY it is sound: rows stamped from the cache share a timestamp, and row order
 * — not stamp distinctness — is authoritative for ordering. Span start and
 * completion always read fresh through the injectable `_timestampNow` seam, so
 * durations, the quantity `axe_execution_duration_seconds` derives from the
 * row-0/row-1 stamps, never coarsen.
 *
 * WHY sixteen: staleness must be bounded by a fraction of buffer capacity, not
 * by wall time — a stalled lane must not accumulate stale stamps in proportion
 * to how long it stalled. Sixteen is a quarter of the 64-row `SPAN_CAPACITY`
 * every lane shares, so a full buffer forces at least four fresh reads and at
 * most sixteen rows ever share one stamp. A lane implementing this must spend
 * one unit of the budget on the row that forced the read, or the real bound is
 * seventeen and this number is a lie.
 */
export const LOG_STAMP_REFRESH = 16;

/**
 * The mutable row-stamp cache.
 *
 * These fields live directly on the TraceRoot (and on the thread lane's span
 * view) rather than inside a clock object: a log row's stamp must be one field
 * read, not a property chain through an indirection, and the declared-field
 * shape keeps every trace of a lane on one hidden class.
 */
export interface StampCache {
  /** Last fresh read, reused by the rows that follow it. */
  _stampCache: bigint;
  /** Rows still allowed to ride {@link StampCache._stampCache} before a forced read. */
  _stampReads: number;
}

/*
 * Lanes on this policy, and the two that are not yet.
 *
 * ON: `traceRoot.node.ts` and `traceRoot.es.ts` import this constant; their
 * `_timestampNow` seam is the fresh/boundary read and also seeds the cache, so
 * a lane layered above them (the WASM thread lane's `boundaryTimestamp`) gets
 * precise boundaries for free. `containium-trace` carries the same number in
 * Rust as `LOG_STAMP_REFRESH` over `lmao_core::CoarseClock`.
 *
 * SEAM 1 — the WASM thread lane (`wasm/threadSpanView.ts`) declares its own
 * `LOG_STAMP_REFRESH = 16` with the same invariant. Same number, so behaviour
 * agrees today, but it is a second declaration rather than an import. It could
 * not be absorbed from here: that file exists only in the unlanded thread-buffer
 * cutover, so there is nothing on this branch to edit. When the cutover lands,
 * the lane must import this constant and implement {@link StampCache}.
 *
 * SEAM 2 — the per-span WASM lane (`wasm/wasmTraceRoot.ts`) stamps log rows
 * inside the WASM module via `allocator.writeLogEntry`, reading the clock
 * through the module's `env.performanceNow` import. It CANNOT adopt this policy
 * from TypeScript: coarsening at that import would coarsen span boundaries too,
 * because the JS side cannot tell a boundary read from a row read, and that
 * would collapse exactly the durations this policy protects. Adopting it needs a
 * change inside the module. Until then the per-span WASM lane stamps every row
 * exactly, which is why `wasm-integration.test.ts` asserts lifecycle-shape
 * parity and per-lane stamp policy rather than timestamp equality.
 */
//#endregion smoo/lmao!n/trace-root-timestamps
