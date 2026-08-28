# Incremental Arrow Trace Files <a id="smoo/lmao!n/incremental-arrow"></a>

## Overview <a id="smoo/lmao!n/incremental-arrow.overview"></a>

Trace output lands in **incremental Arrow IPC files**: batch-appended, always-valid, mmap-readable while being written.
One file per module/time-window; sealed windows become immutable content-addressed data objects. The format serves three
consumers with one artifact:

1. **Live tail readers** (the Console log stream, `session.grep`-class searches) follow committed record batches by
   offset over mmap, with bounded staleness.
2. **Search** runs over the mmap-able columns of live and sealed files alike — no load step, no conversion.
3. **The by-ref signal lane**: a sealed file is absorbed into the CAS and referenced by a single datom in the event log
   (`{:e sig :a :lmao/traceBatch :v #cas "b3:…"}` — the datom-log specification), so monitoring agents reduce trace
   columns without the trace pipeline ever copying its own data.

## Ownership Boundary <a id="smoo/lmao!n/incremental-arrow.ownership"></a>

**LMAO owns**: the file format contract below, the incremental writer, the tail/search reader primitives, batch-size
self-tuning, sealing (finalize + hash). **The consuming system owns**: rotation policy (window boundaries), CAS
absorption wiring, signal emission, retention, and indexing of sealed objects (the window-index plane is a consumer-side
decision).

## File Contract <a id="smoo/lmao!n/incremental-arrow.contract"></a>

The file is Arrow IPC **file** format — magic, schema message, record batches, footer with the block index, trailing
magic — with one incremental rule:

**Append = truncate footer → append record batch(es) → rewrite footer + trailing magic.** After every flush the file is
a complete, valid Arrow IPC file that any off-the-shelf reader opens. The footer is the _metadata write_ of the flush;
batch payload bytes are written exactly once and never move.

- **Crash window**: between footer truncation and rewrite, the file momentarily lacks a valid footer. Recovery is
  deterministic without a sidecar: IPC record batches are self-delimiting encapsulated messages (continuation marker
  - metadata length), so a scan from the schema message rebuilds the block index and rewrites the footer, discarding a
    torn trailing batch. A trace file loses at most its last unflushed batch, never earlier data.
- **Tail readers never need the footer.** They read stream-style over the mmap: follow the self-delimiting batch frames
  from the last known offset, stopping at the first incomplete frame. The committed high-water offset is the watermark;
  readers poll file growth (or receive it via the platform's change notification) and resume by offset — the same cursor
  discipline as every other stream in the system.
- **Columns are mmap-borrowable**: batch buffers keep IPC 8/64-byte alignment, so a reader hands out bounded
  `subarray()` views over the mapped region under the [01f](./01f_arrow_table_structure.md) ownership contract — the
  file plays the "contiguous primitive region" row of that table, with the map as the lease.
- **No in-place resize, no compression in the live file.** Growth is append-only; Zstd belongs to sealed archive
  projections, not the live file (compressed batches would break mmap column borrowing).

## Batch Size: Self-Tuned Per Time Window <a id="smoo/lmao!n/incremental-arrow.tuning"></a>

The starting point is ~1000 rows per record batch; the operative contract is **bounded staleness and bounded overhead**,
tuned per module and time window with the same measured-feedback approach as
[buffer self-tuning](./01b2_buffer_self_tuning.md), which this composes with (SpanBuffer flush feeds the writer; the
writer decides batch boundaries):

- A batch flushes when it reaches the current row target **or** its age exceeds the staleness bound — quiet windows
  still flush, so the tail never lags a live system by more than the bound.
- The row target adapts per window from observed fill rate: busy windows grow batches toward fewer, denser flushes
  (footer rewrites are per-flush overhead); quiet windows shrink them. The tuner's inputs are the flush cadence and
  batch fill ratio already tracked by the buffer layer; no configuration is exposed.
- Degenerate protection both ways: a floor stops per-row batches under bursty-tiny load; a ceiling stops multi-second
  invisibility under floods.

## Sealing and Rotation <a id="smoo/lmao!n/incremental-arrow.sealing"></a>

At a window boundary (the consumer's rotation policy — size, time, or module lifecycle):

1. Final flush; write the definitive footer. The file is now immutable.
2. **Absorb into CAS**: BLAKE3 the file (incremental hashing may run alongside appends so sealing is metadata-only),
   link into the content-addressed store.
3. Hand the consumer the ref. The sealed object is adopted in place by archive window indexes (RowRef terminals point
   directly into it — zero rewrite), and GC roots it through the referencing log.
4. Open the next window's file.

A sealed file and a live file answer the same reads through the same reader — sealing changes lifecycle, never format.

## Related

- [01b2 Buffer Self-Tuning](./01b2_buffer_self_tuning.md) — the tuning feedback this composes with
- [01f Arrow Table Structure](./01f_arrow_table_structure.md) — column schema and the ownership/copy contract
- [01t Trace Archive Primitives](./01t_trace_archive_pipeline.md) — chunk identity and archive-side compaction over
  sealed objects
- The consuming system's datom-log and console specifications define the by-ref signal lane and the log-stream screen
  consuming live tails.
