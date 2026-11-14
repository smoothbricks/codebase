# lmao: two defects in the Arrow output, observed 2026-11-13

A probe declared
`{ status: S.number(), method: S.enum([...]), url: S.category(), note: S.text(), retries: S.optional(S.number()), succeeded: S.boolean() }`
once and flushed it twice through the public API: 5 rows, then 302 rows with more distinct `url` and `note` values.

## A field declared `S.optional` is missing from the Arrow output

`retries` was written on rows of both flushes. Neither batch has a `retries` column: the written values are lost without
an error. `S.optional` has no physical lowering (`packages/lmao/src/lib/convertToArrow.ts:922-1173` has no case for it;
storage falls back to a `Uint32Array`).

- Red first: a flush of a schema with an optional number, boolean, enum and category field, written on some rows and not
  on others, has each column with its type, the written values, and null where nothing was written.
- An optional field is its inner type with the validity bitmap as the empty state. There is one empty state.

## One declaration yields two different Arrow schemas

The two flushes produced different Schema messages for the same declaration (sha256 `47f4ea87…` against `6ffb5526…`):
`url` and `note` are `Dictionary<UInt8, Utf8>` in the small flush and `Dictionary<UInt16, Utf8>` in the wide one,
because the index type follows the number of distinct values in the batch. `note` is `S.text()`, which
`specs/lmao/01a_trace_schema_system.md` describes as not dictionary-encoded by identity.

A reader that keys on the schema (a log that stores one schema per shape, a table a second batch is appended to) sees
two shapes for one declaration; three Arrow readers in use reject or mishandle the varying index.

- Design to settle, not a patch: a column's Arrow type follows from its declaration alone. That means a fixed index type
  per dictionary column (or per schema), and a stated rule for which string kinds are dictionaries at all.
- Red first: two flushes of one declaration with 5 and with 70,000 distinct category values have byte-identical Schema
  messages.
- The rule for text is stated three ways, and the code follows none of them: `specs/lmao/01a_trace_schema_system.md:161`
  says `S.text()` is Utf8, `specs/lmao/01f_arrow_table_structure.md` §3 "String Types (text)" makes it a dictionary when
  that saves more than 128 bytes, and arrow-builder's `text` kind declares `physical: utf8`
  (`packages/arrow-builder/src/lib/schema/field.ts:361`), while `convertToArrow.ts` always builds a dictionary
  (`buildTextDictionary`, `:1034`) and its header (`:7`) says text columns are plain strings. Settle one rule in the
  specs, then make the writer follow it.

## The JSON trace rows and the SQLite table leave `thread_id` out of a row's key

A span's identity is `(trace_id, thread_id, span_id)` (`specs/lmao/01b4_span_identity.md`: "Globally:
`(trace_id, thread_id, span_id)` is globally unique"; `span_id` is a counter per thread). Two outputs drop the thread:

- `packages/lmao/src/lib/cloudflare/traceRows.ts:42-53`: a `TraceRow` carries `trace_id`, `span_id`, `parent_span_id`
  and `row_index`, and no `thread_id` or `parent_thread_id`.
- `packages/lmao/src/lib/sqlite/sqlite-common.ts:13-19`: the `spans` table has no `thread_id` column and its key is
  `PRIMARY KEY (trace_id, span_id, row_index)`.

Two workers of one trace both number their spans 1, 2, 3, so their rows collide in either output, and a parent cannot be
found by `(parent_thread_id, parent_span_id)`.

- Red first: one trace with spans from two threads that share span numbers, written through each output, keeps every row
  and resolves each child's parent.
- Both outputs carry `thread_id` and `parent_thread_id`; the SQLite key is `(trace_id, thread_id, span_id, row_index)`.

## A span's row number is not readable from a thread-lane batch

`row_index` in the JSON rows is the row's number within its span, counted across the span's buffers
(`sqlite-common.ts:248-263`). In the TypeScript Arrow conversion a span is emitted whole and contiguous, so the number
is the row's position within its span. In the Rust thread lanes it is not recoverable: rows of all spans on a thread are
emitted in write order (`crates/lmao-core/src/thread_buffer.rs:1194-1228`), an open span's start row is sent again at
every flush, earlier log rows are not, and the slot index stays inside the store
(`crates/lmao-arrow/src/thread_convert.rs:331-344` writes no such column).

- Design to settle with the owner before a patch: whether the thread lanes write the row number as a column or order
  their output so that position gives it. A reader must get the same number for a row from both tracers.

## A start row is sent once; a tag after a flush is a row of its own

Decided by the owner for the two preceding sections' open design. A span's rows are facts that are never replaced, so a
reader that identifies a row by `(trace_id, thread_id, span_id, row)` can keep one bit per row.

- The thread lanes write each row's number within its span as a `row` column (0 the start row, 1 the completion row, 2
  and up the log rows, as `specs/lmao/01c_context_flow_and_op_wrappers.md` numbers them).
- Once a span's start row has left in a flush while the span is still open, `tag()` no longer overwrites row 0
  (`specs/lmao/01h_entry_types_and_logging_primitives.md`: "Row 0: span-start | tag writes HERE (overwrites)"); it
  appends a tag row, a new entry type, in the current buffers. The start row is sent once and never again; lmao 05's "A
  reader takes the latest start row of a span" becomes "a reader folds the span's tag rows over its start row". A span
  that ends before any flush behaves as today.
- The same holds for a host that hands spans over as they happen (`convertSpanStartToArrowTable`,
  `specs/lmao/01k_tree_walker_and_arrow_conversion.md`): after the start row was handed over, a tag is a row.
- Red first: an open span flushed twice with a `tag()` between the flushes emits its start row once, the tag as its own
  row with the next row number, and no row whose `(trace_id, thread_id, span_id, row)` repeats.

## An open span is announced at the second flush it is open across

Decided by the owner with the tag row above ("good solution, does need reasonable flush intervals"). A flush fires on a
timer, so a short span that happens to be open at that instant is announced by its start row and completed in the next
flush: torn for no reason.

- A flush announces an open span only if the previous flush already found it open. A span shorter than one interval is
  never torn: it leaves whole in the flush after it ends. A tag set right after a span opens is in its start row before
  the row is ever sent.
- A span that hangs is visible after at most two intervals instead of one. The flush interval is therefore part of the
  contract: state its value and its bound in `specs/lmao/05_span_writer_lanes.md`; no spec states it today.
- Red first: a span opened just before a flush and ended just after it is emitted once, whole, with no row announced
  early; a span open across two flushes is announced at the second.

## A user timestamp field is declared and never written

Need: a span attribute that holds an instant (the instant a sleep's timer fires) is exact only as an integer of
nanoseconds; a double holds a multiple of 256 ns at current instants. On main `@smoothbricks/arrow-builder`'s `S` has
`S.timestamp(unit)` and `S.duration(unit)` (`packages/arrow-builder/src/lib/schema/builder.ts`), and lmao's `S` spreads
it (`packages/lmao/src/lib/schema/builder.ts:67-68`), but nothing writes them: `storageLaneOf` answers `undefined` for
both kinds (`packages/arrow-builder/src/lib/schema/typeGuards.ts:14-29`), `convertToArrow.ts` has no branch for them,
and `lmao-core` has no user timestamp field kind and no tagging call for one.

- Red first: a span attribute declared `S.timestamp('ns')` and tagged `1_763_000_000_123_456_789` reads back exactly as
  an Arrow `Timestamp(Nanosecond, "UTC")` cell, from the TypeScript and the Rust tracer.
- The field is an i64 in the span buffer; the Rust side gains the field kind and `tag_timestamp(field, nanos: i64)`;
  `S.duration(unit)` follows the same lowering as an Arrow `Duration`.
