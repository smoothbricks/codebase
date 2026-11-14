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
