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
