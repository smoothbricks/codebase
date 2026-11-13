# lmao: sharing a column between two libraries is refused, against its own spec

`specs/lmao/01e_library_integration_pattern.md` ("Column Mapping API", "Mapping Capabilities") and
`01l_op_context_pattern.md` ("Dependencies with Column Mapping") state that two libraries may write one column:
`pg: postgresOps.mapColumns({ query: 'query' })` beside `mysql: mysqlOps.mapColumns({ query: 'query' })`, "Both pg and
mysql write to 'query'".

Measured on 2026-11-13 through the public API (a Bun probe):

| wiring                                                              | result                              |
| ------------------------------------------------------------------- | ----------------------------------- |
| two libraries each map a `S.text()` field to `query`                | throws                              |
| the application declares `query` and a library maps a field onto it | throws                              |
| two libraries with different types mapped to one name               | throws (for the name, not the type) |
| two libraries each given a prefix                                   | works                               |

Cause: `packages/lmao/src/lib/defineOpContext.ts:176-183` throws
`Schema conflict: field '…' contributed by dep '…' conflicts with field from '…'` for any output name contributed twice.
Its message recommends `.mapColumns()`, the verb that was used.

Two more facts found beside it:

- Nothing checks that two fields mapped to one column have the same type: the mapping's type is `string | null`
  (`packages/lmao/src/lib/opContext/opGroupTypes.ts:40-42`).
- `S.optional` has no physical effect in the Arrow output (`packages/lmao/src/lib/convertToArrow.ts:922-1173`; read, not
  run).

## Fix shape

- Red first, through `defineOpContext`: the spec's own `query: 'query'` example composes, both libraries' rows appear
  under the one output column, and the remap descriptor of each names its own source column.
- A shared output column requires equal schemas on both sides; unequal ones are refused naming both libraries, both
  fields and both types. That is a law the type level can hold as well: the mapped name's schema type must match.
- The spec keeps its sentence; the code follows it.
