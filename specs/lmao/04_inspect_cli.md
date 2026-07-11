# Inspection CLI (`lmao-inspect`) <a id="smoo/lmao!n/inspect-cli"></a>

> **Implementation status (unbuilt).** This spec defines a small, generic, local-first inspection binary over
> `lmao-query` (spec 02's tracer-agnostic selector surface; the Rust port lives in
> `packages/lmao-rs/crates/lmao-query`). It exists because every lmao consumer (cowshed, AxE sim runs, jcode) otherwise
> grows its own ad-hoc log reader. It is **not** the Inspector (spec 03) — no UI, no server; a terminal tool for Arrow
> segment files on local disk.

## Scope

One binary, `lmao-inspect`, domain-agnostic: it knows lmao's Arrow schema family (spec 01f/01k), not any consumer's
vocabulary. Consumers wrap it with domain verbs (e.g. cowshed's `cowshed logs` / `cowshed audit` / `cowshed trace` —
`specs/cowshed/13_telemetry.md`) rather than re-implementing scan/filter/render.

## Commands

| Command                              | Behavior                                                                                                                                                                         |
| ------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `lmao-inspect scan <dir\|file>…`     | Read Arrow IPC segments (day-partitioned dirs or single files), print a human table. Column projection with `--cols`, row filters with `--where <col>=<val>` (dictionary-aware). |
| `lmao-inspect tail -f <dir>`         | Follow a segment directory: render batches as they are sealed; picks up rotation to new segment files.                                                                           |
| `lmao-inspect query <selector\|SQL>` | Run an `lmao-query` selector (`count`/`never`/`every`) or SQL over the given segments; selectors return exit 0/1 for assertion use in scripts and CI.                            |
| `lmao-inspect trace <trace-id>`      | Reassemble one trace across segments and render a terminal waterfall (span tree, durations, links).                                                                              |

Output contract: human tables on a TTY; `--json` (one envelope) and `--ndjson` (one event per line) for pipes. NDJSON is
an **export encoding only** — lmao storage is Arrow; this tool is the sanctioned way to get line-oriented views.

## Non-goals

- No storage, no daemon, no server mode (the Inspector's stream/archive sources cover live UI needs — spec 03).
- No consumer-specific schema knowledge: kind-specific `attrs` render as-is; domain filtering above raw columns belongs
  to wrappers.
- No SQLite backend in v1 (lmao-query's parity backend can join later; Arrow scan is the primary path).

## Determinism

`lmao-inspect query` over a golden trace fixture must be byte-stable in `--ndjson` mode (sorted dictionaries, fixed
float formatting — the same rules as spec 01k's encoder), so CI can diff exported views, not just assert selectors.
