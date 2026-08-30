# lmao-query

Scope: `packages/lmao-rs/crates/lmao-query/Cargo.toml` (25), `src/lib.rs` (94), `src/arrow_backend.rs` (145),
`src/sqlite_backend.rs` (177), `src/datafusion_backend.rs` (93). Additionally read for TESTS / SSOT greps:
`tests/parity.rs` (248); `packages/lmao/src/lib/sqlite/sqlite-common.ts`;
`packages/lmao/src/lib/testing/trace-query.ts`; `packages/lmao-rs/crates/lmao-arrow/src/convert.rs` (schema names only);
`packages/lmao-rs/Cargo.lock`; `packages/lmao-rs/justfile`.

## Summary

- Three query backends in 509 source lines; only the Arrow scan is load-bearing. Drop `datafusion` and `sqlite`.
- `datafusion` 47 (27 `datafusion*` lock packages + `arrow` umbrella, `object_store`, `sqlparser`, `uuid`) is optional,
  unused by any other crate, and never compiled by `cargo test --workspace`.
- `rusqlite` `features=["bundled"]` compiles SQLite from C for a parity shim. Test-suite traces go through TS
  `bun:sqlite` / `DEFAULT_TRACE_DB_PATH`, not this crate. `rusqlite`/`libsqlite3-sys` appear only in
  `packages/lmao-rs/Cargo.lock` (not cowshed/columine) — bundled SQLite is compiled at most once, and only if
  `--features sqlite`.
- Live schema split: Arrow column `timestamp` vs SQLite `timestamp_ns`. Parity tests translate the name rather than
  sharing one schema.
- `count`/`never`/`all_children_of` on both SQL backends map query failure to `0` / `true`.
- Keep the hand-rolled Arrow scan (`arrow-array` / `arrow-schema` only; workspace already has those).

## Findings

### F1 — HIGH — DEP-BLOAT — Drop the DataFusion backend

Evidence: `packages/lmao-rs/crates/lmao-query/Cargo.toml:12-21`

```toml
rusqlite = { version = "0.37", features = ["bundled"], optional = true }
datafusion = { version = "47", default-features = false, features = ["nested_expressions"], optional = true }
tokio = { version = "1", features = ["rt"], optional = true }
[features]
default = []
sqlite = ["dep:rusqlite"]
datafusion = ["dep:datafusion", "dep:tokio"]
```

`packages/lmao-rs/Cargo.lock:583-630` (`datafusion` 47.0.0 pulls `arrow`, `datafusion-datasource-csv`,
`datafusion-datasource-json`, `object_store`, `sqlparser`, `uuid`, `tempfile`, `regex`, `chrono`, plus 26 sibling
`datafusion-*` crates at 47.0.0). Workspace policy at `packages/lmao-rs/Cargo.toml:23-26` is Arrow subcrates only, not
the `arrow` umbrella; DataFusion is what puts `arrow` 55.2.0 + `arrow-csv`/`arrow-json` in the lock.

`packages/lmao-rs/crates/lmao-query/src/datafusion_backend.rs:22-32,74-77` — the whole backend is `SessionContext` +
string SQL for the same `count` the Arrow scan already does, plus a current-thread tokio runtime:

```rust
let table = MemTable::try_new(first.schema(), vec![batches.clone()])?;
...
self.sql_count(&format!("SELECT * FROM spans WHERE {where_sql}"))
    .unwrap_or(0)
```

Repo usage: no crate depends on `lmao-query` except itself (`Cargo.lock:1750-1761`). Grep of `packages/` for
`lmao_query` / feature activation finds only this crate, docs, and specs. `specs/lmao/02_query_engine.md` names DuckDB
(unbuilt) as the SQL engine, not DataFusion. `specs/lmao/04_inspect_cli.md` is unbuilt and says Arrow scan is the v1
path.

Problem: ~90 lines of SQL-string `TraceQuery` to justify one of the largest crates in the ecosystem.
`default-features = false` + `nested_expressions` still resolves CSV/JSON datasources and `object_store`. The feature is
off by default, but the lock still vendors the tree. Duplicate versions in the same lock that DataFusion participates
in: `itertools` 0.10.5 and 0.14.0; `hashbrown` 0.14.5 / 0.15.5 / 0.17.1 (`datafusion-common` pins `hashbrown 0.14.5`).

Fix: delete `src/datafusion_backend.rs`, the `datafusion` feature, `datafusion`/`tokio` deps, and `tests/parity.rs`
`datafusion_backend_parity`. Exploratory SQL stays with the spec (DuckDB) or with `ArrowTraceQuery`. Do not replace
DataFusion with another SQL engine in this crate.

Cost/Risk: none in-tree — zero callers. Lockfile shrinks on the next `cargo generate-lockfile`. `nested_expressions` is
unused by the Arrow schema (no List/Struct columns); [INFERENCE] it is not required for `NOT EXISTS` SQL.

Verdict: **DROP**.

### F2 — HIGH — DEP-BLOAT — Drop bundled rusqlite; the sink is TS bun:sqlite

Evidence: `packages/lmao-rs/crates/lmao-query/Cargo.toml:12,20` (`rusqlite` 0.37, `features = ["bundled"]`).
`libsqlite3-sys` 0.35.0 in `Cargo.lock:1684-1693` depends on `cc` / `pkg-config` / `vcpkg` (the compile-SQLite-from-C
path).

`packages/lmao-rs/crates/lmao-query/src/lib.rs:7-8` and `src/sqlite_backend.rs:1-2` claim `.cache/trace-results.db` /
`SQLiteTracer` parity. Production path is TS:

- `packages/lmao/src/lib/sqlite/trace-db-path.ts` owns `DEFAULT_TRACE_DB_PATH`
- `packages/lmao/src/lib/testing/bun-harness.ts` opens `bun:sqlite` `Database` and `SQLiteTracer`
- `packages/lmao/src/lib/testing/trace-query.ts:143-329` is the post-run query API (`failures` / `slowest` / `findSpans`
  / `testTree`)

No Rust caller opens that path through `SqliteTraceQuery::open`. `sqlite_backend` is only constructed in
`tests/parity.rs:186-218` under `cfg(feature = "sqlite")`. `packages/lmao-rs/justfile:5-6` is `cargo test --workspace`
with no `--features sqlite`.

Problem: compiling C SQLite into a 177-line optional module that default CI never builds, to re-query a table the
TypeScript writer already owns. Precedent in this repo: `git2` was removed because PATH `git` was enough. Here the
analogous move is not “shell out to `sqlite3` from a Rust library” — that would be worse (no typed errors, not a
`TraceQuery` impl). The TS `TraceQuery` + `bun:sqlite` already is the SQLite assertion surface. `sqlite3` CLI is what
humans already use on the sink (docs); it is not a replacement for an in-process Rust backend.

`rusqlite` / `libsqlite3-sys` do not appear in cowshed or columine locks. Bundled SQLite is **not** compiled twice
today. It would be if another crate later adds `rusqlite` with `bundled`.

Fix: delete `src/sqlite_backend.rs`, the `sqlite` feature, and the rusqlite dependency. Keep SQLite I/O in
`packages/lmao`. If a future Rust reader of the TS-written DB is required, add `rusqlite` **without** `bundled`
(macOS/Linux libsqlite3 is present) at the crate that actually opens the file — not here, and not to compile SQLite from
C for tests.

Cost/Risk: loses a Rust↔SQLite table-shape oracle that is not run anyway. `lmao-inspect` spec (`04_inspect_cli.md:32`)
explicitly excludes SQLite in v1.

Verdict: **DROP**. Do not replace with `sqlite3` on PATH inside this crate.

### F3 — HIGH — SSOT — Arrow `timestamp` vs SQLite `timestamp_ns` (live divergence)

Evidence: Arrow schema `packages/lmao-rs/crates/lmao-arrow/src/convert.rs:52-63`

```rust
Field::new("timestamp", DataType::Int64, false),
Field::new("trace_id", dict_type(DataType::UInt32), false),
...
Field::new("parent_span_id", DataType::UInt32, true),
```

SQLite DDL `packages/lmao-rs/crates/lmao-query/src/sqlite_backend.rs:28-36` and TS
`packages/lmao/src/lib/sqlite/sqlite-common.ts:10-16` use `timestamp_ns`. Parity tests paper over it:

`packages/lmao-rs/crates/lmao-query/tests/parity.rs:86-97`

```rust
/// The Arrow backend uses lmao-arrow column names; SQLite/DataFusion-over-sqlite-shape
/// use the SQLiteTracer names. Translate the two columns that differ.
fn arrow_flavored(s: &Selector) -> Selector {
    let mut out = s.clone();
    for (name, _) in out.constraints.iter_mut() {
        if name == "timestamp_ns" {
            *name = "timestamp".to_string();
        }
    }
    out
}
```

A selector `.with("timestamp_ns", 0i64)` against `ArrowTraceQuery` is a miss; `.with("timestamp", …)` against
`SqliteTraceQuery` is a miss. `thread_id` / `parent_thread_id` / `line_number` exist only on the Arrow schema. The crate
docs (`lib.rs:5-8`) say the same `Selector` runs against both.

Problem: two names for one column. The test translator is the bug made visible. Comment on `arrow_flavored` is also
wrong for DataFusion: `datafusion_backend_parity` uses Arrow names because DataFusion registers the RecordBatch as-is
(`parity.rs:229-231`).

Fix: one column name. Decision: Arrow `timestamp` is the in-process schema (`trace_schema` SSOT in lmao-arrow). SQLite
sink keeps `timestamp_ns` as the TS writer’s column — map at the SQLite adapter only, in one function owned by that
adapter, and make `Selector` take an interned column id / enum so a string rename cannot silently no-op. Delete
`arrow_flavored` once selectors are schema-typed.

Cost/Risk: TS writer + every SQL snippet using `timestamp_ns` if you rename the sink; smaller blast if the map stays in
the (to-be-deleted) SQLite adapter. Cross-slice: lmao-arrow `trace_schema`, TS `sqlite-common.ts`.

### F4 — HIGH — STRUCTURE — SQL backends turn operational failure into “never happened”

Evidence: `packages/lmao-rs/crates/lmao-query/src/sqlite_backend.rs:144-175`

```rust
fn query_count(&self, sql: &str, params: &[rusqlite::types::Value]) -> usize {
    self.conn
        .query_row(sql, rusqlite::params_from_iter(params.iter()), |r| {
            r.get::<_, i64>(0)
        })
        .unwrap_or(0) as usize
}
...
self.query_count(&sql, &params) == 0  // all_children_of
```

`packages/lmao-rs/crates/lmao-query/src/datafusion_backend.rs:31,74-91`

```rust
.build()
.expect("tokio current-thread runtime");
...
self.sql_count(&format!("SELECT * FROM spans WHERE {where_sql}"))
    .unwrap_or(0)
...
self.sql_count(&sql).map(|n| n == 0).unwrap_or(false)
```

`lib.rs:86-87`: `never` is `count == 0`. A prepare/execute/SQL error therefore makes `never` return true and
`all_children_of` return true.

Problem: operational failure (bad column, type mismatch, DF plan error) is indistinguishable from a passing negative
assertion. Doctrine: Result for operational failure; do not `/dev/null` it. DataFusion also panics on runtime build
(`expect`).

Fix: if the backends survive, `count` / `all_children_of` return `Result`. If they are deleted (F1/F2), this dies with
them. Do not keep `unwrap_or(0)`.

Cost/Risk: trait change on `TraceQuery` — only this crate implements it.

### F5 — MEDIUM — SSOT — `SPANS_DDL` is a hand restatement of TS `SPANS_TABLE_INIT_SQL`

Evidence: `packages/lmao-rs/crates/lmao-query/src/sqlite_backend.rs:28-41`

```rust
pub const SPANS_DDL: &str = "
  CREATE TABLE IF NOT EXISTS spans (
    trace_id TEXT NOT NULL,
    span_id INTEGER NOT NULL,
    parent_span_id INTEGER NOT NULL,
    row_index INTEGER NOT NULL,
    entry_type INTEGER NOT NULL,
    timestamp_ns INTEGER NOT NULL,
    message TEXT,
    PRIMARY KEY (trace_id, span_id, row_index)
  );
  CREATE INDEX IF NOT EXISTS idx_spans_trace ON spans(trace_id);
  CREATE INDEX IF NOT EXISTS idx_spans_parent ON spans(trace_id, parent_span_id);
";
```

`packages/lmao/src/lib/sqlite/sqlite-common.ts:9-23` is the same SQL (currently agrees). TS `quoteSqlIdentifier`
(`sqlite-common.ts:186-190`) rejects non-`[A-Za-z_][A-Za-z0-9_]*` names; Rust `sqlite_backend.rs:126` /
`datafusion_backend.rs:51` quote with `"` + `""` escape and accept anything.

Problem: writer schema lives in TS. Rust restates it. They agree today; F3 shows the rest of the schema already does
not. Identifier policy already diverged.

Fix: TS `SPANS_TABLE_INIT_SQL` stays SSOT. Delete `SPANS_DDL` with the sqlite backend (F2). Do not generate Rust SQL
from TS in this crate.

Cost/Risk: none if F2 lands.

### F6 — MEDIUM — TESTS — sqlite/datafusion parity never runs on the workspace test command

Evidence: `packages/lmao-rs/justfile:5-6` `cargo test --workspace`.
`packages/lmao-rs/crates/lmao-query/tests/parity.rs:186-222`

```rust
#[cfg(feature = "sqlite")]
#[test]
fn sqlite_backend_parity() { ... }

#[cfg(feature = "datafusion")]
#[test]
fn datafusion_backend_parity() { ... }
```

`Cargo.toml` `default = []`. No nx/CI invocation enables these features (grep of `.github` and `packages/lmao-rs` for
`--features sqlite` / `datafusion` is empty). `datafusion_backend_parity` does not call `never` (sqlite parity does).
`selectors()` timestamp case is the only typed constraint besides `span_id`/`trace_id`, and it depends on F3’s
translator.

Problem: the backends that pull the heavy deps have no default-suite evidence. PH §7.10bb: a guard that cannot go red is
not a guard. These tests cannot go red because they are not compiled.

Fix: delete the backends (F1/F2) rather than adding `--all-features` to CI (that would compile SQLite C + DataFusion on
every test). Keep `arrow_scan_answers_the_fixture` and the vocabulary parity test — those assert typed counts, not
rendered strings.

Cost/Risk: none.

### F7 — MEDIUM — DUPLICATION — two SQL `where_clause` builders for one selector

Evidence: `packages/lmao-rs/crates/lmao-query/src/sqlite_backend.rs:116-141` (parameterized `message = ?` + quoted
ident) vs `src/datafusion_backend.rs:45-70` (interpolated `message = '...'` + `escape`).

`all_children_of` SQL is copied (`sqlite_backend.rs:168-173`, `datafusion_backend.rs:83-87`) with the same `NOT EXISTS`
/ `p.span_id = c.parent_span_id` shape.

Problem: one predicate, two string assemblers, already different (params vs interpolation). DataFusion `escape` only
doubles single quotes.

Fix: delete both with F1/F2. The Arrow scan is the single predicate implementation (`row_matches`).

Cost/Risk: none if those backends go.

### F8 — MEDIUM — STRUCTURE — `load_batches` addresses Arrow columns by ordinal

Evidence: `packages/lmao-rs/crates/lmao-query/src/sqlite_backend.rs:76-83`

```rust
let ts = batch.column(0).as_primitive::<Int64Type>();
let trace = batch.column(1).as_dictionary::<UInt32Type>();
...
let span_ids = batch.column(3).as_primitive::<UInt32Type>();
let parent_span = batch.column(5).as_primitive::<UInt32Type>();
let entry = batch.column(6).as_dictionary::<UInt8Type>();
let message = batch.column(7).as_dictionary::<UInt32Type>();
```

Coupled to `lmao-arrow` `trace_schema` field order (`convert.rs:54-61`: 0 timestamp, 1 trace_id, 2 thread_id skipped, 3
span_id, 4 parent_thread_id skipped, 5 parent_span_id, 6 entry_type, 7 message). Insert ignores `thread_id` /
`line_number`. `entry_type` is recovered as `keys().value(row) as i64 + 1` (`sqlite_backend.rs:97-98`), which matches
`convert.rs:253` (`entry_keys.push(entry_type - 1)`) — that part is consistent, not a bug.

Problem: a column insert in `trace_schema` silently shifts every ordinal. Named lookup (as `arrow_backend` does via
`schema().index_of`) is the adapter’s job if this function stays.

Fix: delete with F2, or look up by name.

Cost/Risk: lmao-arrow schema changes.

### F9 — LOW — SSOT — crate comments restate `.cache/trace-results.db`

Evidence: `packages/lmao-rs/crates/lmao-query/src/lib.rs:8`, `src/sqlite_backend.rs:2`. AGENTS.md / CLAUDE.md:
`DEFAULT_TRACE_DB_PATH` owns the path; never restate the literal except the CI glob.

Problem: the forbidden literal is in this crate, and the crate never opens the file.

Fix: delete the comments with F2, or point at `trace-db-path.ts` by module path with no filename.

Cost/Risk: none.

### F10 — LOW — COPIES — per-row `to_string` + repeated `schema().index_of` on the Arrow scan

Evidence: `packages/lmao-rs/crates/lmao-query/src/arrow_backend.rs:34-54,68-77,103-142`.

`dict_str_value` returns `Option<String>` (`value(key).to_string()`). `row_matches` allocates that String to compare to
`selector.template`. `column_equals` for `Str` calls `dict_str_value` again (second `index_of` + second copy).
`all_children_of` builds `HashSet<(String, u64)>` of parent identities, copying `trace_id` per parent row, then again
per child. `schema().index_of` runs per row per constraint (L7 re-validation of immutable schema).

`sqlite_backend.rs:73-90`: `HashMap<(String, u32), i64>` keys `trace_id.to_string()` per row while loading.

Regime: assertion / test scan, currently small fixtures. Not a hot production loop. PH §4.1: do not treat this as a perf
incident. It becomes a scan-loop cost if `lmao-inspect` tails real segments through this code.

Fix: `dict_str_value` → `Option<&str>`; resolve column indices once per batch; intern `trace_id` as the dict key
(`u32`), not `String`. Only worth it if the Arrow backend remains the inspect path.

Cost/Risk: local to `arrow_backend.rs`.

## Cross-slice questions

- **lmao-arrow** (`crates/lmao-arrow/src/convert.rs` `trace_schema`): owns `timestamp` vs the SQLite `timestamp_ns`
  name. F3 depends on that field list; this slice should not rename Arrow columns.
- **TS lmao sqlite** (`packages/lmao/src/lib/sqlite/sqlite-common.ts`, `testing/trace-query.ts`): production sink + a
  different `TraceQuery` class (`failures`/`slowest`/…). Not the Rust `TraceQuery` trait. Confirm no one planned to
  implement the Rust trait against bun:sqlite from TS/napi.
- **XcutDeps / XcutRustTs**: DataFusion lock gravity and TS/Rust schema copies overlap those slices; F1/F3/F5 are the
  lmao-query-side half.
- **lmao-inspect** (unbuilt, `specs/lmao/04_inspect_cli.md`): only planned Rust consumer. Spec says no SQLite in v1 —
  agrees with F2.

## Non-findings (checked, clean)

- **Arrow backend deps:** `arrow-array` + `arrow-schema` only; workspace already pins those. **KEEP.** No extra crate
  for the default feature set.
- **Feature gating of the heavy deps:** `default = []`, both backends `optional = true`. Default
  `cargo test --workspace` does not compile rusqlite or DataFusion. The bloat is lockfile + anyone who flips the
  feature, not the default artifact.
- **Bundled SQLite twice:** not in this monorepo today. Only `packages/lmao-rs/Cargo.lock` has
  `rusqlite`/`libsqlite3-sys`.
- **`unsafe`:** none. No `cfg(target_os)` arms.
- **God files / 100-line functions:** largest impl body is `load_batches` (~67 lines). 94/145/177/93-line modules.
- **Arrow tests that do run:** `arrow_scan_answers_the_fixture` and
  `stable_and_dynamic_vocabulary_have_query_and_archive_parity` assert typed `count`/`never`/`all_children_of`, not
  rendered log text. Vocabulary test also checks archive envelope identity — real oracle.
- **SQLite parameterized SQL** (if kept): `where_clause` uses `?` binds; not the DataFusion interpolation path.
- **`entry_type` + 1 in `load_batches`:** inverts Arrow dict keys (`entry_type - 1` at convert). Not a mismatch with TS
  `resolveEntryType`.
- **No `unwrap` on the Arrow scan path.** Infallible given in-memory batches.
- **Selector vs TS types:** Rust `Selector`/`ColumnValue` is not a field-for-field restatement of a TS DTO; the
  duplication is the _name_ `TraceQuery` and the docs claim, not a JSON schema clone (covered under F3/cross-slice, not
  a third type table).
