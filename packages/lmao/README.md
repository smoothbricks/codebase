# @smoothbricks/lmao

A high-performance, type-safe structured tracing and observability library for TypeScript.

Instrumented code writes spans directly into columnar [Apache Arrow](https://arrow.apache.org/) buffers — near-zero
hot-path overhead — and emits queryable Arrow tables that persist to SQLite (a local file, Node, or Cloudflare D1). A
headline feature is **trace-testing**: assert on the span tree your code emits instead of on return values.

- **Type-safe by construction.** Your log schema drives the types of `ctx.tag`, `ctx.log`, feature flags, and results —
  autocomplete everywhere, no casts.
- **Columnar & cheap.** Attributes are written to fixed buffer positions; string columns are dictionary-encoded. No
  per-event object allocation on the hot path.
- **Arrow-native.** Convert a trace to an Arrow table and persist it to SQLite/D1, or analyze it with the companion
  inspector.

> Part of the `smoothbricks` monorepo. Full documentation lives in [`targets/lmao-docs/`](../../targets/lmao-docs) (an
> Astro Starlight site). Runnable examples are in [`examples/`](./examples).

## Install

```bash
bun add @smoothbricks/lmao
```

## Quick start

```ts
import { defineLogSchema, defineOpContext, JsBufferStrategy, S, StdioTracer } from '@smoothbricks/lmao';
import { createTraceRoot } from '@smoothbricks/lmao/node'; // or '/es' for browsers/Workers

// 1. Describe your columns. Pick a string strategy per field:
//    S.enum (known set, 1 byte) · S.category (repeating, dictionary) · S.text (mostly unique)
const schema = defineLogSchema({
  userId: S.category(),
  operation: S.enum(['SELECT', 'INSERT', 'UPDATE', 'DELETE']),
  duration: S.number(),
});

// 2. Bundle the schema into an op context.
const opContext = defineOpContext({ logSchema: schema });
const { defineOp } = opContext;

// 3. Define an op. Its body receives a typed SpanContext (`ctx`).
const createUser = defineOp('create-user', async (ctx, email: string) => {
  ctx.tag.userId(email).operation('INSERT');
  ctx.log.info('creating {{userId}}').userId(email); // {{field}} = template, value set via the chain

  const validated = await ctx.span('validate', async (child) => {
    child.tag.operation('SELECT');
    return child.ok({ valid: true });
  });
  if (!validated.success) return ctx.err(new Error('validation failed'));

  return ctx.ok({ id: 'user-1', email }).with({ duration: 12 });
});

// 4. Build a tracer and run at the request boundary.
const { trace } = new StdioTracer(opContext, {
  bufferStrategy: new JsBufferStrategy(),
  createTraceRoot,
});

const result = await trace('create-user', createUser, 'ada@example.com');
console.log(result.success ? result.value : result.error);
```

## Core API

### Schema (`S` + `defineLogSchema`)

| Builder         | Use                                             |
| --------------- | ----------------------------------------------- |
| `S.enum([...])` | A fixed set of known values (stored as 1 byte). |
| `S.category()`  | Repeating strings (dictionary-encoded).         |
| `S.text()`      | Mostly-unique strings (no dictionary).          |
| `S.number()`    | Numeric column.                                 |
| `S.boolean()`   | Boolean column (bit-packed).                    |

### `defineOpContext`

```ts
const opContext = defineOpContext({
  logSchema, // required
  flags, // optional: defineFeatureFlags(...).schema
  deps, // optional: other op groups, wired with .prefix()/.mapColumns()
  ctx: {
    // optional: user context carried on every span
    requestId: undefined as string | undefined,
  },
});
const { defineOp, defineOps } = opContext;
```

`defineOp(name, fn)` creates a single op; `defineOps({ ... })` batches ops into a reusable group.

### `SpanContext` (`ctx`)

The object every op body receives:

| Member                               | What it does                                                                      |
| ------------------------------------ | --------------------------------------------------------------------------------- |
| `ctx.tag.field(value)`               | Set span-start attributes (row 0). Chainable; `ctx.tag.with({...})` sets several. |
| `ctx.log.info/debug/warn/error(msg)` | Append a log event. `msg` is a `{{field}}` template; attach values via the chain. |
| `ctx.span(name, opOrFn, ...args)`    | Open a child span; returns `Promise<Result>`.                                     |
| `ctx.spanSync(name, fn)`             | Open a **synchronous** child span; `fn` returns a `Result` directly (no `await`). |
| `ctx.ok(value)` / `ctx.err(error)`   | Complete the span. Both chain `.with({...})` and `.message('...')`.               |
| `ctx.setScope({...})` / `ctx.scope`  | Set/read attributes inherited by all subsequent logs and child spans.             |
| `ctx.ff`                             | Feature-flag access (present when `flags` is declared).                           |
| `ctx.deps`                           | Declared dependency op groups.                                                    |
| `ctx.buffer`                         | The underlying span buffer (used in tests and for Arrow conversion).              |
| user context                         | Anything declared in `ctx: {...}` (e.g. `ctx.requestId`).                         |

### Results & typed errors

Ops return a `Result` — check `result.success`, then read `result.value` or `result.error`. Error codes are typed
factories:

```ts
import { defineCodeError } from '@smoothbricks/lmao';

const NOT_FOUND = defineCodeError('NOT_FOUND')<{ userId: string }>();

const getUser = defineOp('get-user', async (ctx, id: string) => {
  const user = await lookup(id);
  return user ? ctx.ok(user) : ctx.err(NOT_FOUND({ userId: id }));
});

const r = await trace('get-user', getUser, 'u1');
if (r.success) {
  console.log(r.value);
} else {
  console.log(r.error.code, r.error.userId); // 'NOT_FOUND' + the typed payload field
}
```

## Tracers

Construct a tracer with `new SomeTracer(opContext, options)`, then destructure `trace`. Every tracer needs a
`bufferStrategy` (`new JsBufferStrategy()`) and a `createTraceRoot` (from `@smoothbricks/lmao/node` for Node, or `/es`
for browsers/Deno/Workers).

| Tracer                               | Use                                                                          |
| ------------------------------------ | ---------------------------------------------------------------------------- |
| `StdioTracer`                        | Print the span tree to the console.                                          |
| `TestTracer`                         | Keep completed traces in memory (`tracer.rootBuffers`) for inspection/tests. |
| `ArrayQueueTracer`                   | Queue completed traces for batch processing (`tracer.drain()`).              |
| `SQLiteTracer` / `SQLiteAsyncTracer` | Persist to a synchronous SQLite DB, or an async one (e.g. D1).               |
| `CompositeTracer`                    | Fan out to several tracers.                                                  |
| `NoOpTracer`                         | Execute ops without emitting.                                                |

```ts
// trace() runs an op (or an inline fn) as the root span; overrides carry user-context values.
await trace('greet', greet);
await trace('greet', { requestId: 'req-1' }, greet, ...args);
```

## Feature flags

Declare flags, pass the schema to `defineOpContext`, and supply an evaluator **to the tracer**:

```ts
import { defineFeatureFlags, InMemoryFlagEvaluator, S } from '@smoothbricks/lmao';

const flags = defineFeatureFlags({
  advancedValidation: S.boolean().default(false).sync(),
  maxRetries: S.number().default(3).sync(),
});

const opContext = defineOpContext({ logSchema: schema, flags: flags.schema });

const { trace } = new StdioTracer(opContext, {
  bufferStrategy: new JsBufferStrategy(),
  createTraceRoot,
  flagEvaluator: new InMemoryFlagEvaluator(flags.schema, { advancedValidation: true, maxRetries: 5 }),
});

// Inside an op — sync flags are read as `ctx.ff.<name>?.value`:
if (ctx.ff.advancedValidation?.value) {
  /* ... */
}
```

## Persist & analyze

Convert a completed trace to an Arrow table, then persist or query it:

```ts
import { convertSpanTreeToArrowTable } from '@smoothbricks/lmao';

const table = convertSpanTreeToArrowTable(tracer.rootBuffers[0]);
console.log(table.numRows, table.names);
```

- **SQLite / D1** — use `SQLiteTracer`/`SQLiteAsyncTracer` with `createNodeSQLiteDatabase`
  (`@smoothbricks/lmao/sqlite/node`) or `createD1SQLiteDatabase` (`@smoothbricks/lmao/sqlite`).
- **Query engine** — the companion package [`@smoothbricks/lmao-inspector`](../lmao-inspector) runs SQL over exported
  Arrow data in the browser.

## Trace-testing

Assert on **what your code did**, not just what it returned. Each test's ops emit a queryable span tree.

```ts
import { describe, it, expect } from 'bun:test';
import { useTestSpan } from '@smoothbricks/lmao/testing/bun';
import { querySpan, findSpan } from '@smoothbricks/lmao/testing';

describe('order processing', () => {
  it('validates and saves', async () => {
    const ctx = useTestSpan();

    await ctx.span('processOrder', async (child) => {
      child.tag.orderId('123');
      await child.span('validate', async (v) => v.ok(true));
      await child.span('save', async (s) => s.ok({ id: 'order-123' }));
      return child.ok({ status: 'saved' });
    });

    const q = querySpan(ctx.buffer);
    expect(q.names()).toEqual(['processOrder', 'validate', 'save']);
    expect(findSpan(ctx.buffer, 'save')).toBeDefined();
  });
});
```

Setup is wiring-only: a preload/setup file calls
`initTraceTestRun(opContext, { sqlite: { dbPath: DEFAULT_TRACE_DB_PATH } })` and installs a transparent mock so tests
import `describe`/`it`/`expect` from their native runner as usual. Bun and Vitest are both supported
(`@smoothbricks/lmao/testing/bun` · `@smoothbricks/lmao/testing/vitest`). `DEFAULT_TRACE_DB_PATH` resolves to
`.cache/trace-results.db` under the package or workspace root: the sink must sit in a directory that project walkers and
file watchers skip, because a SQLite database churns its directory's membership through journal sidecars and a compiler
watching that directory would call its own transform generation incoherent. Traces flush to a SQLite sink you can query
with the `TraceQuery` API or the `sqlite3` CLI. See the docs for the full harness setup, the SQLite schema, and query
recipes.

## Package exports

| Import                                                          | Provides                                                                                                                                                                                                             |
| --------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@smoothbricks/lmao`                                            | Core API: `defineOpContext`, `defineLogSchema`, `defineFeatureFlags`, `S`, `JsBufferStrategy`, all tracers, Arrow conversion, results (`Ok`/`Err`/`defineCodeError`), `InMemoryFlagEvaluator`, entry-type constants. |
| `@smoothbricks/lmao/node`                                       | `createTraceRoot` using `process.hrtime.bigint()`.                                                                                                                                                                   |
| `@smoothbricks/lmao/es`                                         | `createTraceRoot` using `performance.now()` (browser/Deno/Workers).                                                                                                                                                  |
| `@smoothbricks/lmao/sqlite`, `/sqlite/node`                     | SQLite/D1 tracers and database factories.                                                                                                                                                                            |
| `@smoothbricks/lmao/cloudflare`                                 | Cloudflare trace-sink adapters (`DiagnosticDrainTracer`, `ClassSplitTracer`, transports). _Partially implemented._                                                                                                   |
| `@smoothbricks/lmao/errors*`                                    | `Transient`, `Blocked`, `defineCodeError`, backoff/retry helpers.                                                                                                                                                    |
| `@smoothbricks/lmao/testing`, `/testing/bun`, `/testing/vitest` | Trace-testing query API and runner harnesses.                                                                                                                                                                        |

### Companion packages

- [`@smoothbricks/lmao-inspector`](../lmao-inspector) — client-side Arrow query engine and trace sources.
- [`@smoothbricks/lmao-transformer`](../lmao-transformer) — optional build-time TypeScript transformer (source-line
  injection and `ctx.tag` inlining).

## Rust crates

`crates/` belongs to the repository-root Cargo workspace and ships inside this npm package as a generated `dist/rust`
workspace. It is not a separate project: the wasm the TypeScript loads and the crates that produced it carry one version
number and are built from one tree, so they cannot disagree.

`lmao-wasm` is the production allocator. Its `[lib] name` is `allocator`, so `cargo build` emits `allocator.wasm`
directly and `nx run lmao:cargo-wasm` only lifts it out of `target/` into `dist/allocator.wasm` — the exact path
`src/lib/wasm/wasmAllocator.ts` resolves at runtime. Its tiered freelist clamps sub-20-byte `col_1b` blocks to the first
≥20-byte tier so free-block bookkeeping always fits.

`lmao-core`'s `build.rs` generates the entry-type enum and capacity-tuning constants from the TypeScript SSOT
(`src/lib/schema/systemSchema.ts`, `src/lib/capacityTuning.ts`). Those files are in the same package, and both ship,
which is what makes the published tree buildable.

### Crate map

| Crate         | Responsibility                                                                                                                                                                                                                                                                             | Primary specs                                      |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------- |
| `lmao-core`   | Entry types (23), span identity (thread_id + counter, `Arc<str>` trace ids, parent by reference), fixed row layout (row 0 span-start / tag overwrite, row 1 pre-armed span-exception, rows 2+ append), per-trace timestamp anchoring behind a `Clock` trait, SoA buffers, capacity ratchet | `01h`, `01b4`, `01b`, `01b1`–`01b5`, `01b3`, `01a` |
| `lmao-arena`  | Buddy/tiered-freelist arena (28 freelists = 7 tiers × 4 size classes, zero-overhead freelists, 192-byte header — layouts pinned with `#[repr(C)]` + const asserts)                                                                                                                         | `01q`                                              |
| `lmao-arrow`  | Two-pass tree walk → one Arrow `RecordBatch` per flush (pass 1 dictionary accumulation, sorted-dict finalize, pass 2 memcpy columns)                                                                                                                                                       | `01k`, `01f`                                       |
| `lmao-macros` | `define_log_schema!` / `span!` proc-macros — compile-time replacement for the TS `new Function()` codegen and AST transformer                                                                                                                                                              | `01a`, `01g`, `01j`, `01o`, `01b6`                 |
| `lmao-wasm`   | `cdylib` for `wasm32-unknown-unknown` exporting the allocator ABI (stable names, sentinel returns, packed-u64 identity convention, `--import-memory`) consumed directly by `wasmAllocator.ts`                                                                                              | `01q`                                              |
| `lmao-query`  | Tracer-agnostic assertion/query surface (selector → count/never), Arrow + SQLite backends behind features                                                                                                                                                                                  | trace-testing spec                                 |

### Consuming the crates from npm

The crate sources ship in the tarball, so a Rust consumer pins a released VERSION instead of a working tree:

```toml
[dependencies]
lmao-core = { path = "node_modules/@smoothbricks/lmao/dist/rust/crates/lmao-core" }
```

`nx build lmao` derives `dist/rust/Cargo.toml`, `Cargo.lock`, `.cargo/config.toml`, and `crates/` from the root
workspace. Workspace inheritance (`version.workspace = true`) therefore resolves inside the installed package without
maintaining a second source workspace. Build output (`target/`) is excluded.

### Determinism constraints (non-negotiable design rules)

These constraints are defined by the deterministic scheduler and trace-testing specs:

1. **No ambient time or entropy.** Wall/monotonic time only via `lmao_core::Clock`; randomness only via
   `lmao_core::Entropy`. A sim run injects seeded impls and must get **bit-identical trace bytes** for the same
   `(build, seed, config)`.
2. **Zero heap allocations per event after warmup** (`tests/alloc_gate.rs` enforces with a counting global allocator).
   Growth = overflow chaining, never realloc.
3. **Overhead gates:** tracing on vs off ≤20% median throughput delta, ≤25% peak RSS, at a 10^6-event run
   (`benches/overhead.rs` harness; gate comparison TBD).
4. **Deterministic encoding:** sorted dictionaries, no HashMap-iteration-order or float-formatting leakage into emitted
   bytes.
5. **Tracer-agnostic queries:** the same selector must run against SQLite (`.cache/trace-results.db` parity) and
   in-process Arrow batches.

### TDD workflow

Red → green → mutate:

1. **Red:** the property tests in `crates/*/tests/properties.rs` marked `#[ignore = "TDD red: ..."]` are the acceptance
   criteria for the next implementation step (arena alloc/free, `SpanBuffer::append`, `convert_span_trees`). Un-ignore
   the one you're implementing; watch it fail.
2. **Green:** implement until `just proptest-heavy` passes (10k cases).
3. **Mutate:** `just mutants` — classify survivors per the kernel rule (equivalent / invalid / budget-timeout /
   unreached / oracle-gap). Remove the corresponding `exclude_re` entry in `mutants.toml` as each stub becomes real.

### Commands

```sh
cd packages/lmao
just test            # fast tier: unit + property tests (green suite)
just proptest-heavy  # heavy tier: 10k proptest cases, includes TDD-red ignored tests
just mutants         # mutation tier (cargo install cargo-mutants)
just wasm            # wasm32 ABI build (rustup target add wasm32-unknown-unknown)
just bench           # criterion harness for the overhead gates
just check           # cargo check + clippy -D warnings
```

Without `just`: the same commands are spelled out in `justfile`, and `.cargo/config.toml` provides `cargo t`,
`cargo proptest-heavy`, `cargo wasm`. Under Nx the inferred targets are `cargo-test`, `cargo-lint`, `cargo-lint-cross`,
`mutation` and `bench`; `cargo-wasm` is declared here because whether this package ships a wasm artifact is a packaging
decision.

### Port order

1. `lmao-arena` alloc/free (the property tests encode buddy conservation, reuse, and no-overlap).
2. `lmao-core` `SpanBuffer::append` + overflow chaining (alloc-gate test goes green).
3. `lmao-arrow` `convert_span_trees` (bit-identical serialization test goes green).
4. `lmao-macros` `define_log_schema!` (then re-include macros in `mutants.toml`).
5. `lmao-wasm` real bodies over linear memory; validate against the TS host's allocator tests in
   `src/lib/wasm/__tests__/`.
6. `lmao-query` Arrow scan backend; SQLite parity backend.

### Backlog (downstream consumers depend on these)

- **`lmao-inspect`** — a thin generic CLI over `lmao-query`: tail/follow Arrow segment dirs, run selectors and SQL,
  render tables, export `--json`/`--ndjson`. Reusable by cowshed, downstream consumers, and jcode; spec at
  `specs/lmao/04_inspect_cli.md`.
- **W3C `traceparent` interop** — the runtime adopts an inbound `TRACEPARENT` (env / carrier) at tracer init as the root
  span context, and stamps outbound `fetch`/HTTP requests with the current span's `traceparent`. This is what lets a
  first-party tool's traffic join the caller's trace end-to-end (cowshed's tier-1 gateway attribution depends on it —
  `specs/cowshed/13_telemetry.md`). Applies to both the TypeScript and Rust tracers; keep the wire format identical
  across them.

## Examples

Runnable with `bun run examples/<name>.ts`:

| Example                  | Shows                                                                  |
| ------------------------ | ---------------------------------------------------------------------- |
| `basic-usage.ts`         | Schema, feature flags, user context, fluent tags, child spans.         |
| `fluent-result-api.ts`   | `ok`/`err` with `.with()`/`.message()`, typed error codes, exceptions. |
| `chaining-showcase.ts`   | Tag/log fluent chaining patterns.                                      |
| `middleware-pattern.ts`  | Wrapping ops with cross-cutting behavior.                              |
| `library-integration.ts` | Composing op groups across packages with `.prefix()`/`.mapColumns()`.  |
| `complete-example.ts`    | An end-to-end request flow.                                            |
| `arrow-export.ts`        | Converting a trace to an Arrow table.                                  |

## License

MIT
