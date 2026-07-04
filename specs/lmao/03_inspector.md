# LMAO Inspector <a id="smoo/lmao!n/inspector"></a>

> **Implementation status (partial — data layer shipped, UI not).** `packages/lmao-inspector/src` ships the **data
> layer**: the stream source (`createStreamSource`, WebSocket + SSE with reconnect/backoff), the archive source
> (`createArchiveSource`), and the DuckDB-WASM query engine (`createQueryEngine`). **Not yet built:** the Tamagui
> **components** (`TraceViewer` / `TraceTimeline` / `SpanDetail` / `LogSearch`), the host-app cross-linking callbacks,
> and the `msgpack_extract` integration in the WASM engine. Two **contract drifts** from the spec's Public API are noted
> inline at each affected heading (the shipped `createStreamSource`/`createArchiveSource` signatures differ, and SSE is
> folded into `createStreamSource` rather than a separate `createSseSource`). The shipped, missing, and drift items are
> staged as `smoo/lmao` nodes.

## Overview <a id="smoo/lmao!n/inspector.overview"></a>

`packages/lmao-inspector` is a standalone trace viewer package built with Tamagui. It provides components for searching,
streaming, and visualizing LMAO traces with client-side Arrow processing. Designed for embedding in any Expo app (web or
native).

The inspector complements the server-side DuckDB query engine (`02_query_engine.md`) with a browser-native query path:
instead of Lambda + S3 queries, the inspector streams or downloads Arrow RecordBatches directly to the browser and
processes them client-side.

## Architecture <a id="smoo/lmao!n/inspector.architecture"></a>

```
                    ┌─────────────────────────────┐
                    │     lmao-inspector (browser) │
                    │                              │
  WebSocket/SSE ──→ │  stream-source ──→ arrow-    │
                    │                    query     │ ──→ Tamagui components
  Archive fetch ──→ │  archive-source ─→ engine    │
                    │                              │
                    └─────────────────────────────┘

  Arrow query engine: DuckDB-WASM or Rust/WASM alternative (web-only)
  All filtering, search, and aggregation runs in the browser.
  Server streams raw Arrow/Parquet — no server-side query API needed.
```

## Sources <a id="smoo/lmao!n/inspector.sources"></a>

### Live Stream Source <a id="smoo/lmao!n/inspector-stream-source"></a>

> **Implementation status (shipped, with a config drift).** `packages/lmao-inspector/src/sources/stream-source.ts` ships
> `createStreamSource` — it opens a WebSocket **or** SSE connection (the `transport` field selects), receives Arrow IPC
> frames (binary over WS; base64-decoded over SSE), parses them with flechette, and auto-reconnects with exponential
> backoff capped at 30s. **Drift:** the shipped config is `{ url, transport: 'websocket' | 'sse', topic }`, not the
> Public-API section's `{ wsUrl, groupId }`; reconcile the two (see `n/inspector-public-api`).

Connects to a MessageRelay Durable Object (see `extern/graphql/src/message-relay.ts`) via WebSocket. The relay pattern:

1. Backend queue processors (CF Worker, Lambda) collect LMAO RecordBatches during execution flushes
2. Processors POST Arrow IPC bytes to `MessageRelay /publish` with topic `traces:{groupId}`
3. Inspector subscribes via WebSocket, receives Arrow chunks as they arrive
4. Chunks are fed into the browser-side Arrow query engine for immediate filtering

The DO acts as a hibernatable WebSocket proxy — the Worker keeps the connection alive but the DO can sleep between
publishes. This is the existing MessageRelay pattern, not a new infrastructure piece.

For Bun backends: direct SSE endpoint that pushes RecordBatches as they arrive (no DO relay needed).

### Archive Source <a id="smoo/lmao!n/inspector-archive-source"></a>

> **Implementation status (shipped, with a config drift).** `packages/lmao-inspector/src/sources/archive-source.ts`
> ships `createArchiveSource`, whose `fetchRange(startTime, endTime)` HTTP-GETs the archive endpoint and yields Arrow
> IPC buffers as an async iterable (progressive loading; splits on the Arrow IPC continuation/EOS framing, falling back
> to a single batch for non-stream responses). **Drift:** the shipped factory takes
> `createArchiveSource(baseUrl: string)`, not the Public-API section's `createArchiveSource({ fetchUrl })`; reconcile
> the two (see `n/inspector-public-api`).

Fetches historical Arrow IPC or Parquet files from object storage (R2, S3) by time range. Used for:

- Historical trace search (beyond live stream buffer)
- Loading traces linked by trace ID
- Bulk analysis of archived trace data

Archive files follow the existing LMAO flush path: `{prefix}/{date}/{batch-id}.arrow`

## Arrow Query Engine <a id="smoo/lmao!n/inspector-query-engine"></a>

> **Implementation status (shipped, minus msgpack_extract).** `packages/lmao-inspector/src/engine/query-engine.ts` ships
> `createQueryEngine` — a lazy singleton DuckDB-WASM engine (CDN-hosted bundle, dynamic-imported so import-time cost is
> zero; `registerArrowBatch` registers an Arrow IPC buffer as a named table via `arrow_scan`; `query` runs SQL; `close`
> tears down files/worker; init failure resets the singleton for retry). **Not yet built:** the `msgpack_extract` path
> for `S.unknown()` Binary columns (neither a WASM extension nor a client-side msgpack pre-deserialize) — tracked as
> `n/inspector-msgpack-wasm`, and it shares the subset/decoder design with the server-side extension (spec 02
> `n/query-engine-msgpack-extract`).

Client-side engine that processes Arrow RecordBatches in the browser. The engine is **DuckDB-WASM**. Candidates
considered:

| Engine             | Language | WASM Size                     | Arrow Native | SQL Support | Maturity   |
| ------------------ | -------- | ----------------------------- | ------------ | ----------- | ---------- |
| DuckDB-WASM        | C++      | ~4MB                          | Yes          | Full SQL    | Production |
| DataFusion         | Rust     | ~6–8 MB (experimental)        | Yes          | Full SQL    | Growing    |
| Polars (polars-js) | Rust     | Node-native; no browser build | Yes          | SQL + API   | Growing    |
| GlareDB            | Rust     | None                          | Yes          | Full SQL    | Early      |

DuckDB-WASM is the chosen engine (proven, ~4MB, full SQL, Arrow-native). The Rust alternatives are recorded (snapshot
2026-07) only as the fallback set should DuckDB-WASM's bundle size or Arrow integration prove inadequate.

The engine must handle the LMAO column schema including `S.unknown()` Binary columns (msgpack-encoded). For DuckDB-WASM,
the `msgpack_extract()` extension from `02_query_engine.md` applies here too — either as a WASM extension or with
client-side msgpack deserialization before query.

## Components <a id="smoo/lmao!n/inspector-components"></a>

> **Implementation status (not built).** None of the Tamagui components below exist in `packages/lmao-inspector/src` —
> the package currently exports only the data layer (sources + engine). All four are staged as one missing-work node
> `n/inspector-components`; the per-component headings (`TraceViewer` / `TraceTimeline` / `SpanDetail` / `LogSearch`)
> anchor to its `.sub` regions.

All components are Tamagui-based (cross-platform). The Arrow query engine is web-only — on native, components show a
simplified list view without client-side SQL.

### TraceViewer <a id="smoo/lmao!n/inspector-components.trace-viewer"></a>

Top-level composed component. Accepts a `TraceSource` (live stream or archive) and renders the trace UI.

```typescript
<TraceViewer
  source={createStreamSource({ wsUrl, groupId })}
  onOpenEvent={(id) => { /* host app navigation */ }}
/>
```

### TraceTimeline <a id="smoo/lmao!n/inspector-components.trace-timeline"></a>

Span timeline visualization. Shows parent/child span relationships, timing, and duration. Clicking a span opens
SpanDetail.

### SpanDetail <a id="smoo/lmao!n/inspector-components.span-detail"></a>

Individual span attributes, timing breakdown, log entries, and links. Displays:

- Span name, duration, status
- All tagged attributes (from `ctx.tag.*`)
- Log entries (from `ctx.log.*`)

### LogSearch <a id="smoo/lmao!n/inspector-components.log-search"></a>

Full-text search + attribute filtering over the Arrow data. Queries run client-side via the Arrow query engine.
Supports:

- Text search across `message` column
- Attribute filters (e.g., `package_name = 'user-service'`)
- Time range selection
- Severity/entry type filtering
- msgpack payload field queries (via `msgpack_extract` or client-side deserialization)

## Data Model <a id="smoo/lmao!n/inspector.data-model"></a>

The inspector queries LMAO's Arrow column schema (`01f_arrow_table_structure.md`):

| Column         | Type      | Usage in Inspector                            |
| -------------- | --------- | --------------------------------------------- |
| trace_id       | category  | Group spans into traces, deep-link target     |
| span_id        | category  | Individual span identity                      |
| parent_span_id | category  | Build span tree for timeline view             |
| message        | text      | Primary display text, full-text search target |
| entry_type     | enum      | Filter by span/info/warn/error                |
| timestamp      | number    | Timeline positioning, time range filtering    |
| duration       | number    | Span duration display, performance analysis   |
| package_name   | category  | Filter by source package                      |
| \*             | S.unknown | Queryable via msgpack extraction              |

## Public API <a id="smoo/lmao!n/inspector-public-api"></a>

> **Implementation status (partial + drift).** `packages/lmao-inspector/src/index.ts` exports the **data layer only**:
> `createStreamSource`, `createArchiveSource`, `createQueryEngine` (+ their types and a test-only
> `_resetEngineForTesting`). The component exports (`TraceViewer` / `TraceTimeline` / `SpanDetail` / `LogSearch`) are
> **absent** (unbuilt). The signatures below also **drift** from what shipped, and the drift must be reconciled
> (`n/inspector-public-api`):
>
> - shipped `createStreamSource({ url, transport, topic })` vs. spec `({ wsUrl, groupId })`;
> - shipped `createArchiveSource(baseUrl: string)` vs. spec `({ fetchUrl })`;
> - **no** separate `createSseSource` — SSE is the `transport: 'sse'` mode of `createStreamSource`;
> - sources return an `onBatch`/`fetchRange` shape, not the spec's abstract `TraceSource`.
>
> Decide which is canonical (the running code, or the `{ wsUrl, groupId }` / `createSseSource` design) and converge the
> spec block + the source together — do not leave both.

```typescript
// Sources
export function createStreamSource(config: { wsUrl: string; groupId: string }): TraceSource;
export function createSseSource(config: { url: string }): TraceSource;
export function createArchiveSource(config: { fetchUrl: string }): TraceSource;

// Components (Tamagui)
export function TraceViewer(props: TraceViewerProps): JSX.Element;
export function TraceTimeline(props: TraceTimelineProps): JSX.Element;
export function SpanDetail(props: SpanDetailProps): JSX.Element;
export function LogSearch(props: LogSearchProps): JSX.Element;

// Engine (web-only)
export function createQueryEngine(): Promise<ArrowQueryEngine>;
```

## Related <a id="smoo/lmao!n/inspector.related"></a>

- `02_query_engine.md` — Server-side DuckDB query engine (Lambda + S3)
- `01f_arrow_table_structure.md` — Arrow column schema
- `01t_trace_archive_pipeline.md` — Archive flush path
