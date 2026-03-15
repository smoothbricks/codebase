# LMAO Inspector

## Overview

`packages/lmao-inspector` is a standalone trace viewer package built with Tamagui. It provides components for searching,
streaming, and visualizing LMAO traces with client-side Arrow processing. Designed for embedding in any Expo app (web or
native).

The inspector complements the server-side DuckDB query engine (`02_query_engine.md`) with a browser-native query path:
instead of Lambda + S3 queries, the inspector streams or downloads Arrow RecordBatches directly to the browser and
processes them client-side.

## Architecture

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

## Sources

### Live Stream Source

Connects to a MessageRelay Durable Object (see `extern/graphql/src/message-relay.ts`) via WebSocket. The relay pattern:

1. Backend queue processors (CF Worker, Lambda) collect LMAO RecordBatches during execution flushes
2. Processors POST Arrow IPC bytes to `MessageRelay /publish` with topic `traces:{groupId}`
3. Inspector subscribes via WebSocket, receives Arrow chunks as they arrive
4. Chunks are fed into the browser-side Arrow query engine for immediate filtering

The DO acts as a hibernatable WebSocket proxy — the Worker keeps the connection alive but the DO can sleep between
publishes. This is the existing MessageRelay pattern, not a new infrastructure piece.

For Bun backends: direct SSE endpoint that pushes RecordBatches as they arrive (no DO relay needed).

### Archive Source

Fetches historical Arrow IPC or Parquet files from object storage (R2, S3) by time range. Used for:

- Historical trace search (beyond live stream buffer)
- Bulk analysis of archived trace data

Archive files follow the existing LMAO flush path: `{prefix}/{date}/{batch-id}.arrow`

## Arrow Query Engine

Client-side engine that processes Arrow RecordBatches in the browser. Candidates (research spike needed):

| Engine             | Language | WASM Size | Arrow Native | SQL Support | Maturity   |
| ------------------ | -------- | --------- | ------------ | ----------- | ---------- |
| DuckDB-WASM        | C++      | ~4MB      | Yes          | Full SQL    | Production |
| DataFusion         | Rust     | TBD       | Yes          | Full SQL    | Growing    |
| Polars (polars-js) | Rust     | TBD       | Yes          | SQL + API   | Growing    |
| GlareDB            | Rust     | TBD       | Yes          | Full SQL    | Early      |

DuckDB-WASM is the proven choice. Rust alternatives may offer smaller bundles or better Arrow integration. Research
spike should evaluate bundle size, startup time, and query performance on typical trace volumes.

The engine must handle the LMAO column schema including `S.unknown()` Binary columns (msgpack-encoded). For DuckDB-WASM,
the `msgpack_extract()` extension from `02_query_engine.md` applies here too — either as a WASM extension or with
client-side msgpack deserialization before query.

## Components

All components are Tamagui-based (cross-platform). The Arrow query engine is web-only — on native, components show a
simplified list view without client-side SQL.

### TraceViewer

Top-level composed component. Accepts a `TraceSource` (live stream or archive) and renders the trace UI.

```typescript
<TraceViewer
  source={createStreamSource({ wsUrl, groupId })}
  onOpenEvent={(axId) => { /* host app navigation */ }}
/>
```

### TraceTimeline

Span timeline visualization. Shows parent/child span relationships, timing, and duration. Clicking a span opens
SpanDetail.

### SpanDetail

Individual span attributes, timing breakdown, log entries, and links. Displays:

- Span name, duration, status
- All tagged attributes (from `ctx.tag.*`)
- Log entries (from `ctx.log.*`)

### LogSearch

Full-text search + attribute filtering over the Arrow data. Queries run client-side via the Arrow query engine.
Supports:

- Text search across `message` column
- Time range selection
- Severity/entry type filtering
- msgpack payload field queries (via `msgpack_extract` or client-side deserialization)

## Data Model

The inspector queries LMAO's Arrow column schema (`01f_arrow_table_structure.md`):

| Column         | Type      | Usage in Inspector                                   |
| -------------- | --------- | ---------------------------------------------------- |
| span_id        | category  | Individual span identity                             |
| parent_span_id | category  | Build span tree for timeline view                    |
| message        | text      | Primary display text, full-text search target        |
| entry_type     | enum      | Filter by span/info/warn/error                       |
| timestamp      | number    | Timeline positioning, time range filtering           |
| duration       | number    | Span duration display, performance analysis          |
| package_name   | category  | Filter by source package                             |
| \*             | S.unknown | Queryable via msgpack extraction                     |


## Public API

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

## Related

- `02_query_engine.md` — Server-side DuckDB query engine (Lambda + S3)
- `01f_arrow_table_structure.md` — Arrow column schema
- `01t_trace_archive_pipeline.md` — Archive flush path
