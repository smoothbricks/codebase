# Cloudflare Fetch Trace Wrapper <a id="smoo/lmao!n/cf-fetch-wrapper"></a>

## Overview <a id="smoo/lmao!n/cf-fetch-wrapper-overview"></a>

This spec defines a Cloudflare Worker fetch-wrapper pattern for LMAO tracing that keeps response latency low while
shipping trace data durably for downstream aggregation.

The wrapper records traces on request path and flushes asynchronously via `ctx.waitUntil(...)`.

## Goals <a id="smoo/lmao!n/cf-fetch-wrapper-goals"></a>

1. Preserve request-path performance (no synchronous log shipping on response path)
2. Produce durable trace chunks suitable for Arrow/Parquet aggregation
3. Keep implementation deterministic and runtime-safe in Cloudflare Workers

## Non-Goals <a id="smoo/lmao!n/cf-fetch-wrapper-non-goals"></a>

- This wrapper is not prepaid/budget enforcement logic.
- This wrapper does not aggregate usage itself; it emits trace chunks for later aggregation.

## Runtime Constraints <a id="smoo/lmao!n/cf-fetch-wrapper-runtime-constraints"></a>

- Worker isolates are ephemeral and may not survive beyond any single request.
- In-memory buffers are best-effort optimizations only.
- Durable delivery must rely on external systems (Queue/object storage), not isolate memory.

## Wrapper API <a id="smoo/lmao!n/cf-fetch-wrapper-api"></a>

The wrapper composes the tracer primitives: `ArrayQueueTracer`, `JsBufferStrategy`, `createTraceRoot`, `Tracer.trace`,
`drain`, `spanBufferToTraceRows`, and `bufferStrategy.releaseBuffer`.

```typescript
import type { OpContextBinding } from '@smoothbricks/lmao';
import { ArrayQueueTracer } from '@smoothbricks/lmao';
import type { TraceRow } from '@smoothbricks/lmao/cloudflare';

type TraceWrapperConfig<B extends OpContextBinding> = {
  tracer: ArrayQueueTracer<B>;
  enqueue: (rows: readonly TraceRow[]) => Promise<void>;
  maxRowsPerChunk: number;
  maxBytesPerChunk: number;
  maxChunkAgeMs: number;
};

type WrappedFetch = (request: Request, env: Env, ctx: ExecutionContext) => Promise<Response>;

function withLmaoTracing<B extends OpContextBinding>(
  handler: WrappedFetch,
  config: TraceWrapperConfig<B>
): WrappedFetch;
```

### Required Tracer Construction (Cloudflare Runtime) <a id="smoo/lmao!n/cf-fetch-wrapper-tracer-construction"></a>

Create the tracer with Cloudflare-compatible timestamping and a queueable sink:

```typescript
import { ArrayQueueTracer, JsBufferStrategy } from '@smoothbricks/lmao';
import { createTraceRoot } from '@smoothbricks/lmao/es';

const tracer = new ArrayQueueTracer(opContext, {
  bufferStrategy: new JsBufferStrategy(),
  createTraceRoot,
});
```

## Canonical Request Flow <a id="smoo/lmao!n/cf-fetch-wrapper-request-flow"></a>

1. Wrapper opens a root trace for the fetch request via `await tracer.trace('fetch', ...)`.
2. Handler executes and returns response.
3. When a flush trigger (§Flush Triggers) is met, wrapper drains completed root buffers via `tracer.drain()`. Otherwise
   the completed buffers stay queued in the tracer for a later request, and steps 4-6 are skipped.
4. Wrapper flattens each drained buffer to rows via `spanBufferToTraceRows(...)`.
5. Wrapper releases each flattened buffer via `tracer.bufferStrategy.releaseBuffer(...)`.
6. Wrapper schedules enqueue via `ctx.waitUntil(config.enqueue(rows))`.
7. Response is returned immediately without waiting for enqueue completion.

## Trace Chunk Envelope <a id="smoo/lmao!n/cf-fetch-wrapper-chunk-envelope"></a>

**Lane note** (per
[01u §Collection Lanes and Fallbacks](./01u_cloudflare_trace_segments.md#smoo/lmao!n/cf-trace-segments-hop)): the
`enqueue` seam sends **rows** (JSON-serializable records) on every lane. On the primary diagnostic lane — the Cloudflare
Pipelines stream binding — the rows go to the stream as they are, because Pipelines owns batching and Parquet encoding
for the raw tier. On the Queues fallback lane, rows are batched into `TraceChunkQueueMessage`s: each message carries one
`TraceChunkEnvelope` ([01t](./01t_trace_archive_pipeline.md#smoo/lmao!n/trace-archive-envelope)) and the rows it
describes. The flush triggers, delivery semantics, and failure handling in this spec apply to both lanes.

```typescript
type TraceChunkEnvelope = {
  chunk_id: string; // idempotency key
  file_ref: string; // the Queues lane uses a logical ref, e.g. 'queue://worker-name'
  chunk_ref: string; // the Queues lane uses a content hash of the message's rows
  row_count: number;
  started_at_ms: number;
  ended_at_ms: number;
  partition_keys: readonly string[];
  metadata?: Record<string, unknown>;
};

type TraceChunkQueueMessage = {
  envelope: TraceChunkEnvelope;
  rows: readonly TraceRow[];
};
```

`QueuesFallbackTransport` slices each flush into messages of at most its `maxRowsPerMessage` rows. The caller sizes that
bound so a message stays under the 128 KB Queues limit.

## Flush Triggers <a id="smoo/lmao!n/cf-fetch-wrapper-flush-triggers"></a>

Flush when any trigger is met:

- `row_count >= maxRowsPerChunk`
- `payload_size >= maxBytesPerChunk`
- `chunk_age_ms >= maxChunkAgeMs`

If none are met during a request, implementation MAY keep best-effort in-memory accumulation for warm isolates.

The triggers decide when the wrapper flushes; they do not size messages. On the Queues lane, `maxRowsPerMessage` slices
one flush into messages.

## Delivery Semantics <a id="smoo/lmao!n/cf-fetch-wrapper-delivery-semantics"></a>

- Queue delivery is at-least-once.
- `chunk_id` must be stable and dedupe-safe.
- Consumers must deduplicate by `chunk_id`.

`chunk_id` is a deterministic FNV-1a hash of a canonical, sorted serialization of the envelope fields
(`buildTraceChunkEnvelope` in `packages/lmao/src/lib/archive/chunkEnvelope.ts`). The Queues lane sets `chunk_ref` to a
content hash of the message's rows (`QueuesFallbackTransport` in `packages/lmao/src/lib/cloudflare/diagnosticDrain.ts`),
so a retried send of the same rows yields an identical id. At-least-once delivery and consumer dedup are queue-transport
contracts, satisfied by the consumer keying on this `chunk_id`.

## Failure Handling <a id="smoo/lmao!n/cf-fetch-wrapper-failure-handling"></a>

- Enqueue failures occur in background via `waitUntil`; they must not block response return.
- Failed enqueue attempts should emit runtime metrics and retry via queue policy/backoff.
- If queue is unavailable for extended periods, runtime should degrade gracefully and surface health alerts.

## Downstream Archival Contract <a id="smoo/lmao!n/cf-fetch-wrapper-archival-contract"></a>

The archive control plane (listener registration, wake/fan-out, per-consumer cursor) is owned by the consuming system,
not this package.

Queue consumers should route drained `TraceChunkQueueMessage`s into an archival flow built on LMAO's data-plane
primitives:

- LMAO owns data-plane chunk primitives in `packages/lmao/src/lib/archive/`: `buildTraceChunkEnvelope` (`chunk_id`),
  `inspectPartitionCardinality` / `splitChunkByPartition` (partition inspection and split), and `compactTraceChunks`
  (compaction).

1. Persists immutable Arrow chunks (for example to R2) with append-only catalog records.
2. Periodically compacts small chunks into time-windowed Arrow/Parquet files.
3. Uses mixed-group split helpers before group-targeted routing.

## Example <a id="smoo/lmao!n/cf-fetch-wrapper-example"></a>

```typescript
export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext) {
    return withLmaoTracing(async (req, env, execCtx) => {
      return router.handle(req, env, execCtx);
    }, traceConfig)(request, env, ctx);
  },
};
```

## Reference Flush Routine <a id="smoo/lmao!n/cf-fetch-wrapper-flush-routine"></a>

```typescript
async function flushTraceBatch<B extends OpContextBinding>(
  tracer: ArrayQueueTracer<B>,
  enqueue: (rows: readonly TraceRow[]) => Promise<void>
): Promise<void> {
  const buffers = tracer.drain();
  for (const buffer of buffers) {
    let rows: TraceRow[];
    try {
      rows = spanBufferToTraceRows(buffer);
    } finally {
      tracer.bufferStrategy.releaseBuffer(buffer);
    }
    await enqueue(rows);
  }
}
```

`DiagnosticDrainTracer.flush()` (`packages/lmao/src/lib/cloudflare/diagnosticDrain.ts`) runs this routine with the
`enqueue` seam as its constructor-injected transport, and reports a failed send through its `onSendError` hook instead
of rejecting.

## Related <a id="smoo/lmao!n/cf-fetch-wrapper-related"></a>

- [Trace Logging System](./01_trace_logging_system.md)
- [Context Flow and Op/Span Pattern](./01c_context_flow_and_op_wrappers.md)
- [Trace Archive Pipeline](./01t_trace_archive_pipeline.md)
