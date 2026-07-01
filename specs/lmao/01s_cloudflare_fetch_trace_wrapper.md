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

## Runtime Constraints <a id="smoo/lmao!n/cf-fetch-wrapper-runtime-constraints"></a>

- Worker isolates are ephemeral and may not survive beyond any single request.
- In-memory buffers are best-effort optimizations only.
- Durable delivery must rely on external systems (Queue/object storage), not isolate memory.

## Wrapper API <a id="smoo/lmao!n/cf-fetch-wrapper-api"></a>

```typescript
import type { OpContextBinding } from '@smoothbricks/lmao';
import { ArrayQueueTracer } from '@smoothbricks/lmao';

type TraceWrapperConfig<B extends OpContextBinding> = {
  tracer: ArrayQueueTracer<B>;
  enqueue: (chunk: TraceChunkEnvelope) => Promise<void>;
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
3. Wrapper drains completed root buffers via `tracer.drain()`.
4. Wrapper converts drained buffers to Arrow payloads via `tracer.bufferStrategy.toArrowTable(...)`.
5. Wrapper releases converted buffers via `tracer.bufferStrategy.releaseBuffer(...)`.
6. Wrapper schedules enqueue via `ctx.waitUntil(config.enqueue(chunk))`.
7. Response is returned immediately without waiting for enqueue completion.

## Trace Chunk Envelope <a id="smoo/lmao!n/cf-fetch-wrapper-chunk-envelope"></a>

```typescript
type TraceChunkEnvelope = {
  chunk_id: string; // idempotency key
  emitted_at: string; // ISO timestamp
  domain: string;
  runtime: 'cloudflare-worker';
  trace_format: 'arrow';
  compression?: 'zstd' | 'gzip' | 'none';
  payload_bytes: Uint8Array; // Arrow table or IPC payload
  metadata: {
    batch_count: number;
    row_count: number;
    first_ts?: string;
    last_ts?: string;
  };
};
```

## Flush Triggers <a id="smoo/lmao!n/cf-fetch-wrapper-flush-triggers"></a>

Flush when any trigger is met:

- `row_count >= maxRowsPerChunk`
- `payload_size >= maxBytesPerChunk`
- `chunk_age_ms >= maxChunkAgeMs`

If none are met during a request, implementation MAY keep best-effort in-memory accumulation for warm isolates.

## Delivery Semantics <a id="smoo/lmao!n/cf-fetch-wrapper-delivery-semantics"></a>

- Queue delivery is at-least-once.
- `chunk_id` must be stable and dedupe-safe.
- Consumers must deduplicate by `chunk_id`.

## Failure Handling <a id="smoo/lmao!n/cf-fetch-wrapper-failure-handling"></a>

- Enqueue failures occur in background via `waitUntil`; they must not block response return.
- Failed enqueue attempts should emit runtime metrics and retry via queue policy/backoff.
- If queue is unavailable for extended periods, runtime should degrade gracefully and surface health alerts.

## Downstream Archival Contract <a id="smoo/lmao!n/cf-fetch-wrapper-archival-contract"></a>

agent:

- LMAO owns data-plane chunk primitives (`chunk_id`, partition inspection/split helpers, compaction helpers).

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
  enqueue: (chunk: TraceChunkEnvelope) => Promise<void>
): Promise<void> {
  const buffers = tracer.drain();
  for (const buffer of buffers) {
    try {
      const table = await tracer.bufferStrategy.toArrowTable(buffer);
      const chunk = encodeArrowTableAsChunkEnvelope(table);
      await enqueue(chunk);
    } finally {
      tracer.bufferStrategy.releaseBuffer(buffer);
    }
  }
}
```

## Related <a id="smoo/lmao!n/cf-fetch-wrapper-related"></a>

- [Trace Logging System](./01_trace_logging_system.md)
- [Context Flow and Op/Span Pattern](./01c_context_flow_and_op_wrappers.md)
- [Trace Archive Pipeline](./01t_trace_archive_pipeline.md)
