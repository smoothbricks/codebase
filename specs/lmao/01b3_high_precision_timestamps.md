# High-Precision Timestamp System <a id="smoo/lmao!n/trace-root-timestamps"></a>

> **Part of [Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**
>
> This document details the high-precision timestamp design that provides sub-millisecond accuracy with minimal
> overhead.

The trace logging system uses a high-precision timestamp design that provides sub-millisecond accuracy while minimizing
overhead. The design captures a single time anchor at trace root creation, then uses high-resolution timers for all
subsequent timestamps.

> **Implementation status** (system state, not aspiration). The anchored-timestamp design is implemented as the
> `TraceRoot` **class** (`ITraceRoot` interface in `packages/lmao/src/lib/traceRoot.ts`), with two platform classes:
> `packages/lmao/src/lib/traceRoot.node.ts` (`process.hrtime.bigint()`, pure-JS writes) and
> `packages/lmao/src/lib/traceRoot.es.ts` (`performance.now()`). Each exposes `getTimestampNanos(): Nanoseconds`, the
> per-trace anchor (`anchorEpochNanos`, `anchorPerfNow`), and the span-start/span-end/log write methods. Anchors and
> `trace_id` live in a single `_system` `ArrayBuffer` (see the layout below `TRACE_ROOT_*_OFFSET`) so the WASM path can
> read them without BigInt extraction. The function-style `getTimestamp(buffer)` sketches below are illustrative; the
> realized API is the class method `getTimestampNanos()`. Behaviour is pinned by
> `packages/lmao/src/lib/__tests__/timestamp.test.ts`.
>
> **Native acceleration is WASM, not NAPI.** A Zig NAPI span-write backend was built and benchmarked against JS and WASM
> (span timestamp writes, M1 Max: WASM ~48 ns, NAPI ~70–86 ns, JS ~85–98 ns); NAPI beat JS but lost to WASM, so the
> node-gyp/NAPI path was removed in favor of the pure-Zig WASM memory architecture (01q). Node's `TraceRoot` writes are
> plain JS today; the WASM allocator is the native fast path. There is no NAPI addon in the package.

### Core Design Principles

1. **Zero allocations** - No object creation per timestamp
2. **High precision** - `performance.now()` gives ~5μs resolution in browsers
3. **Comparable** - All spans in a trace share the same anchor
4. **DST/NTP safe** - Anchor per trace, traces are short-lived
5. **Cross-platform** - Same API, different implementations

### Timestamp Precision Guarantees

**Storage Format**: All timestamps stored as nanoseconds (ns) in `BigInt64Array` during hot path.

**Precision by Platform**:

- **Browser**: ~5μs resolution from `performance.now()`, stored as nanoseconds
- **Node.js**: Nanosecond resolution from `process.hrtime.bigint()`, stored directly as nanoseconds
- **Fallback**: Millisecond resolution from `Date.now()` when high-resolution timers unavailable, converted to
  nanoseconds

**Safe Range**: `BigInt64Array` can exactly represent nanoseconds for all practical purposes:

- Maximum nanosecond timestamp: ~292 years from epoch (signed 64-bit)
- Well beyond any practical trace lifetime
- **Conclusion**: BigInt storage provides full nanosecond precision without loss

**Why Nanoseconds**:

- `BigInt64Array` provides full nanosecond precision without truncation
- Sub-microsecond precision enables detailed performance analysis
- Compatible with ClickHouse's `DateTime64(9)` type
- Matches Arrow's native timestamp precision

**Arrow Format**: During cold path conversion, timestamps converted to Arrow `TimestampNanosecond` type.

### Date.now() Usage Guidelines

**CRITICAL**: `Date.now()` has different usage rules depending on context:

**For Trace Timestamps** (Performance-Critical Path):

- ❌ **NEVER use `Date.now()` for span/log timestamps**
- ✅ **ALWAYS use anchored approach**: `traceRoot.getTimestampNanos()` (anchored on `anchorEpochNanos` /
  `anchorPerfNow`)
- Why: Anchored timestamps provide sub-millisecond precision and avoid repeated system calls

**For Scheduling/Background Tasks** (Non-Performance-Critical):

- ✅ **OK to use `Date.now()` for scheduling**: Background flush intervals, timeout calculations
- ✅ **OK for file naming**: `traces-${Date.now()}.parquet`
- ✅ **OK for logging system metadata**: Capacity tuning events, system diagnostics
- Why: Millisecond precision acceptable, system clock alignment desired

**Example**:

```typescript
// ✅ CORRECT: Trace timestamps use anchored approach
buffer.timestamp[idx] = buffer._traceRoot.getTimestampNanos();

// ❌ WRONG: Don't use Date.now() for trace timestamps
buffer.timestamp[idx] = BigInt(Date.now()) * 1_000_000n; // No sub-ms precision, repeated syscalls

// ✅ CORRECT: Scheduling uses Date.now()
const nextFlushTime = Date.now() + flushIntervalMs;

// ✅ CORRECT: File naming uses Date.now()
const filename = `traces-${Date.now()}.parquet`;
```

### Browser Implementation

```typescript
// ONE Date.now() captured at trace root (when Tracer.trace() creates TraceRoot)
// ONE performance.now() captured at trace root
// All subsequent timestamps use delta calculation

// TraceRoot - stored on root SpanBuffer, shared by all spans via reference
interface TraceRoot {
  readonly trace_id: TraceId;

  // Time anchor - flat primitives, not nested object
  readonly anchorEpochNanos: bigint; // BigInt(Date.now()) * 1_000_000n at trace root
  readonly anchorPerfNow: number; // performance.now() at trace root

  // Reference to tracer for lifecycle hooks
  readonly tracer: TracerLifecycleHooks;
}

// Created by Tracer.trace() before root span execution
function createTraceRoot(tracer: Tracer, traceId?: TraceId): TraceRoot {
  return {
    trace_id: traceId ?? generateTraceId(),
    anchorEpochNanos: BigInt(Date.now()) * 1_000_000n,
    anchorPerfNow: performance.now(),
    tracer,
  };
}

// All subsequent timestamps derived from anchor (accessed via buffer._traceRoot)
function getTimestamp(buffer: SpanBuffer): bigint {
  // Returns nanoseconds since epoch.
  // performance.now() resolution is ~microseconds, so the elapsed ms is floored to
  // microseconds → the last 3 nanosecond digits are always 000 (see traceRoot.es.ts).
  const { anchorEpochNanos, anchorPerfNow } = buffer._traceRoot;
  const elapsedMs = performance.now() - anchorPerfNow;
  return anchorEpochNanos + BigInt(Math.floor(elapsedMs * 1000)) * 1000n;
}
```

**Benefits**:

- No `Date.now()` calls per span - only `performance.now()` deltas
- Sub-millisecond precision for all spans within a trace
- Single anchor ensures all timestamps in trace are comparable

### Node.js Implementation

```typescript
// ONE Date.now() captured at trace root (when Tracer.trace() creates TraceRoot)
// ONE process.hrtime.bigint() captured at trace root
// All subsequent timestamps use hrtime delta

// TraceRoot - stored on root SpanBuffer, shared by all spans via reference
// (Same interface, but anchorPerfNow stores hrtime.bigint() result)
interface TraceRoot {
  readonly trace_id: TraceId;

  // Time anchor - flat primitives
  readonly anchorEpochNanos: bigint; // BigInt(Date.now()) * 1_000_000n at trace root
  readonly anchorPerfNow: number; // Number(process.hrtime.bigint()) at trace root

  // Reference to tracer for lifecycle hooks
  readonly tracer: TracerLifecycleHooks;
}

// Created by Tracer.trace() before root span execution
function createTraceRoot(tracer: Tracer, traceId?: TraceId): TraceRoot {
  return {
    trace_id: traceId ?? generateTraceId(),
    anchorEpochNanos: BigInt(Date.now()) * 1_000_000n,
    anchorPerfNow: Number(process.hrtime.bigint()),
    tracer,
  };
}

// All subsequent timestamps derived from anchor (accessed via buffer._traceRoot)
function getTimestamp(buffer: SpanBuffer): bigint {
  // Returns nanoseconds since epoch; hrtime.bigint() gives nanoseconds directly.
  // The realized class keeps the EXACT hrtime anchor as a BigInt (_anchorHrtimeBigInt),
  // not BigInt(Math.round(anchorPerfNow)) — the f64 anchorPerfNow loses integer
  // precision after ~104 days of uptime, which would corrupt the delta (traceRoot.node.ts).
  const { anchorEpochNanos } = buffer._traceRoot;
  const elapsedNanos = process.hrtime.bigint() - buffer._traceRoot._anchorHrtimeBigInt;
  return anchorEpochNanos + elapsedNanos;
}
```

**Benefits**:

- Nanosecond precision available from hrtime, stored directly as nanoseconds
- No `Date.now()` calls during span execution
- Single anchor ensures all timestamps in trace are comparable

### Arrow Timestamp Format

Timestamps are stored as nanoseconds since epoch in BigInt64Array during the hot path, then converted to Arrow's
TimestampNanosecond type during cold path conversion:

```typescript
// Hot path: BigInt64Array storage (nanoseconds since epoch)
buffer.timestamp[idx] = getTimestamp(ctx); // e.g., 1704067200000000000n

// Cold path: Arrow conversion
const arrowTimestamps = arrow.TimestampNanosecond.from(buffer.timestamp.subarray(0, buffer._writeIndex));
```

**Why Nanoseconds**:

- `BigInt64Array` provides full nanosecond precision without loss
- Sub-microsecond precision enables detailed performance analysis
- Compatible with ClickHouse's `DateTime64(9)` type
- Matches Arrow's native timestamp precision

### Why Flattened TraceRoot

The `TraceRoot` uses flat primitives instead of nested objects:

```typescript
// ✅ CORRECT: Flat primitives
interface TraceRoot {
  anchorEpochNanos: bigint;
  anchorPerfNow: number;
  // ...
}

// ❌ WRONG: Nested object
interface TraceRoot {
  timeAnchor: {
    epochNanos: bigint;
    perfNow: number;
  };
  // ...
}
```

**Benefits of Flat Structure**:

- Better V8 hidden class optimization
- One less pointer chase per timestamp read
- Simpler serialization if context needs to cross boundaries
- Matches the "flat deferred structure" design principle

## Platform-Specific Entry Points <a id="smoo/lmao!n/trace-root-timestamps.entry-points"></a>

The package provides separate entry points for optimal tree-shaking and platform-specific timestamp implementations:

### Node.js Entry Point

```typescript
// Import from /node for Node.js-specific optimizations
import { TestTracer, defineOpContext } from '@smoothbricks/lmao/node';
```

Uses `process.hrtime.bigint()` for true nanosecond precision timestamps.

### Browser/ES Entry Point

```typescript
// Default import for browsers/generic ES environments
import { TestTracer, defineOpContext } from '@smoothbricks/lmao';
```

Uses `performance.now()` with microsecond precision (~5-20μs resolution), converted to nanoseconds for storage.

### Why Separate Entry Points?

1. **Tree-shaking**: Node.js-specific code (e.g., `process.hrtime.bigint()`) doesn't bundle into browser builds
2. **Precision**: Node.js gets true nanosecond precision via hrtime, browsers get microsecond precision via
   performance.now()
3. **Compatibility**: Browser entry works in any ES environment (Deno, Cloudflare Workers, etc.)
4. **Zero overhead**: Implementation is set once at module load via `setTimestampNanosImpl()`, no runtime branching

### Implementation

Both entry points provide the same `ITraceRoot` API (`getTimestampNanos()`, anchors, span/log writes) with different
underlying implementations. The realized form is a **class per platform** plus a `createTraceRoot` factory; the factory
is what each entry point re-exports, so the unused platform class is tree-shaken (there is **no**
`setTimestampNanosImpl` module-load mutation — the factory is passed to the `Tracer` instead):

```typescript
// traceRoot.node.ts (Node.js - TRUE nanosecond precision)
class TraceRoot implements ITraceRoot {
  // anchors + trace_id live in _system ArrayBuffer; _anchorHrtimeBigInt kept as exact BigInt
  getTimestampNanos(): Nanoseconds {
    const currentHrtime = process.hrtime.bigint();
    const elapsedNanos = currentHrtime - this._anchorHrtimeBigInt; // exact BigInt delta
    return Nanoseconds.unsafe(this.anchorEpochNanos + elapsedNanos);
  }
}
export function createTraceRoot(trace_id: string, tracer: TracerLifecycleHooks): TraceRoot {
  const anchorEpochNanos = BigInt(Date.now()) * 1_000_000n;
  const anchorHrtimeBigInt = process.hrtime.bigint();
  return new TraceRoot(
    createTraceId(trace_id),
    anchorEpochNanos,
    Number(anchorHrtimeBigInt),
    anchorHrtimeBigInt,
    tracer
  );
}

// traceRoot.es.ts (Browser - microsecond precision, last 3 digits always 000)
class TraceRoot implements ITraceRoot {
  getTimestampNanos(): Nanoseconds {
    const elapsedMs = performance.now() - this.anchorPerfNow;
    const elapsedNanos = BigInt(Math.floor(elapsedMs * 1000)) * 1000n;
    return Nanoseconds.unsafe(this.anchorEpochNanos + elapsedNanos);
  }
}
export function createTraceRoot(trace_id: string, tracer: TracerLifecycleHooks): TraceRoot {
  const anchorEpochNanos = BigInt(Date.now()) * 1_000_000n;
  return new TraceRoot(createTraceId(trace_id), anchorEpochNanos, performance.now(), tracer);
}
```

**Key Details**:

- **Anchor captured once** at trace creation inside `createTraceRoot()` (the factory)
- All subsequent timestamps use delta from anchor for consistency
- Both implementations expose the same anchors (`anchorEpochNanos`, `anchorPerfNow`)
- The Node class additionally retains the **exact** `process.hrtime.bigint()` anchor as a BigInt (`_anchorHrtimeBigInt`)
  so deltas stay correct even after the f64 `anchorPerfNow` loses integer precision (~104 days of uptime); a regression
  test pins this. This supersedes a `BigInt(Math.round(anchorPerfNow))` approach, which would drift.
- Entry points (`node.ts`, `es.ts`) re-export the matching `createTraceRoot` factory; `Tracer` calls it
- No runtime platform detection — the platform is chosen at import time (which factory) for zero overhead
