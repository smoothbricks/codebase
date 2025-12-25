# High-Precision Timestamp System

> **Part of [Columnar Buffer Architecture](./01b_columnar_buffer_architecture.md)**
>
> This document details the high-precision timestamp design that provides sub-millisecond accuracy with minimal
> overhead.

The trace logging system uses a high-precision timestamp design that provides sub-millisecond accuracy while minimizing
overhead. The design captures a single time anchor at trace root creation, then uses high-resolution timers for all
subsequent timestamps.

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
- ✅ **ALWAYS use anchored approach**: `getTimestampMicros(anchorEpochMicros, anchorPerfNow)`
- Why: Anchored timestamps provide sub-millisecond precision and avoid repeated system calls

**For Scheduling/Background Tasks** (Non-Performance-Critical):

- ✅ **OK to use `Date.now()` for scheduling**: Background flush intervals, timeout calculations
- ✅ **OK for file naming**: `traces-${Date.now()}.parquet`
- ✅ **OK for logging system metadata**: Capacity tuning events, system diagnostics
- Why: Millisecond precision acceptable, system clock alignment desired

**Example**:

```typescript
// ✅ CORRECT: Trace timestamps use anchored approach
buffer.timestamp[idx] = getTimestampMicros(ctx.anchorEpochMicros, ctx.anchorPerfNow);

// ❌ WRONG: Don't use Date.now() for trace timestamps
buffer.timestamp[idx] = Date.now() * 1000; // No sub-ms precision, repeated syscalls

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
  // Returns nanoseconds since epoch
  // performance.now() - anchorPerfNow gives elapsed ms with sub-ms precision
  const { anchorEpochNanos, anchorPerfNow } = buffer._traceRoot;
  return anchorEpochNanos + BigInt(Math.round((performance.now() - anchorPerfNow) * 1_000_000));
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
  // Returns nanoseconds since epoch
  // hrtime.bigint() gives nanoseconds directly
  const { anchorEpochNanos, anchorPerfNow } = buffer._traceRoot;
  const anchorHrtime = BigInt(Math.round(anchorPerfNow));
  const elapsedNanos = process.hrtime.bigint() - anchorHrtime;
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

## Platform-Specific Entry Points

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

Both entry points provide the same API but with different underlying implementations:

```typescript
// timestamp.node.ts (Node.js - TRUE nanosecond precision)
export function getTimestampNanos(anchorEpochNanos: bigint, anchorPerfNow: number): Nanoseconds {
  const anchorHrtime = BigInt(Math.round(anchorPerfNow));
  const currentHrtime = process.hrtime.bigint();
  const elapsedNanos = currentHrtime - anchorHrtime;
  return Nanoseconds.unsafe(anchorEpochNanos + elapsedNanos);
}

export function createTimestampAnchor(): { anchorEpochNanos: bigint; anchorPerfNow: number } {
  return {
    anchorEpochNanos: BigInt(Date.now()) * 1_000_000n,
    anchorPerfNow: Number(process.hrtime.bigint()),
  };
}

// timestamp.ts (Browser - microsecond precision, last 3 digits always 000)
export function getTimestampNanos(anchorEpochNanos: bigint, anchorPerfNow: number): Nanoseconds {
  const elapsedMs = performance.now() - anchorPerfNow;
  const elapsedNanos = BigInt(Math.floor(elapsedMs * 1000)) * 1000n;
  return (anchorEpochNanos + elapsedNanos) as Nanoseconds;
}

export function createTimestampAnchor(): { anchorEpochNanos: bigint; anchorPerfNow: number } {
  return {
    anchorEpochNanos: BigInt(Date.now()) * 1_000_000n,
    anchorPerfNow: performance.now(),
  };
}
```

**Key Details**:

- **Anchor captured once** at trace creation via `createTimestampAnchor()`
- All subsequent timestamps use delta from anchor for consistency
- Both implementations use the same anchor structure (`anchorEpochNanos`, `anchorPerfNow`)
- Entry points (`node.ts`, `es.ts`) call `setTimestampNanosImpl()` before re-exporting main functionality
- No runtime platform detection - implementation chosen at import time for zero overhead
