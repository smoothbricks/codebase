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
// ONE Date.now() captured at trace root (TraceContext creation)
// ONE performance.now() captured at trace root
// All subsequent timestamps use delta calculation

// TraceContext system props (Extra props defined via .ctx<Extra>())
interface TraceContextSystem<FF> {
  trace_id: string;

  // Time anchor - flat primitives, not nested object
  anchorEpochMicros: number; // Date.now() * 1000 at trace root
  anchorPerfNow: number; // performance.now() at trace root

  // Thread/worker ID for distributed tracing
  thread_id: bigint;

  ff: FeatureFlagEvaluator<FF>;
  span: RootSpanFn;
}

// Full TraceContext = System + Extra (user-defined)
type TraceContext<FF, Extra> = TraceContextSystem<FF> & Extra;

// Internal implementation of module.traceContext()
function createTraceContext<FF, Extra>(params: { ff: FeatureFlagEvaluator<FF> } & Extra): TraceContext<FF, Extra> {
  const epochMs = Date.now();
  const perfNow = performance.now();

  return {
    trace_id: generateTraceId(),
    anchorEpochMicros: epochMs * 1000,
    anchorPerfNow: perfNow,
    thread_id: workerThreadId,
    ff: params.ff,
    span: createRootSpanFn(),
    ...params, // Spread Extra (e.g., env, requestId, userId)
  } as TraceContext<FF, Extra>;
}

// All subsequent timestamps derived from anchor
function getTimestamp(ctx: TraceContext): bigint {
  // Returns nanoseconds since epoch
  // performance.now() - anchorPerfNow gives elapsed ms with sub-ms precision
  return ctx.anchorEpochNanos + BigInt(Math.round((performance.now() - ctx.anchorPerfNow) * 1_000_000));
}
```

**Benefits**:

- No `Date.now()` calls per span - only `performance.now()` deltas
- Sub-millisecond precision for all spans within a trace
- Single anchor ensures all timestamps in trace are comparable

### Node.js Implementation

```typescript
// ONE Date.now() captured at trace root
// ONE process.hrtime.bigint() captured at trace root
// All subsequent timestamps use hrtime delta

// TraceContext system props (Extra props defined via .ctx<Extra>())
interface TraceContextSystem<FF> {
  trace_id: string;

  // Time anchor - flat primitives
  anchorEpochMicros: number; // Date.now() * 1000 at trace root
  anchorHrTime: bigint; // process.hrtime.bigint() at trace root

  // Thread/worker ID for distributed tracing
  thread_id: bigint;

  ff: FeatureFlagEvaluator<FF>;
  span: RootSpanFn;
}

// Full TraceContext = System + Extra (user-defined)
type TraceContext<FF, Extra> = TraceContextSystem<FF> & Extra;

// Internal implementation of module.traceContext()
function createTraceContext<FF, Extra>(params: { ff: FeatureFlagEvaluator<FF> } & Extra): TraceContext<FF, Extra> {
  const epochMs = Date.now();
  const hrTime = process.hrtime.bigint();

  return {
    trace_id: generateTraceId(),
    anchorEpochMicros: epochMs * 1000,
    anchorHrTime: hrTime,
    thread_id: workerThreadId,
    ff: params.ff,
    span: createRootSpanFn(),
    ...params, // Spread Extra (e.g., env, requestId, userId)
  } as TraceContext<FF, Extra>;
}

// All subsequent timestamps derived from anchor
function getTimestamp(ctx: TraceContext): bigint {
  // Returns nanoseconds since epoch
  // hrtime.bigint() gives nanoseconds directly
  const elapsedNanos = process.hrtime.bigint() - ctx.anchorHrTime;
  return ctx.anchorEpochNanos + elapsedNanos;
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

### Why Flattened TraceContext

The `TraceContext` uses flat primitives instead of nested objects:

```typescript
// ✅ CORRECT: Flat primitives
interface TraceContext {
  anchorEpochMicros: number;
  anchorPerfNow: number;
  // ...
}

// ❌ WRONG: Nested object
interface TraceContext {
  timeAnchor: {
    epochMicros: number;
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
