# Buffer Self-Tuning

> **📚 PART OF COLUMNAR BUFFER ARCHITECTURE**
>
> This document details the zero-config memory management that makes columnar buffers "just work" in any environment.
> Read the [main overview](./01b_columnar_buffer_architecture.md) first.

## WHY: Developers Shouldn't Configure Memory

Traditional logging requires configuration:

- Buffer sizes
- Flush intervals
- Memory limits
- Growth strategies

This is wrong. The system should adapt automatically.

**Key Insight**: Self-tuning is part of the trace logging system, not a general-purpose buffer library. The tuning
mechanism has clear metrics (overflow rate, utilization) from the specific use case of span logging. Buffer chaining
handles overflow gracefully while the system learns optimal capacity per module. The op's internal wrapper creates
SpanBuffers, enabling per-op tuning.

### Self-Tuning Benefits

1. **Zero Configuration** - Works out of the box
2. **Adaptive Performance** - Adjusts to workload
3. **Memory Safety** - Never causes OOM
4. **Optimal Throughput** - Balances memory vs performance
5. **Environment Agnostic** - Same code everywhere
6. **Per-Span Buffers** - Each span gets its own buffer (created by op wrapper), avoiding traceId/spanId TypedArrays
7. **Buffer Chaining** - Graceful overflow handling while learning optimal capacity
8. **Freelist Pooling** - Pooling buffers for V8 GC optimization (future experiment, not yet implemented)
9. **Lazy-to-Eager Column Promotion** - Frequently-used columns automatically pre-allocate (see
   [Column Promotion](#lazy-to-eager-column-promotion))

## Self-Tuning Architecture

### Core Principle: Observe and Adapt

```typescript
interface SelfTuningBuffer {
  // Current capacity adapts to usage
  capacity: number;

  // Track usage patterns
  stats: {
    writesPerSecond: number;
    averageBatchSize: number;
    growthCount: number;
    compactionCount: number;
    lastFlushTime: number;
  };

  // Adaptive thresholds
  thresholds: {
    growthFactor: number; // How much to grow
    compactionRatio: number; // When to shrink
    maxCapacity: number; // Memory limit
  };
}
```

## Growth Strategy

### Exponential Growth with Damping

Start small, grow fast, then slow down:

```typescript
class AdaptiveGrowth {
  private growthHistory: number[] = [];

  calculateNewCapacity(current: number, needed: number): number {
    // Minimum growth to satisfy immediate need
    const minCapacity = Math.max(needed, current * 1.5);

    // Adaptive growth factor based on history
    const growthFactor = this.getAdaptiveGrowthFactor();
    const targetCapacity = current * growthFactor;

    // Apply capacity based on environment
    const maxCapacity = this.getEnvironmentMaxCapacity();

    return Math.min(Math.max(minCapacity, targetCapacity), maxCapacity);
  }

  private getAdaptiveGrowthFactor(): number {
    const recentGrowths = this.growthHistory.slice(-5);

    if (recentGrowths.length === 0) {
      return 2.0; // Initial aggressive growth
    }

    // Slow down if growing frequently
    const growthFrequency = recentGrowths.length / 5;
    return Math.max(1.5, 2.0 - growthFrequency * 0.5);
  }

  private getEnvironmentMaxCapacity(): number {
    // Browser environment
    if (typeof window !== 'undefined') {
      // Estimate available memory (conservative)
      const estimatedMemory = (navigator as any).deviceMemory || 4; // GB
      return Math.floor(estimatedMemory * 1024 * 1024 * 0.1); // 10% of RAM
    }

    // Node.js environment
    if (typeof process !== 'undefined') {
      const maxHeap = require('v8').getHeapStatistics().heap_size_limit;
      return Math.floor(maxHeap * 0.1); // 10% of heap
    }

    // Default: 100MB worth of entries
    return 1000000;
  }
}
```

### Smart Pre-allocation

Predict future needs:

```typescript
class PredictiveBuffer {
  private writeRateHistory: number[] = [];
  private lastMeasurement = Date.now();
  private writesInPeriod = 0;

  write(entry: any) {
    this.writesInPeriod++;

    // Update write rate every second
    const now = Date.now();
    if (now - this.lastMeasurement > 1000) {
      const writeRate = this.writesInPeriod / ((now - this.lastMeasurement) / 1000);
      this.writeRateHistory.push(writeRate);

      // Keep last 60 seconds
      if (this.writeRateHistory.length > 60) {
        this.writeRateHistory.shift();
      }

      this.writesInPeriod = 0;
      this.lastMeasurement = now;

      // Predictive growth
      this.checkPredictiveGrowth();
    }

    // Normal write logic...
  }

  private checkPredictiveGrowth() {
    if (this.writeRateHistory.length < 5) return;

    // Calculate trend
    const recentRate = this.writeRateHistory.slice(-5).reduce((a, b) => a + b, 0) / 5;
    const olderRate = this.writeRateHistory.slice(-10, -5).reduce((a, b) => a + b, 0) / 5;

    // If write rate is increasing
    if (recentRate > olderRate * 1.2) {
      const projectedWrites = recentRate * 10; // Next 10 seconds
      const neededCapacity = this.writeIndex + projectedWrites;

      if (neededCapacity > this.capacity * 0.8) {
        this.grow(neededCapacity);
      }
    }
  }
}
```

## Compaction Strategy

### Automatic Shrinking

Free memory when not needed:

```typescript
class CompactingBuffer {
  private lastCompaction = Date.now();
  private highWaterMark = 0;

  compact() {
    const now = Date.now();
    const timeSinceCompaction = now - this.lastCompaction;

    // Don't compact too frequently
    if (timeSinceCompaction < 60000) return; // 1 minute minimum

    const utilization = this.writeIndex / this.capacity;
    const shouldCompact =
      utilization < 0.25 && // Less than 25% used
      this.capacity > this.initialCapacity * 4 && // Grown significantly
      this.highWaterMark < this.capacity * 0.5; // Never used >50%

    if (shouldCompact) {
      this.performCompaction();
    }

    this.lastCompaction = now;
  }

  private performCompaction() {
    // Calculate new size
    const newCapacity = Math.max(
      this.initialCapacity,
      this.highWaterMark * 1.5, // 50% headroom
      alignToCache(this.writeIndex * 2)
    );

    // Create new arrays
    const newArrays = this.createArrays(newCapacity);

    // Copy active data
    for (let i = 0; i < this.writeIndex; i++) {
      newArrays.timestamps[i] = this.timestamps[i];
      newArrays.operations[i] = this.operations[i];
      // ... copy other columns
    }

    // Swap arrays
    Object.assign(this, newArrays);
    this.capacity = newCapacity;

    // Reset high water mark
    this.highWaterMark = this.writeIndex;
  }
}
```

### Memory Pressure Response

React to system memory pressure:

```typescript
class MemoryAwareBuffer {
  constructor() {
    this.setupMemoryMonitoring();
  }

  private setupMemoryMonitoring() {
    // Browser: PerformanceObserver
    if (typeof PerformanceObserver !== 'undefined') {
      const observer = new PerformanceObserver((list) => {
        for (const entry of list.getEntries()) {
          if (entry.entryType === 'measure' && entry.name === 'memory') {
            this.handleMemoryPressure(entry);
          }
        }
      });

      observer.observe({ entryTypes: ['measure'] });
    }

    // Node.js: Monitor heap usage
    if (typeof process !== 'undefined') {
      setInterval(() => {
        const usage = process.memoryUsage();
        const heapUsed = usage.heapUsed / usage.heapTotal;

        if (heapUsed > 0.8) {
          this.handleMemoryPressure({ level: 'high' });
        } else if (heapUsed > 0.6) {
          this.handleMemoryPressure({ level: 'moderate' });
        }
      }, 5000);
    }
  }

  private handleMemoryPressure(event: any) {
    if (event.level === 'high') {
      // Aggressive compaction
      this.forceCompact();

      // Reduce growth factor
      this.growthFactor = Math.max(1.2, this.growthFactor * 0.8);

      // Flush more frequently
      this.autoFlush();
    } else if (event.level === 'moderate') {
      // Normal compaction
      this.compact();
    }
  }
}
```

## Flush Strategy

### FlushScheduler Design

The `FlushScheduler` manages background flushing of multiple root buffers (one per HTTP request/trace) to Arrow tables.

**Key Design Decisions**:

1. **Array-Based Buffer Collection**: Uses `SpanBuffer[]` array for buffer collection. Buffers are added via
   `register(buffer)` and the array is cleared after each flush.

2. **No Individual Unregister**: The entire array is cleared after flush (`this.buffers = []`). This matches the flush
   cycle - buffers are registered, flushed together, then cleared.

3. **Single RecordBatch Per Flush**: All root buffers in a flush are converted to a **single RecordBatch** for maximum
   dictionary reuse. This requires all buffers to share the same schema (enforced at runtime).

4. **Schema Requirement**: All buffers in a flush must share the same schema. This is guaranteed because:
   - The application composes all library schemas into a single `ModuleContext` at startup
   - All buffers created from that module share the same `module.logSchema` schema
   - The conversion function validates this requirement and throws if schemas differ

**Flush Cycle**:

```typescript
class FlushScheduler {
  private buffers: SpanBuffer[] = [];

  register(buffer: SpanBuffer): void {
    this.buffers.push(buffer);
    // Start scheduler if not running
  }

  // Buffers cleared after flush

  private async doFlush(): Promise<void> {
    if (this.buffers.length === 0) return;

    const buffersToFlush = this.buffers;
    this.buffers = []; // Clear after collecting

    // Convert all buffers to single RecordBatch
    const table = convertSpanTreeToArrowTable(
      buffersToFlush, // Array of root buffers
      undefined,
      modulesToLogStats
    );

    // Write table to storage
    await this.flushHandler(table, metadata);
  }
}
```

### Adaptive Flushing

Balance latency vs memory usage. Thresholds are configurable via `FlushSchedulerConfig`:

```typescript
interface FlushSchedulerConfig {
  /** Flush when buffer reaches this capacity ratio (default: 0.8) */
  capacityThreshold: number;
  /** Maximum interval between flushes in ms (default: 10000) */
  maxIntervalMs: number;
  /** Minimum interval between flushes in ms (default: 1000) */
  minIntervalMs: number;
  /** Flush after this many ms of inactivity (default: 5000) */
  idleTimeoutMs: number;
}

const DEFAULT_CONFIG: FlushSchedulerConfig = {
  capacityThreshold: 0.8,
  maxIntervalMs: 10000,
  minIntervalMs: 1000,
  idleTimeoutMs: 5000,
};

class FlushScheduler {
  private config: FlushSchedulerConfig;
  private flushTimer?: NodeJS.Timer;
  private lastFlushTime = Date.now();
  private flushInterval: number;

  constructor(config: Partial<FlushSchedulerConfig> = {}) {
    this.config = { ...DEFAULT_CONFIG, ...config };
    this.flushInterval = this.config.minIntervalMs;
  }

  private checkFlush() {
    const reasons = this.getFlushReasons();

    if (reasons.length > 0) {
      this.flush(reasons);
      this.adaptFlushInterval(reasons);
    }
  }

  private getFlushReasons(): string[] {
    const reasons: string[] = [];
    const now = Date.now();

    // Size-based flush
    if (this.writeIndex > this.capacity * this.config.capacityThreshold) {
      reasons.push('capacity');
    }

    // Time-based flush
    if (now - this.lastFlushTime > this.flushInterval) {
      reasons.push('interval');
    }

    // Memory pressure flush
    if (this.isMemoryPressure()) {
      reasons.push('memory');
    }

    // Idle flush (no writes for a while)
    if (this.writeIndex > 0 && now - this.lastWriteTime > this.config.idleTimeoutMs) {
      reasons.push('idle');
    }

    return reasons;
  }

  private adaptFlushInterval(reasons: string[]) {
    // Frequent capacity flushes = need larger buffer or faster flushing
    if (reasons.includes('capacity')) {
      this.flushInterval = Math.max(this.config.minIntervalMs, this.flushInterval * 0.8);
    }

    // Idle flushes = can flush less frequently
    else if (reasons.includes('idle')) {
      this.flushInterval = Math.min(this.config.maxIntervalMs, this.flushInterval * 1.2);
    }
  }
}
```

**Note**: These thresholds are sensible defaults that work for most workloads. Configuration is available for advanced
use cases (e.g., low-latency requirements, memory-constrained environments) but is not required for normal operation.

## Capacity Constraints

### Multiple of 8 Requirement

**All buffer capacities MUST be multiples of 8.** This constraint enables efficient null bitmap operations:

1. **Byte-aligned concatenation**: When converting multiple buffers to Arrow, null bitmaps can be bulk-copied with
   `TypedArray.set()` instead of bit-by-bit loops
2. **Efficient clearing**: Clearing null bits for a buffer's rows uses `TypedArray.fill(0)` for full bytes
3. **Simple offset math**: `rowOffset / 8` gives exact byte offset, no bit shifting needed at boundaries

The constraint is enforced in `createSpanBuffer()` and `createNextBuffer()`:

```typescript
// Align capacity to multiple of 8
const alignedCapacity = (requestedCapacity + 7) & ~7;
```

Since self-tuning uses powers of 2 (8, 16, 32, 64, 128, 256, 512, 1024), this constraint is naturally satisfied. The
explicit alignment ensures correctness for any custom capacity input.

## Initial Capacity Selection

### Environment-Based Defaults

```typescript
function getInitialCapacity(): number {
  // Browser: Start small
  if (typeof window !== 'undefined') {
    // Mobile device
    if (window.innerWidth < 768) {
      return 64; // Minimal memory
    }

    // Desktop
    return 256;
  }

  // Node.js: Check available memory
  if (typeof process !== 'undefined') {
    const totalMemory = require('os').totalmem();

    // Server with lots of RAM
    if (totalMemory > 8 * 1024 * 1024 * 1024) {
      return 1024;
    }

    // Modest server
    return 512;
  }

  // Edge workers, etc
  return 128;
}
```

### Workload Detection

Adapt to usage patterns:

```typescript
class WorkloadAdaptiveBuffer {
  private pattern: 'bursty' | 'steady' | 'sparse' = 'steady';

  detectPattern() {
    const writeRates = this.writeRateHistory.slice(-30);
    if (writeRates.length < 10) return;

    // Calculate variance
    const mean = writeRates.reduce((a, b) => a + b, 0) / writeRates.length;
    const variance = writeRates.reduce((sum, rate) => sum + Math.pow(rate - mean, 2), 0) / writeRates.length;

    const cv = Math.sqrt(variance) / mean; // Coefficient of variation

    // High variance = bursty
    if (cv > 1.0) {
      this.pattern = 'bursty';
      this.growthFactor = 3.0; // Aggressive growth
      this.compactionThreshold = 0.1; // Keep extra capacity
    }

    // Low variance = steady
    else if (cv < 0.3) {
      this.pattern = 'steady';
      this.growthFactor = 1.5; // Conservative growth
      this.compactionThreshold = 0.3; // Reclaim memory sooner
    }

    // Very low usage = sparse
    else if (mean < 10) {
      this.pattern = 'sparse';
      this.growthFactor = 1.2; // Minimal growth
      this.compactionThreshold = 0.5; // Aggressive compaction
    }
  }
}
```

## Capacity Coordination

### Multi-Buffer Coordination

When using multiple buffers:

```typescript
class BufferCoordinator {
  private buffers: Set<SelfTuningBuffer> = new Set();
  private globalMemoryLimit: number;

  register(buffer: SelfTuningBuffer) {
    this.buffers.add(buffer);
    buffer.coordinator = this;
  }

  requestCapacity(buffer: SelfTuningBuffer, requested: number): number {
    // Calculate total memory usage
    const totalUsed = Array.from(this.buffers).reduce((sum, b) => sum + b.memoryUsage(), 0);

    const available = this.globalMemoryLimit - totalUsed;

    // If plenty of memory, approve
    if (requested < available * 0.5) {
      return requested;
    }

    // If tight on memory, coordinate
    this.rebalanceCapacity();

    // Give what we can
    return Math.min(requested, available * 0.8);
  }

  private rebalanceCapacity() {
    // Find underutilized buffers
    const underutilized = Array.from(this.buffers)
      .filter((b) => b.utilization() < 0.3)
      .sort((a, b) => a.utilization() - b.utilization());

    // Reduce capacity for underutilized modules (affects future buffers)
    for (const buffer of underutilized) {
      buffer.reduceNextCapacity();
    }
  }
}
```

## Performance Monitoring

### Self-Tuning Metrics

```typescript
interface TuningMetrics {
  // Capacity metrics
  capacityHistory: Array<{
    timestamp: number;
    capacity: number;
    used: number;
    reason: string;
  }>;

  // Performance metrics
  writeLatencies: CircularBuffer<number>;
  flushLatencies: CircularBuffer<number>;

  // Efficiency metrics
  memoryEfficiency: number; // Used / allocated
  growthEfficiency: number; // Useful grows / total grows

  // Pattern detection
  workloadPattern: 'bursty' | 'steady' | 'sparse';
  predictedCapacityNeeds: number;
}

class MonitoredBuffer extends SelfTuningBuffer {
  private metrics: TuningMetrics = {
    capacityHistory: [],
    writeLatencies: new CircularBuffer(1000),
    flushLatencies: new CircularBuffer(100),
    memoryEfficiency: 0,
    growthEfficiency: 0,
    workloadPattern: 'steady',
    predictedCapacityNeeds: 0,
  };

  write(entry: any) {
    const start = performance.now();

    super.write(entry);

    const latency = performance.now() - start;
    this.metrics.writeLatencies.add(latency);

    this.updateEfficiencyMetrics();
  }

  private updateEfficiencyMetrics() {
    // Memory efficiency
    this.metrics.memoryEfficiency = this.writeIndex / this.capacity;

    // Growth efficiency (did we grow at the right time?)
    const recentGrowths = this.metrics.capacityHistory.filter((h) => h.reason === 'growth').slice(-10);

    const usefulGrowths = recentGrowths.filter((h) => {
      // Growth was useful if we used >80% within next period
      const nextEntries = this.metrics.capacityHistory.filter((e) => e.timestamp > h.timestamp).slice(0, 5);

      return nextEntries.some((e) => e.used > h.capacity * 0.8);
    });

    this.metrics.growthEfficiency = usefulGrowths.length / Math.max(1, recentGrowths.length);
  }

  getMetrics(): TuningMetrics {
    return { ...this.metrics };
  }
}
```

## Implementation Example

Complete self-tuning buffer:

```typescript
class SelfTuningSpanBuffer {
  // Initial small capacity
  private capacity = getInitialCapacity();

  // Typed arrays
  private timestamps: BigInt64Array;
  private operations: Uint8Array;
  private attributes: Map<string, Uint32Array> = new Map();

  // Write position
  private writeIndex = 0;

  // Self-tuning state
  private stats = {
    writes: 0,
    flushes: 0,
    grows: 0,
    compactions: 0,
    lastWriteTime: Date.now(),
    lastFlushTime: Date.now(),
  };

  constructor() {
    this.allocateArrays();
    this.startMonitoring();
  }

  write(timestamp: number, operation: number, attrs: Record<string, number>) {
    const idx = this.writeIndex++;

    // Hot path: just writes
    this.timestamps[idx] = timestamp;
    this.operations[idx] = operation;

    for (const [key, value] of Object.entries(attrs)) {
      const array = this.attributes.get(key);
      if (array) array[idx] = value;
    }

    this.stats.writes++;
    this.stats.lastWriteTime = Date.now();

    // Check if action needed
    if (idx >= this.capacity - 1) {
      this.grow();
    } else if (idx % 1000 === 0) {
      this.checkCompaction();
    }
  }

  private grow() {
    const newCapacity = this.calculateNewCapacity();

    // Allocate new arrays
    const newTimestamps = new BigInt64Array(newCapacity);
    const newOperations = new Uint8Array(newCapacity);

    // Copy data
    newTimestamps.set(this.timestamps);
    newOperations.set(this.operations);

    // Copy attribute arrays
    for (const [key, array] of this.attributes) {
      const newArray = new Uint32Array(newCapacity);
      newArray.set(array);
      this.attributes.set(key, newArray);
    }

    // Swap
    this.timestamps = newTimestamps;
    this.operations = newOperations;
    this.capacity = newCapacity;

    this.stats.grows++;
  }

  private calculateNewCapacity(): number {
    // Recent write rate
    const timeDiff = Date.now() - this.stats.lastFlushTime;
    const writeRate = this.stats.writes / (timeDiff / 1000);

    // Predict next period needs
    const predictedWrites = writeRate * 5; // 5 seconds

    // Growth factor based on history
    const growthFactor = this.stats.grows < 3 ? 2.0 : 1.5;

    return Math.min(Math.max(this.capacity * growthFactor, this.writeIndex + predictedWrites), this.getMaxCapacity());
  }

  // Auto-flush when needed
  private checkAutoFlush() {
    const shouldFlush =
      this.writeIndex > this.capacity * 0.8 || Date.now() - this.stats.lastFlushTime > 1000 || this.isMemoryPressure();

    if (shouldFlush) {
      this.flush();
    }
  }

  flush(): ArrowBatch {
    const batch = this.toArrow();

    // Reset for next batch
    this.writeIndex = 0;
    this.stats.lastFlushTime = Date.now();
    this.stats.flushes++;

    // Maybe compact
    this.checkCompaction();

    return batch;
  }
}
```

## Testing Self-Tuning

```typescript
describe('Self-tuning buffer', () => {
  test('starts small, grows as needed', () => {
    const buffer = new SelfTuningSpanBuffer();

    expect(buffer.capacity).toBeLessThan(1000);

    // Write a lot
    for (let i = 0; i < 10000; i++) {
      buffer.write(Date.now(), 1, { userId: i });
    }

    expect(buffer.capacity).toBeGreaterThan(10000);
  });

  test('compacts when underutilized', async () => {
    const buffer = new SelfTuningSpanBuffer();

    // Force growth
    for (let i = 0; i < 5000; i++) {
      buffer.write(Date.now(), 1, { userId: i });
    }

    const largeCapacity = buffer.capacity;

    // Flush and write less
    buffer.flush();

    for (let i = 0; i < 100; i++) {
      buffer.write(Date.now(), 1, { userId: i });
    }

    // Wait for compaction
    await new Promise((resolve) => setTimeout(resolve, 61000));

    expect(buffer.capacity).toBeLessThan(largeCapacity);
  });

  test('adapts to workload pattern', () => {
    const buffer = new SelfTuningSpanBuffer();

    // Simulate bursty workload
    for (let burst = 0; burst < 10; burst++) {
      // Burst
      for (let i = 0; i < 1000; i++) {
        buffer.write(Date.now(), 1, { userId: i });
      }

      // Quiet period
      jest.advanceTimersByTime(5000);
    }

    expect(buffer.getMetrics().workloadPattern).toBe('bursty');
  });
});
```

## Lazy-to-Eager Column Promotion

### Overview

Beyond capacity self-tuning, the system also adapts column access patterns. Columns start as lazy (memory-efficient) and
automatically promote to eager (performance-optimized) when heavily used.

**Key Insight**: Promotion happens during background flush (cold path), ensuring zero hot-path impact while optimizing
for actual usage patterns.

**Ownership**: This is LMAO's concern, not arrow-builder's. Arrow-builder provides the codegen infrastructure to support
both eager and lazy columns, but LMAO owns the tracking, promotion logic, and class recompilation.

### Stats Object Design

Stats are tracked via a single `stats` object passed to `generateColumnBufferClass()` and captured in the generated
class closure. This design minimizes closure size while enabling direct property access.

```typescript
// Stats object shape - one per schema, created at module initialization
// Uses fixed shape (not a Map) for V8 hidden class optimization
interface LazyColumnStats {
  _totalSpanBuffersCreated: number; // Prefixed with _ to avoid collision with user column names
  userId: { instantiationCount: number }; // One property per lazy column
  requestId: { instantiationCount: number };
  // ... one property per lazy column in schema
}
```

**Why this design:**

1. **Single closure reference**: The generated class captures one `stats` object, not N per-column references. The
   closure already contains only `helpers` + extension dependencies - keeping it minimal matters for V8.

2. **Direct property access**: `stats.userId.instantiationCount++` is a direct property access on a fixed-shape object,
   V8 hidden class optimized.

3. **No Map lookups**: Fixed object shape means no `Map.get()` in hot path.

4. **Underscore prefix**: `_totalSpanBuffersCreated` prevents collision with user column names like
   `totalSpanBuffersCreated`.

### Stats Injection via Closure

The stats object is passed to `generateColumnBufferClass()` via `extension.dependencies` and captured in the generated
class closure:

```typescript
// At module initialization (cold path)
const stats: LazyColumnStats = {
  _totalSpanBuffersCreated: 0,
  userId: { instantiationCount: 0 },
  requestId: { instantiationCount: 0 },
};

// Pass to class generator
const SpanBufferClass = getColumnBufferClass(schema, {
  dependencies: { stats },
  // ... other extension options
});
```

The generated constructor increments `stats._totalSpanBuffersCreated++`, and each lazy getter increments
`stats.{columnName}.instantiationCount++` on first allocation.

### Generated Code

```typescript
// Generated constructor (stats captured in closure)
constructor(requestedCapacity) {
  // ... system columns ...
  stats._totalSpanBuffersCreated++;
  // ... rest of initialization ...
}

// Generated lazy getter (stats captured in closure)
get userId_nulls() {
  let v = this._userId_nulls;
  if (v === undefined) {
    stats.userId.instantiationCount++;  // Direct property access, no lookup
    // ... allocation code ...
  }
  return v;
}
```

**Hot path cost**: One integer increment in constructor, one integer increment on first allocation of each lazy column.
Both are direct property access on the closure-captured `stats` object.

### Promotion Ratio Calculation

To decide if a column should promote to eager:

```typescript
const ratio = stats.userId.instantiationCount / stats._totalSpanBuffersCreated;
if (ratio >= 0.8 && stats._totalSpanBuffersCreated >= 100) {
  // Promote userId to eager
}
```

The `>= 100` sample threshold prevents premature promotion based on small sample sizes.

### Promotion Criteria

A lazy column is promoted to eager when **ALL** of the following are true:

1. **Statistical Significance**: `totalSpanBuffersCreated >= 100`
   - Ensures we have enough data to make a confident decision
   - Prevents premature promotion on small samples

2. **High Usage Ratio**: `instantiationRatio >= 0.80`
   - Column is used in 80% or more of spans
   - Indicates the getter overhead outweighs memory savings

3. **Not Already Promoted**: `!isEager`
   - Prevents redundant recompilation

### Recompilation via `new Function()`

When promotion criteria are met, the system recompiles the SpanBuffer class with promoted columns as eager. The new
`stats` object is passed via `extension.dependencies`, and the generated class captures it in its closure.

```typescript
// Find columns to promote based on stats
const eagerColumns: string[] = [];
for (const columnName of schemaFields) {
  const ratio = stats[columnName].instantiationCount / stats._totalSpanBuffersCreated;
  if (ratio >= 0.8) {
    eagerColumns.push(columnName);
  }
}

// Create fresh stats for new class (reset counters)
const newStats: LazyColumnStats = {
  _totalSpanBuffersCreated: 0,
  // ... one property per remaining lazy column
};

// Generate new class with promoted columns eager, new stats in closure
const newSpanBufferClass = getColumnBufferClass(schema, {
  eagerColumns,
  dependencies: { stats: newStats },
});

// Atomic replacement
module.SpanBufferClass = newSpanBufferClass;
module.lazyColumnStats = newStats;
```

The new class is generated with promoted columns as eager properties (allocated in constructor) rather than lazy
getters.

### Arrow-Builder Codegen

The `generateColumnBufferClass()` function already supports eager vs lazy columns via the schema's `__eager` flag. When
a column is marked eager, it's allocated in the constructor. When lazy, it uses a getter that allocates on first access.

For promotion tracking, the generated code references `stats` from the closure:

```typescript
// Generated constructor
constructor(requestedCapacity) {
  // ... system columns ...
  stats._totalSpanBuffersCreated++;  // stats from closure
  // ... lazy column initialization (this._col_nulls = undefined) ...
}

// Generated lazy getter
get userId_nulls() {
  let v = this._userId_nulls;
  if (v === undefined) {
    stats.userId.instantiationCount++;  // stats from closure
    const cap = this._alignedCapacity;
    // ... allocation code ...
  }
  return v;
}
```

The `stats` object is passed via `extension.dependencies` and becomes available in the generated class closure.

### In-Flight Buffer Handling

The system handles the transition gracefully:

- **Old buffers** continue using the original class with the original lazy/eager pattern
- **New buffers** instantiate from the recompiled class with promoted columns as eager
- **Natural convergence** occurs as old buffers flush and new ones are created

No migration or data copying is needed. The system simply creates new SpanBuffer instances using the updated class.

### Performance Trade-offs

| Aspect             | Before (Lazy)             | After (Eager)          | Impact                 |
| ------------------ | ------------------------- | ---------------------- | ---------------------- |
| Memory per buffer  | Optimal (no allocation)   | +256-512 bytes         | Minimal for 80%+ usage |
| Hot path access    | Getter overhead (~5-10ns) | Direct property (~1ns) | 5-10x faster           |
| Best for           | <80% column usage         | ≥80% column usage      | Auto-optimizes         |
| Recompilation cost | N/A                       | One-time, cold path    | Zero hot-path impact   |

### Example: Promotion in Action

```typescript
// Initial state: All columns lazy
const module = createModuleContext({
  moduleMetadata: { gitSha: 'abc', packageName: '@mycompany/app', packagePath: 'src/user.ts' },
  logSchema: {
    userId: S.category(),
    requestId: S.category(),
    sessionId: S.category(),
    experimentId: S.category(),
  },
});

// After 100 spans:
// - userId: accessed in 95 buffers (95% ratio) → Promotes to eager
// - requestId: accessed in 90 buffers (90% ratio) → Promotes to eager
// - sessionId: accessed in 45 buffers (45% ratio) → Stays lazy
// - experimentId: accessed in 10 buffers (10% ratio) → Stays lazy

// Trigger promotion check (happens automatically during backgroundFlush)
backgroundFlush(module);

// New SpanBuffer class generated with:
// - userId: eager (allocated in constructor)
// - requestId: eager (allocated in constructor)
// - sessionId: lazy (getter with symbol-based allocation)
// - experimentId: lazy (getter with symbol-based allocation)
```

### Testing Promotion

```typescript
import { describe, expect, it } from 'bun:test';
import { createModuleContext } from '../lmao.js';
import { S } from '../schema/builder.js';

describe('Lazy-to-eager column promotion', () => {
  it('promotes high-usage columns to eager after 100 buffers', () => {
    const module = createModuleContext({
      moduleMetadata: { gitSha: 'test', packageName: '@test/app', packagePath: 'src/test.ts' },
      logSchema: {
        userId: S.category(),
        sessionId: S.category(),
      },
    });

    // Simulate 100 spans with userId used 90% of the time
    for (let i = 0; i < 100; i++) {
      const buffer = new module.SpanBufferClass(64);

      // Access userId in 90% of buffers (triggers lazy instantiation)
      if (i < 90) {
        buffer.userId_values[0] = i;
      }

      module.lazyColumnStats.totalSpanBuffersCreated++;
    }

    // Trigger promotion check
    const didPromote = promoteColumnsToEager(module);

    // Verify promotion
    expect(didPromote).toBe(true);
    expect(module.lazyColumnStats.perColumn.get('userId')?.isEager).toBe(true);
    expect(module.lazyColumnStats.perColumn.get('userId')?.instantiationRatio).toBe(0.9);
    expect(module.lazyColumnStats.perColumn.get('sessionId')?.isEager).toBe(false);
  });

  it('does not promote columns below 80% usage threshold', () => {
    const module = createModuleContext({
      moduleMetadata: { gitSha: 'test', packageName: '@test/app', packagePath: 'src/test.ts' },
      logSchema: {
        userId: S.category(),
        sessionId: S.category(),
      },
    });

    // Simulate 100 spans with userId used 75% of the time (below threshold)
    for (let i = 0; i < 100; i++) {
      const buffer = new module.SpanBufferClass(64);

      if (i < 75) {
        buffer.userId_values[0] = i;
      }

      module.lazyColumnStats.totalSpanBuffersCreated++;
    }

    const didPromote = promoteColumnsToEager(module);

    // No promotion should occur
    expect(didPromote).toBe(false);
    expect(module.lazyColumnStats.perColumn.get('userId')?.isEager).toBe(false);
  });

  it('requires 100 samples before promotion', () => {
    const module = createModuleContext({
      moduleMetadata: { gitSha: 'test', packageName: '@test/app', packagePath: 'src/test.ts' },
      logSchema: {
        userId: S.category(),
      },
    });

    // Only 50 buffers (below minimum)
    for (let i = 0; i < 50; i++) {
      const buffer = new module.SpanBufferClass(64);
      buffer.userId_values[0] = i; // 100% usage
      module.lazyColumnStats.totalSpanBuffersCreated++;
    }

    const didPromote = promoteColumnsToEager(module);

    // No promotion due to insufficient samples
    expect(didPromote).toBe(false);
    expect(module.lazyColumnStats.perColumn.get('userId')?.isEager).toBe(false);
  });
});
```

## Buffer Statistics Logging

Buffer statistics are logged periodically (every 100 flushes by default) to provide visibility into buffer tuning
performance. Statistics use structured entry types with the `uint64_value` system column for efficient columnar storage.

### Entry Types

| Entry Type               | Description                                    | uint64_value              |
| ------------------------ | ---------------------------------------------- | ------------------------- |
| `buffer-writes`          | Total entries written across all buffers       | Count of log entries      |
| `buffer-overflow-writes` | Entries written to overflow buffers            | Count of overflow entries |
| `buffer-created`         | Number of SpanBuffers allocated                | Buffer count              |
| `buffer-overflows`       | Times a buffer overflowed (triggered chaining) | Overflow event count      |

### Why Structured Entry Types

1. **Proper columnar data**: No JSON parsing needed - `uint64_value` is a native column type
2. **Consistent with op metrics**: Same pattern as `op-invocations`, `op-errors`, etc.
3. **Dictionary-encoded strings**: `package_name`, `entry_type` are already dictionary types - efficient storage
4. **Single `uint64_value` column**: Reused across all metric types (ops, buffers, feature flags)

### How It Works

1. **Buffer Collection**: The `FlushScheduler` maintains a `SpanBuffer[]` array of root buffers registered for flushing.
   Buffers are added via `register(buffer)` and the array is cleared after each flush (no individual unregister needed).

2. **Module Tracking**: During flush, the scheduler collects a `Set<ModuleContext>` of all unique modules that have been
   flushed since the last stats log.

3. **Flush Counter**: A global flush counter tracks how many flushes have occurred. When this counter reaches
   `BUFFER_STATS_FLUSH_INTERVAL` (default: 100), buffer stats are logged.

4. **Stats Emission**: When the interval is reached, each tracked module emits 4 entries (one per metric type) with:
   - `timestamp`: Current time
   - `thread_id`: Current thread
   - `package_name`: Module's package name (dictionary-encoded)
   - `entry_type`: One of `buffer-writes`, `buffer-overflow-writes`, `buffer-created`, `buffer-overflows`
   - `uint64_value`: The metric value
   - All other columns: null (buffer stats don't have trace/span context)

5. **Reset After Logging**: After logging buffer stats, the flush counter and module tracking set are reset.

### Flush Output Example

```
timestamp | thread_id | package_name | entry_type              | uint64_value
----------|-----------|--------------|-------------------------|---------------
1000      | 1         | @myco/http   | period-start            | 0
1000      | 1         | @myco/http   | buffer-writes           | 50000
1000      | 1         | @myco/http   | buffer-overflow-writes  | 1200
1000      | 1         | @myco/http   | buffer-created          | 47
1000      | 1         | @myco/http   | buffer-overflows        | 3
```

### Query Example (ClickHouse)

```sql
SELECT
  package_name,
  anyIf(uint64_value, entry_type = 'buffer-writes') as total_writes,
  anyIf(uint64_value, entry_type = 'buffer-overflow-writes') as overflow_writes,
  anyIf(uint64_value, entry_type = 'buffer-overflow-writes') /
    nullIf(anyIf(uint64_value, entry_type = 'buffer-writes'), 0) as overflow_rate,
  anyIf(uint64_value, entry_type = 'buffer-created') as buffers_created,
  anyIf(uint64_value, entry_type = 'buffer-overflows') as overflow_events
FROM traces
WHERE entry_type LIKE 'buffer-%'
GROUP BY timestamp, package_name
ORDER BY overflow_rate DESC
```

### Benefits

- **Native columnar storage**: `uint64_value` is a typed column, no JSON parsing overhead
- **Efficient aggregation**: ClickHouse/DuckDB can use SIMD on `uint64_value` column
- **Consistent pattern**: Same structure as op metrics (`op-invocations`, `op-errors`, etc.)
- **Dictionary compression**: Entry type strings compress well (only 4 unique values)

## Summary

Self-tuning buffers provide:

- **Zero configuration** - Works out of the box
- **Adaptive capacity** - Grows and shrinks automatically
- **Memory awareness** - Responds to system pressure
- **Workload optimization** - Adapts to usage patterns
- **Global coordination** - Multiple buffers share resources
- **Per-span isolation** - Each span gets its own buffer for sorted output
- **Buffer chaining** - Overflow handled gracefully while tuning learns optimal size
- **Freelist optimization** - Buffer pooling for V8 GC optimization (future experiment)
- **Column promotion** - Lazy columns automatically promote to eager when heavily used
- **Buffer statistics** - Periodic logging of tuning metrics via structured entry types

**Important Note**: This self-tuning is specifically designed for trace logging, where we have clear metrics (spans per
module, overflow rates, utilization patterns, column usage). The tuning mechanism is part of the trace logging system,
not a general-purpose buffer library. Each span's own buffer eliminates the need for traceId/spanId TypedArrays since
they're constant per buffer.

The result: a logging system that "just works" whether you're on a Raspberry Pi or a 64-core server.
