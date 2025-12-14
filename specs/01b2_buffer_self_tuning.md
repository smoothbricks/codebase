# Buffer Self-Tuning

> **📚 PART OF COLUMNAR BUFFER ARCHITECTURE**
>
> This document details the zero-config memory management that makes columnar buffers "just work" in any environment.
> Read the [main overview](./01b_columnar_buffer_architecture_overview.md) first.

## WHY: Developers Shouldn't Configure Memory

Traditional logging requires configuration:

- Buffer sizes
- Flush intervals
- Memory limits
- Growth strategies

This is wrong. The system should adapt automatically.

**Key Insight**: Self-tuning is part of the trace logging system, not a general-purpose buffer library. The tuning
mechanism has clear metrics (overflow rate, utilization) from the specific use case of span logging. Buffer chaining
handles overflow gracefully while the system learns optimal capacity per module.

### Self-Tuning Benefits

1. **Zero Configuration** - Works out of the box
2. **Adaptive Performance** - Adjusts to workload
3. **Memory Safety** - Never causes OOM
4. **Optimal Throughput** - Balances memory vs performance
5. **Environment Agnostic** - Same code everywhere
6. **Per-Span Buffers** - Each span gets its own buffer, avoiding traceId/spanId TypedArrays
7. **Buffer Chaining** - Graceful overflow handling while learning optimal capacity
8. **Freelist Consideration** - May pool buffers if long-lived TypedArrays help V8's GC

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

### Adaptive Flushing

Balance latency vs memory usage:

```typescript
class AdaptiveFlushBuffer {
  private flushTimer?: NodeJS.Timer;
  private lastFlushTime = Date.now();
  private flushInterval = 1000; // Start with 1 second

  write(entry: any) {
    // Normal write logic...

    // Check if we should flush
    this.checkFlush();
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
    if (this.writeIndex > this.capacity * 0.8) {
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
    if (this.writeIndex > 0 && now - this.lastWriteTime > 5000) {
      reasons.push('idle');
    }

    return reasons;
  }

  private adaptFlushInterval(reasons: string[]) {
    // Frequent capacity flushes = need larger buffer or faster flushing
    if (reasons.includes('capacity')) {
      this.flushInterval = Math.max(100, this.flushInterval * 0.8);
    }

    // Idle flushes = can flush less frequently
    else if (reasons.includes('idle')) {
      this.flushInterval = Math.min(5000, this.flushInterval * 1.2);
    }
  }
}
```

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
  private timestamps: Float64Array;
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
    const newTimestamps = new Float64Array(newCapacity);
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

## Summary

Self-tuning buffers provide:

- **Zero configuration** - Works out of the box
- **Adaptive capacity** - Grows and shrinks automatically
- **Memory awareness** - Responds to system pressure
- **Workload optimization** - Adapts to usage patterns
- **Global coordination** - Multiple buffers share resources
- **Per-span isolation** - Each span gets its own buffer for sorted output
- **Buffer chaining** - Overflow handled gracefully while tuning learns optimal size
- **Freelist optimization** - Consider pooling buffers if long-lived TypedArrays benefit V8 GC

**Important Note**: This self-tuning is specifically designed for trace logging, where we have clear metrics (spans per
module, overflow rates, utilization patterns). The tuning mechanism is part of the trace logging system, not a
general-purpose buffer library. Each span's own buffer eliminates the need for traceId/spanId TypedArrays since they're
constant per buffer.

The result: a logging system that "just works" whether you're on a Raspberry Pi or a 64-core server.
