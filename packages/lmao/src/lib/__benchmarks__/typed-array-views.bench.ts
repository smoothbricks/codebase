/**
 * Benchmark: Lazy vs Cached TypedArray/DataView allocation strategies
 *
 * Compares three patterns for SpanBuffer identity access:
 * - Lazy: Create Uint8Array/DataView on each access
 * - Cached: Store views as instance properties, reuse
 * - Hybrid: Cache Uint8Array only, lazy DataView (cold path)
 *
 * Run: bun run packages/lmao/src/lib/__benchmarks__/typed-array-views.bench.ts
 */

import { bench, boxplot, group, run, summary } from 'mitata';

// =============================================================================
// Pattern A: Lazy - create view on demand
// =============================================================================
class LazyIdentity {
  readonly buffer: ArrayBuffer;

  constructor() {
    this.buffer = new ArrayBuffer(25);
    // Initialize with some data
    const init = new Uint8Array(this.buffer);
    init[0] = 1; // hasParent
    init[9] = 0x12;
    init[10] = 0x34;
    init[11] = 0x56;
    init[12] = 0x78; // spanId bytes
  }

  get spanId(): number {
    return new DataView(this.buffer, 9, 4).getUint32(0, true);
  }

  get hasParent(): boolean {
    return new Uint8Array(this.buffer)[0] === 1;
  }

  getBytes(): Uint8Array {
    return new Uint8Array(this.buffer);
  }

  // Compare first 12 bytes (traceId + spanId portion)
  equals12(other: LazyIdentity): boolean {
    const a = new Uint8Array(this.buffer, 0, 12);
    const b = new Uint8Array(other.buffer, 0, 12);
    for (let i = 0; i < 12; i++) {
      if (a[i] !== b[i]) return false;
    }
    return true;
  }

  copyTo(target: ArrayBuffer): void {
    new Uint8Array(target).set(new Uint8Array(this.buffer));
  }
}

// =============================================================================
// Pattern B: Cached - store views as properties
// =============================================================================
class CachedIdentity {
  readonly buffer: ArrayBuffer;
  readonly bytes: Uint8Array;
  readonly view: DataView;

  constructor() {
    this.buffer = new ArrayBuffer(25);
    this.bytes = new Uint8Array(this.buffer);
    this.view = new DataView(this.buffer);
    // Initialize with some data
    this.bytes[0] = 1; // hasParent
    this.bytes[9] = 0x12;
    this.bytes[10] = 0x34;
    this.bytes[11] = 0x56;
    this.bytes[12] = 0x78; // spanId bytes
  }

  get spanId(): number {
    return this.view.getUint32(9, true);
  }

  get hasParent(): boolean {
    return this.bytes[0] === 1;
  }

  getBytes(): Uint8Array {
    return this.bytes;
  }

  // Compare first 12 bytes
  equals12(other: CachedIdentity): boolean {
    const a = this.bytes;
    const b = other.bytes;
    for (let i = 0; i < 12; i++) {
      if (a[i] !== b[i]) return false;
    }
    return true;
  }

  copyTo(target: ArrayBuffer): void {
    new Uint8Array(target).set(this.bytes);
  }
}

// =============================================================================
// Pattern C: Hybrid - cache Uint8Array only, lazy DataView
// =============================================================================
class HybridIdentity {
  readonly buffer: ArrayBuffer;
  readonly bytes: Uint8Array;

  constructor() {
    this.buffer = new ArrayBuffer(25);
    this.bytes = new Uint8Array(this.buffer);
    // Initialize with some data
    this.bytes[0] = 1; // hasParent
    this.bytes[9] = 0x12;
    this.bytes[10] = 0x34;
    this.bytes[11] = 0x56;
    this.bytes[12] = 0x78; // spanId bytes
  }

  get spanId(): number {
    return new DataView(this.buffer, 9, 4).getUint32(0, true);
  }

  get hasParent(): boolean {
    return this.bytes[0] === 1;
  }

  getBytes(): Uint8Array {
    return this.bytes;
  }

  // Compare first 12 bytes
  equals12(other: HybridIdentity): boolean {
    const a = this.bytes;
    const b = other.bytes;
    for (let i = 0; i < 12; i++) {
      if (a[i] !== b[i]) return false;
    }
    return true;
  }

  copyTo(target: ArrayBuffer): void {
    new Uint8Array(target).set(this.bytes);
  }
}

// =============================================================================
// Pattern D: Direct byte access (no DataView for spanId)
// =============================================================================
class DirectIdentity {
  readonly buffer: ArrayBuffer;
  readonly bytes: Uint8Array;

  constructor() {
    this.buffer = new ArrayBuffer(25);
    this.bytes = new Uint8Array(this.buffer);
    // Initialize with some data
    this.bytes[0] = 1; // hasParent
    this.bytes[9] = 0x12;
    this.bytes[10] = 0x34;
    this.bytes[11] = 0x56;
    this.bytes[12] = 0x78; // spanId bytes
  }

  get spanId(): number {
    // Manual little-endian read from bytes 9-12
    const b = this.bytes;
    return b[9] | (b[10] << 8) | (b[11] << 16) | (b[12] << 24);
  }

  get hasParent(): boolean {
    return this.bytes[0] === 1;
  }

  getBytes(): Uint8Array {
    return this.bytes;
  }

  equals12(other: DirectIdentity): boolean {
    const a = this.bytes;
    const b = other.bytes;
    for (let i = 0; i < 12; i++) {
      if (a[i] !== b[i]) return false;
    }
    return true;
  }

  copyTo(target: ArrayBuffer): void {
    new Uint8Array(target).set(this.bytes);
  }
}

// =============================================================================
// Pre-create instances for read benchmarks
// =============================================================================
const INSTANCE_COUNT = 10_000;

const lazyInstances: LazyIdentity[] = [];
const cachedInstances: CachedIdentity[] = [];
const hybridInstances: HybridIdentity[] = [];
const directInstances: DirectIdentity[] = [];

for (let i = 0; i < INSTANCE_COUNT; i++) {
  lazyInstances.push(new LazyIdentity());
  cachedInstances.push(new CachedIdentity());
  hybridInstances.push(new HybridIdentity());
  directInstances.push(new DirectIdentity());
}

// Target buffers for copy benchmarks
const targetBuffers: ArrayBuffer[] = [];
for (let i = 0; i < INSTANCE_COUNT; i++) {
  targetBuffers.push(new ArrayBuffer(25));
}

// =============================================================================
// Benchmarks
// =============================================================================

console.log('TypedArray/DataView Allocation Strategy Benchmark\n');
console.log('Patterns:');
console.log('  Lazy   - Create Uint8Array/DataView on each access');
console.log('  Cached - Store views as instance properties');
console.log('  Hybrid - Cache Uint8Array only, lazy DataView');
console.log('  Direct - Cache Uint8Array, manual byte reads for spanId');
console.log('');

// -----------------------------------------------------------------------------
// 1. Construction Cost
// -----------------------------------------------------------------------------
group('Construction (create 10,000 instances)', () => {
  summary(() => {
    bench('Lazy', () => {
      const arr: LazyIdentity[] = [];
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        arr.push(new LazyIdentity());
      }
      return arr.length;
    });

    bench('Cached', () => {
      const arr: CachedIdentity[] = [];
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        arr.push(new CachedIdentity());
      }
      return arr.length;
    });

    bench('Hybrid', () => {
      const arr: HybridIdentity[] = [];
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        arr.push(new HybridIdentity());
      }
      return arr.length;
    });

    bench('Direct', () => {
      const arr: DirectIdentity[] = [];
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        arr.push(new DirectIdentity());
      }
      return arr.length;
    });
  });
});

// -----------------------------------------------------------------------------
// 2. spanId reads (cold path - 1 read per instance)
// -----------------------------------------------------------------------------
group('spanId read (1x per instance, cold path)', () => {
  summary(() => {
    bench('Lazy', () => {
      let sum = 0;
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        sum += lazyInstances[i].spanId;
      }
      return sum;
    });

    bench('Cached', () => {
      let sum = 0;
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        sum += cachedInstances[i].spanId;
      }
      return sum;
    });

    bench('Hybrid', () => {
      let sum = 0;
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        sum += hybridInstances[i].spanId;
      }
      return sum;
    });

    bench('Direct', () => {
      let sum = 0;
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        sum += directInstances[i].spanId;
      }
      return sum;
    });
  });
});

// -----------------------------------------------------------------------------
// 3. hasParent reads (single byte read)
// -----------------------------------------------------------------------------
group('hasParent read (single byte)', () => {
  summary(() => {
    bench('Lazy', () => {
      let count = 0;
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        if (lazyInstances[i].hasParent) count++;
      }
      return count;
    });

    bench('Cached', () => {
      let count = 0;
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        if (cachedInstances[i].hasParent) count++;
      }
      return count;
    });

    bench('Hybrid', () => {
      let count = 0;
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        if (hybridInstances[i].hasParent) count++;
      }
      return count;
    });

    bench('Direct', () => {
      let count = 0;
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        if (directInstances[i].hasParent) count++;
      }
      return count;
    });
  });
});

// -----------------------------------------------------------------------------
// 4. 12-byte comparison (hot path - isParentOf/isChildOf)
// -----------------------------------------------------------------------------
group('12-byte comparison (hot path)', () => {
  summary(() => {
    bench('Lazy', () => {
      let matches = 0;
      for (let i = 0; i < INSTANCE_COUNT - 1; i++) {
        if (lazyInstances[i].equals12(lazyInstances[i + 1])) matches++;
      }
      return matches;
    });

    bench('Cached', () => {
      let matches = 0;
      for (let i = 0; i < INSTANCE_COUNT - 1; i++) {
        if (cachedInstances[i].equals12(cachedInstances[i + 1])) matches++;
      }
      return matches;
    });

    bench('Hybrid', () => {
      let matches = 0;
      for (let i = 0; i < INSTANCE_COUNT - 1; i++) {
        if (hybridInstances[i].equals12(hybridInstances[i + 1])) matches++;
      }
      return matches;
    });

    bench('Direct', () => {
      let matches = 0;
      for (let i = 0; i < INSTANCE_COUNT - 1; i++) {
        if (directInstances[i].equals12(directInstances[i + 1])) matches++;
      }
      return matches;
    });
  });
});

// -----------------------------------------------------------------------------
// 5. Bulk copy with set() (createChild scenario)
// -----------------------------------------------------------------------------
group('copyTo with set() (createChild)', () => {
  summary(() => {
    bench('Lazy', () => {
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        lazyInstances[i].copyTo(targetBuffers[i]);
      }
    });

    bench('Cached', () => {
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        cachedInstances[i].copyTo(targetBuffers[i]);
      }
    });

    bench('Hybrid', () => {
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        hybridInstances[i].copyTo(targetBuffers[i]);
      }
    });

    bench('Direct', () => {
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        directInstances[i].copyTo(targetBuffers[i]);
      }
    });
  });
});

// -----------------------------------------------------------------------------
// 6. Mixed workload (realistic span lifecycle)
// Each span: construct + 1 spanId read + 2 hasParent + 1 copy
// -----------------------------------------------------------------------------
group('Mixed workload (realistic span lifecycle)', () => {
  summary(() => {
    bench('Lazy', () => {
      const instances: LazyIdentity[] = [];
      for (let i = 0; i < 1000; i++) {
        const inst = new LazyIdentity();
        instances.push(inst);
        // Read spanId once (for logging/tracing)
        void inst.spanId;
        // Check hasParent twice (common pattern)
        void inst.hasParent;
        void inst.hasParent;
        // Copy to child
        inst.copyTo(targetBuffers[i]);
      }
      return instances.length;
    });

    bench('Cached', () => {
      const instances: CachedIdentity[] = [];
      for (let i = 0; i < 1000; i++) {
        const inst = new CachedIdentity();
        instances.push(inst);
        void inst.spanId;
        void inst.hasParent;
        void inst.hasParent;
        inst.copyTo(targetBuffers[i]);
      }
      return instances.length;
    });

    bench('Hybrid', () => {
      const instances: HybridIdentity[] = [];
      for (let i = 0; i < 1000; i++) {
        const inst = new HybridIdentity();
        instances.push(inst);
        void inst.spanId;
        void inst.hasParent;
        void inst.hasParent;
        inst.copyTo(targetBuffers[i]);
      }
      return instances.length;
    });

    bench('Direct', () => {
      const instances: DirectIdentity[] = [];
      for (let i = 0; i < 1000; i++) {
        const inst = new DirectIdentity();
        instances.push(inst);
        void inst.spanId;
        void inst.hasParent;
        void inst.hasParent;
        inst.copyTo(targetBuffers[i]);
      }
      return instances.length;
    });
  });
});

// -----------------------------------------------------------------------------
// 7. getBytes() access pattern
// -----------------------------------------------------------------------------
group('getBytes() access', () => {
  summary(() => {
    bench('Lazy', () => {
      let sum = 0;
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        sum += lazyInstances[i].getBytes()[0];
      }
      return sum;
    });

    bench('Cached', () => {
      let sum = 0;
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        sum += cachedInstances[i].getBytes()[0];
      }
      return sum;
    });

    bench('Hybrid', () => {
      let sum = 0;
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        sum += hybridInstances[i].getBytes()[0];
      }
      return sum;
    });

    bench('Direct', () => {
      let sum = 0;
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        sum += directInstances[i].getBytes()[0];
      }
      return sum;
    });
  });
});

// Enable boxplot visualization
boxplot(() => {
  group('All patterns - spanId read', () => {
    bench('Lazy', () => {
      let sum = 0;
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        sum += lazyInstances[i].spanId;
      }
      return sum;
    });
    bench('Cached', () => {
      let sum = 0;
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        sum += cachedInstances[i].spanId;
      }
      return sum;
    });
    bench('Hybrid', () => {
      let sum = 0;
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        sum += hybridInstances[i].spanId;
      }
      return sum;
    });
    bench('Direct', () => {
      let sum = 0;
      for (let i = 0; i < INSTANCE_COUNT; i++) {
        sum += directInstances[i].spanId;
      }
      return sum;
    });
  });
});

// Run all benchmarks
await run({
  colors: true,
});

// Print summary and recommendations
console.log(`\n${'='.repeat(80)}`);
console.log('ANALYSIS & RECOMMENDATIONS');
console.log('='.repeat(80));
console.log(`
Memory overhead per instance:
  - Lazy:   24 bytes (ArrayBuffer only)
  - Cached: 24 + 16 + 16 = 56 bytes (ArrayBuffer + Uint8Array + DataView refs)
  - Hybrid: 24 + 16 = 40 bytes (ArrayBuffer + Uint8Array ref)
  - Direct: 24 + 16 = 40 bytes (ArrayBuffer + Uint8Array ref)

Trade-offs:
  - Lazy:   Lowest memory, highest CPU for repeated access
  - Cached: Highest memory, fastest for repeated access
  - Hybrid: Medium memory, fast Uint8Array access, slow DataView (cold path OK)
  - Direct: Medium memory, no DataView allocation, manual byte manipulation

Recommendations for SpanBuffer:
  1. Cache Uint8Array (used in hot paths: equals12, copyTo, hasParent)
  2. For spanId: Use Direct pattern (manual byte read) if spanId is cold path
     OR Cache DataView if spanId is accessed frequently
  3. The extra 16-32 bytes per span is negligible for the performance gain

Best pattern depends on access frequency:
  - spanId cold (1x per span):     Hybrid or Direct
  - spanId hot (multiple reads):   Cached
  - byte comparison hot path:      Must cache Uint8Array (Cached/Hybrid/Direct)
`);
