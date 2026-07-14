import { WASM_NO_LAYOUT_OFFSET, WASM_SPAN_IDENTITY_CHILD, type WasmAllocator } from '@smoothbricks/lmao/wasm';

const NANOSECONDS_PER_MILLISECOND = 1_000_000;
const IDENTITY_BYTES = 128;
const TRACE_ROOT_BYTES = 16;
const F64_ALIGNMENT = Float64Array.BYTES_PER_ELEMENT;
const U32_ALIGNMENT = Uint32Array.BYTES_PER_ELEMENT;

export interface SuperblockBenchmarkOptions {
  readonly capacity?: number;
  readonly warmupIterations?: number;
  readonly sampleCount?: number;
  readonly iterationsPerSample?: number;
}

export interface AllocationSampleSummary {
  readonly samplesNsPerOp: readonly number[];
  readonly medianNsPerOp: number;
  readonly meanNsPerOp: number;
  readonly minNsPerOp: number;
  readonly maxNsPerOp: number;
}

export interface AllocationComparison {
  readonly legacy: AllocationSampleSummary;
  readonly packed: AllocationSampleSummary;
  readonly speedup: number;
  readonly reductionPercent: number;
}

export interface SuperblockBenchmarkResult {
  readonly capacity: number;
  readonly warmupIterations: number;
  readonly sampleCount: number;
  readonly iterationsPerSample: number;
  readonly unit: 'ns/op';
  readonly root: AllocationComparison;
  readonly overflow: AllocationComparison;
  readonly checksum: number;
}

interface AllocationCycles {
  readonly legacyRoot: () => number;
  readonly packedRoot: () => number;
  readonly legacyOverflow: () => number;
  readonly packedOverflow: () => number;
  readonly release: () => void;
}

function requirePositiveInteger(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${label} must be a positive safe integer, received ${value}`);
  }
  return value;
}

function summarize(samples: readonly number[]): AllocationSampleSummary {
  const sorted = [...samples].sort((left, right) => left - right);
  const middle = sorted.length >>> 1;
  const median = sorted.length % 2 === 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
  let total = 0;
  for (const sample of samples) total += sample;
  return Object.freeze({
    samplesNsPerOp: Object.freeze([...samples]),
    medianNsPerOp: median,
    meanNsPerOp: total / samples.length,
    minNsPerOp: sorted[0],
    maxNsPerOp: sorted[sorted.length - 1],
  });
}

function compare(legacySamples: readonly number[], packedSamples: readonly number[]): AllocationComparison {
  const legacy = summarize(legacySamples);
  const packed = summarize(packedSamples);
  return Object.freeze({
    legacy,
    packed,
    speedup: legacy.medianNsPerOp / packed.medianNsPerOp,
    reductionPercent: (1 - packed.medianNsPerOp / legacy.medianNsPerOp) * 100,
  });
}

function createAllocationCycles(allocator: WasmAllocator, capacity: number): AllocationCycles {
  const nullBytes = Math.ceil(capacity / 8);
  const systemBytes = capacity * (BigInt64Array.BYTES_PER_ELEMENT + Uint8Array.BYTES_PER_ELEMENT);
  const u8FamilyBytes = nullBytes + capacity * Uint8Array.BYTES_PER_ELEMENT;
  const u32FamilyBytes = nullBytes + capacity * Uint32Array.BYTES_PER_ELEMENT;
  const f64FamilyBytes = nullBytes + capacity * Float64Array.BYTES_PER_ELEMENT;
  const systemOffset = IDENTITY_BYTES;
  const entryTypeOffset = capacity * BigInt64Array.BYTES_PER_ELEMENT;
  const superblockBytes = systemOffset + systemBytes + u8FamilyBytes + u32FamilyBytes + f64FamilyBytes;
  const overflowSuperblockBytes = superblockBytes - IDENTITY_BYTES;
  const traceRootOffset = allocator.allocExact(TRACE_ROOT_BYTES, F64_ALIGNMENT);
  if (traceRootOffset === 0) throw new Error('Unable to allocate browser benchmark trace root');
  allocator.initTraceRoot(traceRootOffset);

  function legacyRoot(): number {
    const identity = allocator.allocIdentityChild();
    const system = allocator.allocExact(systemBytes, F64_ALIGNMENT);
    const u8 = allocator.allocExact(u8FamilyBytes, Uint8Array.BYTES_PER_ELEMENT);
    const u32 = allocator.allocExact(u32FamilyBytes, U32_ALIGNMENT);
    const f64 = allocator.allocExact(f64FamilyBytes, F64_ALIGNMENT);
    allocator.spanStart(system, identity, traceRootOffset, capacity);
    allocator.freeExact(f64, f64FamilyBytes, F64_ALIGNMENT);
    allocator.freeExact(u32, u32FamilyBytes, U32_ALIGNMENT);
    allocator.freeExact(u8, u8FamilyBytes, Uint8Array.BYTES_PER_ELEMENT);
    allocator.freeExact(system, systemBytes, F64_ALIGNMENT);
    allocator.freeIdentity(identity);
    return identity ^ system ^ u8 ^ u32 ^ f64;
  }

  function packedRoot(): number {
    const span = allocator.createAndStartSpan(
      WASM_SPAN_IDENTITY_CHILD,
      0,
      superblockBytes,
      systemOffset,
      entryTypeOffset,
      WASM_NO_LAYOUT_OFFSET,
      traceRootOffset,
    );
    allocator.freeSpanSuperblock(span, superblockBytes);
    return span;
  }

  function legacyOverflow(): number {
    const system = allocator.allocExact(systemBytes, F64_ALIGNMENT);
    const u8 = allocator.allocExact(u8FamilyBytes, Uint8Array.BYTES_PER_ELEMENT);
    const u32 = allocator.allocExact(u32FamilyBytes, U32_ALIGNMENT);
    const f64 = allocator.allocExact(f64FamilyBytes, F64_ALIGNMENT);
    allocator.freeExact(f64, f64FamilyBytes, F64_ALIGNMENT);
    allocator.freeExact(u32, u32FamilyBytes, U32_ALIGNMENT);
    allocator.freeExact(u8, u8FamilyBytes, Uint8Array.BYTES_PER_ELEMENT);
    allocator.freeExact(system, systemBytes, F64_ALIGNMENT);
    return system ^ u8 ^ u32 ^ f64;
  }

  function packedOverflow(): number {
    const overflow = allocator.createOverflowSpan(overflowSuperblockBytes);
    allocator.freeSpanSuperblock(overflow, overflowSuperblockBytes);
    return overflow;
  }

  const semanticSpan = allocator.createAndStartSpan(
    WASM_SPAN_IDENTITY_CHILD,
    0,
    superblockBytes,
    systemOffset,
    entryTypeOffset,
    WASM_NO_LAYOUT_OFFSET,
    traceRootOffset,
  );
  const semanticSystem = semanticSpan + systemOffset;
  if (
    semanticSpan === 0 ||
    allocator.readWriteIndex(semanticSpan) !== 2 ||
    allocator.readEntryType(semanticSystem, 0, capacity) !== 1
  ) {
    throw new Error('Browser packed-superblock semantic preflight failed');
  }
  allocator.freeSpanSuperblock(semanticSpan, superblockBytes);

  return {
    legacyRoot,
    packedRoot,
    legacyOverflow,
    packedOverflow,
    release: () => allocator.freeExact(traceRootOffset, TRACE_ROOT_BYTES, F64_ALIGNMENT),
  };
}

function measure(cycle: () => number, iterations: number, now: () => number, checksum: { value: number }): number {
  const startedAt = now();
  for (let iteration = 0; iteration < iterations; iteration++) checksum.value ^= cycle();
  const elapsedMilliseconds = now() - startedAt;
  if (!Number.isFinite(elapsedMilliseconds) || elapsedMilliseconds <= 0) {
    throw new Error(`Allocation benchmark clock did not advance: ${elapsedMilliseconds}`);
  }
  return (elapsedMilliseconds * NANOSECONDS_PER_MILLISECOND) / iterations;
}

export function runSuperblockBenchmark(
  allocator: WasmAllocator,
  now: () => number,
  options: SuperblockBenchmarkOptions = {},
): SuperblockBenchmarkResult {
  const capacity = requirePositiveInteger(options.capacity ?? 64, 'capacity');
  const warmupIterations = requirePositiveInteger(options.warmupIterations ?? 4_096, 'warmupIterations');
  const sampleCount = requirePositiveInteger(options.sampleCount ?? 30, 'sampleCount');
  const iterationsPerSample = requirePositiveInteger(options.iterationsPerSample ?? 8_192, 'iterationsPerSample');
  const cycles = createAllocationCycles(allocator, capacity);
  const checksum = { value: 0 };

  try {
    for (let iteration = 0; iteration < warmupIterations; iteration++) {
      checksum.value ^= cycles.legacyRoot();
      checksum.value ^= cycles.packedRoot();
      checksum.value ^= cycles.legacyOverflow();
      checksum.value ^= cycles.packedOverflow();
    }

    const legacyRoot = new Array<number>(sampleCount);
    const packedRoot = new Array<number>(sampleCount);
    const legacyOverflow = new Array<number>(sampleCount);
    const packedOverflow = new Array<number>(sampleCount);

    for (let sample = 0; sample < sampleCount; sample++) {
      if ((sample & 1) === 0) {
        legacyRoot[sample] = measure(cycles.legacyRoot, iterationsPerSample, now, checksum);
        packedRoot[sample] = measure(cycles.packedRoot, iterationsPerSample, now, checksum);
        legacyOverflow[sample] = measure(cycles.legacyOverflow, iterationsPerSample, now, checksum);
        packedOverflow[sample] = measure(cycles.packedOverflow, iterationsPerSample, now, checksum);
      } else {
        packedOverflow[sample] = measure(cycles.packedOverflow, iterationsPerSample, now, checksum);
        legacyOverflow[sample] = measure(cycles.legacyOverflow, iterationsPerSample, now, checksum);
        packedRoot[sample] = measure(cycles.packedRoot, iterationsPerSample, now, checksum);
        legacyRoot[sample] = measure(cycles.legacyRoot, iterationsPerSample, now, checksum);
      }
    }

    return Object.freeze({
      capacity,
      warmupIterations,
      sampleCount,
      iterationsPerSample,
      unit: 'ns/op' as const,
      root: compare(legacyRoot, packedRoot),
      overflow: compare(legacyOverflow, packedOverflow),
      checksum: checksum.value,
    });
  } finally {
    cycles.release();
  }
}
