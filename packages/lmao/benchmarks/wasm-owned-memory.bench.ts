/**
 * Native Mitata diagnostics for WASM-owned memory before ownership/layout
 * optimizations. Labels containing "production" execute the shipped allocator
 * wrapper/exports. Labels containing "model" are isolated candidate models and
 * must not be interpreted as production measurements.
 *
 * Run: bun packages/lmao/benchmarks/wasm-owned-memory.bench.ts --quick
 */

import { bench, do_not_optimize, group, run } from 'mitata';
import { createWasmAllocator, type WasmAllocator } from '../src/lib/wasm/wasmAllocator.js';

const quick = process.argv.includes('--quick');
const capacities = quick ? [64] : [8, 64, 256];
const writeIterations = quick ? 256 : 4_096;
const ownershipIterations = quick ? 16 : 256;
const stringRows = quick ? 64 : 256;
const PAGE_BYTES = 65_536;
const IDENTITY_BYTES = 48;
const TRACE_ID = '0123456789abcdef0123456789abcdef';
const TRACE_BYTES = new TextEncoder().encode(TRACE_ID);

function invariant(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(`wasm-owned-memory semantic check failed: ${message}`);
}

type NativeFormat = 'json' | 'markdown' | 'mitata' | 'quiet';

function requestedFormat(): NativeFormat {
  if (process.argv.includes('--json')) return 'json';
  if (process.argv.includes('--markdown')) return 'markdown';
  if (process.argv.includes('--quiet')) return 'quiet';
  const value = process.argv.find((argument) => argument.startsWith('--format='))?.slice('--format='.length);
  if (value === undefined) return 'mitata';
  invariant(
    value === 'json' || value === 'markdown' || value === 'mitata' || value === 'quiet',
    `unsupported Mitata format ${value}`,
  );
  return value;
}

function align(value: number, alignment: number): number {
  return (value + alignment - 1) & ~(alignment - 1);
}

function col8ValueOffset(pointer: number, capacity: number): number {
  return align(pointer + Math.ceil(capacity / 8), 8);
}

function mix(hash: number, value: number): number {
  return Math.imul(hash ^ value, 16_777_619) >>> 0;
}

function expectedNumericChecksum(iterations: number, capacity: number): number {
  let checksum = 2_166_136_261;
  for (let index = 0; index < iterations; index++) {
    const row = index & (capacity - 1);
    const value = row * 3.25 + 0.5;
    checksum = mix(checksum, row);
    checksum = mix(checksum, Math.trunc(value * 4));
  }
  return checksum;
}

interface ColumnDescriptor {
  readonly offset: number;
  readonly valueOffset: number;
  readonly capacity: number;
  generation: number;
}

function makeNumericRunners(allocator: WasmAllocator, capacity: number, pointer: number) {
  const valueOffset = col8ValueOffset(pointer, capacity);
  const descriptor: ColumnDescriptor = { offset: pointer, valueOffset, capacity, generation: 1 };
  const modelSlab = new ArrayBuffer(valueOffset + capacity * 8 + 64);
  const modelBytes = new Uint8Array(modelSlab);
  const modelF64 = new Float64Array(modelSlab);
  let canonicalBuffer = allocator.memory.buffer;
  let canonicalBytes = new Uint8Array(canonicalBuffer);
  let canonicalF64 = new Float64Array(canonicalBuffer);
  let canonicalGeneration = 1;

  const writeChecksum = (write: (row: number, value: number) => void): number => {
    let checksum = 2_166_136_261;
    for (let index = 0; index < writeIterations; index++) {
      const row = index & (capacity - 1);
      const value = row * 3.25 + 0.5;
      write(row, value);
      checksum = mix(checksum, row);
      checksum = mix(checksum, Math.trunc(value * 4));
    }
    return checksum;
  };

  const productionWrapper = (): number =>
    writeChecksum((row, value) => allocator.writeColF64(pointer, row, value, capacity));
  const productionExport = (): number =>
    writeChecksum((row, value) => allocator.exports.write_col_f64(pointer, row, value, capacity));
  const productionGetter = (): number => {
    const bytes = allocator.u8;
    const values = allocator.f64;
    const base = valueOffset >>> 3;
    return writeChecksum((row, value) => {
      bytes[pointer + (row >>> 3)] |= 1 << (row & 7);
      values[base + row] = value;
    });
  };
  const productionCanonical = (): number => {
    // Refresh is intentionally once per operation, outside the row hot loop.
    if (canonicalBuffer !== allocator.memory.buffer) {
      canonicalBuffer = allocator.memory.buffer;
      canonicalBytes = new Uint8Array(canonicalBuffer);
      canonicalF64 = new Float64Array(canonicalBuffer);
      canonicalGeneration++;
    }
    const base = descriptor.valueOffset >>> 3;
    return writeChecksum((row, value) => {
      canonicalBytes[descriptor.offset + (row >>> 3)] |= 1 << (row & 7);
      canonicalF64[base + row] = value;
    });
  };
  const exactLayoutModel = (): number => {
    const base = descriptor.valueOffset >>> 3;
    return writeChecksum((row, value) => {
      modelBytes[descriptor.offset + (row >>> 3)] |= 1 << (row & 7);
      modelF64[base + row] = value;
    });
  };

  return {
    descriptor,
    get canonicalGeneration() {
      return canonicalGeneration;
    },
    productionWrapper,
    productionExport,
    productionGetter,
    productionCanonical,
    exactLayoutModel,
  };
}

async function validateGrowthInvalidation(): Promise<void> {
  const allocator = await createWasmAllocator({ capacity: 64, initialPages: 17, maxPages: 20 });
  const staleGetterView = allocator.f64;
  const staleDirectView = new Float64Array(allocator.memory.buffer);
  const oldBuffer = allocator.memory.buffer;
  allocator.memory.grow(1);
  invariant(oldBuffer !== allocator.memory.buffer, 'memory.grow must replace the ArrayBuffer');
  invariant(staleGetterView.byteLength === 0, 'cached allocator view must be detached by growth');
  invariant(staleDirectView.byteLength === 0, 'canonical direct view must be detached by growth');
  const refreshed = allocator.f64;
  invariant(refreshed.buffer === allocator.memory.buffer, 'allocator getter must refresh after growth');
  invariant(refreshed.byteLength === 18 * PAGE_BYTES, 'refreshed getter must cover the full grown memory');

  let generation = 1;
  let generationBuffer = oldBuffer;
  let generationView = staleDirectView;
  if (generationBuffer !== allocator.memory.buffer) {
    generationBuffer = allocator.memory.buffer;
    generationView = new Float64Array(generationBuffer);
    generation++;
  }
  invariant(generation === 2, 'growth-aware direct view must increment generation exactly once');
  invariant(generationView.buffer === allocator.memory.buffer, 'growth-aware direct view must own current buffer');
}

interface OwnedDescriptor {
  index: number;
  generation: number;
  active: boolean;
  kind: 1 | 2 | 3;
  identityOffset: number;
  systemOffset: number;
  columnOffset: number;
  parent: number;
  overflow: number;
}

class ExactLayoutDescriptorSlab {
  readonly memory: ArrayBuffer;
  readonly bytes: Uint8Array;
  readonly descriptors: OwnedDescriptor[];
  private readonly freeDescriptors: number[];
  private readonly freeIdentities: number[] = [];
  private readonly freeSystems: number[] = [];
  private readonly freeColumns: number[] = [];
  private bump: number;

  constructor(
    readonly capacity: number,
    descriptorCapacity: number,
  ) {
    this.memory = new ArrayBuffer(2 * 1024 * 1024);
    this.bytes = new Uint8Array(this.memory);
    this.bump = 64;
    this.descriptors = Array.from({ length: descriptorCapacity }, (_, index) => ({
      index,
      generation: 0,
      active: false,
      kind: 1 as const,
      identityOffset: 0,
      systemOffset: 0,
      columnOffset: 0,
      parent: -1,
      overflow: -1,
    }));
    this.freeDescriptors = Array.from({ length: descriptorCapacity }, (_, index) => descriptorCapacity - index - 1);
  }

  private block(free: number[], bytes: number, alignment: number): number {
    const reused = free.pop();
    if (reused !== undefined) return reused;
    const offset = align(this.bump, alignment);
    this.bump = offset + bytes;
    invariant(this.bump <= this.memory.byteLength, 'modeled slab exhausted');
    return offset;
  }

  acquire(kind: 1 | 2 | 3, parent = -1): OwnedDescriptor {
    const index = this.freeDescriptors.pop();
    invariant(index !== undefined, 'modeled descriptor slab exhausted');
    const descriptor = this.descriptors[index]!;
    invariant(!descriptor.active, 'descriptor acquired while still owned');
    descriptor.generation++;
    descriptor.active = true;
    descriptor.kind = kind;
    descriptor.identityOffset = this.block(this.freeIdentities, IDENTITY_BYTES, 8);
    descriptor.systemOffset = this.block(this.freeSystems, this.capacity * 9, 8);
    descriptor.columnOffset = this.block(
      this.freeColumns,
      align(Math.ceil(this.capacity / 8), 8) + this.capacity * 8,
      8,
    );
    descriptor.parent = parent;
    descriptor.overflow = -1;
    return descriptor;
  }

  release(descriptor: OwnedDescriptor, generation: number): void {
    invariant(descriptor.active, 'double release detected');
    invariant(descriptor.generation === generation, 'stale generation release detected');
    descriptor.active = false;
    this.freeIdentities.push(descriptor.identityOffset);
    this.freeSystems.push(descriptor.systemOffset);
    this.freeColumns.push(descriptor.columnOffset);
    this.freeDescriptors.push(descriptor.index);
  }
}

function productionOwnershipCycle(allocator: WasmAllocator, capacity: number): number {
  let semantic = 0;
  for (let iteration = 0; iteration < ownershipIterations; iteration++) {
    const rootPacked = allocator.allocIdentityRootForJsWrite(TRACE_BYTES.length);
    const rootIdentity = Number(rootPacked >> 32n);
    allocator.u8.set(TRACE_BYTES, Number(rootPacked & 0xffff_ffffn));
    const rootSystem = allocator.allocSpanSystem(capacity);
    const rootColumn = allocator.alloc8B(capacity);

    const childIdentity = allocator.allocIdentityChild();
    const childSystem = allocator.allocSpanSystem(capacity);
    const childColumn = allocator.alloc8B(capacity);

    const overflowPacked = allocator.allocIdentityRootForJsWrite(TRACE_BYTES.length);
    const overflowIdentity = Number(overflowPacked >> 32n);
    allocator.u8.set(TRACE_BYTES, Number(overflowPacked & 0xffff_ffffn));
    const overflowSystem = allocator.allocSpanSystem(capacity);
    const overflowColumn = allocator.alloc8B(capacity);

    semantic = mix(semantic, 1);
    semantic = mix(semantic, 2);
    semantic = mix(semantic, 3);

    allocator.free8B(overflowColumn, capacity);
    allocator.freeSpanSystem(overflowSystem, capacity);
    allocator.freeIdentity(overflowIdentity);
    allocator.free8B(childColumn, capacity);
    allocator.freeSpanSystem(childSystem, capacity);
    allocator.freeIdentity(childIdentity);
    allocator.free8B(rootColumn, capacity);
    allocator.freeSpanSystem(rootSystem, capacity);
    allocator.freeIdentity(rootIdentity);
  }
  return semantic;
}

function modelOwnershipCycle(slab: ExactLayoutDescriptorSlab): number {
  let semantic = 0;
  for (let iteration = 0; iteration < ownershipIterations; iteration++) {
    const root = slab.acquire(1);
    const child = slab.acquire(2, root.index);
    const overflow = slab.acquire(3, root.index);
    root.overflow = overflow.index;
    semantic = mix(semantic, root.kind);
    semantic = mix(semantic, child.kind);
    semantic = mix(semantic, overflow.kind);
    slab.release(overflow, overflow.generation);
    slab.release(child, child.generation);
    slab.release(root, root.generation);
  }
  return semantic;
}

function validateProductionOwnership(allocator: WasmAllocator, capacity: number): void {
  const beforeAlloc = allocator.getAllocCount();
  const beforeFree = allocator.getFreeCount();
  productionOwnershipCycle(allocator, capacity);
  const allocationDelta = allocator.getAllocCount() - beforeAlloc;
  const freeDelta = allocator.getFreeCount() - beforeFree;
  const logicalBlockCount = ownershipIterations * 9;
  invariant(
    allocationDelta === logicalBlockCount,
    `production allocation count mismatch: expected ${logicalBlockCount}, observed ${allocationDelta}`,
  );
  invariant(
    freeDelta >= logicalBlockCount,
    `production free counter must include all ${logicalBlockCount} releases; observed ${freeDelta}`,
  );
}

function validateModeledOwnership(slab: ExactLayoutDescriptorSlab): void {
  const root = slab.acquire(1);
  const generation = root.generation;
  slab.release(root, generation);
  const reused = slab.acquire(1);
  invariant(reused.index === root.index, 'released descriptor must be reused');
  invariant(reused.generation === generation + 1, 'descriptor reuse must advance generation');
  let staleReleaseRejected = false;
  try {
    slab.release(reused, generation);
  } catch {
    staleReleaseRejected = true;
  }
  invariant(staleReleaseRejected, 'stale generation release must be rejected');
  slab.release(reused, reused.generation);
}

const dynamicStrings = Array.from({ length: stringRows }, (_, index) => `request-${index % 23}/user-${index % 11}`);
const staticStrings = ['GET', 'POST', 'PUT', 'DELETE'] as const;

function stringChecksum(): number {
  let checksum = 2_166_136_261;
  for (let index = 0; index < stringRows; index++) {
    const dynamic = dynamicStrings[index]!;
    const fixed = staticStrings[index & 3]!;
    for (let char = 0; char < dynamic.length; char++) checksum = mix(checksum, dynamic.charCodeAt(char));
    for (let char = 0; char < fixed.length; char++) checksum = mix(checksum, fixed.charCodeAt(char));
  }
  return checksum;
}

function makeDynamicJsSidecar() {
  const dynamic = new Array<string>(stringRows);
  const fixed = new Array<string>(stringRows);
  const validity = new Uint8Array(Math.ceil(stringRows / 8));
  return (): number => {
    let semantic = 2_166_136_261;
    for (let index = 0; index < stringRows; index++) {
      dynamic[index] = dynamicStrings[index]!;
      fixed[index] = staticStrings[index & 3]!;
      validity[index >>> 3] |= 1 << (index & 7);
      for (let char = 0; char < dynamic[index]!.length; char++)
        semantic = mix(semantic, dynamic[index]!.charCodeAt(char));
      for (let char = 0; char < fixed[index]!.length; char++) semantic = mix(semantic, fixed[index]!.charCodeAt(char));
    }
    return semantic;
  };
}

function makeStaticIdSidecar() {
  const dynamic = new Array<string>(stringRows);
  const fixedIds = new Uint8Array(stringRows);
  const validity = new Uint8Array(Math.ceil(stringRows / 8));
  return (): number => {
    let semantic = 2_166_136_261;
    for (let index = 0; index < stringRows; index++) {
      dynamic[index] = dynamicStrings[index]!;
      fixedIds[index] = index & 3;
      validity[index >>> 3] |= 1 << (index & 7);
      const fixed = staticStrings[fixedIds[index]!]!;
      for (let char = 0; char < dynamic[index]!.length; char++)
        semantic = mix(semantic, dynamic[index]!.charCodeAt(char));
      for (let char = 0; char < fixed.length; char++) semantic = mix(semantic, fixed.charCodeAt(char));
    }
    return semantic;
  };
}

function makeUtf8SlabSidecar() {
  const encoder = new TextEncoder();
  const bytes = new Uint8Array(stringRows * 40);
  const offsets = new Uint32Array(stringRows + 1);
  const fixedIds = new Uint8Array(stringRows);
  return (): number => {
    let offset = 0;
    let semantic = 2_166_136_261;
    offsets[0] = 0;
    for (let index = 0; index < stringRows; index++) {
      const dynamic = dynamicStrings[index]!;
      const fixed = staticStrings[index & 3]!;
      const encoded = encoder.encodeInto(dynamic, bytes.subarray(offset));
      offset += encoded.written;
      offsets[index + 1] = offset;
      fixedIds[index] = index & 3;
      for (let char = 0; char < dynamic.length; char++) semantic = mix(semantic, dynamic.charCodeAt(char));
      for (let char = 0; char < fixed.length; char++) semantic = mix(semantic, fixed.charCodeAt(char));
    }
    return semantic;
  };
}

await validateGrowthInvalidation();

for (const capacity of capacities) {
  invariant((capacity & (capacity - 1)) === 0, 'capacity must be a power of two');
  const allocator = await createWasmAllocator({ capacity, initialPages: 17, maxPages: 256 });
  const column = allocator.alloc8B(capacity);
  invariant(column !== 0, 'production column allocation failed');
  const numeric = makeNumericRunners(allocator, capacity, column);
  const expected = expectedNumericChecksum(writeIterations, capacity);
  for (const numericRun of [
    numeric.productionWrapper,
    numeric.productionExport,
    numeric.productionGetter,
    numeric.productionCanonical,
    numeric.exactLayoutModel,
  ]) {
    invariant(numericRun() === expected, 'numeric variant checksum mismatch before timing');
  }
  invariant(numeric.descriptor.valueOffset % 8 === 0, 'exact column value offset must be aligned');
  invariant(numeric.canonicalGeneration === 1, 'non-growing hot path must retain view generation');

  group(`WASM numeric layout | capacity=${capacity} rows=${writeIterations}`, () => {
    bench('current/production-wrapper-writeColF64', () => do_not_optimize(numeric.productionWrapper())).baseline();
    bench('production-raw-export/write_col_f64', () => do_not_optimize(numeric.productionExport()));
    bench('production-wrapper/full-memory-getters', () => do_not_optimize(numeric.productionGetter()));
    bench('production-canonical/full-memory-direct-view-refresh-outside-loop', () =>
      do_not_optimize(numeric.productionCanonical()),
    );
    bench('isolated-model/exact-layout-descriptor-slab', () => do_not_optimize(numeric.exactLayoutModel()));
  });

  const ownershipAllocator = await createWasmAllocator({ capacity, initialPages: 17, maxPages: 256 });
  const modelSlab = new ExactLayoutDescriptorSlab(capacity, 3);
  // Allocator counters and ownership/generation assertions stay outside Mitata's timed bodies.
  validateProductionOwnership(ownershipAllocator, capacity);
  validateModeledOwnership(modelSlab);
  const productionOwnership = () => productionOwnershipCycle(ownershipAllocator, capacity);
  const modeledOwnership = () => modelOwnershipCycle(modelSlab);
  invariant(
    productionOwnership() === modeledOwnership(),
    'production/model ownership semantic checksum mismatch before timing',
  );

  group(`WASM root/child/overflow ownership | capacity=${capacity} cycles=${ownershipIterations}`, () => {
    bench('current/production-allocator-acquire-release', () => do_not_optimize(productionOwnership())).baseline();
    bench('isolated-model/exact-layout-descriptor-slab-acquire-release', () => do_not_optimize(modeledOwnership()));
  });
}

const dynamicSidecar = makeDynamicJsSidecar();
const staticIdSidecar = makeStaticIdSidecar();
const utf8SlabSidecar = makeUtf8SlabSidecar();
const expectedStrings = stringChecksum();
invariant(dynamicSidecar() === expectedStrings, 'dynamic string sidecar checksum mismatch');
invariant(staticIdSidecar() === expectedStrings, 'static ID sidecar checksum mismatch');
invariant(utf8SlabSidecar() === expectedStrings, 'UTF-8 slab sidecar checksum mismatch');

group(`WASM string sidecar lifecycle | rows=${stringRows} static-vocabulary=${staticStrings.length}`, () => {
  bench('current/allocation-inclusive-dynamic-js-array-sidecars', () =>
    do_not_optimize(makeDynamicJsSidecar()()),
  ).baseline();
  bench('isolated-model/allocation-inclusive-static-id-plus-dynamic-js-sidecar', () =>
    do_not_optimize(makeStaticIdSidecar()()),
  );
  bench('isolated-model/allocation-inclusive-static-id-plus-dynamic-utf8-slab-sidecar', () =>
    do_not_optimize(makeUtf8SlabSidecar()()),
  );
});

group(`WASM string sidecar reused data path | rows=${stringRows} static-vocabulary=${staticStrings.length}`, () => {
  bench('current/reused-dynamic-js-array-sidecars', () => do_not_optimize(dynamicSidecar())).baseline();
  bench('isolated-model/reused-static-id-plus-dynamic-js-sidecar', () => do_not_optimize(staticIdSidecar()));
  bench('isolated-model/reused-static-id-plus-dynamic-utf8-slab-sidecar', () => do_not_optimize(utf8SlabSidecar()));
});

const format = requestedFormat();
console.error(
  'WASM owned-memory preflight passed: growth/view generation, exact-layout ownership, and semantic checksums. ' +
    `Quick=${quick}; capacities=${capacities.join(',')}; writes=${writeIterations}; ownership cycles=${ownershipIterations}; string rows=${stringRows}.`,
);

await run({
  colors: format === 'mitata' && !process.argv.includes('--no-colors'),
  format,
});
