import { bench, do_not_optimize, group, run, summary } from 'mitata';
import { defineOpContext } from '../src/lib/defineOpContext.js';
import { createOpMetadata } from '../src/lib/opContext/defineOp.js';
import {
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_MESSAGE_LAYOUT_MIXED,
  RUNTIME_HINT_RESULT,
} from '../src/lib/runtimeHint.js';
import { S } from '../src/lib/schema/builder.js';
import { defineLogSchema } from '../src/lib/schema/defineLogSchema.js';
import { ENTRY_TYPE_INFO } from '../src/lib/schema/systemSchema.js';
import type { TracerLifecycleHooks } from '../src/lib/traceRoot.js';
import { iterateSpanTree } from '../src/lib/traceTopology.js';
import { WasmBufferStrategy } from '../src/lib/wasm/WasmBufferStrategy.js';
import { createWasmAllocator, type WasmAllocator } from '../src/lib/wasm/wasmAllocator.js';
import { createWasmTraceRoot } from '../src/lib/wasm/wasmTraceRoot.js';
import { registerBenchmarkVocabulary } from './vocabularyFixture.js';

const QUICK = process.argv.includes('--quick');
const CAPACITIES: readonly number[] = QUICK ? Object.freeze([64]) : Object.freeze([8, 64, 256]);
const WRITE_ITERATIONS = QUICK ? 256 : 4_096;
const OWNERSHIP_ITERATIONS = QUICK ? 16 : 256;
const STRING_ROWS = QUICK ? 64 : 256;
const PAGE_BYTES = 65_536;
const TRACE_ID = '0123456789abcdef0123456789abcdef';
const STATIC_STRINGS: readonly string[] = Object.freeze(['GET', 'POST', 'PUT', 'DELETE']);
const STATIC_BINDING = registerBenchmarkVocabulary(STATIC_STRINGS);
const SCHEMA = defineLogSchema({ metric: S.number() });
const CONTEXT = defineOpContext({ logSchema: SCHEMA });
const RUNTIME_SCHEMA = CONTEXT.logBinding.logSchema;
const METADATA = createOpMetadata('wasm-owned-memory', '@smoothbricks/lmao', 'wasm-owned-memory.bench.ts', 'bench', 1);
const LAYOUT_OP = CONTEXT.defineOp('wasm-owned-memory-layout', (ctx) => ctx.ok(null), undefined, {
  runtimeHint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_MESSAGE_LAYOUT_MIXED | RUNTIME_HINT_RESULT | 64,
});

type NativeFormat = 'json' | 'markdown' | 'mitata' | 'quiet';

function invariant(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(`wasm-owned-memory semantic check failed: ${message}`);
}

function requestedFormat(): NativeFormat {
  const prefix = '--format=';
  for (let index = 0; index < process.argv.length; index++) {
    const argument = process.argv[index];
    if (argument === undefined) continue;
    const value = argument.startsWith(prefix)
      ? argument.slice(prefix.length)
      : argument === '--format'
        ? process.argv[index + 1]
        : undefined;
    if (value === undefined) continue;
    if (value === 'json' || value === 'markdown' || value === 'mitata' || value === 'quiet') return value;
    throw new Error(`Unknown Mitata format: ${value}`);
  }
  return 'mitata';
}

function mix(hash: number, value: number): number {
  return Math.imul(hash ^ value, 16_777_619) >>> 0;
}

function requireNumber(values: Uint32Array, index: number, label: string): number {
  const value = values[index];
  if (value === undefined) throw new RangeError(`${label} index ${index} is out of range`);
  return value;
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

function makeNumericRunners(allocator: WasmAllocator, capacity: number) {
  const layout = LAYOUT_OP.callsitePlan.wasmLayout.forCapacity(capacity);
  const column = layout.columns.find((candidate) => candidate.name === 'value');
  const slab = layout.slabs.f64;
  if (column === undefined || slab === null) throw new Error('PhysicalLayoutPlan omitted the eager numeric WASM slab');
  const pointer = allocator.allocExact(slab.byteLength, slab.alignment);
  invariant(pointer !== 0, 'production exact numeric slab allocation failed');
  const nullOffset = pointer + column.nullOffset;
  const valueOffset = pointer + column.valueOffset;

  const writeChecksum = (bytes: Uint8Array, values: Float64Array): number => {
    let checksum = 2_166_136_261;
    const valueBase = valueOffset >>> 3;
    for (let index = 0; index < WRITE_ITERATIONS; index++) {
      const row = index & (capacity - 1);
      const value = row * 3.25 + 0.5;
      bytes[nullOffset + (row >>> 3)] |= 1 << (row & 7);
      values[valueBase + row] = value;
      checksum = mix(checksum, row);
      checksum = mix(checksum, Math.trunc(value * 4));
    }
    return checksum;
  };

  return {
    layout,
    getterViews(): number {
      return writeChecksum(allocator.u8, allocator.f64);
    },
    canonicalViews(): number {
      return writeChecksum(new Uint8Array(allocator.memory.buffer), new Float64Array(allocator.memory.buffer));
    },
    pinnedEpoch(): number {
      const pin = allocator.pinMemoryEpoch();
      try {
        return writeChecksum(new Uint8Array(pin.buffer), new Float64Array(pin.buffer));
      } finally {
        pin.release();
      }
    },
  };
}

async function validateGrowthAndPinning(): Promise<void> {
  const allocator = await createWasmAllocator({ capacity: 64, initialPages: 17, maxPages: 20 });
  const staleView = allocator.f64;
  const oldBuffer = allocator.memory.buffer;
  const pin = allocator.pinMemoryEpoch();
  allocator.memory.grow(1);
  let pinRejectedRefresh = false;
  try {
    allocator.refreshViews();
  } catch (error) {
    pinRejectedRefresh = error instanceof Error && error.message.includes('Arrow lease pinned');
  }
  invariant(pinRejectedRefresh, 'memory refresh must reject growth while an epoch lease is active');
  pin.release();
  invariant(allocator.refreshViews() === 2, 'released epoch must refresh exactly once');
  invariant(oldBuffer !== allocator.memory.buffer, 'memory.grow must replace the ArrayBuffer');
  invariant(staleView.byteLength === 0, 'pre-growth canonical view must detach');
  invariant(allocator.f64.byteLength === 18 * PAGE_BYTES, 'refreshed getter must cover grown memory');
}

function lifecycleFor(
  strategy: WasmBufferStrategy<typeof RUNTIME_SCHEMA>,
): TracerLifecycleHooks<typeof RUNTIME_SCHEMA> {
  return {
    onTraceStart: () => undefined,
    onTraceEnd: () => undefined,
    onSpanStart: () => undefined,
    onSpanEnd: () => undefined,
    onStatsWillResetFor: () => undefined,
    getFlagEvaluatorForContext: () => undefined,
    bufferStrategy: strategy,
  };
}

function ownershipCycle(
  strategy: WasmBufferStrategy<typeof RUNTIME_SCHEMA>,
  capacity: number,
  iterations: number,
): number {
  let checksum = 2_166_136_261;
  const lifecycle = lifecycleFor(strategy);
  for (let iteration = 0; iteration < iterations; iteration++) {
    const traceRoot = createWasmTraceRoot(strategy.allocator, TRACE_ID, lifecycle);
    const root = strategy.createSpanBuffer(
      RUNTIME_SCHEMA,
      traceRoot,
      METADATA,
      capacity,
      LAYOUT_OP.callsitePlan.SpanBufferClass,
    );
    const child = strategy.createChildSpanBuffer(
      root,
      METADATA,
      METADATA,
      capacity,
      RUNTIME_SCHEMA,
      LAYOUT_OP.callsitePlan.SpanBufferClass,
    );
    const overflow = strategy.createOverflowBuffer(child);
    root.timestamp[0] = BigInt(iteration * 3 + 1);
    child.timestamp[0] = BigInt(iteration * 3 + 2);
    overflow.timestamp[0] = BigInt(iteration * 3 + 3);
    const rootEntryTypes = root.entry_type;
    const childEntryTypes = child.entry_type;
    const overflowEntryTypes = overflow.entry_type;
    invariant(rootEntryTypes !== undefined, 'root entry_type lane must be allocated');
    invariant(childEntryTypes !== undefined, 'child entry_type lane must be allocated');
    invariant(overflowEntryTypes !== undefined, 'overflow entry_type lane must be allocated');
    rootEntryTypes[0] = ENTRY_TYPE_INFO;
    childEntryTypes[0] = ENTRY_TYPE_INFO;
    overflowEntryTypes[0] = ENTRY_TYPE_INFO;
    root.message(0, `root-${iteration}`);
    child.message(0, `child-${iteration}`);
    overflow.message(0, `overflow-${iteration}`);
    root.metric(0, iteration + 0.25);
    child.metric(0, iteration + 0.5);
    overflow.metric(0, iteration + 0.75);
    root._writeIndex = 1;
    child._writeIndex = 1;
    overflow._writeIndex = 1;
    const nodes = Array.from(iterateSpanTree(root));
    invariant(nodes.length === 3, 'root/child/overflow topology must expose three physical segments');
    checksum = mix(checksum, nodes.length);
    checksum = mix(checksum, root._nodeIndex);
    checksum = mix(checksum, child._nodeIndex);
    checksum = mix(checksum, overflow._nodeIndex);
    const generation = traceRoot._topology.generation;
    strategy.releaseBuffer(root);
    invariant(traceRoot._topology.generation === generation + 1, 'released topology must advance generation');
  }
  return checksum;
}

function stringChecksum(): number {
  let checksum = 2_166_136_261;
  for (let index = 0; index < STRING_ROWS; index++) {
    const dynamic = `request-${index % 23}/user-${index % 11}`;
    const fixed = STATIC_STRINGS[index & 3];
    if (fixed === undefined) throw new RangeError(`Missing static string ${index & 3}`);
    for (let char = 0; char < dynamic.length; char++) checksum = mix(checksum, dynamic.charCodeAt(char));
    for (let char = 0; char < fixed.length; char++) checksum = mix(checksum, fixed.charCodeAt(char));
  }
  return checksum;
}

function dynamicStringSidecars(): number {
  const dynamic = new Array<string>(STRING_ROWS);
  const fixed = new Array<string>(STRING_ROWS);
  let checksum = 2_166_136_261;
  for (let index = 0; index < STRING_ROWS; index++) {
    const dynamicValue = `request-${index % 23}/user-${index % 11}`;
    const fixedValue = STATIC_STRINGS[index & 3];
    if (fixedValue === undefined) throw new RangeError(`Missing static string ${index & 3}`);
    dynamic[index] = dynamicValue;
    fixed[index] = fixedValue;
    for (let char = 0; char < dynamicValue.length; char++) checksum = mix(checksum, dynamicValue.charCodeAt(char));
    for (let char = 0; char < fixedValue.length; char++) checksum = mix(checksum, fixedValue.charCodeAt(char));
  }
  return checksum;
}

function vocabularyStringSidecar(): number {
  const dynamic = new Array<string>(STRING_ROWS);
  const denseIds = new Uint32Array(STRING_ROWS);
  let checksum = 2_166_136_261;
  for (let index = 0; index < STRING_ROWS; index++) {
    const dynamicValue = `request-${index % 23}/user-${index % 11}`;
    const ordinal = index & 3;
    const fixedValue = STATIC_STRINGS[ordinal];
    if (fixedValue === undefined) throw new RangeError(`Missing static string ${ordinal}`);
    dynamic[index] = dynamicValue;
    denseIds[index] = requireNumber(STATIC_BINDING, ordinal, 'static vocabulary binding');
    for (let char = 0; char < dynamicValue.length; char++) checksum = mix(checksum, dynamicValue.charCodeAt(char));
    for (let char = 0; char < fixedValue.length; char++) checksum = mix(checksum, fixedValue.charCodeAt(char));
  }
  return checksum;
}

await validateGrowthAndPinning();

for (const capacity of CAPACITIES) {
  invariant((capacity & (capacity - 1)) === 0, 'capacity must be a power of two');
  const allocator = await createWasmAllocator({ capacity, initialPages: 17, maxPages: 256 });
  const numeric = makeNumericRunners(allocator, capacity);
  const expected = expectedNumericChecksum(WRITE_ITERATIONS, capacity);
  invariant(numeric.getterViews() === expected, 'getter-view numeric checksum mismatch');
  invariant(numeric.canonicalViews() === expected, 'canonical-view numeric checksum mismatch');
  invariant(numeric.pinnedEpoch() === expected, 'pinned-epoch numeric checksum mismatch');
  invariant(numeric.layout.messageLayoutFamily === 'mixed', 'CallsitePlan chose the wrong message family');
  invariant(numeric.layout.capacity === capacity, 'PhysicalLayoutPlan chose the wrong capacity');

  group(`WASM numeric exact layout | capacity=${capacity} rows=${WRITE_ITERATIONS}`, () => {
    summary(() => {
      bench('production/allocator-getter-views', () => do_not_optimize(numeric.getterViews())).baseline(true);
      bench('production/canonical-memory-views', () => do_not_optimize(numeric.canonicalViews()));
      bench('production/pinned-memory-epoch', () => do_not_optimize(numeric.pinnedEpoch()));
    });
  });

  const strategy = await WasmBufferStrategy.create<typeof RUNTIME_SCHEMA>({
    capacity,
    initialPages: 17,
    maxPages: 256,
  });
  const expectedOwnership = ownershipCycle(strategy, capacity, 1);
  invariant(expectedOwnership !== 0, 'production ownership checksum must be observable');
  group(`WASM root/child/overflow ownership | capacity=${capacity} cycles=${OWNERSHIP_ITERATIONS}`, () => {
    bench('production/topology-acquire-release', () =>
      do_not_optimize(ownershipCycle(strategy, capacity, OWNERSHIP_ITERATIONS)),
    ).baseline(true);
  });
}

const expectedStrings = stringChecksum();
invariant(dynamicStringSidecars() === expectedStrings, 'dynamic string sidecar checksum mismatch');
invariant(vocabularyStringSidecar() === expectedStrings, 'vocabulary string sidecar checksum mismatch');
group(`WASM string sidecar lifecycle | rows=${STRING_ROWS} vocabulary=${STATIC_STRINGS.length}`, () => {
  summary(() => {
    bench('production/dynamic-js-sidecars', () => do_not_optimize(dynamicStringSidecars())).baseline(true);
    bench('production/dense-vocabulary-plus-dynamic-sidecar', () => do_not_optimize(vocabularyStringSidecar()));
  });
});

const format = requestedFormat();
console.error(
  'WASM owned-memory preflight passed: PhysicalLayoutPlan exact slabs, memory-epoch pinning, topology release, and vocabulary checksums. ' +
    `quick=${QUICK}; capacities=${CAPACITIES.join(',')}; writes=${WRITE_ITERATIONS}; ownership cycles=${OWNERSHIP_ITERATIONS}; string rows=${STRING_ROWS}.`,
);
await run({ colors: format === 'mitata' && !process.argv.includes('--no-colors'), format, throw: true });
