import { bench, do_not_optimize, group, run, summary } from 'mitata';
import { createTraceRoot as createEsTraceRoot, type TraceRoot as EsTraceRoot } from '../src/lib/traceRoot.es.js';
import { createTraceRoot as createNodeTraceRoot, type TraceRoot as NodeTraceRoot } from '../src/lib/traceRoot.node.js';
import type { TracerLifecycleHooks } from '../src/lib/traceRoot.js';
import type { AnySpanBuffer } from '../src/lib/types.js';
import { createWasmAllocator } from '../src/lib/wasm/wasmAllocator.js';
import { createWasmTraceRoot, type WasmSpanBufferLike } from '../src/lib/wasm/wasmTraceRoot.js';

const QUICK = process.argv.includes('--quick');
const ROWS = QUICK ? 32 : 256;
const CAPACITY = ROWS + 2;
const ENTRY_TYPE = 6;

const clockLabel =
  process.platform === 'darwin'
    ? 'mach_absolute_time via process.hrtime.bigint()'
    : process.platform === 'linux'
      ? 'clock_gettime(CLOCK_MONOTONIC) via process.hrtime.bigint()'
      : `${process.platform} monotonic clock via process.hrtime.bigint()`;

const tracer = {} as TracerLifecycleHooks;

function makeBuffer(traceRoot: NodeTraceRoot | EsTraceRoot): AnySpanBuffer {
  const timestamp = new BigInt64Array(CAPACITY);
  const entry_type = new Uint8Array(CAPACITY);
  return {
    _traceRoot: traceRoot,
    timestamp,
    entry_type,
    _writeIndex: 2,
    _capacity: CAPACITY,
  } as AnySpanBuffer;
}

interface Variant {
  readonly label: string;
  readonly buffer: AnySpanBuffer;
  readonly appendBatch: () => number;
}

function nodeBeforeModel(root: NodeTraceRoot, buffer: AnySpanBuffer): number {
  const anchorHrtime = root.anchorEpochNanos - root._epochHrtimeOffset;
  buffer._writeIndex = 2;
  let checksum = 0;
  for (let row = 0; row < ROWS; row++) {
    const idx = buffer._writeIndex;
    buffer.timestamp[idx] = root.anchorEpochNanos + (process.hrtime.bigint() - anchorHrtime);
    buffer.entry_type[idx] = ENTRY_TYPE;
    buffer._writeIndex = idx + 1;
    checksum = Math.imul(checksum ^ idx ^ ENTRY_TYPE, 16_777_619) >>> 0;
  }
  return checksum;
}

function nodeCandidate(root: NodeTraceRoot, buffer: AnySpanBuffer): number {
  const append = root._appendLogEntry;
  buffer._writeIndex = 2;
  let checksum = 0;
  for (let row = 0; row < ROWS; row++) {
    const idx = append(root, buffer, ENTRY_TYPE);
    checksum = Math.imul(checksum ^ idx ^ ENTRY_TYPE, 16_777_619) >>> 0;
  }
  return checksum;
}

function esBeforeModel(root: EsTraceRoot, buffer: AnySpanBuffer): number {
  buffer._writeIndex = 2;
  let checksum = 0;
  for (let row = 0; row < ROWS; row++) {
    const idx = buffer._writeIndex;
    const elapsedNanos = BigInt(Math.floor((performance.now() - root.anchorPerfNow) * 1000)) * 1000n;
    buffer.timestamp[idx] = root.anchorEpochNanos + elapsedNanos;
    buffer.entry_type[idx] = ENTRY_TYPE;
    buffer._writeIndex = idx + 1;
    checksum = Math.imul(checksum ^ idx ^ ENTRY_TYPE, 16_777_619) >>> 0;
  }
  return checksum;
}

function esCandidate(root: EsTraceRoot, buffer: AnySpanBuffer): number {
  const append = root._appendLogEntry;
  buffer._writeIndex = 2;
  let checksum = 0;
  for (let row = 0; row < ROWS; row++) {
    const idx = append(root, buffer, ENTRY_TYPE);
    checksum = Math.imul(checksum ^ idx ^ ENTRY_TYPE, 16_777_619) >>> 0;
  }
  return checksum;
}

function semanticChecksum(variant: Variant): number {
  const checksum = variant.appendBatch();
  const { buffer } = variant;
  if (buffer._writeIndex !== CAPACITY) throw new Error(`${variant.label}: wrong write index ${buffer._writeIndex}`);
  let previous = 0n;
  let semantic = checksum;
  for (let idx = 2; idx < CAPACITY; idx++) {
    const timestamp = buffer.timestamp[idx]!;
    if (timestamp <= 0n || timestamp < previous) throw new Error(`${variant.label}: non-monotonic timestamp at row ${idx}`);
    if (buffer.entry_type[idx] !== ENTRY_TYPE) throw new Error(`${variant.label}: wrong entry type at row ${idx}`);
    previous = timestamp;
    semantic = Math.imul(semantic ^ idx ^ buffer.entry_type[idx]!, 16_777_619) >>> 0;
  }
  return semantic;
}

const nodeRoot = createNodeTraceRoot('timestamp-node-benchmark', tracer);
const esRoot = createEsTraceRoot('timestamp-es-benchmark', tracer);
const variants: readonly Variant[] = [
  {
    label: `before-model/node exact epoch ns [${clockLabel}]`,
    buffer: makeBuffer(nodeRoot),
    appendBatch: () => nodeBeforeModel(nodeRoot, variants[0]!.buffer),
  },
  {
    label: `production-candidate/node fixed primitive [${clockLabel}]`,
    buffer: makeBuffer(nodeRoot),
    appendBatch: () => nodeCandidate(nodeRoot, variants[1]!.buffer),
  },
  {
    label: 'before-model/es performance.now microsecond-normalized ns',
    buffer: makeBuffer(esRoot),
    appendBatch: () => esBeforeModel(esRoot, variants[2]!.buffer),
  },
  {
    label: 'production-candidate/es fixed primitive microsecond-normalized ns',
    buffer: makeBuffer(esRoot),
    appendBatch: () => esCandidate(esRoot, variants[3]!.buffer),
  },
];

const nodeSemantic = semanticChecksum(variants[0]!);
if (semanticChecksum(variants[1]!) !== nodeSemantic) throw new Error('Node before/candidate semantic checksum mismatch');
const esSemantic = semanticChecksum(variants[2]!);
if (semanticChecksum(variants[3]!) !== esSemantic) throw new Error('ES before/candidate semantic checksum mismatch');
console.log(`timestamp semantic checksums: node=${nodeSemantic} es=${esSemantic}; rows/sample=${ROWS}`);

for (const [name, before, candidate] of [
  ['node timestamp append', variants[0]!, variants[1]!],
  ['es timestamp append', variants[2]!, variants[3]!],
] as const) {
  summary(() => {
    group(`${name} [phase=warmed-entry-write, rows=${ROWS}]`, () => {
      bench(before.label, () => do_not_optimize(before.appendBatch())).baseline();
      bench(candidate.label, () => do_not_optimize(candidate.appendBatch()));
    });
  });
}

const wasmCapacity = QUICK ? 64 : 512;
const wasmAllocator = await createWasmAllocator({ capacity: wasmCapacity, initialPages: 17, maxPages: 32 });
const wasmRoot = createWasmTraceRoot(wasmAllocator, 'timestamp-wasm-benchmark', tracer);
const wasmSystemPtr = wasmAllocator.allocSpanSystem();
const wasmIdentityPtr = wasmAllocator.allocIdentityChild();
if (wasmSystemPtr === 0 || wasmIdentityPtr === 0) throw new Error('WASM timestamp benchmark allocation failed');
const wasmBuffer = {
  _systemPtr: wasmSystemPtr,
  _identityPtr: wasmIdentityPtr,
  _identityOwner: true,
} as WasmSpanBufferLike & AnySpanBuffer;

function wasmBatch(candidate: boolean): number {
  wasmAllocator.spanStart(wasmSystemPtr, wasmIdentityPtr, wasmRoot._traceRootPtr, wasmCapacity);
  const append = wasmRoot._appendLogEntry;
  let checksum = 0;
  for (let row = 0; row < ROWS; row++) {
    const idx = candidate
      ? append(wasmRoot, wasmBuffer, ENTRY_TYPE)
      : wasmRoot.writeLogEntry(wasmBuffer, ENTRY_TYPE);
    checksum = Math.imul(checksum ^ idx ^ ENTRY_TYPE, 16_777_619) >>> 0;
  }
  return checksum;
}

const wasmBeforeSemantic = wasmBatch(false);
const wasmCandidateSemantic = wasmBatch(true);
if (wasmCandidateSemantic !== wasmBeforeSemantic) throw new Error('WASM before/candidate semantic checksum mismatch');
let wasmPrevious = 0n;
for (let idx = 2; idx < ROWS + 2; idx++) {
  const timestamp = wasmAllocator.readTimestamp(wasmSystemPtr, idx, wasmCapacity);
  if (timestamp <= 0n || timestamp < wasmPrevious) throw new Error(`WASM non-monotonic timestamp at row ${idx}`);
  if (wasmAllocator.readEntryType(wasmSystemPtr, idx, wasmCapacity) !== ENTRY_TYPE) {
    throw new Error(`WASM wrong entry type at row ${idx}`);
  }
  wasmPrevious = timestamp;
}
console.log(`WASM timestamp semantic checksum=${wasmCandidateSemantic}`);

summary(() => {
  group(`wasm timestamp append [phase=warmed-entry-write, rows=${ROWS}]`, () => {
    bench('before-model/TraceRoot method to WASM allocator', () => do_not_optimize(wasmBatch(false))).baseline();
    bench('production-candidate/fixed primitive to WASM allocator', () => do_not_optimize(wasmBatch(true)));
  });
});

const format = process.argv.includes('--json') ? 'json' : process.argv.includes('--markdown') ? 'markdown' : 'mitata';
await run({ format, colors: format === 'mitata' });
