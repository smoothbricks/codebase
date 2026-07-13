import { bench, do_not_optimize, group, run, summary } from 'mitata';
import { defineLogSchema, defineOpContext, JsBufferStrategy, TestTracer } from '../src/index.js';
import { createSpanBuffer } from '../src/lib/spanBuffer.js';
import { createTraceRoot as createEsTraceRoot, type TraceRoot as EsTraceRoot } from '../src/lib/traceRoot.es.js';
import { createTraceRoot as createNodeTraceRoot, type TraceRoot as NodeTraceRoot } from '../src/lib/traceRoot.node.js';
import type { AnySpanBuffer } from '../src/lib/types.js';
import { WasmBufferStrategy } from '../src/lib/wasm/WasmBufferStrategy.js';
import { createWasmTraceRoot } from '../src/lib/wasm/wasmTraceRoot.js';

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

const benchmarkContext = defineOpContext({ logSchema: defineLogSchema({}) });
const benchmarkOp = benchmarkContext.defineOp('timestamp-append', (ctx) => ctx.ok(undefined));
const plan = benchmarkOp.callsitePlan;

const nodeTracer = new TestTracer(benchmarkContext, {
  bufferStrategy: new JsBufferStrategy(),
  createTraceRoot: createNodeTraceRoot,
});
const esTracer = new TestTracer(benchmarkContext, {
  bufferStrategy: new JsBufferStrategy(),
  createTraceRoot: createEsTraceRoot,
});

function makeBuffer(traceRoot: NodeTraceRoot | EsTraceRoot, label: string): AnySpanBuffer {
  const buffer = createSpanBuffer(plan.schema, traceRoot, plan.metadata, CAPACITY, plan.SpanBufferClass);
  plan.appenders.writeSpanStart(buffer, label);
  return buffer;
}

function requireEntryTypes(buffer: AnySpanBuffer): Uint8Array {
  const entryTypes = buffer.entry_type;
  if (entryTypes === undefined) throw new Error('Timestamp benchmark requires the entry_type lane');
  return entryTypes;
}

interface Variant {
  readonly label: string;
  readonly buffer: AnySpanBuffer;
  readonly appendBatch: () => number;
}

function nodeBeforeModel(root: NodeTraceRoot, buffer: AnySpanBuffer): number {
  const anchorHrtime = root.anchorEpochNanos - root._epochHrtimeOffset;
  const entryTypes = requireEntryTypes(buffer);
  buffer._writeIndex = 2;
  let checksum = 0;
  for (let row = 0; row < ROWS; row++) {
    const index = buffer._writeIndex;
    buffer.timestamp[index] = root.anchorEpochNanos + (process.hrtime.bigint() - anchorHrtime);
    entryTypes[index] = ENTRY_TYPE;
    buffer._writeIndex = index + 1;
    checksum = Math.imul(checksum ^ index ^ ENTRY_TYPE, 16_777_619) >>> 0;
  }
  return checksum;
}

function planCandidate(buffer: AnySpanBuffer): number {
  const append = plan.appenders.writeLogEntry;
  buffer._writeIndex = 2;
  let checksum = 0;
  for (let row = 0; row < ROWS; row++) {
    const index = append(buffer, ENTRY_TYPE);
    checksum = Math.imul(checksum ^ index ^ ENTRY_TYPE, 16_777_619) >>> 0;
  }
  return checksum;
}

function esBeforeModel(root: EsTraceRoot, buffer: AnySpanBuffer): number {
  const entryTypes = requireEntryTypes(buffer);
  buffer._writeIndex = 2;
  let checksum = 0;
  for (let row = 0; row < ROWS; row++) {
    const index = buffer._writeIndex;
    const elapsedNanos = BigInt(Math.floor((performance.now() - root.anchorPerfNow) * 1000)) * 1000n;
    buffer.timestamp[index] = root.anchorEpochNanos + elapsedNanos;
    entryTypes[index] = ENTRY_TYPE;
    buffer._writeIndex = index + 1;
    checksum = Math.imul(checksum ^ index ^ ENTRY_TYPE, 16_777_619) >>> 0;
  }
  return checksum;
}

function semanticChecksum(variant: Variant): number {
  const checksum = variant.appendBatch();
  const { buffer } = variant;
  if (buffer._writeIndex !== CAPACITY) throw new Error(`${variant.label}: wrong write index ${buffer._writeIndex}`);
  let previous = 0n;
  let semantic = checksum;
  const entryTypes = requireEntryTypes(buffer);
  for (let index = 2; index < CAPACITY; index++) {
    const timestamp = buffer.timestamp[index];
    const entryType = entryTypes[index];
    if (timestamp === undefined || timestamp <= 0n || timestamp < previous) {
      throw new Error(`${variant.label}: non-monotonic timestamp at row ${index}`);
    }
    if (entryType !== ENTRY_TYPE) throw new Error(`${variant.label}: wrong entry type at row ${index}`);
    previous = timestamp;
    semantic = Math.imul(semantic ^ index ^ entryType, 16_777_619) >>> 0;
  }
  return semantic;
}

const nodeBeforeRoot = createNodeTraceRoot('timestamp-node-before', nodeTracer);
const nodeCandidateRoot = createNodeTraceRoot('timestamp-node-candidate', nodeTracer);
const esBeforeRoot = createEsTraceRoot('timestamp-es-before', esTracer);
const esCandidateRoot = createEsTraceRoot('timestamp-es-candidate', esTracer);
const nodeBeforeBuffer = makeBuffer(nodeBeforeRoot, 'node-before');
const nodeCandidateBuffer = makeBuffer(nodeCandidateRoot, 'node-candidate');
const esBeforeBuffer = makeBuffer(esBeforeRoot, 'es-before');
const esCandidateBuffer = makeBuffer(esCandidateRoot, 'es-candidate');

const nodeBefore: Variant = {
  label: `before-model/node exact epoch ns [${clockLabel}]`,
  buffer: nodeBeforeBuffer,
  appendBatch: () => nodeBeforeModel(nodeBeforeRoot, nodeBeforeBuffer),
};
const nodeCandidate: Variant = {
  label: `production/CallsitePlan appender [${clockLabel}]`,
  buffer: nodeCandidateBuffer,
  appendBatch: () => planCandidate(nodeCandidateBuffer),
};
const esBefore: Variant = {
  label: 'before-model/es performance.now microsecond-normalized ns',
  buffer: esBeforeBuffer,
  appendBatch: () => esBeforeModel(esBeforeRoot, esBeforeBuffer),
};
const esCandidate: Variant = {
  label: 'production/CallsitePlan appender microsecond-normalized ns',
  buffer: esCandidateBuffer,
  appendBatch: () => planCandidate(esCandidateBuffer),
};

const nodeSemantic = semanticChecksum(nodeBefore);
if (semanticChecksum(nodeCandidate) !== nodeSemantic) {
  throw new Error('Node before/CallsitePlan semantic checksum mismatch');
}
const esSemantic = semanticChecksum(esBefore);
if (semanticChecksum(esCandidate) !== esSemantic) {
  throw new Error('ES before/CallsitePlan semantic checksum mismatch');
}
console.log(`timestamp semantic checksums: node=${nodeSemantic} es=${esSemantic}; rows/sample=${ROWS}`);

for (const comparison of [
  { name: 'node timestamp append', before: nodeBefore, candidate: nodeCandidate },
  { name: 'es timestamp append', before: esBefore, candidate: esCandidate },
]) {
  summary(() => {
    group(`${comparison.name} [phase=warmed-entry-write, rows=${ROWS}]`, () => {
      bench(comparison.before.label, () => do_not_optimize(comparison.before.appendBatch())).baseline();
      bench(comparison.candidate.label, () => do_not_optimize(comparison.candidate.appendBatch()));
    });
  });
}

const wasmCapacity = QUICK ? 64 : 512;
const wasmStrategy = await WasmBufferStrategy.create<typeof plan.schema>({
  capacity: wasmCapacity,
  initialPages: 17,
  maxPages: 32,
});
const wasmTracer = new TestTracer(benchmarkContext, {
  bufferStrategy: wasmStrategy,
  createTraceRoot: (traceId, tracer) => createWasmTraceRoot(wasmStrategy.allocator, traceId, tracer),
});
const wasmRoot = createWasmTraceRoot(wasmStrategy.allocator, 'timestamp-wasm-benchmark', wasmTracer);
const wasmBuffer = wasmStrategy.createSpanBuffer(
  plan.schema,
  wasmRoot,
  plan.metadata,
  wasmCapacity,
  plan.SpanBufferClass,
);

function wasmBatch(candidate: boolean): number {
  plan.appenders.writeSpanStart(wasmBuffer, 'timestamp-wasm');
  let checksum = 0;
  for (let row = 0; row < ROWS; row++) {
    const index = candidate
      ? plan.appenders.writeLogEntry(wasmBuffer, ENTRY_TYPE)
      : wasmRoot.writeLogEntry(wasmBuffer, ENTRY_TYPE);
    checksum = Math.imul(checksum ^ index ^ ENTRY_TYPE, 16_777_619) >>> 0;
  }
  return checksum;
}

const wasmBeforeSemantic = wasmBatch(false);
const wasmCandidateSemantic = wasmBatch(true);
if (wasmCandidateSemantic !== wasmBeforeSemantic) {
  throw new Error('WASM TraceRoot/CallsitePlan semantic checksum mismatch');
}
let wasmPrevious = 0n;
const wasmEntryTypes = requireEntryTypes(wasmBuffer);
for (let index = 2; index < ROWS + 2; index++) {
  const timestamp = wasmBuffer.timestamp[index];
  const entryType = wasmEntryTypes[index];
  if (timestamp === undefined || timestamp <= 0n || timestamp < wasmPrevious) {
    throw new Error(`WASM non-monotonic timestamp at row ${index}`);
  }
  if (entryType !== ENTRY_TYPE) throw new Error(`WASM wrong entry type at row ${index}`);
  wasmPrevious = timestamp;
}
console.log(`WASM timestamp semantic checksum=${wasmCandidateSemantic}`);

summary(() => {
  group(`wasm timestamp append [phase=warmed-entry-write, rows=${ROWS}]`, () => {
    bench('TraceRoot method to WASM allocator', () => do_not_optimize(wasmBatch(false))).baseline();
    bench('CallsitePlan appender to WASM allocator', () => do_not_optimize(wasmBatch(true)));
  });
});

const format = process.argv.includes('--json') ? 'json' : process.argv.includes('--markdown') ? 'markdown' : 'mitata';
await run({ format, colors: format === 'mitata' });
wasmStrategy.releaseBuffer(wasmBuffer);
