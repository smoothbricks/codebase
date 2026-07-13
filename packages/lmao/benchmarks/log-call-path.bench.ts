/**
 * Mitata benchmark for the LMAO log hot path.
 *
 * Run a short smoke pass with:
 *   bun packages/lmao/benchmarks/log-call-path.bench.ts --quick
 */

import { bench, do_not_optimize, group, run, summary } from 'mitata';
import type { WriterState } from '../src/lib/codegen/fixedPositionWriterGenerator.js';
import { createSpanLogger, type SpanLoggerImpl } from '../src/lib/codegen/spanLoggerGenerator.js';
import { JsBufferStrategy } from '../src/lib/JsBufferStrategy.js';
import { DEFAULT_METADATA } from '../src/lib/opContext/defineOp.js';
import type { OpContext } from '../src/lib/opContext/types.js';
import { getPhysicalLayoutPlan, sealCallsitePlan } from '../src/lib/physicalLayoutPlan.js';
import { resolveMessage } from '../src/lib/resolveMessage.js';
import { RUNTIME_HINT_ANALYZED_VALID, RUNTIME_HINT_FULL_CAPABILITIES } from '../src/lib/runtimeHint.js';
import { S } from '../src/lib/schema/builder.js';
import { LogSchema } from '../src/lib/schema/LogSchema.js';
import { ENTRY_TYPE_INFO, mergeWithSystemSchema } from '../src/lib/schema/systemSchema.js';
import { createOverflowBuffer, createSpanBuffer, getSpanBufferClass } from '../src/lib/spanBuffer.js';
import { createSpanContextClass } from '../src/lib/spanContext.js';
import type { TracerLifecycleHooks } from '../src/lib/traceRoot.js';
import { createTraceRoot } from '../src/lib/traceRoot.node.js';
import type { SpanBuffer } from '../src/lib/types.js';
import { registerBenchmarkVocabulary } from './vocabularyFixture.js';

type MessageMode = 'literal' | 'dynamic';
type AppendMode = 'trace-root-method' | 'inline';
type OverflowMode = 'method' | 'inline-compare';
type MessageBranchMode = 'optional' | 'required';
type AccountingMode = 'per-entry' | 'segment';

const LOG_COUNTS: readonly number[] = [0, 1, 50];
const LOG_COUNT_ARGUMENT = process.argv.find((argument) => argument.startsWith('--logs='));
const SELECTED_LOG_COUNT =
  LOG_COUNT_ARGUMENT === undefined ? undefined : Number(LOG_COUNT_ARGUMENT.slice('--logs='.length));
const LOG_COUNT_FILTER =
  SELECTED_LOG_COUNT === undefined ? LOG_COUNTS : LOG_COUNTS.filter((count) => count === SELECTED_LOG_COUNT);
if (LOG_COUNT_ARGUMENT !== undefined && LOG_COUNT_FILTER.length === 0) {
  throw new Error(`Unsupported ${LOG_COUNT_ARGUMENT}; expected 0, 1, or 50`);
}
const MODE_ARGUMENT = process.argv.find((argument) => argument.startsWith('--mode='));
const SELECTED_MODE = MODE_ARGUMENT?.slice('--mode='.length);
const MESSAGE_MODE_VALUES: readonly MessageMode[] = ['literal', 'dynamic'];
const MESSAGE_MODES = MESSAGE_MODE_VALUES.filter((mode) => SELECTED_MODE === undefined || mode === SELECTED_MODE);
if (MODE_ARGUMENT !== undefined && MESSAGE_MODES.length === 0) {
  throw new Error(`Unsupported ${MODE_ARGUMENT}; expected literal or dynamic`);
}
const LITERAL_MESSAGE = 'request accepted';
const LITERAL_BINDING = registerBenchmarkVocabulary([LITERAL_MESSAGE]);
const LITERAL_VOCABULARY_INDEX = LITERAL_BINDING[0];
if (LITERAL_VOCABULARY_INDEX === undefined) throw new Error('Literal vocabulary binding is missing');
const LINE = 137;
const QUICK = process.argv.includes('--quick');
const REQUESTS_PER_SAMPLE = QUICK ? 4 : 200;
const CAPACITY = 8;

const schema = new LogSchema(mergeWithSystemSchema({ marker: S.number() }));
type BenchmarkContext = OpContext<typeof schema>;
type BenchmarkSpanBuffer = SpanBuffer<typeof schema>;

const SpanBufferClass = getSpanBufferClass(schema);
const SpanContextClass = createSpanContextClass<BenchmarkContext>(schema, { logSchema: schema });
const physicalLayoutPlan = getPhysicalLayoutPlan<typeof schema, BenchmarkContext>(
  SpanBufferClass,
  RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_FULL_CAPABILITIES | CAPACITY,
  SpanContextClass,
);
const BENCHMARK_METADATA = Object.freeze({ ...DEFAULT_METADATA, _physicalLayoutPlan: physicalLayoutPlan });
const callsitePlan = sealCallsitePlan(physicalLayoutPlan, BENCHMARK_METADATA);

interface BenchmarkWriterState extends WriterState {
  readonly _spanBuffer: BenchmarkSpanBuffer;
  _buffer: BenchmarkSpanBuffer;
  _spanLogger?: SpanLoggerImpl<typeof schema>;
}

function requireBenchmarkSpanBuffer(buffer: unknown): BenchmarkSpanBuffer {
  if (!(buffer instanceof SpanBufferClass)) {
    throw new TypeError('Benchmark tracer received a buffer from a different schema');
  }
  return buffer;
}

function createBenchmarkSpanLogger(root: BenchmarkSpanBuffer): {
  logger: SpanLoggerImpl<typeof schema>;
  state: BenchmarkWriterState;
} {
  const state: BenchmarkWriterState = {
    _spanBuffer: root,
    _buffer: root,
    _appendLogEntry: callsitePlan.appendLogEntry,
    _physicalLayoutPlan: callsitePlan,
    _appendWriterEntry(entryType: number): number {
      if (state._buffer._writeIndex >= state._buffer._capacity) {
        state._buffer = requireBenchmarkSpanBuffer(state._buffer.getOrCreateOverflow());
        state._spanLogger?._prefillScopedAttributesOn(state._buffer);
      }
      return state._appendLogEntry(state._buffer._traceRoot, state._buffer, entryType);
    },
  };
  const logger = createSpanLogger(schema, state);
  state._spanLogger = logger;
  return { logger, state };
}

interface Observation {
  semantic: string;
  rows: number;
  writes: number;
  buffersCreated: number;
  writerObjects: number;
  dynamicMessageObjects: number;
  statsUpdateOps: number;
  segmentAccountingOps: number;
}

interface Counters {
  buffersCreated: number;
  writerObjects: number;
  dynamicMessageObjects: number;
  statsUpdateOps: number;
  segmentAccountingOps: number;
}

function emptyCounters(): Counters {
  return {
    buffersCreated: 0,
    writerObjects: 0,
    dynamicMessageObjects: 0,
    statsUpdateOps: 0,
    segmentAccountingOps: 0,
  };
}

const bufferStrategy = new JsBufferStrategy<typeof schema>();
const tracer: TracerLifecycleHooks<typeof schema> = {
  bufferStrategy,
  onTraceStart() {},
  onTraceEnd() {},
  onSpanStart() {},
  onSpanEnd() {},
  onStatsWillResetFor(buffer) {
    const stats = requireBenchmarkSpanBuffer(buffer)._stats;
    stats.totalWrites = 0;
    stats.spansCreated = 1;
    stats.capacity = CAPACITY;
  },
  getFlagEvaluatorForContext() {
    return undefined;
  },
};

function resetSharedStats(): void {
  const stats = getSpanBufferClass(schema).stats;
  stats.capacity = CAPACITY;
  stats.totalWrites = 0;
  stats.spansCreated = 0;
}

function newRootBuffer(_mode: MessageMode, counters: Counters): BenchmarkSpanBuffer {
  resetSharedStats();
  const root = createTraceRoot('log-call-path', tracer);
  counters.buffersCreated++;
  const buffer = createSpanBuffer(schema, root, BENCHMARK_METADATA, CAPACITY);
  buffer._writeIndex = 2;
  return buffer;
}

function messageFor(mode: MessageMode, request: number, log: number, counters: Counters): string {
  if (mode === 'literal') return LITERAL_MESSAGE;
  counters.dynamicMessageObjects++;
  return `request-${request}-log-${log}`;
}

function setLine(buffer: BenchmarkSpanBuffer, row: number): void {
  buffer.line_values[row] = LINE;
  if (buffer.line_nulls) {
    buffer.line_nulls[row >> 3] |= 1 << (row & 7);
  }
}

function resolvedMessage(buffer: BenchmarkSpanBuffer, row: number): string {
  return resolveMessage(buffer, row) ?? '';
}

function mix(hash: number, value: number): number {
  return Math.imul(hash ^ value, 16_777_619) >>> 0;
}

function mixString(hash: number, value: string): number {
  let result = hash;
  for (let index = 0; index < value.length; index++) result = mix(result, value.charCodeAt(index));
  return result;
}

/** Hashes all non-timestamp row data and counts physical writes/buffers. */
function inspectRequest(root: BenchmarkSpanBuffer): { hash: number; rows: number; buffers: number } {
  let hash = 2_166_136_261;
  let rows = 0;
  let buffers = 0;
  let buffer: BenchmarkSpanBuffer | undefined = root;
  while (buffer) {
    buffers++;
    for (let row = buffer === root ? 2 : 0; row < buffer._writeIndex; row++) {
      const entryType = buffer.entry_type?.[row];
      if (entryType === undefined) throw new Error(`Missing entry type at row ${row}`);
      hash = mix(hash, entryType);
      hash = mixString(hash, resolvedMessage(buffer, row));
      hash = mix(hash, buffer.line_values[row] ?? 0);
      rows++;
    }
    buffer = buffer._overflow;
  }
  return { hash, rows, buffers };
}

function finalize(counters: Counters, hash: number, rows: number, writes: number): Observation {
  return {
    semantic: `${rows}:${writes}:${hash >>> 0}`,
    rows,
    writes,
    ...counters,
  };
}

function runCurrentFluent(logCount: number, mode: MessageMode): Observation {
  const counters = emptyCounters();
  let hash = 2_166_136_261;
  let rows = 0;
  for (let request = 0; request < REQUESTS_PER_SAMPLE; request++) {
    const root = newRootBuffer(mode, counters);
    const { logger } = createBenchmarkSpanLogger(root);
    counters.writerObjects++;
    for (let log = 0; log < logCount; log++) {
      logger.info(messageFor(mode, request, log, counters)).line(LINE);
      counters.statsUpdateOps++;
    }
    const observed = inspectRequest(root);
    hash = mix(hash, observed.hash);
    rows += observed.rows;
    counters.buffersCreated += observed.buffers - 1;
  }
  return finalize(counters, hash, rows, rows);
}

/** The current compile-time literal shape: dense vocabulary header followed by injected line output. */
function runCurrentTransformerOutput(logCount: number): Observation {
  const counters = emptyCounters();
  let hash = 2_166_136_261;
  let rows = 0;
  for (let request = 0; request < REQUESTS_PER_SAMPLE; request++) {
    const root = newRootBuffer('literal', counters);
    const { logger } = createBenchmarkSpanLogger(root);
    counters.writerObjects++;
    for (let log = 0; log < logCount; log++) {
      logger._infoTemplate(LITERAL_VOCABULARY_INDEX).line(LINE);
      counters.statsUpdateOps++;
    }
    const observed = inspectRequest(root);
    hash = mix(hash, observed.hash);
    rows += observed.rows;
    counters.buffersCreated += observed.buffers - 1;
  }
  return finalize(counters, hash, rows, rows);
}

interface ModelOptions {
  append: AppendMode;
  overflow: OverflowMode;
  messageBranch: MessageBranchMode;
  accounting: AccountingMode;
}

/** Isolated challenger model. It writes real SpanBuffers but is not a production API. */
function runModel(logCount: number, mode: MessageMode, options: ModelOptions): Observation {
  const counters = emptyCounters();
  let hash = 2_166_136_261;
  let rows = 0;
  for (let request = 0; request < REQUESTS_PER_SAMPLE; request++) {
    const root = newRootBuffer(mode, counters);
    root._writeIndex = 2;
    let buffer = root;
    let segmentWrites = 0;
    for (let log = 0; log < logCount; log++) {
      if (buffer._writeIndex >= buffer._capacity) {
        if (options.accounting === 'segment' && segmentWrites !== 0) {
          SpanBufferClass.stats.totalWrites += segmentWrites;
          counters.statsUpdateOps++;
          counters.segmentAccountingOps++;
          segmentWrites = 0;
        }
        if (options.overflow === 'method') {
          buffer = requireBenchmarkSpanBuffer(buffer.getOrCreateOverflow());
        } else {
          buffer = buffer._overflow ?? createOverflowBuffer(buffer);
        }
        counters.buffersCreated++;
      }

      let row: number;
      if (options.append === 'trace-root-method') {
        row = buffer._traceRoot.writeLogEntry(buffer, ENTRY_TYPE_INFO);
      } else {
        row = buffer._writeIndex;
        buffer.timestamp[row] = buffer._traceRoot.getTimestampNanos();
        const entryTypes = buffer.entry_type;
        if (entryTypes === undefined) throw new TypeError('Benchmark buffer has no entry-type lane');
        entryTypes[row] = ENTRY_TYPE_INFO;
        buffer._writeIndex = row + 1;
      }

      const message = messageFor(mode, request, log, counters);
      const messageValues = buffer.message_values;
      const messageNulls = buffer.message_nulls;
      if (options.messageBranch === 'optional') {
        if (messageValues !== undefined) {
          messageValues[row] = message;
          if (messageNulls !== undefined) messageNulls[row >> 3] |= 1 << (row & 7);
        }
      } else {
        if (messageValues === undefined) throw new TypeError('Benchmark buffer has no message lane');
        messageValues[row] = message;
        if (messageNulls !== undefined) messageNulls[row >> 3] |= 1 << (row & 7);
      }
      setLine(buffer, row);

      if (options.accounting === 'per-entry') {
        SpanBufferClass.stats.totalWrites++;
        counters.statsUpdateOps++;
      } else {
        segmentWrites++;
      }
    }
    if (options.accounting === 'segment' && segmentWrites !== 0) {
      SpanBufferClass.stats.totalWrites += segmentWrites;
      counters.statsUpdateOps++;
      counters.segmentAccountingOps++;
    }
    const observed = inspectRequest(root);
    hash = mix(hash, observed.hash);
    rows += observed.rows;
  }
  return finalize(counters, hash, rows, rows);
}
function newTimedRoot(_mode: MessageMode): BenchmarkSpanBuffer {
  resetSharedStats();
  const buffer = createSpanBuffer(schema, createTraceRoot('log-call-path', tracer), BENCHMARK_METADATA, CAPACITY);
  buffer._writeIndex = 2;
  return buffer;
}

function timedMessage(mode: MessageMode, request: number, log: number): string {
  return mode === 'literal' ? LITERAL_MESSAGE : `request-${request}-log-${log}`;
}

function consumeFinalBuffer(buffer: BenchmarkSpanBuffer): number {
  const row = buffer._writeIndex - 1;
  const messageLength = buffer.message_values?.[row]?.length ?? 0;
  const messageIdentity = buffer._logHeaders?.[row] ?? buffer._messageIds?.[row] ?? buffer._rowHeaders?.[row] ?? 0;
  return buffer._writeIndex + (buffer.entry_type?.[row] ?? 0) + messageLength + messageIdentity;
}

function timeCurrentFluent(logCount: number, mode: MessageMode): number {
  let sink = 0;
  for (let request = 0; request < REQUESTS_PER_SAMPLE; request++) {
    const { logger, state } = createBenchmarkSpanLogger(newTimedRoot(mode));
    for (let log = 0; log < logCount; log++) {
      logger.info(timedMessage(mode, request, log)).line(LINE);
    }
    sink ^= consumeFinalBuffer(state._buffer);
  }
  return sink;
}

function timeCurrentTransformerOutput(logCount: number): number {
  let sink = 0;
  for (let request = 0; request < REQUESTS_PER_SAMPLE; request++) {
    const { logger, state } = createBenchmarkSpanLogger(newTimedRoot('literal'));
    for (let log = 0; log < logCount; log++) logger._infoTemplate(LITERAL_VOCABULARY_INDEX).line(LINE);
    sink ^= consumeFinalBuffer(state._buffer);
  }
  return sink;
}

function timeModel(logCount: number, mode: MessageMode, options: ModelOptions): number {
  let sink = 0;
  for (let request = 0; request < REQUESTS_PER_SAMPLE; request++) {
    let buffer = newTimedRoot(mode);
    buffer._writeIndex = 2;
    let segmentWrites = 0;
    for (let log = 0; log < logCount; log++) {
      if (buffer._writeIndex >= buffer._capacity) {
        if (options.accounting === 'segment' && segmentWrites !== 0) {
          SpanBufferClass.stats.totalWrites += segmentWrites;
          segmentWrites = 0;
        }
        buffer =
          options.overflow === 'method'
            ? requireBenchmarkSpanBuffer(buffer.getOrCreateOverflow())
            : (buffer._overflow ?? createOverflowBuffer(buffer));
      }

      let row: number;
      if (options.append === 'trace-root-method') {
        row = buffer._traceRoot.writeLogEntry(buffer, ENTRY_TYPE_INFO);
      } else {
        row = buffer._writeIndex;
        buffer.timestamp[row] = buffer._traceRoot.getTimestampNanos();
        const entryTypes = buffer.entry_type;
        if (entryTypes === undefined) throw new TypeError('Benchmark buffer has no entry-type lane');
        entryTypes[row] = ENTRY_TYPE_INFO;
        buffer._writeIndex = row + 1;
      }

      const message = timedMessage(mode, request, log);
      const messageValues = buffer.message_values;
      const messageNulls = buffer.message_nulls;
      if (options.messageBranch === 'optional') {
        if (messageValues !== undefined) {
          messageValues[row] = message;
          if (messageNulls !== undefined) messageNulls[row >> 3] |= 1 << (row & 7);
        }
      } else {
        if (messageValues === undefined) throw new TypeError('Benchmark buffer has no message lane');
        messageValues[row] = message;
        if (messageNulls !== undefined) messageNulls[row >> 3] |= 1 << (row & 7);
      }
      setLine(buffer, row);

      if (options.accounting === 'per-entry') SpanBufferClass.stats.totalWrites++;
      else segmentWrites++;
    }
    if (options.accounting === 'segment' && segmentWrites !== 0) {
      SpanBufferClass.stats.totalWrites += segmentWrites;
    }
    sink ^= consumeFinalBuffer(buffer);
  }
  return sink;
}

const MODEL_CURRENT: ModelOptions = {
  append: 'trace-root-method',
  overflow: 'method',
  messageBranch: 'optional',
  accounting: 'per-entry',
};

interface ScenarioVariant {
  label: string;
  verify: () => Observation;
  timed: () => number;
}

function variantsFor(logCount: number, mode: MessageMode): ScenarioVariant[] {
  const variants: ScenarioVariant[] = [
    {
      label: 'current/fluent-info-line',
      verify: () => runCurrentFluent(logCount, mode),
      timed: () => timeCurrentFluent(logCount, mode),
    },
  ];
  if (mode === 'literal') {
    variants.push({
      label: 'current/transformer-dense-header-output',
      verify: () => runCurrentTransformerOutput(logCount),
      timed: () => timeCurrentTransformerOutput(logCount),
    });
  }
  const inlineOverflow: ModelOptions = { ...MODEL_CURRENT, overflow: 'inline-compare' };
  const inlineAppend: ModelOptions = { ...MODEL_CURRENT, append: 'inline' };
  const requiredMessage: ModelOptions = { ...MODEL_CURRENT, messageBranch: 'required' };
  const segmentAccounting: ModelOptions = { ...MODEL_CURRENT, accounting: 'segment' };
  variants.push(
    {
      label: 'model-only/current-mechanics',
      verify: () => runModel(logCount, mode, MODEL_CURRENT),
      timed: () => timeModel(logCount, mode, MODEL_CURRENT),
    },
    {
      label: 'model-only/inline-overflow-compare',
      verify: () => runModel(logCount, mode, inlineOverflow),
      timed: () => timeModel(logCount, mode, inlineOverflow),
    },
    {
      label: 'model-only/inline-trace-root-append',
      verify: () => runModel(logCount, mode, inlineAppend),
      timed: () => timeModel(logCount, mode, inlineAppend),
    },
    {
      label: 'model-only/required-message-lanes',
      verify: () => runModel(logCount, mode, requiredMessage),
      timed: () => timeModel(logCount, mode, requiredMessage),
    },
    {
      label: 'model-only/segment-stats-accounting',
      verify: () => runModel(logCount, mode, segmentAccounting),
      timed: () => timeModel(logCount, mode, segmentAccounting),
    },
  );
  return variants;
}

function variantAt(variants: readonly ScenarioVariant[], index: number): ScenarioVariant {
  const variant = variants[index];
  if (variant === undefined) throw new RangeError(`Missing benchmark variant at index ${index}`);
  return variant;
}

/** Semantic validation is deliberately outside every timed Mitata callback. */
function preflight(logCount: number, messageMode: MessageMode, variants: readonly ScenarioVariant[]): void {
  const baseline = variantAt(variants, 0).verify();
  const expectedWrites = logCount * REQUESTS_PER_SAMPLE;
  if (baseline.rows !== expectedWrites || baseline.writes !== expectedWrites) {
    throw new Error(
      `Preflight write-count mismatch for ${messageMode}/${logCount}: expected ${expectedWrites}, got rows=${baseline.rows}, writes=${baseline.writes}`,
    );
  }
  for (let index = 1; index < variants.length; index++) {
    const variant = variantAt(variants, index);
    const observed = variant.verify();
    if (
      observed.semantic !== baseline.semantic ||
      observed.rows !== baseline.rows ||
      observed.writes !== baseline.writes
    ) {
      throw new Error(
        `Preflight semantic mismatch for ${messageMode}/${logCount}: ${variant.label} produced ${observed.semantic} (${observed.rows} rows/${observed.writes} writes), baseline produced ${baseline.semantic} (${baseline.rows} rows/${baseline.writes} writes)`,
      );
    }
  }
}

for (const messageMode of MESSAGE_MODES) {
  for (const logCount of LOG_COUNT_FILTER) {
    const variants = variantsFor(logCount, messageMode);
    preflight(logCount, messageMode, variants);
    summary(() => {
      group(
        `log-call-path/${messageMode}/${logCount}-logs [capacity=${CAPACITY}, requests=${REQUESTS_PER_SAMPLE}, timestamps=excluded]`,
        () => {
          for (let index = 0; index < variants.length; index++) {
            const variant = variantAt(variants, index);
            bench(variant.label, () => {
              const sink = variant.timed();
              do_not_optimize(sink);
              return sink;
            }).baseline(index === 0);
          }
        },
      );
    });
  }
}

const format = process.argv.includes('--json') ? 'json' : process.argv.includes('--markdown') ? 'markdown' : 'mitata';
const filterArgument = process.argv.find((argument) => argument.startsWith('--filter='));
const filter = filterArgument === undefined ? undefined : new RegExp(filterArgument.slice('--filter='.length));

if (format === 'json') await run({ format: { json: { samples: true } }, filter, colors: false });
else await run({ format, filter, colors: format === 'mitata' });
