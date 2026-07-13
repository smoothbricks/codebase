/**
 * Compiler log-inline benchmark.
 *
 * Every case uses a production CallsitePlan, SpanContext-owned WriterState,
 * message-family-specific SpanBuffer, and compiler vocabulary binding. The
 * direct cases model the current transformer seam: WriterState allocates the
 * row, then generated code stores the packed header and proven fields.
 *
 * Run: bun packages/lmao-ttsc/benchmarks/log-inline.bench.ts --quick
 */

import { bench, do_not_optimize, group, run } from 'mitata';
import type { WriterState } from '../../lmao/src/lib/codegen/fixedPositionWriterGenerator.ts';
import { resolveMessage } from '../../lmao/src/lib/resolveMessage.ts';
import {
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_LOG,
  RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY,
  RUNTIME_HINT_MESSAGE_LAYOUT_MIXED,
  RUNTIME_HINT_MESSAGE_LAYOUT_STATIC_ONLY,
  RUNTIME_HINT_MESSAGE_PHYSICAL_PACKED,
  RUNTIME_HINT_RESULT,
  type MessageLayoutFamily,
} from '../../lmao/src/lib/runtimeHint.ts';
import {
  createSpanBuffer,
  createTraceRoot,
  defineLogSchema,
  defineOpContext,
  ENTRY_TYPE_INFO,
  ENTRY_TYPE_WARN,
  JsBufferStrategy,
  Ok,
  S,
  type SpanBuffer,
  type SpanLoggerImpl,
  TestTracer,
} from '../../lmao/src/node.ts';
import { registerBenchmarkVocabulary } from '../../lmao/benchmarks/vocabularyFixture.ts';

const QUICK = process.argv.includes('--quick');
const FORMAT = process.argv.includes('--json') ? 'json' : process.argv.includes('--markdown') ? 'markdown' : 'mitata';
const CAPACITY = 1024;
const OPERATIONS: readonly ['DELETE', 'INSERT', 'SELECT', 'UPDATE'] = ['DELETE', 'INSERT', 'SELECT', 'UPDATE'];
const EAGER_COLUMNS: readonly string[] = ['userId', 'retries', 'operation'];
const VOCABULARY_TEXTS: readonly string[] = [
  'user created',
  'slow query',
  'hello',
  'request completed',
  'order submitted',
  'cache refreshed',
];
const VOCABULARY_BINDING = registerBenchmarkVocabulary(VOCABULARY_TEXTS);

function boundVocabularyIndex(ordinal: number): number {
  const index = VOCABULARY_BINDING[ordinal];
  if (index === undefined) throw new Error(`Vocabulary ordinal ${ordinal} was not bound`);
  return index;
}

const USER_CREATED = boundVocabularyIndex(0);
const SLOW_QUERY = boundVocabularyIndex(1);
const HELLO = boundVocabularyIndex(2);
const REQUEST_COMPLETED = boundVocabularyIndex(3);
const ORDER_SUBMITTED = boundVocabularyIndex(4);
const CACHE_REFRESHED = boundVocabularyIndex(5);

const schema = defineLogSchema({
  userId: S.category(),
  retries: S.number(),
  operation: S.enum(OPERATIONS),
});
const opContext = defineOpContext({ logSchema: schema });
const tracer = new TestTracer(opContext, {
  bufferStrategy: new JsBufferStrategy(),
  createTraceRoot,
});

type RuntimeSchema = typeof opContext.logBinding.logSchema;
type RuntimeLogger = SpanLoggerImpl<RuntimeSchema>;

interface LogContext extends WriterState {
  readonly _spanLogger: RuntimeLogger;
}

interface ResultContext extends WriterState {
  ok<V>(value: V): Ok<V, RuntimeSchema>;
}

interface NumericViews {
  lineValues: Float64Array;
  retriesValues: Float64Array;
  operationValues: Uint8Array;
}

interface LogBundle {
  buffer: SpanBuffer<RuntimeSchema>;
  context: LogContext;
  views: NumericViews;
}

interface ResultBundle {
  buffer: SpanBuffer<RuntimeSchema>;
  context: ResultContext;
  views: NumericViews;
}

function messageLayoutHint(family: MessageLayoutFamily): number {
  switch (family) {
    case 'static-only':
      return RUNTIME_HINT_MESSAGE_LAYOUT_STATIC_ONLY;
    case 'dynamic-only':
      return RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY;
    case 'mixed':
      return RUNTIME_HINT_MESSAGE_LAYOUT_MIXED;
  }
}

let bundleId = 0;

function float64Values(value: unknown, name: string): Float64Array {
  if (!(value instanceof Float64Array)) throw new TypeError(`${name} did not expose Float64Array storage`);
  return value;
}

function uint8Values(value: unknown, name: string): Uint8Array {
  if (!(value instanceof Uint8Array)) throw new TypeError(`${name} did not expose Uint8Array storage`);
  return value;
}

function numericViews(buffer: SpanBuffer<RuntimeSchema>): NumericViews {
  return {
    lineValues: float64Values(buffer.line_values, 'line_values'),
    retriesValues: float64Values(buffer.retries_values, 'retries_values'),
    operationValues: uint8Values(buffer.operation_values, 'operation_values'),
  };
}

function makeLogBundle(family: MessageLayoutFamily): LogBundle {
  const id = bundleId++;
  const benchmarkOp = opContext.defineOp(
    `log-inline-${family}-${id}`,
    () => new Ok(undefined),
    undefined,
    {
      runtimeHint:
        RUNTIME_HINT_ANALYZED_VALID |
        RUNTIME_HINT_LOG |
        messageLayoutHint(family) |
        RUNTIME_HINT_MESSAGE_PHYSICAL_PACKED |
        CAPACITY,
      eagerColumns: EAGER_COLUMNS,
    },
  );
  const plan = benchmarkOp.callsitePlan;
  if (plan.messageLayoutFamily !== family) {
    throw new Error(`Log CallsitePlan selected ${plan.messageLayoutFamily}; expected ${family}`);
  }
  if (plan.messagePhysicalLayout !== 'packed') {
    throw new Error(`Log CallsitePlan selected ${plan.messagePhysicalLayout}; expected packed`);
  }
  const traceRoot = createTraceRoot(`log-inline-${family}-${id}`, tracer);
  const buffer = createSpanBuffer(plan.schema, traceRoot, plan.metadata, CAPACITY, plan.SpanBufferClass);
  plan.appenders.writeSpanStart(buffer, benchmarkOp.metadata.name);
  const context = new plan.SpanContextClass(buffer, plan.schema, plan);
  return { buffer, context, views: numericViews(buffer) };
}

function makeResultBundle(): ResultBundle {
  const id = bundleId++;
  const benchmarkOp = opContext.defineOp(
    `result-inline-${id}`,
    () => new Ok(undefined),
    undefined,
    {
      runtimeHint:
        RUNTIME_HINT_ANALYZED_VALID |
        RUNTIME_HINT_RESULT |
        RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY |
        CAPACITY,
      eagerColumns: EAGER_COLUMNS,
    },
  );
  const plan = benchmarkOp.callsitePlan;
  const traceRoot = createTraceRoot(`result-inline-${id}`, tracer);
  const buffer = createSpanBuffer(plan.schema, traceRoot, plan.metadata, CAPACITY, plan.SpanBufferClass);
  plan.appenders.writeSpanStart(buffer, benchmarkOp.metadata.name);
  const context = new plan.SpanContextClass(buffer, plan.schema, plan);
  return { buffer, context, views: numericViews(buffer) };
}

function prepareLog(bundle: LogBundle): void {
  if (bundle.context._buffer._writeIndex < bundle.context._buffer._capacity) return;
  bundle.buffer._writeIndex = 2;
  bundle.context._buffer = bundle.buffer;
}

function packedHeaders(bundle: LogBundle): Uint32Array {
  const headers = bundle.buffer._rowHeaders;
  if (!headers) throw new Error('Packed CallsitePlan did not provide row headers');
  return headers;
}

function dynamicMessages(bundle: LogBundle): (string | undefined)[] {
  const messages = bundle.buffer.message_values;
  if (!messages) throw new Error('Dynamic or mixed CallsitePlan did not provide message storage');
  return messages;
}

function setPresent(nulls: Uint8Array, row: number): void {
  nulls[row >>> 3] |= 1 << (row & 7);
}

function packedVocabularyHeader(vocabularyIndex: number, entryType: number): number {
  return (((vocabularyIndex + 1) << 8) | entryType) >>> 0;
}

function infoChecksum(bundle: LogBundle): number {
  const row = bundle.context._buffer._writeIndex - 1;
  return (
    (resolveMessage(bundle.buffer, row)?.length ?? 0) * 1_000_003 +
    Math.trunc(bundle.views.lineValues[row]) * 1009 +
    (bundle.buffer.userId_values[row]?.length ?? 0) * 101 +
    Math.trunc(bundle.views.retriesValues[row])
  );
}

function warnChecksum(bundle: LogBundle): number {
  const row = bundle.context._buffer._writeIndex - 1;
  return (
    (resolveMessage(bundle.buffer, row)?.length ?? 0) * 1_000_003 +
    Math.trunc(bundle.views.lineValues[row]) * 1009 +
    bundle.views.operationValues[row]
  );
}

function bareChecksum(bundle: LogBundle): number {
  const row = bundle.context._buffer._writeIndex - 1;
  return (resolveMessage(bundle.buffer, row)?.length ?? 0) ^ row;
}

function writeFluentInfo(bundle: LogBundle, iteration: number): number {
  prepareLog(bundle);
  bundle.context._spanLogger.info('user created').line(24).userId(`u${iteration}`).retries(iteration);
  return infoChecksum(bundle);
}

function writeInlinedInfo(bundle: LogBundle, headers: Uint32Array, iteration: number): number {
  prepareLog(bundle);
  const buffer = bundle.buffer;
  const row = bundle.context._appendWriterEntry(ENTRY_TYPE_INFO);
  headers[row] = packedVocabularyHeader(USER_CREATED, ENTRY_TYPE_INFO);
  buffer.line_values[row] = 24;
  setPresent(buffer.line_nulls, row);
  buffer.userId_values[row] = `u${iteration}`;
  setPresent(buffer.userId_nulls, row);
  buffer.retries_values[row] = iteration;
  setPresent(buffer.retries_nulls, row);
  return infoChecksum(bundle);
}

function writeFluentWarn(bundle: LogBundle): number {
  prepareLog(bundle);
  bundle.context._spanLogger.warn('slow query').line(25).operation('SELECT');
  return warnChecksum(bundle);
}

function writeInlinedWarn(bundle: LogBundle, headers: Uint32Array): number {
  prepareLog(bundle);
  const buffer = bundle.buffer;
  const row = bundle.context._appendWriterEntry(ENTRY_TYPE_WARN);
  headers[row] = packedVocabularyHeader(SLOW_QUERY, ENTRY_TYPE_WARN);
  buffer.line_values[row] = 25;
  setPresent(buffer.line_nulls, row);
  buffer.operation_values[row] = 2;
  setPresent(buffer.operation_nulls, row);
  return warnChecksum(bundle);
}

function writeFluentBare(bundle: LogBundle): number {
  prepareLog(bundle);
  bundle.context._spanLogger.info('hello');
  return bareChecksum(bundle);
}

function writeInlinedBare(bundle: LogBundle, headers: Uint32Array): number {
  prepareLog(bundle);
  const row = bundle.context._appendWriterEntry(ENTRY_TYPE_INFO);
  headers[row] = packedVocabularyHeader(HELLO, ENTRY_TYPE_INFO);
  return bareChecksum(bundle);
}

function assertEqual(label: string, actual: number, expected: number): void {
  if (actual !== expected) throw new Error(`${label} semantic checksum differed before timing`);
}

const fluentInfoBundle = makeLogBundle('dynamic-only');
const inlineInfoBundle = makeLogBundle('static-only');
const inlineInfoHeaders = packedHeaders(inlineInfoBundle);
const fluentWarnBundle = makeLogBundle('dynamic-only');
const inlineWarnBundle = makeLogBundle('static-only');
const inlineWarnHeaders = packedHeaders(inlineWarnBundle);
const fluentBareBundle = makeLogBundle('dynamic-only');
const inlineBareBundle = makeLogBundle('static-only');
const inlineBareHeaders = packedHeaders(inlineBareBundle);

assertEqual('inlined info', writeInlinedInfo(inlineInfoBundle, inlineInfoHeaders, 17), writeFluentInfo(fluentInfoBundle, 17));
assertEqual('inlined warn', writeInlinedWarn(inlineWarnBundle, inlineWarnHeaders), writeFluentWarn(fluentWarnBundle));
assertEqual('inlined bare log', writeInlinedBare(inlineBareBundle, inlineBareHeaders), writeFluentBare(fluentBareBundle));

let infoIteration = 0;
let warnIteration = 0;
let bareIteration = 0;

group('log.info + line + 2 attrs', () => {
  bench('A fluent (runtime path)', () => do_not_optimize(writeFluentInfo(fluentInfoBundle, infoIteration++))).baseline();
  bench('B inlined (WriterState + vocabulary header)', () =>
    do_not_optimize(writeInlinedInfo(inlineInfoBundle, inlineInfoHeaders, infoIteration++)),
  );
});

group('log.warn + line + literal enum', () => {
  bench('A fluent (runtime path)', () => do_not_optimize(writeFluentWarn(fluentWarnBundle))).baseline();
  bench('B inlined (vocabulary + enum folded)', () =>
    do_not_optimize(writeInlinedWarn(inlineWarnBundle, inlineWarnHeaders) ^ warnIteration++),
  );
});

group('bare log.info (row allocation floor)', () => {
  bench('A fluent', () => do_not_optimize(writeFluentBare(fluentBareBundle))).baseline();
  bench('B inlined', () => do_not_optimize(writeInlinedBare(inlineBareBundle, inlineBareHeaders) ^ bareIteration++));
});

function dynamicMessage(iteration: number): string {
  switch (iteration & 3) {
    case 0:
      return 'request 0';
    case 1:
      return 'request 1';
    case 2:
      return 'request 2';
    default:
      return 'request 3';
  }
}

function callsiteText(iteration: number): string {
  switch (iteration & 3) {
    case 0:
      return 'user created';
    case 1:
      return 'order submitted';
    case 2:
      return 'cache refreshed';
    default:
      return 'request completed';
  }
}

function callsiteIndex(iteration: number): number {
  switch (iteration & 3) {
    case 0:
      return USER_CREATED;
    case 1:
      return ORDER_SUBMITTED;
    case 2:
      return CACHE_REFRESHED;
    default:
      return REQUEST_COMPLETED;
  }
}

function makeRepeatedStringStore(bundle: LogBundle): () => number {
  const messages = dynamicMessages(bundle);
  return () => {
    prepareLog(bundle);
    const row = bundle.context._appendWriterEntry(ENTRY_TYPE_INFO);
    messages[row] = 'request completed';
    return row ^ messages[row].length;
  };
}

function makeRepeatedDenseHeaderStore(bundle: LogBundle): () => number {
  const headers = packedHeaders(bundle);
  return () => {
    prepareLog(bundle);
    const row = bundle.context._appendWriterEntry(ENTRY_TYPE_INFO);
    headers[row] = packedVocabularyHeader(REQUEST_COMPLETED, ENTRY_TYPE_INFO);
    return row ^ headers[row];  };
}

function makeCallsiteStringStores(bundle: LogBundle): () => number {
  const messages = dynamicMessages(bundle);
  let iteration = 0;
  return () => {
    prepareLog(bundle);
    const row = bundle.context._appendWriterEntry(ENTRY_TYPE_INFO);
    messages[row] = callsiteText(iteration++);
    return row ^ messages[row].length;
  };
}

function makeCallsiteDenseHeaderStores(bundle: LogBundle): () => number {
  const headers = packedHeaders(bundle);
  let iteration = 0;
  return () => {
    prepareLog(bundle);
    const row = bundle.context._appendWriterEntry(ENTRY_TYPE_INFO);
    headers[row] = packedVocabularyHeader(callsiteIndex(iteration++), ENTRY_TYPE_INFO);
    return row ^ headers[row];
  };
}

function makeStringStoreWithAttrs(bundle: LogBundle): () => number {
  const messages = dynamicMessages(bundle);
  let iteration = 0;
  return () => {
    prepareLog(bundle);
    const row = bundle.context._appendWriterEntry(ENTRY_TYPE_INFO);
    messages[row] = 'user created';
    bundle.buffer.line_values[row] = 24;
    setPresent(bundle.buffer.line_nulls, row);
    bundle.buffer.userId_values[row] = 'u42';
    setPresent(bundle.buffer.userId_nulls, row);
    bundle.buffer.retries_values[row] = iteration++;
    setPresent(bundle.buffer.retries_nulls, row);
    return row ^ Math.trunc(bundle.buffer.retries_values[row]);
  };
}

function makeDenseHeaderStoreWithAttrs(bundle: LogBundle): () => number {
  const headers = packedHeaders(bundle);
  let iteration = 0;
  return () => {
    prepareLog(bundle);
    const row = bundle.context._appendWriterEntry(ENTRY_TYPE_INFO);
    headers[row] = packedVocabularyHeader(USER_CREATED, ENTRY_TYPE_INFO);
    bundle.buffer.line_values[row] = 24;
    setPresent(bundle.buffer.line_nulls, row);
    bundle.buffer.userId_values[row] = 'u42';
    setPresent(bundle.buffer.userId_nulls, row);
    bundle.buffer.retries_values[row] = iteration++;
    setPresent(bundle.buffer.retries_nulls, row);
    return headers[row] ^ Math.trunc(bundle.buffer.retries_values[row]);
  };
}

function makeMixedStringStores(bundle: LogBundle): () => number {
  const messages = dynamicMessages(bundle);
  let iteration = 0;
  return () => {
    prepareLog(bundle);
    const row = bundle.context._appendWriterEntry(ENTRY_TYPE_INFO);
    messages[row] = iteration % 10 === 0 ? dynamicMessage(iteration / 10) : 'request completed';
    iteration++;
    return row ^ messages[row].length;
  };
}

function makeMixedDenseHeaderStores(bundle: LogBundle): () => number {
  const messages = dynamicMessages(bundle);
  const headers = packedHeaders(bundle);
  let iteration = 0;
  return () => {
    prepareLog(bundle);
    const row = bundle.context._appendWriterEntry(ENTRY_TYPE_INFO);
    if (iteration % 10 === 0) messages[row] = dynamicMessage(iteration / 10);
    else headers[row] = packedVocabularyHeader(REQUEST_COMPLETED, ENTRY_TYPE_INFO);
    iteration++;
    return row ^ headers[row] ^ (messages[row]?.length ?? 0);
  };
}

function makeDynamicControl(bundle: LogBundle): () => number {
  const messages = dynamicMessages(bundle);
  let iteration = 0;
  return () => {
    prepareLog(bundle);
    const row = bundle.context._appendWriterEntry(ENTRY_TYPE_INFO);
    messages[row] = dynamicMessage(iteration++);
    return row ^ messages[row].length;
  };
}

function addPositionBalancedPair(
  stringLabel: string,
  denseLabel: string,
  makeStringCase: (bundle: LogBundle) => () => number,
  makeDenseCase: (bundle: LogBundle) => () => number,
  family: MessageLayoutFamily = 'static-only',
): void {
  const stringFirst = makeStringCase(makeLogBundle('dynamic-only'));
  const denseSecond = makeDenseCase(makeLogBundle(family));
  const denseFirst = makeDenseCase(makeLogBundle(family));
  const stringSecond = makeStringCase(makeLogBundle('dynamic-only'));
  bench(`A1 ${stringLabel} [pair 1: first]`, () => do_not_optimize(stringFirst()));
  bench(`B1 ${denseLabel} [pair 1: second]`, () => do_not_optimize(denseSecond()));
  bench(`B2 ${denseLabel} [pair 2: first]`, () => do_not_optimize(denseFirst()));
  bench(`A2 ${stringLabel} [pair 2: second]`, () => do_not_optimize(stringSecond()));
}

group('matched message store: one repeated literal', () => {
  addPositionBalancedPair(
    'dynamic string-reference store',
    'static packed vocabulary-header store',
    makeRepeatedStringStore,
    makeRepeatedDenseHeaderStore,
  );
});

if (!QUICK) {
  group('matched message store: four literal callsites', () => {
    addPositionBalancedPair(
      'dynamic string-reference stores',
      'static packed vocabulary-header stores',
      makeCallsiteStringStores,
      makeCallsiteDenseHeaderStores,
    );
  });

  group('matched message store: literal + line + 2 attrs', () => {
    addPositionBalancedPair(
      'dynamic string store + attrs',
      'static vocabulary header + attrs',
      makeStringStoreWithAttrs,
      makeDenseHeaderStoreWithAttrs,
    );
  });
}

group('matched message store: 90% literal / 10% dynamic', () => {
  addPositionBalancedPair(
    'mixed plan string stores for both branches',
    'mixed plan vocabulary header / dynamic fallback',
    makeMixedStringStores,
    makeMixedDenseHeaderStores,
    'mixed',
  );
});

if (!QUICK) {
  group('matched message store: dynamic-only position control', () => {
    const firstA = makeDynamicControl(makeLogBundle('dynamic-only'));
    const secondB = makeDynamicControl(makeLogBundle('dynamic-only'));
    const firstB = makeDynamicControl(makeLogBundle('dynamic-only'));
    const secondA = makeDynamicControl(makeLogBundle('dynamic-only'));
    bench('A1 dynamic control [pair 1: first]', () => do_not_optimize(firstA()));
    bench('B1 dynamic control [pair 1: second]', () => do_not_optimize(secondB()));
    bench('B2 dynamic control [pair 2: first]', () => do_not_optimize(firstB()));
    bench('A2 dynamic control [pair 2: second]', () => do_not_optimize(secondA()));
  });
}

function resultChecksum(bundle: ResultBundle): number {
  return (
    Math.trunc(bundle.views.lineValues[1]) * 1009 +
    (bundle.buffer.userId_values[1]?.length ?? 0) * 101 +
    Math.trunc(bundle.views.retriesValues[1])
  );
}

function writeFluentResult(bundle: ResultBundle, iteration: number): number {
  bundle.context.ok(iteration).line(19).with({ userId: `u${iteration}`, retries: 2 });
  return resultChecksum(bundle);
}

function writeInlinedResult(bundle: ResultBundle, iteration: number): number {
  bundle.context.ok(iteration);
  const buffer = bundle.buffer;
  buffer.line_values[1] = 19;
  setPresent(buffer.line_nulls, 1);
  buffer.userId_values[1] = `u${iteration}`;
  setPresent(buffer.userId_nulls, 1);
  buffer.retries_values[1] = 2;
  setPresent(buffer.retries_nulls, 1);
  return resultChecksum(bundle);}

const fluentResultBundle = makeResultBundle();
const inlineResultBundle = makeResultBundle();
assertEqual('inlined result', writeInlinedResult(inlineResultBundle, 23), writeFluentResult(fluentResultBundle, 23));
let resultIteration = 0;

group('ctx.ok + line + with(2 fields)', () => {
  bench('A result fluent', () => do_not_optimize(writeFluentResult(fluentResultBundle, resultIteration++))).baseline();
  bench('B result inlined (WriterState row 1)', () =>
    do_not_optimize(writeInlinedResult(inlineResultBundle, resultIteration++)),
  );
});

await run({ format: FORMAT, throw: true });
