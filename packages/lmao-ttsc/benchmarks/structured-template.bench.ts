/**
 * Structured-log lowering benchmark.
 *
 * Static compiler paths use the registered vocabulary binding and a static-only
 * CallsitePlan. Dynamic paths use a dynamic-only plan. Every runner owns a real
 * production SpanContext/WriterState and verifies evaluation order plus stored
 * semantics before Mitata starts timing.
 *
 * Run: bun packages/lmao-ttsc/benchmarks/structured-template.bench.ts --quick
 */

import { bench, do_not_optimize, group, run, summary } from 'mitata';
import type { WriterState } from '../../lmao/src/lib/codegen/fixedPositionWriterGenerator.ts';
import { resolveMessage } from '../../lmao/src/lib/resolveMessage.ts';
import {
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_LOG,
  RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY,
  RUNTIME_HINT_MESSAGE_LAYOUT_STATIC_ONLY,
} from '../../lmao/src/lib/runtimeHint.ts';
import {
  createSpanBuffer,
  createTraceRoot,
  defineLogSchema,
  defineOpContext,
  ENTRY_TYPE_DEBUG,
  ENTRY_TYPE_INFO,
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
const ITERATIONS = QUICK ? 128 : 10_000;
const CAPACITY = ITERATIONS + 2;
const STATIC_MESSAGE = 'request completed';
const MASK_64 = (1n << 64n) - 1n;
const EAGER_COLUMNS: readonly string[] = ['userId', 'retries', 'region', 'latencyMs', 'cached'];
const STATIC_BINDING = registerBenchmarkVocabulary([STATIC_MESSAGE]);
const STATIC_MESSAGE_INDEX = STATIC_BINDING[0];
if (STATIC_MESSAGE_INDEX === undefined) throw new Error('Static benchmark vocabulary did not bind its message');

const schema = defineLogSchema({
  userId: S.category(),
  retries: S.number(),
  region: S.category(),
  latencyMs: S.number(),
  cached: S.boolean(),
});
const opContext = defineOpContext({ logSchema: schema });
const tracer = new TestTracer(opContext, {
  bufferStrategy: new JsBufferStrategy(),
  createTraceRoot,
});

type RuntimeSchema = typeof opContext.logBinding.logSchema;
type RuntimeLogger = SpanLoggerImpl<RuntimeSchema>;

interface AttributeViews {
  userIdValues: string[];
  userIdNulls: Uint8Array;
  retriesValues: Float64Array;
  retriesNulls: Uint8Array;
  regionValues: string[];
  regionNulls: Uint8Array;
  latencyMsValues: Float64Array;
  latencyMsNulls: Uint8Array;
  cachedValues: Uint8Array;
  cachedNulls: Uint8Array;
}

interface RuntimeBundle {
  buffer: SpanBuffer<RuntimeSchema>;
  context: WriterState & { readonly _spanLogger: RuntimeLogger };
  staticLowered: boolean;
  staticMessageId: number;
  views: AttributeViews;
}

type AttributeValues = {
  userId: string;
  retries: number;
  region: string;
  latencyMs: number;
  cached: boolean;
};

type Implementation = 'current' | 'conceptual' | 'unrolled' | 'staged' | 'raw-debug' | 'interpolation';

interface RunResult {
  checksum: bigint;
}

interface Tracker {
  count: number;
  hash: bigint;
  trace: string[] | undefined;
}

interface Runner {
  label: string;
  run(): number;
  preflight(): RunResult;
  tracker: Tracker;
}

function stringValues(value: unknown, name: string): string[] {
  if (!Array.isArray(value) || !value.every((item) => typeof item === 'string')) {
    throw new TypeError(`${name} did not expose string-array storage`);
  }
  return value;
}

function uint8Values(value: unknown, name: string): Uint8Array {
  if (!(value instanceof Uint8Array)) throw new TypeError(`${name} did not expose Uint8Array storage`);
  return value;
}

function float64Values(value: unknown, name: string): Float64Array {
  if (!(value instanceof Float64Array)) throw new TypeError(`${name} did not expose Float64Array storage`);
  return value;
}

function attributeViews(buffer: SpanBuffer<RuntimeSchema>): AttributeViews {
  return {
    userIdValues: stringValues(buffer.userId_values, 'userId_values'),
    userIdNulls: uint8Values(buffer.userId_nulls, 'userId_nulls'),
    retriesValues: float64Values(buffer.retries_values, 'retries_values'),
    retriesNulls: uint8Values(buffer.retries_nulls, 'retries_nulls'),
    regionValues: stringValues(buffer.region_values, 'region_values'),
    regionNulls: uint8Values(buffer.region_nulls, 'region_nulls'),
    latencyMsValues: float64Values(buffer.latencyMs_values, 'latencyMs_values'),
    latencyMsNulls: uint8Values(buffer.latencyMs_nulls, 'latencyMs_nulls'),
    cachedValues: uint8Values(buffer.cached_values, 'cached_values'),
    cachedNulls: uint8Values(buffer.cached_nulls, 'cached_nulls'),
  };
}

function makeRuntimeBundle(label: string, dynamic: boolean, implementation: Implementation): RuntimeBundle {
  const staticLowered = !dynamic && (implementation === 'unrolled' || implementation === 'staged');
  const messageLayout = staticLowered
    ? RUNTIME_HINT_MESSAGE_LAYOUT_STATIC_ONLY
    : RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY;
  const benchmarkOp = opContext.defineOp(
    `structured-template-${label}`,
    () => new Ok(undefined),
    undefined,
    {
      runtimeHint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_LOG | messageLayout | CAPACITY,
      eagerColumns: EAGER_COLUMNS,
      localMessageDictionary: staticLowered ? [STATIC_MESSAGE_INDEX] : [],
    },
  );
  const plan = benchmarkOp.callsitePlan;
  const staticMessageId = staticLowered ? plan.encodeLocalMessage(STATIC_MESSAGE_INDEX) : 0;
  if (staticLowered && staticMessageId === 0) {
    throw new Error('Static CallsitePlan did not bind the benchmark vocabulary message');
  }
  const traceRoot = createTraceRoot(`structured-template-${label}`, tracer);
  const buffer = createSpanBuffer(plan.schema, traceRoot, plan.metadata, CAPACITY, plan.SpanBufferClass);
  plan.appenders.writeSpanStart(buffer, benchmarkOp.metadata.name);
  const context = new plan.SpanContextClass(buffer, plan.schema, plan);
  return { buffer, context, staticLowered, staticMessageId, views: attributeViews(buffer) };
}

function resetBundle(bundle: RuntimeBundle): void {
  bundle.buffer._writeIndex = 2;
  bundle.context._buffer = bundle.buffer;
}

function sourceMessage(dynamic: boolean, iteration: number): string {
  return dynamic ? `request-${iteration & 31}` : STATIC_MESSAGE;
}

function sourceValues(count: number, iteration: number): AttributeValues {
  return {
    userId: count >= 1 ? `user-${iteration & 15}` : '',
    retries: count >= 2 ? iteration & 3 : 0,
    region: count >= 3 ? ((iteration & 1) === 0 ? 'us-east' : 'eu-west') : '',
    latencyMs: count >= 4 ? iteration + 0.25 : 0,
    cached: count >= 5 && (iteration & 1) === 0,
  };
}

function sourceObject(count: number, iteration: number): Partial<AttributeValues> {
  switch (count) {
    case 1:
      return { userId: `user-${iteration & 15}` };
    case 2:
      return { userId: `user-${iteration & 15}`, retries: iteration & 3 };
    case 5:
      return {
        userId: `user-${iteration & 15}`,
        retries: iteration & 3,
        region: (iteration & 1) === 0 ? 'us-east' : 'eu-west',
        latencyMs: iteration + 0.25,
        cached: (iteration & 1) === 0,
      };
    default:
      throw new Error(`Unsupported attribute count: ${count}`);
  }
}

function note(tracker: Tracker, token: string): void {
  tracker.count++;
  tracker.hash = ((tracker.hash * 1_099_511_628_211n) ^ BigInt(token.charCodeAt(0))) & MASK_64;
  tracker.trace?.push(token);
}

function evaluateMessage(tracker: Tracker, dynamic: boolean, iteration: number): string {
  if (!dynamic) return STATIC_MESSAGE;
  note(tracker, 'm');
  return `request-${iteration & 31}`;
}

function evaluateValues(tracker: Tracker, count: number, iteration: number): AttributeValues {
  let userId = '';
  let retries = 0;
  let region = '';
  let latencyMs = 0;
  let cached = false;
  if (count >= 1) {
    note(tracker, 'u');
    userId = `user-${iteration & 15}`;
  }
  if (count >= 2) {
    note(tracker, 'r');
    retries = iteration & 3;
  }
  if (count >= 3) {
    note(tracker, 'g');
    region = (iteration & 1) === 0 ? 'us-east' : 'eu-west';
  }
  if (count >= 4) {
    note(tracker, 'l');
    latencyMs = iteration + 0.25;
  }
  if (count >= 5) {
    note(tracker, 'c');
    cached = (iteration & 1) === 0;
  }
  return { userId, retries, region, latencyMs, cached };
}

function evaluateObject(tracker: Tracker, count: number, iteration: number): Partial<AttributeValues> {
  switch (count) {
    case 1:
      note(tracker, 'u');
      return { userId: `user-${iteration & 15}` };
    case 2:
      note(tracker, 'u');
      const userId = `user-${iteration & 15}`;
      note(tracker, 'r');
      return { userId, retries: iteration & 3 };
    case 5:
      note(tracker, 'u');
      const fullUserId = `user-${iteration & 15}`;
      note(tracker, 'r');
      const retries = iteration & 3;
      note(tracker, 'g');
      const region = (iteration & 1) === 0 ? 'us-east' : 'eu-west';
      note(tracker, 'l');
      const latencyMs = iteration + 0.25;
      note(tracker, 'c');
      return { retries, region, latencyMs, cached: (iteration & 1) === 0, userId: fullUserId };
    default:
      throw new Error(`Unsupported attribute count: ${count}`);
  }
}

function checksumIteration(checksum: bigint, message: string, values: AttributeValues, count: number): bigint {
  let next = (checksum * 1_000_003n + BigInt(message.length)) & MASK_64;
  if (count >= 1) next = (next * 131n + BigInt(values.userId.length)) & MASK_64;
  if (count >= 2) next = (next * 131n + BigInt(Math.trunc(values.retries * 4))) & MASK_64;
  if (count >= 3) next = (next * 131n + BigInt(values.region.length)) & MASK_64;
  if (count >= 4) next = (next * 131n + BigInt(Math.trunc(values.latencyMs * 4))) & MASK_64;
  if (count >= 5) next = (next * 131n + BigInt(values.cached ? 1 : 0)) & MASK_64;
  return next;
}

function applyUnrolled(bundle: RuntimeBundle, message: string, values: AttributeValues, count: number, debug: boolean): void {
  const logger = bundle.context._spanLogger;
  const entry = bundle.staticLowered
    ? logger._infoTemplate(STATIC_MESSAGE_INDEX)
    : debug
      ? logger.debug(message)
      : logger.info(message);
  switch (count) {
    case 1:
      entry.userId(values.userId);
      return;
    case 2:
      entry.userId(values.userId).retries(values.retries);
      return;
    case 5:
      entry
        .userId(values.userId)
        .retries(values.retries)
        .region(values.region)
        .latencyMs(values.latencyMs)
        .cached(values.cached);
      return;
    default:
      throw new Error(`Unsupported attribute count: ${count}`);
  }
}

function directWrite(bundle: RuntimeBundle, message: string, values: AttributeValues, count: number): void {
  const buffer = bundle.buffer;
  const views = bundle.views;
  const row = bundle.context._appendWriterEntry(ENTRY_TYPE_INFO);
  if (bundle.staticLowered) {
    const messageIds = buffer._messageIds;
    if (!messageIds || bundle.staticMessageId === 0) {
      throw new Error('Static CallsitePlan did not provide its bound local message storage');
    }
    messageIds[row] = bundle.staticMessageId;
  } else {
    const messageValues = buffer.message_values;
    if (!messageValues) throw new Error('Dynamic CallsitePlan did not provide message storage');
    messageValues[row] = message;
  }
  const byte = row >>> 3;
  const mask = 1 << (row & 7);
  if (count >= 1) {
    views.userIdValues[row] = values.userId;
    views.userIdNulls[byte] |= mask;
  }
  if (count >= 2) {
    views.retriesValues[row] = values.retries;
    views.retriesNulls[byte] |= mask;
  }
  if (count >= 3) {
    views.regionValues[row] = values.region;
    views.regionNulls[byte] |= mask;
  }
  if (count >= 4) {
    views.latencyMsValues[row] = values.latencyMs;
    views.latencyMsNulls[byte] |= mask;
  }
  if (count >= 5) {
    if (values.cached) views.cachedValues[byte] |= mask;
    else views.cachedValues[byte] &= ~mask;
    views.cachedNulls[byte] |= mask;
  }
}

function storedChecksum(bundle: RuntimeBundle, row: number, count: number): bigint {
  const message = resolveMessage(bundle.buffer, row);
  const views = bundle.views;
  let checksum = BigInt(message?.length ?? 0) << 8n;
  if (count >= 1) checksum ^= BigInt(views.userIdValues[row]?.length ?? 0) << 16n;
  if (count >= 2) checksum ^= BigInt(Math.trunc(views.retriesValues[row] * 4)) << 21n;
  if (count >= 3) checksum ^= BigInt(views.regionValues[row]?.length ?? 0) << 26n;
  if (count >= 4) checksum ^= BigInt(Math.trunc(views.latencyMsValues[row] * 4)) << 31n;
  if (count >= 5) {
    const cached = (views.cachedValues[row >>> 3] & (1 << (row & 7))) !== 0;
    checksum ^= BigInt(cached ? 1 : 0) << 36n;
  }
  return checksum;
}

function finishChecksum(
  semantic: bigint,
  tracker: Tracker,
  bundle: RuntimeBundle,
  attributeCount: number,
): RunResult {
  const row = bundle.buffer._writeIndex - 1;
  return {
    checksum: (semantic ^ tracker.hash ^ BigInt(tracker.count) ^ storedChecksum(bundle, row, attributeCount)) & MASK_64,
  };
}

function expectedTrace(dynamic: boolean, attributeCount: number): string[] {
  const perIteration: string[] = [];
  if (dynamic) perIteration.push('m');
  if (attributeCount >= 1) perIteration.push('u');
  if (attributeCount >= 2) perIteration.push('r');
  if (attributeCount >= 3) perIteration.push('g');
  if (attributeCount >= 4) perIteration.push('l');
  if (attributeCount >= 5) perIteration.push('c');
  const trace: string[] = [];
  for (let iteration = 0; iteration < ITERATIONS; iteration++) trace.push(...perIteration);
  return trace;
}

function makeRunner(
  label: string,
  attributeCount: 1 | 2 | 5,
  dynamic: boolean,
  implementation: Implementation,
): Runner {
  const bundle = makeRuntimeBundle(`${label}-${attributeCount}-${dynamic ? 'dynamic' : 'static'}`, dynamic, implementation);
  const tracker: Tracker = { count: 0, hash: 0n, trace: undefined };

  function execute(tracked: boolean): RunResult {
    resetBundle(bundle);
    if (tracked) {
      tracker.count = 0;
      tracker.hash = 0n;
    }
    let semantic = 0n;
    for (let iteration = 0; iteration < ITERATIONS; iteration++) {
      const message = tracked
        ? evaluateMessage(tracker, implementation === 'interpolation' || dynamic, iteration)
        : sourceMessage(implementation === 'interpolation' || dynamic, iteration);
      if (implementation === 'current' || implementation === 'conceptual') {
        const attributes = tracked
          ? evaluateObject(tracker, attributeCount, iteration)
          : sourceObject(attributeCount, iteration);
        if (implementation === 'current') bundle.context._spanLogger.info(message).with(attributes);
        else bundle.context._spanLogger.info(message, attributes);
        if (tracked) semantic = checksumIteration(semantic, message, sourceValues(attributeCount, iteration), attributeCount);
      } else {
        const values = tracked
          ? evaluateValues(tracker, attributeCount, iteration)
          : sourceValues(attributeCount, iteration);
        if (implementation === 'staged') directWrite(bundle, message, values, attributeCount);
        else applyUnrolled(bundle, message, values, attributeCount, implementation === 'raw-debug');
        if (tracked) semantic = checksumIteration(semantic, message, values, attributeCount);
      }
    }
    return finishChecksum(semantic, tracker, bundle, attributeCount);
  }

  return {
    label,
    tracker,
    run() {
      execute(false);
      const row = bundle.buffer._writeIndex - 1;
      return bundle.buffer._writeIndex ^ (resolveMessage(bundle.buffer, row)?.length ?? 0);
    },
    preflight() {
      return execute(true);
    },
  };
}

function assertSemantics(runners: readonly Runner[], dynamic: boolean, attributeCount: number): void {
  const expected = expectedTrace(dynamic, attributeCount);
  let checksum: bigint | undefined;
  for (const runner of runners) {
    runner.tracker.trace = [];
    const result = runner.preflight();
    const actual = runner.tracker.trace;
    runner.tracker.trace = undefined;
    if (!actual || actual.length !== expected.length || actual.some((token, index) => token !== expected[index])) {
      throw new Error(`${runner.label} violated exactly-once source evaluation order`);
    }
    if (checksum === undefined) checksum = result.checksum;
    else if (result.checksum !== checksum) throw new Error(`${runner.label} semantic checksum differed before timing`);
  }
}

function registerScenario(attributeCount: 1 | 2 | 5, dynamic: boolean): void {
  const runners: Runner[] = [
    makeRunner('current-info-with-object', attributeCount, dynamic, 'current'),
    makeRunner('current-info-two-argument', attributeCount, dynamic, 'conceptual'),
    makeRunner('compiler-unrolled-chain', attributeCount, dynamic, 'unrolled'),
    makeRunner('compiler-staged-direct-writes', attributeCount, dynamic, 'staged'),
  ];
  if (dynamic) {
    runners.push(
      makeRunner('raw-dynamic-debug-control', attributeCount, true, 'raw-debug'),
      makeRunner('interpolation-control', attributeCount, true, 'interpolation'),
    );
  }

  assertSemantics(runners, dynamic, attributeCount);
  group(`structured-template | attrs=${attributeCount} | message=${dynamic ? 'dynamic' : 'static'}`, () => {
    for (const runner of runners) {
      const benchmark = bench(runner.label, () => do_not_optimize(runner.run()));
      if (runner.label === 'current-info-with-object') benchmark.baseline(true);
    }
  });
}

const ATTRIBUTE_COUNTS: readonly (1 | 2 | 5)[] = QUICK ? [2] : [1, 2, 5];
for (const attributeCount of ATTRIBUTE_COUNTS) {
  summary(() => registerScenario(attributeCount, false));
  summary(() => registerScenario(attributeCount, true));
}

await run({ format: FORMAT, throw: true });
