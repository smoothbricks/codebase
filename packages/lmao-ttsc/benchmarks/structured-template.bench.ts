import { bench, group, run, summary } from 'mitata';
import { DEFAULT_METADATA } from '../../lmao/src/lib/opContext/defineOp.ts';
import {
  createSpanBuffer,
  createSpanLogger,
  createTraceRoot,
  defineLogSchema,
  ENTRY_TYPE_INFO,
  mergeWithSystemSchema,
  S,
} from '../../lmao/src/node.ts';

const QUICK = process.argv.includes('--quick');
const FORMAT = process.argv.includes('--json') ? 'json' : process.argv.includes('--markdown') ? 'markdown' : 'mitata';
const ITERATIONS = QUICK ? 250 : 10_000;
const CAPACITY = ITERATIONS + 2;
const STATIC_MESSAGE = 'request completed';
const ATTRIBUTE_NAMES = ['userId', 'retries', 'region', 'latencyMs', 'cached'] as const;
const MASK_64 = (1n << 64n) - 1n;

const schema = defineLogSchema(
  mergeWithSystemSchema({
    userId: S.category(),
    retries: S.number(),
    region: S.category(),
    latencyMs: S.number(),
    cached: S.boolean(),
  }),
  { _skipReservedNameValidation: true },
);

type AttributeName = (typeof ATTRIBUTE_NAMES)[number];
type AttributeValues = {
  userId: string;
  retries: number;
  region: string;
  latencyMs: number;
  cached: boolean;
};
type FluentEntry = {
  userId(value: string): FluentEntry;
  retries(value: number): FluentEntry;
  region(value: string): FluentEntry;
  latencyMs(value: number): FluentEntry;
  cached(value: boolean): FluentEntry;
  with(attributes: Partial<AttributeValues>): void;
};
type BenchmarkBuffer = {
  _writeIndex: number;
  _traceRoot: { writeLogEntry(buffer: BenchmarkBuffer, entryType: number): number };
  constructor: { stats: { totalWrites: number } };
  message_values: (string | undefined)[];
  message_nulls: Uint8Array | undefined;
  userId_values: (string | undefined)[];
  userId_nulls: Uint8Array;
  retries_values: Float64Array;
  retries_nulls: Uint8Array;
  region_values: (string | undefined)[];
  region_nulls: Uint8Array;
  latencyMs_values: Float64Array;
  latencyMs_nulls: Uint8Array;
  cached_values: Uint8Array;
  cached_nulls: Uint8Array;
};
type BenchmarkLogger = {
  _buffer: BenchmarkBuffer;
  _writeIndex: number;
  _checkOverflow(): void;
  info(message: string): FluentEntry;
  debug(message: string): FluentEntry;
};
type RuntimeBundle = {
  buffer: BenchmarkBuffer;
  logger: BenchmarkLogger;
};
type RunResult = {
  checksum: bigint;
};
type Tracker = {
  count: number;
  hash: bigint;
  trace: string[] | undefined;
};
type Runner = {
  label: string;
  run: () => number;
  preflight: () => RunResult;
  tracker: Tracker;
};

const tracerHooks = {
  onTraceStart() {},
  onTraceEnd() {},
  onSpanStart() {},
  onSpanEnd() {},
} satisfies Parameters<typeof createTraceRoot>[1];

function makeRuntimeBundle(label: string): RuntimeBundle {
  const traceRoot = createTraceRoot(`structured-template-${label}`, tracerHooks);
  // The generated buffer/logger expose schema-specific columns and fluent methods
  // that the public factory return types cannot express together.
  const buffer = createSpanBuffer(schema, traceRoot, DEFAULT_METADATA, CAPACITY) as unknown as BenchmarkBuffer;
  const logger = createSpanLogger(schema, buffer) as unknown as BenchmarkLogger;

  // Force lazy user columns to allocate outside all timed bodies.
  void buffer.userId_values;
  void buffer.userId_nulls;
  void buffer.retries_values;
  void buffer.retries_nulls;
  void buffer.region_values;
  void buffer.region_nulls;
  void buffer.latencyMs_values;
  void buffer.latencyMs_nulls;
  void buffer.cached_values;
  void buffer.cached_nulls;
  return { buffer, logger };
}

function resetBundle(bundle: RuntimeBundle): void {
  bundle.logger._buffer = bundle.buffer;
  bundle.buffer._writeIndex = 2;
  bundle.logger._writeIndex = 1;
}

function sourceMessage(dynamic: boolean, iteration: number): string {
  return dynamic ? `request-${iteration & 31}` : STATIC_MESSAGE;
}

function sourceAttribute(name: AttributeName, iteration: number): AttributeValues[AttributeName] {
  switch (name) {
    case 'userId':
      return `user-${iteration & 15}`;
    case 'retries':
      return iteration & 3;
    case 'region':
      return (iteration & 1) === 0 ? 'us-east' : 'eu-west';
    case 'latencyMs':
      return iteration + 0.25;
    case 'cached':
      return (iteration & 1) === 0;
  }
}

function sourceAttributes(count: number, iteration: number): AttributeValues {
  const values = {} as AttributeValues;
  for (let index = 0; index < count; index++) {
    const name = ATTRIBUTE_NAMES[index]!;
    values[name] = sourceAttribute(name, iteration) as never;
  }
  return values;
}

function sourceObject(count: number, iteration: number): Partial<AttributeValues> {
  switch (count) {
    case 1:
      return { userId: sourceAttribute('userId', iteration) as string };
    case 2:
      return {
        userId: sourceAttribute('userId', iteration) as string,
        retries: sourceAttribute('retries', iteration) as number,
      };
    case 5:
      return {
        userId: sourceAttribute('userId', iteration) as string,
        retries: sourceAttribute('retries', iteration) as number,
        region: sourceAttribute('region', iteration) as string,
        latencyMs: sourceAttribute('latencyMs', iteration) as number,
        cached: sourceAttribute('cached', iteration) as boolean,
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

function evaluateAttribute(tracker: Tracker, name: AttributeName, iteration: number): AttributeValues[AttributeName] {
  switch (name) {
    case 'userId':
      note(tracker, 'u');
      return `user-${iteration & 15}`;
    case 'retries':
      note(tracker, 'r');
      return iteration & 3;
    case 'region':
      note(tracker, 'g');
      return (iteration & 1) === 0 ? 'us-east' : 'eu-west';
    case 'latencyMs':
      note(tracker, 'l');
      return iteration + 0.25;
    case 'cached':
      note(tracker, 'c');
      return (iteration & 1) === 0;
  }
}

function evaluateAttributes(tracker: Tracker, count: number, iteration: number): AttributeValues {
  const values = {} as AttributeValues;
  for (let index = 0; index < count; index++) {
    const name = ATTRIBUTE_NAMES[index]!;
    values[name] = evaluateAttribute(tracker, name, iteration) as never;
  }
  return values;
}

function checksumIteration(checksum: bigint, message: string, attributes: AttributeValues, count: number): bigint {
  let next = (checksum * 1_000_003n + BigInt(message.length)) & MASK_64;
  for (let index = 0; index < count; index++) {
    const value = attributes[ATTRIBUTE_NAMES[index]!];
    if (typeof value === 'string') next = (next * 131n + BigInt(value.length)) & MASK_64;
    else if (typeof value === 'number') next = (next * 131n + BigInt(Math.trunc(value * 4))) & MASK_64;
    else next = (next * 131n + BigInt(value ? 1 : 0)) & MASK_64;
  }
  return next;
}

function storedAttribute(
  buffer: BenchmarkBuffer,
  name: AttributeName,
  row: number,
): string | number | boolean | undefined {
  switch (name) {
    case 'userId':
      return buffer.userId_values[row];
    case 'retries':
      return buffer.retries_values[row];
    case 'region':
      return buffer.region_values[row];
    case 'latencyMs':
      return buffer.latencyMs_values[row];
    case 'cached':
      return (buffer.cached_values[row >>> 3]! & (1 << (row & 7))) !== 0;
  }
}

function finishChecksum(checksum: bigint, tracker: Tracker, bundle: RuntimeBundle, count: number): RunResult {
  const activeBuffer = bundle.logger._buffer;
  const row = activeBuffer._writeIndex - 1;
  let result = checksum ^ tracker.hash ^ BigInt(tracker.count);
  const storedMessage = activeBuffer.message_values[row] as string | undefined;
  result ^= BigInt(storedMessage?.length ?? 0) << 8n;
  for (let index = 0; index < count; index++) {
    const stored = storedAttribute(activeBuffer, ATTRIBUTE_NAMES[index]!, row);
    if (typeof stored === 'string') result ^= BigInt(stored.length) << BigInt(16 + index * 5);
    else if (typeof stored === 'number') result ^= BigInt(Math.trunc(stored * 4)) << BigInt(16 + index * 5);
  }
  return { checksum: result & MASK_64 };
}

function expectedTrace(dynamic: boolean, attributeCount: number, iterations: number): string[] {
  const tokens = [...(dynamic ? ['m'] : []), ...['u', 'r', 'g', 'l', 'c'].slice(0, attributeCount)];
  return Array.from({ length: iterations }, () => tokens).flat();
}

function makeObject(tracker: Tracker, count: number, iteration: number): Partial<AttributeValues> {
  // This explicit object-literal matrix preserves property evaluation order while
  // ensuring each source expression is evaluated exactly once.
  switch (count) {
    case 1:
      return { userId: evaluateAttribute(tracker, 'userId', iteration) as string };
    case 2:
      return {
        userId: evaluateAttribute(tracker, 'userId', iteration) as string,
        retries: evaluateAttribute(tracker, 'retries', iteration) as number,
      };
    case 5:
      return {
        userId: evaluateAttribute(tracker, 'userId', iteration) as string,
        retries: evaluateAttribute(tracker, 'retries', iteration) as number,
        region: evaluateAttribute(tracker, 'region', iteration) as string,
        latencyMs: evaluateAttribute(tracker, 'latencyMs', iteration) as number,
        cached: evaluateAttribute(tracker, 'cached', iteration) as boolean,
      };
    default:
      throw new Error(`Unsupported attribute count: ${count}`);
  }
}

function applyUnrolled(
  logger: BenchmarkLogger,
  message: string,
  values: AttributeValues,
  count: number,
  level: 'info' | 'debug',
): void {
  const entry = logger[level](message);
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
  }
}

function directWrite(bundle: RuntimeBundle, message: string, values: AttributeValues, count: number): void {
  const logger = bundle.logger;
  logger._checkOverflow();
  const buffer = logger._buffer;
  const row = buffer._traceRoot.writeLogEntry(buffer, ENTRY_TYPE_INFO);
  logger._writeIndex = row;
  buffer.message_values[row] = message;
  if (buffer.message_nulls) buffer.message_nulls[row >>> 3] |= 1 << (row & 7);
  buffer.constructor.stats.totalWrites++;

  const write = (name: AttributeName, value: AttributeValues[AttributeName]): void => {
    const byte = row >>> 3;
    const mask = 1 << (row & 7);
    switch (name) {
      case 'userId':
        buffer.userId_values[row] = value as string;
        buffer.userId_nulls[byte] |= mask;
        return;
      case 'retries':
        buffer.retries_values[row] = value as number;
        buffer.retries_nulls[byte] |= mask;
        return;
      case 'region':
        buffer.region_values[row] = value as string;
        buffer.region_nulls[byte] |= mask;
        return;
      case 'latencyMs':
        buffer.latencyMs_values[row] = value as number;
        buffer.latencyMs_nulls[byte] |= mask;
        return;
      case 'cached':
        if (value) buffer.cached_values[byte] |= mask;
        else buffer.cached_values[byte] &= ~mask;
        buffer.cached_nulls[byte] |= mask;
    }
  };
  for (let index = 0; index < count; index++) {
    const name = ATTRIBUTE_NAMES[index]!;
    write(name, values[name]);
  }
}

function conceptualInfoObject(logger: BenchmarkLogger, message: string, attributes: Partial<AttributeValues>): void {
  // Isolated model of the proposed two-argument surface using today's runtime
  // operations. It measures call/evaluation shape, not an implemented fused writer.
  logger.info(message).with(attributes);
}

function makeRunner(
  label: string,
  attributeCount: number,
  dynamic: boolean,
  implementation: 'current' | 'conceptual' | 'unrolled' | 'staged' | 'raw-debug' | 'interpolation',
): Runner {
  const bundle = makeRuntimeBundle(`${label}-${attributeCount}-${dynamic ? 'dynamic' : 'static'}`);
  const tracker: Tracker = { count: 0, hash: 0n, trace: undefined };

  const run = (): number => {
    resetBundle(bundle);
    for (let iteration = 0; iteration < ITERATIONS; iteration++) {
      if (implementation === 'current') {
        const message = sourceMessage(dynamic, iteration);
        const attributes = sourceObject(attributeCount, iteration);
        bundle.logger.info(message).with(attributes);
      } else if (implementation === 'conceptual') {
        const message = sourceMessage(dynamic, iteration);
        const attributes = sourceObject(attributeCount, iteration);
        conceptualInfoObject(bundle.logger, message, attributes);
      } else {
        const message = sourceMessage(implementation === 'interpolation' || dynamic, iteration);
        const attributes = sourceAttributes(attributeCount, iteration);
        if (implementation === 'staged') directWrite(bundle, message, attributes, attributeCount);
        else
          applyUnrolled(
            bundle.logger,
            message,
            attributes,
            attributeCount,
            implementation === 'raw-debug' ? 'debug' : 'info',
          );
      }
    }
    const activeBuffer = bundle.logger._buffer;
    const row = activeBuffer._writeIndex - 1;
    return activeBuffer._writeIndex ^ ((activeBuffer.message_values[row] as string | undefined)?.length ?? 0);
  };

  const preflight = (): RunResult => {
    resetBundle(bundle);
    tracker.count = 0;
    tracker.hash = 0n;
    let semantic = 0n;

    for (let iteration = 0; iteration < ITERATIONS; iteration++) {
      if (implementation === 'current') {
        const message = evaluateMessage(tracker, dynamic, iteration);
        const attributes = makeObject(tracker, attributeCount, iteration);
        bundle.logger.info(message).with(attributes);
        semantic = checksumIteration(semantic, message, attributes as AttributeValues, attributeCount);
      } else if (implementation === 'conceptual') {
        const message = evaluateMessage(tracker, dynamic, iteration);
        const attributes = makeObject(tracker, attributeCount, iteration);
        conceptualInfoObject(bundle.logger, message, attributes);
        semantic = checksumIteration(semantic, message, attributes as AttributeValues, attributeCount);
      } else {
        const message =
          implementation === 'interpolation'
            ? evaluateMessage(tracker, true, iteration)
            : evaluateMessage(tracker, dynamic, iteration);
        const attributes = evaluateAttributes(tracker, attributeCount, iteration);
        if (implementation === 'staged') directWrite(bundle, message, attributes, attributeCount);
        else
          applyUnrolled(
            bundle.logger,
            message,
            attributes,
            attributeCount,
            implementation === 'raw-debug' ? 'debug' : 'info',
          );
        semantic = checksumIteration(semantic, message, attributes, attributeCount);
      }
    }
    return finishChecksum(semantic, tracker, bundle, attributeCount);
  };

  return { label, run, preflight, tracker };
}

function assertSemantics(runners: readonly Runner[], dynamic: boolean, attributeCount: number): void {
  const expected = expectedTrace(dynamic, attributeCount, ITERATIONS);
  let checksum: bigint | undefined;
  for (const runner of runners) {
    runner.tracker.trace = [];
    const result = runner.preflight();
    const actual = runner.tracker.trace;
    runner.tracker.trace = undefined;
    if (actual.length !== expected.length || actual.some((token, index) => token !== expected[index])) {
      throw new Error(`${runner.label} violated exactly-once source evaluation order`);
    }
    if (checksum === undefined) checksum = result.checksum;
    else if (result.checksum !== checksum) {
      throw new Error(`${runner.label} semantic checksum differed before timing`);
    }
  }
}

function registerScenario(attributeCount: 1 | 2 | 5, dynamic: boolean): void {
  const runners: Runner[] = [
    makeRunner('current-info-with-object', attributeCount, dynamic, 'current'),
    makeRunner('conceptual-info-object-api', attributeCount, dynamic, 'conceptual'),
    makeRunner('fluent-unrolled-chain', attributeCount, dynamic, 'unrolled'),
    makeRunner('compiler-staged-direct-writes', attributeCount, dynamic, 'staged'),
  ];
  if (dynamic) {
    runners.push(
      makeRunner('raw-dynamic-debug-control', attributeCount, true, 'raw-debug'),
      makeRunner('rejected-interpolation-control', attributeCount, true, 'interpolation'),
    );
  }

  assertSemantics(runners, dynamic, attributeCount);
  group(`structured-template | attrs=${attributeCount} | message=${dynamic ? 'dynamic' : 'static'}`, () => {
    for (const runner of runners) {
      const benchmark = bench(runner.label, runner.run);
      if (runner.label === 'current-info-with-object') benchmark.baseline(true);
    }
  });
}

for (const attributeCount of [1, 2, 5] as const) {
  summary(() => registerScenario(attributeCount, false));
  summary(() => registerScenario(attributeCount, true));
}

await run({ format: FORMAT, throw: true });
