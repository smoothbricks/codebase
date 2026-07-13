/**
 * Transformer-output-shape benchmark for six fixed-position tag writes.
 *
 * The benchmark uses a compiler-style CallsitePlan and its real SpanContext-owned
 * WriterState. It compares the generic object fallback, the generated fluent
 * writer, direct transformer output, and the staged batching candidate.
 *
 * Run: bun packages/lmao-ttsc/benchmarks/inline-vs-codegen.bench.ts --quick
 */

import { bench, do_not_optimize, group, run } from 'mitata';
import type { WriterState } from '../../lmao/src/lib/codegen/fixedPositionWriterGenerator.ts';
import type { TagWriter as ContextTagWriter } from '../../lmao/src/lib/opContext/spanContextTypes.ts';
import {
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY,
  RUNTIME_HINT_TAG,
} from '../../lmao/src/lib/runtimeHint.ts';
import {
  createSpanBuffer,
  createTraceRoot,
  defineLogSchema,
  defineOpContext,
  JsBufferStrategy,
  Ok,
  S,
  type SpanBuffer,
  TestTracer,
} from '../../lmao/src/node.ts';

const CAPACITY = 64;
const FORMAT = process.argv.includes('--json') ? 'json' : process.argv.includes('--markdown') ? 'markdown' : 'mitata';
const OPERATIONS: readonly ['DELETE', 'INSERT', 'SELECT', 'UPDATE'] = ['DELETE', 'INSERT', 'SELECT', 'UPDATE'];
const EAGER_COLUMNS: readonly string[] = ['operation', 'userId', 'region', 'latencyMs', 'retries', 'cached'];

const schema = defineLogSchema({
  operation: S.enum(OPERATIONS),
  userId: S.category(),
  region: S.category(),
  latencyMs: S.number(),
  retries: S.number(),
  cached: S.boolean(),
});
const opContext = defineOpContext({ logSchema: schema });
const tracer = new TestTracer(opContext, {
  bufferStrategy: new JsBufferStrategy(),
  createTraceRoot,
});
const benchmarkOp = opContext.defineOp('inline-vs-codegen', () => new Ok(undefined), undefined, {
  runtimeHint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_MESSAGE_LAYOUT_DYNAMIC_ONLY | RUNTIME_HINT_TAG | CAPACITY,
  eagerColumns: EAGER_COLUMNS,
});
const plan = benchmarkOp.callsitePlan;

interface TagViews {
  operationNulls: Uint8Array;
  operationValues: Uint8Array;
  userIdNulls: Uint8Array;
  userIdValues: string[];
  regionNulls: Uint8Array;
  regionValues: string[];
  latencyMsNulls: Uint8Array;
  latencyMsValues: Float64Array;
  retriesNulls: Uint8Array;
  retriesValues: Float64Array;
  cachedNulls: Uint8Array;
  cachedValues: Uint8Array;
}

interface Bundle {
  buffer: SpanBuffer<typeof plan.schema>;
  context: WriterState & { readonly tag: ContextTagWriter<typeof plan.schema> };
  views: TagViews;
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

function makeBundle(label: string): Bundle {
  const traceRoot = createTraceRoot(`inline-vs-codegen-${label}`, tracer);
  const buffer = createSpanBuffer(plan.schema, traceRoot, plan.metadata, CAPACITY, plan.SpanBufferClass);
  plan.appenders.writeSpanStart(buffer, benchmarkOp.metadata.name);
  const context = new plan.SpanContextClass(buffer, plan.schema, plan);
  const views: TagViews = {
    operationNulls: uint8Values(buffer.operation_nulls, 'operation_nulls'),
    operationValues: uint8Values(buffer.operation_values, 'operation_values'),
    userIdNulls: uint8Values(buffer.userId_nulls, 'userId_nulls'),
    userIdValues: stringValues(buffer.userId_values, 'userId_values'),
    regionNulls: uint8Values(buffer.region_nulls, 'region_nulls'),
    regionValues: stringValues(buffer.region_values, 'region_values'),
    latencyMsNulls: uint8Values(buffer.latencyMs_nulls, 'latencyMs_nulls'),
    latencyMsValues: float64Values(buffer.latencyMs_values, 'latencyMs_values'),
    retriesNulls: uint8Values(buffer.retries_nulls, 'retries_nulls'),
    retriesValues: float64Values(buffer.retries_values, 'retries_values'),
    cachedNulls: uint8Values(buffer.cached_nulls, 'cached_nulls'),
    cachedValues: uint8Values(buffer.cached_values, 'cached_values'),
  };
  return { buffer, context, views };
}

type TagValues = {
  operation: 'SELECT';
  userId: string;
  region: string;
  latencyMs: number;
  retries: number;
  cached: boolean;
};

function valuesFor(iteration: number): TagValues {
  return {
    operation: 'SELECT',
    userId: `u${iteration}`,
    region: 'eu-west',
    latencyMs: iteration,
    retries: 2,
    cached: (iteration & 1) === 0,
  };
}

function writeObjectFallback(bundle: Bundle, iteration: number): number {
  bundle.context.tag.with(valuesFor(iteration));
  return checksum(bundle);
}

function writeGeneratedFluent(bundle: Bundle, iteration: number): number {
  bundle.context.tag
    .operation('SELECT')
    .userId(`u${iteration}`)
    .region('eu-west')
    .latencyMs(iteration)
    .retries(2)
    .cached((iteration & 1) === 0);
  return checksum(bundle);
}

function writeDirect(bundle: Bundle, iteration: number): number {
  const views = bundle.views;
  views.operationNulls[0] |= 1;
  views.operationValues[0] = 2;
  views.userIdNulls[0] |= 1;
  views.userIdValues[0] = `u${iteration}`;
  views.regionNulls[0] |= 1;
  views.regionValues[0] = 'eu-west';
  views.latencyMsNulls[0] |= 1;
  views.latencyMsValues[0] = iteration;
  views.retriesNulls[0] |= 1;
  views.retriesValues[0] = 2;
  views.cachedNulls[0] |= 1;
  if ((iteration & 1) === 0) views.cachedValues[0] |= 1;
  else views.cachedValues[0] &= ~1;
  return checksum(bundle);
}

const staging = new Float64Array(3);

function writeBatched(bundle: Bundle, iteration: number): number {
  const views = bundle.views;
  staging[0] = 2;
  staging[1] = iteration;
  staging[2] = 2;
  views.userIdValues[0] = `u${iteration}`;
  views.regionValues[0] = 'eu-west';
  views.operationNulls[0] |= 1;
  views.userIdNulls[0] |= 1;
  views.regionNulls[0] |= 1;
  views.latencyMsNulls[0] |= 1;
  views.retriesNulls[0] |= 1;
  views.cachedNulls[0] |= 1;
  if ((iteration & 1) === 0) views.cachedValues[0] |= 1;
  else views.cachedValues[0] &= ~1;
  views.operationValues[0] = staging[0];
  views.latencyMsValues[0] = staging[1];
  views.retriesValues[0] = staging[2];
  return checksum(bundle);
}

function checksum(bundle: Bundle): number {
  const views = bundle.views;
  const userId = views.userIdValues[0];
  const region = views.regionValues[0];
  return (
    views.operationValues[0] * 1_000_003 +
    (userId?.length ?? 0) * 10_007 +
    (region?.length ?? 0) * 101 +
    Math.trunc(views.latencyMsValues[0]) * 17 +
    Math.trunc(views.retriesValues[0]) * 5 +
    ((views.cachedValues[0] & 1) !== 0 ? 1 : 0)
  );
}

const objectBundle = makeBundle('object');
const generatedBundle = makeBundle('generated');
const directBundle = makeBundle('direct');
const batchedBundle = makeBundle('batched');

const expected = writeObjectFallback(objectBundle, 42);
for (const observation of [
  ['generated WriterState fluent chain', writeGeneratedFluent(generatedBundle, 42)],
  ['compiler direct writes', writeDirect(directBundle, 42)],
  ['staged direct writes', writeBatched(batchedBundle, 42)],
]) {
  const label = observation[0];
  const checksumValue = observation[1];
  if (typeof label !== 'string' || typeof checksumValue !== 'number' || checksumValue !== expected) {
    throw new Error(`${String(label)} semantic checksum differed before timing`);
  }
}

let objectIteration = 0;
let generatedIteration = 0;
let directIteration = 0;
let batchedIteration = 0;

group('6 tag writes (enum + 2 category + 2 number + boolean)', () => {
  bench('A fluent with-object (runtime fallback)', () => {
    do_not_optimize(writeObjectFallback(objectBundle, objectIteration++));
  });

  bench('B plan-generated fluent chain (WriterState)', () => {
    do_not_optimize(writeGeneratedFluent(generatedBundle, generatedIteration++));
  });

  bench('C inlined direct (transformer output)', () => {
    do_not_optimize(writeDirect(directBundle, directIteration++));
  });

  bench('D inlined batched (staging candidate)', () => {
    do_not_optimize(writeBatched(batchedBundle, batchedIteration++));
  });
});

await run({ format: FORMAT, throw: true });
