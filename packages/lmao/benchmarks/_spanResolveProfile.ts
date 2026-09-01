/**
 * Scratch instrument: span-open/span-end path cost per lane, split by dispatch
 * shape, with JSC tier-up evidence for the callsite-plan resolver.
 *
 * Shapes:
 * - root-fn:   trace_fn root span, zero logs (resolver early-return path)
 * - root-op:   trace(op) root span (resolver runs in _createRootContext)
 * - child-op:  root trace_fn + 8 op-target child spans (resolver runs in _spanPre)
 * - child-fn:  root trace_fn + 8 closure child spans (control: receiver plan)
 *
 * Prints per-shape min/mean ns per span open+end, then numberOfDFGCompiles /
 * reoptimizationRetryCount for the functions on the path, then a sampling
 * profile of the hottest shape per lane.
 */
import {
  numberOfDFGCompiles,
  reoptimizationRetryCount,
  samplingProfilerStackTraces,
  startSamplingProfiler,
} from 'bun:jsc';
import { defineOpContext } from '../src/lib/defineOpContext.js';
import { JsBufferStrategy } from '../src/lib/JsBufferStrategy.js';
import { S } from '../src/lib/schema/builder.js';
import { defineLogSchema } from '../src/lib/schema/defineLogSchema.js';
import { ThreadBufferStrategy } from '../src/lib/ThreadBufferStrategy.js';
import { createTraceRoot } from '../src/lib/traceRoot.node.js';
import { TestTracer } from '../src/lib/tracers/TestTracer.js';

const schema = defineLogSchema({ n: S.number() });
const context = defineOpContext({ logSchema: schema });
type Binding = typeof context;

const childOp = context.defineOp('leaf', (ctx) => ctx.ok(1));
const rootOp = context.defineOp('root', (ctx) => ctx.ok(1));

const js = new JsBufferStrategy<Binding['logBinding']['logSchema']>();
const thread = await ThreadBufferStrategy.create<Binding['logBinding']['logSchema']>({ capacity: 64 });
const jsTracer = new TestTracer(context, { bufferStrategy: js, createTraceRoot });
const threadTracer = new TestTracer(context, { bufferStrategy: thread, createTraceRoot });

type Lane = 'js-heap' | 'thread-buffer';
const CHILDREN = 8;

function makeWork(lane: Lane, shape: string): { work: () => void; spansPerIter: number } {
  const tracer = lane === 'js-heap' ? jsTracer : threadTracer;
  switch (shape) {
    case 'root-fn':
      return {
        spansPerIter: 1,
        work: () => {
          tracer.clear();
          tracer.trace_fn(0, 'r', {}, (span) => span.ok(1));
        },
      };
    case 'root-op':
      return {
        spansPerIter: 1,
        work: () => {
          tracer.clear();
          void tracer.trace('r', rootOp);
        },
      };
    case 'child-op':
      return {
        spansPerIter: CHILDREN + 1,
        work: () => {
          tracer.clear();
          tracer.trace_fn(0, 'r', {}, (span) => {
            for (let i = 0; i < CHILDREN; i++) void span.span('c', childOp);
            return span.ok(1);
          });
        },
      };
    case 'child-fn':
      return {
        spansPerIter: CHILDREN + 1,
        work: () => {
          tracer.clear();
          tracer.trace_fn(0, 'r', {}, (span) => {
            for (let i = 0; i < CHILDREN; i++) void span.span('c', (c: typeof span) => c.ok(1));
            return span.ok(1);
          });
        },
      };
    default:
      throw new Error(`unknown shape ${shape}`);
  }
}

function timeShape(lane: Lane, shape: string, iterations: number): { floorNs: number; meanNs: number } {
  const { work, spansPerIter } = makeWork(lane, shape);
  for (let i = 0; i < 2000; i++) work();
  const blocks = 5;
  let sum = 0;
  let floorNs = Number.POSITIVE_INFINITY;
  for (let block = 0; block < blocks; block++) {
    const start = Bun.nanoseconds();
    for (let i = 0; i < iterations; i++) work();
    const ns = (Bun.nanoseconds() - start) / (iterations * spansPerIter);
    sum += ns;
    if (ns < floorNs) floorNs = ns;
  }
  return { floorNs, meanNs: sum / blocks };
}

const shapes = ['root-fn', 'root-op', 'child-op', 'child-fn'];
const lanes: Lane[] = ['js-heap', 'thread-buffer'];

for (const shape of shapes) {
  for (const lane of lanes) {
    // ABBA-lite: lane pairing is per-line; three repetitions for direction.
    const reps = [1, 2, 3].map(() => timeShape(lane, shape, shape.startsWith('root') ? 6000 : 1500));
    const floors = reps.map((r) => r.floorNs.toFixed(0)).join('/');
    const means = reps.map((r) => r.meanNs.toFixed(0)).join('/');
    console.log(`${shape.padEnd(9)} ${lane.padEnd(13)} floor ns/span ${floors}  mean ${means}`);
  }
}

// Tier-up evidence for the resolver and lifecycle functions.
const spanContextProto: object | null = Object.getPrototypeOf(rootOp.callsitePlan.SpanContextClass.prototype);
const report = (name: string, fn: unknown): void => {
  if (typeof fn !== 'function') {
    console.log(`${name}: <not a function>`);
    return;
  }
  console.log(
    `${name}: DFG compiles ${numberOfDFGCompiles(fn as never)} reopt retries ${reoptimizationRetryCount(fn as never)}`,
  );
};
console.log('--- tier-up evidence ---');
report('SpanContextClass.prototype._spanPre', Reflect.get(rootOp.callsitePlan.SpanContextClass.prototype, '_spanPre'));
report('spanContextProto._spanPre (base)', Reflect.get(spanContextProto ?? {}, '_spanPre'));
{
  const jsBuffer =
    jsTracer.rootBuffers[0] ?? (jsTracer.trace_fn(0, 'probe', {}, (span) => span.ok(1)), jsTracer.rootBuffers[0]);
  if (jsBuffer) {
    report('js buffer._appenders.writeSpanEnd', jsBuffer._appenders.writeSpanEnd);
    report('js buffer._appendLogEntry', jsBuffer._appendLogEntry);
  }
  threadTracer.clear();
  threadTracer.trace_fn(0, 'probe', {}, (span) => span.ok(1));
  const threadBuffer = threadTracer.rootBuffers[0];
  if (threadBuffer) {
    report('thread buffer._appenders.writeSpanEnd', threadBuffer._appenders.writeSpanEnd);
    report('thread buffer._appendLogEntry', threadBuffer._appendLogEntry);
  }
}

// Sampling profile: the child-op shape on each lane.
for (const lane of lanes) {
  const { work } = makeWork(lane, 'child-op');
  for (let i = 0; i < 2000; i++) work();
  startSamplingProfiler();
  const iters = 120_000;
  const start = Bun.nanoseconds();
  for (let i = 0; i < iters; i++) work();
  const elapsed = Bun.nanoseconds() - start;
  const traces = samplingProfilerStackTraces();
  const self: Record<string, number> = {};
  const chains: Record<string, Record<string, number>> = {};
  const INTEREST = /resolve|WeakMap|spanPre|writeSpan|createChild|SpanContext|openSpan|end/i;
  let samples = 0;
  for (const trace of traces.traces) {
    const top = trace.frames[0];
    if (top === undefined) continue;
    samples += 1;
    const file = (top.sourceURL ?? '').split('/').slice(-1)[0];
    const label = `${top.name || '(anonymous)'} [${top.category}] ${file}:${top.line === 4294967295 ? '-' : top.line}`;
    self[label] = (self[label] ?? 0) + 1;
    if (!INTEREST.test(label)) continue;
    const chain = trace.frames
      .slice(1, 5)
      .map((f) => f.name || '(anon)')
      .join(' < ');
    let bucket = chains[label];
    if (bucket === undefined) {
      bucket = {};
      chains[label] = bucket;
    }
    bucket[chain] = (bucket[chain] ?? 0) + 1;
  }
  console.log(
    `--- sampling profile ${lane} child-op: ${samples} samples, ${(elapsed / iters / (CHILDREN + 1)).toFixed(0)} ns/span ---`,
  );
  for (const [label, count] of Object.entries(self)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 20)) {
    console.log(`  ${((count / samples) * 100).toFixed(1).padStart(5)}%  ${count.toString().padStart(6)}  ${label}`);
  }
  for (const [label, bucket] of Object.entries(chains)) {
    console.log(`  chains under ${label}`);
    for (const [chain, count] of Object.entries(bucket)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 4)) {
      console.log(
        `     ${((count / samples) * 100).toFixed(1).padStart(5)}%  ${count.toString().padStart(6)}  ${chain}`,
      );
    }
  }
}
