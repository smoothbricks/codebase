/**
 * Scratch instrument: JSC sampling profile of the js-heap row path, with the
 * caller chain under every timestamp-adjacent frame.
 *
 * `sudo sample` shows `???` for every JIT frame under Bun, so it cannot name JS
 * work; JSC's own profiler can. The point of the caller chain is that a frame
 * label like `bigint` is a *frame*, not a mechanism: it could be `BigInt()`
 * construction, `+`/`<=` arithmetic, the `hrtime.bigint()` clock read, or the
 * `BigInt64Array` store. Only the parent frame distinguishes them.
 */

import { samplingProfilerStackTraces, startSamplingProfiler } from 'bun:jsc';
import { defineOpContext } from '../src/lib/defineOpContext.js';
import { JsBufferStrategy } from '../src/lib/JsBufferStrategy.js';
import { S } from '../src/lib/schema/builder.js';
import { defineLogSchema } from '../src/lib/schema/defineLogSchema.js';
import { createTraceRoot } from '../src/lib/traceRoot.node.js';
import { TestTracer } from '../src/lib/tracers/TestTracer.js';

const LOGS = Number(Bun.argv[2] ?? 50);
const schema = defineLogSchema({ n: S.number() });
const context = defineOpContext({ logSchema: schema });
type Binding = typeof context;

const strategy = new JsBufferStrategy<Binding['logBinding']['logSchema']>();
const tracer = new TestTracer(context, { bufferStrategy: strategy, createTraceRoot });

const work = (): void => {
  tracer.clear();
  tracer.trace_fn(0, 'c', {}, (span) => {
    for (let i = 0; i < LOGS; i++) span.log.info(`m${i}`).n(i);
    return span.ok(1);
  });
};

for (let i = 0; i < 3000; i++) work();

const frameLabel = (frame: { name?: string; category?: string; sourceURL?: string; line?: number }): string => {
  const file = (frame.sourceURL ?? '').split('/').slice(-1)[0];
  const line = frame.line === 4294967295 ? '-' : frame.line;
  return `${frame.name || '(anonymous)'} [${frame.category}] ${file}:${line}`;
};

startSamplingProfiler();
const spans = Number(Bun.argv[3] ?? 2_000_000);
const start = Bun.nanoseconds();
for (let i = 0; i < spans; i++) work();
const elapsed = Bun.nanoseconds() - start;

const traces = samplingProfilerStackTraces();
const self: Record<string, number> = {};
/** For every self-frame of interest, which caller chain produced it. */
const chains: Record<string, Record<string, number>> = {};
const INTEREST = /bigint|BigInt|nextTimestamp|hrtime|timestamp/i;
let samples = 0;
for (const trace of traces.traces) {
  const top = trace.frames[0];
  if (top === undefined) continue;
  samples += 1;
  const label = frameLabel(top);
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

console.log(`arm=js-heap rows/span=${LOGS} spans=${spans} samples=${samples}`);
console.log(`wall ${(elapsed / spans / 1000).toFixed(2)} us/span`);
console.log('--- self frames ---');
for (const [label, count] of Object.entries(self)
  .sort((a, b) => b[1] - a[1])
  .slice(0, 24)) {
  console.log(`  ${((count / samples) * 100).toFixed(1).padStart(5)}%  ${count.toString().padStart(6)}  ${label}`);
}
console.log('--- caller chains under timestamp-adjacent frames ---');
for (const [label, bucket] of Object.entries(chains)) {
  console.log(`  ${label}`);
  for (const [chain, count] of Object.entries(bucket)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 6)) {
    console.log(`     ${((count / samples) * 100).toFixed(1).padStart(5)}%  ${count.toString().padStart(6)}  ${chain}`);
  }
}
