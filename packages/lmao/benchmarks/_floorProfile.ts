/**
 * Scratch instrument: JSC sampling profile of the EMPTY-span floor (logs-000).
 *
 * Every prior profile targeted the marginal row path (128 rows/span). This one
 * has zero rows so span open/close cost is 100% of the signal. `sudo sample`
 * cannot name Bun JIT frames; bun:jsc can.
 */

import { samplingProfilerStackTraces, startSamplingProfiler } from 'bun:jsc';
import { defineOpContext } from '../src/lib/defineOpContext.js';
import { JsBufferStrategy } from '../src/lib/JsBufferStrategy.js';
import { S } from '../src/lib/schema/builder.js';
import { defineLogSchema } from '../src/lib/schema/defineLogSchema.js';
import { ThreadBufferStrategy } from '../src/lib/ThreadBufferStrategy.js';
import { createTraceRoot } from '../src/lib/traceRoot.node.js';
import { TestTracer } from '../src/lib/tracers/TestTracer.js';

const which = Bun.argv[2] ?? 'thread';
const schema = defineLogSchema({ n: S.number() });
const context = defineOpContext({ logSchema: schema });
type Binding = typeof context;

const strategy =
  which === 'js'
    ? new JsBufferStrategy<Binding['logBinding']['logSchema']>()
    : await ThreadBufferStrategy.create<Binding['logBinding']['logSchema']>({ capacity: 64 });
const tracer = new TestTracer(context, { bufferStrategy: strategy, createTraceRoot });

const work = (): void => {
  tracer.clear();
  tracer.trace_fn(0, 'c', {}, (span) => span.ok(1));
};

for (let i = 0; i < 20_000; i++) work();

startSamplingProfiler();
const spans = 3_000_000;
const start = Bun.nanoseconds();
for (let i = 0; i < spans; i++) work();
const elapsed = Bun.nanoseconds() - start;

const traces = samplingProfilerStackTraces();
const self: Record<string, number> = {};
let samples = 0;
for (const trace of traces.traces) {
  const top = trace.frames[0];
  if (top === undefined) continue;
  samples += 1;
  const file = (top.sourceURL ?? '').split('/').slice(-1)[0];
  const line = top.line === 4294967295 ? '-' : top.line;
  const label = `${top.name || '(anonymous)'}  [${top.category}]  ${file}:${line}`;
  self[label] = (self[label] ?? 0) + 1;
}
console.log(`arm=${which} spans=${spans} rows/span=0`);
console.log(`wall ${(elapsed / spans).toFixed(0)} ns/span`);
console.log(`samples=${samples}`);
const ranked = Object.entries(self).sort((a, b) => b[1] - a[1]);
for (const [label, count] of ranked.slice(0, 30)) {
  console.log(`  ${((count / samples) * 100).toFixed(1).padStart(5)}%  ${count.toString().padStart(6)}  ${label}`);
}
