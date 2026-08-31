/**
 * A-B-B-A paired js-heap vs thread-buffer write-path harness.
 *
 * Sequential OFF-then-ON (or js-then-thread) charges in-run drift to the
 * second arm and has flipped a sign on this machine between load 8 and 33.
 * Positions 1+4 average to js-heap, 2+3 to thread-buffer, so monotonic
 * drift cancels. Every printed row carries the 1-minute load average.
 *
 * Do not treat a single ns/row as the production number: report the four
 * raw positions plus the paired means, and the load, so variance under
 * contention is visible.
 *
 * Run: bun packages/lmao/benchmarks/abba-lanes.ts
 */
import os from 'node:os';
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

const js = new JsBufferStrategy<Binding['logBinding']['logSchema']>();
const thread = await ThreadBufferStrategy.create<Binding['logBinding']['logSchema']>({ capacity: 64 });
const jsTracer = new TestTracer(context, { bufferStrategy: js, createTraceRoot });
const threadTracer = new TestTracer(context, { bufferStrategy: thread, createTraceRoot });

type Lane = 'js-heap' | 'thread-buffer';

function load1(): number {
  return os.loadavg()[0] ?? 0;
}

function timeLane(lane: Lane, logs: number, iterations: number): { ns: number; load: number } {
  const tracer = lane === 'js-heap' ? jsTracer : threadTracer;
  const work = (): void => {
    tracer.clear();
    tracer.trace_fn(0, 'abba', {}, (span) => {
      for (let i = 0; i < logs; i++) span.log.info(`m${i}`).n(i);
      return span.ok(1);
    });
  };
  for (let i = 0; i < Math.min(1500, iterations); i++) work();
  const start = Bun.nanoseconds();
  for (let i = 0; i < iterations; i++) work();
  return { ns: (Bun.nanoseconds() - start) / iterations, load: load1() };
}

function printRow(label: string, jsNs: number, threadNs: number, load: number): void {
  const ratio = threadNs / jsNs;
  const verdict = threadNs < jsNs ? 'WIN' : threadNs < jsNs * 1.05 ? 'PARITY' : 'LOSS';
  const jsRow = jsNs;
  const threadRow = threadNs;
  console.log(
    `${label.padEnd(14)}  js ${(jsRow / 1000).toFixed(2)} us   thread ${(threadRow / 1000).toFixed(2)} us   ` +
      `ratio ${ratio.toFixed(2)}x  ${verdict}   load1 ${load.toFixed(2)}`,
  );
}

const shapes: ReadonlyArray<{ name: string; logs: number; iterations: number }> = [
  { name: 'logs-000', logs: 0, iterations: 8000 },
  { name: 'logs-032', logs: 32, iterations: 4000 },
  { name: 'logs-128', logs: 128, iterations: 2000 },
];

console.log('A-B-B-A  (js, thread, thread, js). Paired means: js=(p1+p4)/2  thread=(p2+p3)/2');
console.log(`1-minute load at start: ${load1().toFixed(2)}`);
console.log('');

for (const shape of shapes) {
  const p1 = timeLane('js-heap', shape.logs, shape.iterations);
  const p2 = timeLane('thread-buffer', shape.logs, shape.iterations);
  const p3 = timeLane('thread-buffer', shape.logs, shape.iterations);
  const p4 = timeLane('js-heap', shape.logs, shape.iterations);
  const jsNs = (p1.ns + p4.ns) / 2;
  const threadNs = (p2.ns + p3.ns) / 2;
  const load = load1();
  console.log(
    `${shape.name} positions  p1-js ${(p1.ns / 1000).toFixed(2)}  p2-th ${(p2.ns / 1000).toFixed(2)}  ` +
      `p3-th ${(p3.ns / 1000).toFixed(2)}  p4-js ${(p4.ns / 1000).toFixed(2)}  load1 ${load.toFixed(2)}`,
  );
  printRow(`${shape.name} paired`, jsNs, threadNs, load);
  if (shape.logs > 0) {
    console.log(
      `${shape.name} ns/row   js ${(jsNs / shape.logs).toFixed(1)}   ` +
        `thread ${(threadNs / shape.logs).toFixed(1)}   ` +
        `(includes floor; subtract logs-000 paired for marginal)   load1 ${load.toFixed(2)}`,
    );
  }
  console.log('');
}

console.log(`1-minute load at end: ${load1().toFixed(2)}`);
console.log('WIN bar: thread max < js min across paired means is not claimed from a single ABBA pass.');
