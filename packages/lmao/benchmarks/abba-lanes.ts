/**
 * A-B-B-A paired js-heap vs thread-buffer write-path harness.
 *
 * Positions 1+4 are js-heap, 2+3 thread-buffer (monotonic drift cancels).
 * Each position is the MINIMUM ns/iter across blocks: contention only adds
 * time, so a mean absorbs it and a floor rejects almost all of it. The mean
 * is printed beside the floor; that gap IS the contention. Three repetitions;
 * a direction is quoted only when every repetition agrees on the sign. Every
 * row carries the 1-minute load average.
 *
 * Run: bun packages/lmao/benchmarks/abba-lanes.ts
 * Canonical table waits on the JS clock; this file is the harness only.
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

function timeLane(lane: Lane, logs: number, iterations: number): { floorNs: number; meanNs: number } {
  const tracer = lane === 'js-heap' ? jsTracer : threadTracer;
  const work = (): void => {
    tracer.clear();
    tracer.trace_fn(0, 'abba', {}, (span) => {
      for (let i = 0; i < logs; i++) span.log.info(`m${i}`).n(i);
      return span.ok(1);
    });
  };
  for (let i = 0; i < Math.min(1500, iterations); i++) work();
  const blocks = 5;
  let sum = 0;
  let floorNs = Number.POSITIVE_INFINITY;
  for (let block = 0; block < blocks; block++) {
    const start = Bun.nanoseconds();
    for (let i = 0; i < iterations; i++) work();
    const ns = (Bun.nanoseconds() - start) / iterations;
    sum += ns;
    if (ns < floorNs) floorNs = ns;
  }
  return { floorNs, meanNs: sum / blocks };
}

const shapes: ReadonlyArray<{ name: string; logs: number; iterations: number }> = [
  { name: 'logs-000', logs: 0, iterations: 8000 },
  { name: 'logs-032', logs: 32, iterations: 4000 },
  { name: 'logs-128', logs: 128, iterations: 2000 },
];

const repeats = 3;
console.log('A-B-B-A floors. js=(p1+p4)/2  thread=(p2+p3)/2. Direction only if all repeats agree.');
console.log(`1-minute load at start: ${(os.loadavg()[0] ?? 0).toFixed(2)}`);
console.log('');

for (const shape of shapes) {
  const ratios: number[] = [];
  for (let repeat = 1; repeat <= repeats; repeat++) {
    const p1 = timeLane('js-heap', shape.logs, shape.iterations);
    const p2 = timeLane('thread-buffer', shape.logs, shape.iterations);
    const p3 = timeLane('thread-buffer', shape.logs, shape.iterations);
    const p4 = timeLane('js-heap', shape.logs, shape.iterations);
    const jsFloor = (p1.floorNs + p4.floorNs) / 2;
    const threadFloor = (p2.floorNs + p3.floorNs) / 2;
    const jsMean = (p1.meanNs + p4.meanNs) / 2;
    const threadMean = (p2.meanNs + p3.meanNs) / 2;
    const load = os.loadavg()[0] ?? 0;
    const ratio = threadFloor / jsFloor;
    ratios.push(ratio);
    console.log(
      `${shape.name} r${repeat} floors  p1-js ${(p1.floorNs / 1000).toFixed(2)}  p2-th ${(p2.floorNs / 1000).toFixed(2)}  ` +
        `p3-th ${(p3.floorNs / 1000).toFixed(2)}  p4-js ${(p4.floorNs / 1000).toFixed(2)}  paired js ${(jsFloor / 1000).toFixed(2)} thread ${(threadFloor / 1000).toFixed(2)}  ` +
        `ratio ${ratio.toFixed(2)}x  mean-gap js ${((jsMean - jsFloor) / 1000).toFixed(2)} thread ${((threadMean - threadFloor) / 1000).toFixed(2)}  load1 ${load.toFixed(2)}`,
    );
  }
  const signs = ratios.map((ratio) => (ratio < 1 ? 'WIN' : ratio < 1.05 ? 'PARITY' : 'LOSS'));
  const direction = signs.every((sign) => sign === signs[0])
    ? `DIRECTION CONSISTENT ${signs[0]}`
    : 'DIRECTION UNRESOLVED';
  console.log(`${shape.name} ${direction}  ratios ${ratios.map((ratio) => ratio.toFixed(2)).join(', ')}`);
  console.log('');
}

console.log(`1-minute load at end: ${(os.loadavg()[0] ?? 0).toFixed(2)}`);
console.log('WIN bar: thread max < js min. A floor that survives ABBA is stronger than a sequential mean.');
