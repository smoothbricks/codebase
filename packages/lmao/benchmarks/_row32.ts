/**
 * Scratch instrument: one arm, 32 log rows, best-of-five. Ablation driver
 * measures against this so each removed piece gets a named nanosecond cost.
 */

import { defineOpContext } from '../src/lib/defineOpContext.js';
import { S } from '../src/lib/schema/builder.js';
import { defineLogSchema } from '../src/lib/schema/defineLogSchema.js';
import { ThreadBufferStrategy } from '../src/lib/ThreadBufferStrategy.js';
import { createTraceRoot } from '../src/lib/traceRoot.node.js';
import { TestTracer } from '../src/lib/tracers/TestTracer.js';

const schema = defineLogSchema({ n: S.number() });
const context = defineOpContext({ logSchema: schema });
type Binding = typeof context;

const thread = await ThreadBufferStrategy.create<Binding['logBinding']['logSchema']>({ capacity: 64 });
const tracer = new TestTracer(context, { bufferStrategy: thread, createTraceRoot });

const LOGS = 32;
const work = (): void => {
  tracer.clear();
  tracer.trace_fn(0, 'c', {}, (span) => {
    for (let i = 0; i < LOGS; i++) span.log.info(`m${i}`).n(i);
    return span.ok(1);
  });
};

for (let i = 0; i < 2000; i++) work();
let best = Number.POSITIVE_INFINITY;
for (let round = 0; round < 5; round++) {
  const start = Bun.nanoseconds();
  for (let i = 0; i < 6000; i++) work();
  best = Math.min(best, (Bun.nanoseconds() - start) / 6000);
}
const empty = 1530;
console.log(`span+32 ${(best / 1000).toFixed(2)} us | marginal ${((best - empty) / LOGS).toFixed(0)} ns/row`);
