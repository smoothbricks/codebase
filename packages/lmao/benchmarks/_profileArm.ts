/**
 * Scratch instrument: one arm, marginal-heavy, run long enough to sample.
 *
 * `bun benchmarks/_profileArm.ts thread|js` then `sudo sample <pid>` — the
 * per-row cost dominates the fixed floor ~40:1 at 128 rows, so the frames a
 * profiler names are the row path, not span setup.
 */

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

const LOGS = 128;
const work = (): void => {
  tracer.clear();
  tracer.trace_fn(0, 'c', {}, (span) => {
    for (let i = 0; i < LOGS; i++) span.log.info(`m${i}`).n(i);
    return span.ok(1);
  });
};

console.log(`arm=${which} pid=${process.pid} logs=${LOGS}`);
const deadline = Bun.nanoseconds() + 20_000_000_000;
let spans = 0;
while (Bun.nanoseconds() < deadline) {
  for (let i = 0; i < 1000; i++) work();
  spans += 1000;
}
console.log(`arm=${which} spans=${spans}`);
