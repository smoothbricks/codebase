/**
 * Scratch instrument: both arms, one process, log-count sweep.
 *
 * mitata's ms-scale tails are noise on a loaded machine, so this reports the
 * best of five timed blocks per arm — the floor a quiet core would give.
 */

import { defineOpContext } from '../src/lib/defineOpContext.js';
import { JsBufferStrategy } from '../src/lib/JsBufferStrategy.js';
import { S } from '../src/lib/schema/builder.js';
import { defineLogSchema } from '../src/lib/schema/defineLogSchema.js';
import { ThreadBufferStrategy } from '../src/lib/ThreadBufferStrategy.js';
import { createTraceRoot } from '../src/lib/traceRoot.node.js';
import { TestTracer } from '../src/lib/tracers/TestTracer.js';
import type { AnySpanBuffer } from '../src/lib/types.js';

const schema = defineLogSchema({ n: S.number() });
const context = defineOpContext({ logSchema: schema });
type Binding = typeof context;

const thread = await ThreadBufferStrategy.create<Binding['logBinding']['logSchema']>({ capacity: 64 });
const js = new JsBufferStrategy<Binding['logBinding']['logSchema']>();
const threadTracer = new TestTracer(context, { bufferStrategy: thread, createTraceRoot });
const jsTracer = new TestTracer(context, { bufferStrategy: js, createTraceRoot });

function arm(
  tracer: TestTracer<Binding>,
  strategy: { toArrowTable(buffer: AnySpanBuffer): unknown } | undefined,
  logs: number,
  iterations: number,
): number {
  const work = (): void => {
    tracer.clear();
    tracer.trace_fn(0, 'c', {}, (span) => {
      for (let i = 0; i < logs; i++) span.log.info(`m${i}`).n(i);
      return span.ok(1);
    });
    if (strategy === undefined) return;
    const root = tracer.rootBuffers[0];
    if (root === undefined) throw new Error('no root buffer to flush');
    strategy.toArrowTable(root);
  };
  for (let i = 0; i < 1500; i++) work();
  let best = Number.POSITIVE_INFINITY;
  for (let round = 0; round < 5; round++) {
    const start = Bun.nanoseconds();
    for (let i = 0; i < iterations; i++) work();
    best = Math.min(best, (Bun.nanoseconds() - start) / iterations);
  }
  return best;
}
const sweep = [0, 1, 8, 32, 50, 128];
const verdictOf = (jsNs: number, threadNs: number): string =>
  threadNs < jsNs ? 'WIN' : threadNs < jsNs * 1.05 ? 'PARITY' : 'LOSS';

console.log('--- write-only (no flush) ---');
for (const logs of sweep) {
  const iterations = logs > 60 ? 2000 : 6000;
  const jsNs = arm(jsTracer, undefined, logs, iterations);
  const threadNs = arm(threadTracer, undefined, logs, iterations);
  console.log(
    `logs-${String(logs).padStart(3, '0')}  js ${(jsNs / 1000).toFixed(2)} us   ` +
      `thread ${(threadNs / 1000).toFixed(2)} us   ratio ${(threadNs / jsNs).toFixed(2)}x  ${verdictOf(jsNs, threadNs)}`,
  );
}

console.log('--- flush-inclusive (span + toArrowTable) ---');
for (const logs of sweep) {
  const iterations = logs > 60 ? 1000 : 3000;
  const jsNs = arm(jsTracer, js, logs, iterations);
  const threadNs = arm(threadTracer, thread, logs, iterations);
  console.log(
    `logs-${String(logs).padStart(3, '0')}  js ${(jsNs / 1000).toFixed(2)} us   ` +
      `thread ${(threadNs / 1000).toFixed(2)} us   ratio ${(threadNs / jsNs).toFixed(2)}x  ${verdictOf(jsNs, threadNs)}`,
  );
}
const stats = thread.runtime.internStats;
const total = stats.hits + stats.misses;
console.log(
  `intern cache: ${stats.hits} hits / ${stats.misses} misses = ` +
    `${((stats.hits / total) * 100).toFixed(2)}% hit rate`,
);
