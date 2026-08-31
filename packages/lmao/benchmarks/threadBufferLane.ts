/**
 * Same-run, same-machine ThreadSpanBuffer arm shared by the six old-path benches.
 * The logical workload is N log rows on one root span; both strategies execute it
 * in the same mitata group so the deleting commit can record both numbers.
 */

import { bench, group, run } from 'mitata';
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

export async function registerThreadBufferLane(benchName: string, logs: number): Promise<void> {
  const js = new JsBufferStrategy<Binding['logBinding']['logSchema']>();
  const thread = await ThreadBufferStrategy.create<Binding['logBinding']['logSchema']>({ capacity: 64 });
  const jsTracer = new TestTracer(context, { bufferStrategy: js, createTraceRoot });
  const threadTracer = new TestTracer(context, { bufferStrategy: thread, createTraceRoot });

  const run = (tracer: TestTracer<Binding>): void => {
    tracer.clear();
    tracer.trace_fn(0, benchName, {}, (span) => {
      for (let i = 0; i < logs; i++) span.log.info(`m${i}`).n(i);
      return span.ok(1);
    });
  };

  group(`${benchName} js-heap vs thread-buffer`, () => {
    bench('js-heap', () => {
      run(jsTracer);
    });
    bench('thread-buffer', () => {
      run(threadTracer);
    });
  });
}

if (import.meta.main) {
  const arms: ReadonlyArray<readonly [string, number]> = [
    ['span-creation-layout', 8],
    ['arrow-flush-path', 50],
    ['message-layout', 32],
    ['span-pooling', 32],
    ['timestamps', 32],
    ['remapped-child-path', 8],
  ];
  for (const [name, logs] of arms) {
    await registerThreadBufferLane(name, logs);
  }
  await run({ colors: false });
}
