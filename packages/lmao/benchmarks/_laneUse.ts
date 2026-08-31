/**
 * Scratch instrument: which write surface does the generated logger use?
 *
 * ThreadSpanView exposes both `${name}_values[i] = v` (Proxy trap) and
 * `${name}(pos, v)` (method). Only one is hot, and the fix differs.
 */

import { defineOpContext } from '../src/lib/defineOpContext.js';
import { S } from '../src/lib/schema/builder.js';
import { defineLogSchema } from '../src/lib/schema/defineLogSchema.js';
import { ThreadBufferStrategy } from '../src/lib/ThreadBufferStrategy.js';
import { createTraceRoot } from '../src/lib/traceRoot.node.js';
import { TestTracer } from '../src/lib/tracers/TestTracer.js';
import { requireThreadSpanView } from '../src/lib/wasm/threadSpanView.js';

const schema = defineLogSchema({ n: S.number() });
const context = defineOpContext({ logSchema: schema });
type Binding = typeof context;

const thread = await ThreadBufferStrategy.create<Binding['logBinding']['logSchema']>({ capacity: 64 });
const tracer = new TestTracer(context, { bufferStrategy: thread, createTraceRoot });

const hits: Record<string, number> = { n_values: 0, n_method: 0, message_values: 0, message_method: 0 };

// Instrument one live view by shadowing its own properties with counting wrappers.
tracer.clear();
tracer.trace_fn(0, 'probe', {}, (span) => {
  const view = requireThreadSpanView(span._spanBuffer ?? span);
  const laneN = Reflect.get(view, 'n_values');
  const methodN = Reflect.get(view, 'n');
  const laneMessage = Reflect.get(view, 'message_values');
  const methodMessage = Reflect.get(view, 'message');
  console.log('n_values is', laneN === undefined ? 'MISSING' : typeof laneN);
  console.log('n is', methodN === undefined ? 'MISSING' : typeof methodN);
  console.log('message_values is', laneMessage === undefined ? 'MISSING' : typeof laneMessage);
  console.log('message is', methodMessage === undefined ? 'MISSING' : typeof methodMessage);

  Reflect.set(
    view,
    'n_values',
    new Proxy([] as unknown[], {
      set(target, prop, value) {
        hits.n_values += 1;
        return Reflect.set(target, prop, value);
      },
    }),
  );
  Reflect.set(view, 'n', (pos: number, value: unknown) => {
    hits.n_method += 1;
    return methodN(pos, value);
  });
  Reflect.set(
    view,
    'message_values',
    new Proxy([] as unknown[], {
      set(target, prop, value) {
        hits.message_values += 1;
        return Reflect.set(target, prop, value);
      },
    }),
  );
  Reflect.set(view, 'message', (pos: number, value: unknown) => {
    hits.message_method += 1;
    return methodMessage.call(view, pos, value);
  });

  for (let i = 0; i < 4; i++) span.log.info(`m${i}`).n(i);
  return span.ok(1);
});

console.log('after 4 log rows:', hits);
