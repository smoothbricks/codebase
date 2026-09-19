import { it } from 'vitest';
import { defineOpContext } from '../../../defineOpContext.js';
import { defineLogSchema } from '../../../schema/defineLogSchema.js';
import { makeVitestTestTracer } from '../../vitest-harness.js';

const tracer = makeVitestTestTracer({ binding: defineOpContext({ logSchema: defineLogSchema({}) }) });
tracer.initTraceTestRun({
  sqlite: {
    createAsyncDatabase: () => ({
      async exec() {
        throw new Error('trace-persistence-refused');
      },
      prepare() {
        throw new Error('cannot prepare after refused initialization');
      },
      async close() {},
    }),
  },
});
it('passing test cannot hide failed durable flush', () => {});
