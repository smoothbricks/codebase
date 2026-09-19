import * as vitest from 'vitest';
import { defineOpContext } from '../../../defineOpContext.js';
import { S } from '../../../schema/builder.js';
import { defineLogSchema } from '../../../schema/defineLogSchema.js';
import { TestTracer } from '../../../tracers/TestTracer.js';
import {
  getTracer,
  installVitestTestTracing,
  makeVitestTestTracer,
  describe as tracedDescribe,
  it as tracedIt,
} from '../../vitest-harness.js';

const binding = defineOpContext({ logSchema: defineLogSchema({ test_field: S.category() }) });
const tracer = makeVitestTestTracer({ binding });
installVitestTestTracing(tracer);
// This assignment checks the real published framework namespace, not a permissive test double.
const wrapped: typeof vitest = tracer.createVitestMock(vitest);
const seen: string[] = [];
const record = async (label: string) => {
  const ctx = tracer.useTestSpan();
  await Promise.resolve();
  vitest.expect(tracer.useTestSpan()).toBe(ctx);
  ctx.tag.test_field(label);
  seen.push(label);
};

wrapped.describe.skipIf(false)('wrapped suite', () => {
  const rows: readonly (readonly [string, number])[] = [
    ['first', 1],
    ['second', 2],
  ];
  wrapped.it.each(rows)('tuple %s', async (label, number) => {
    vitest.expect(number).toBe(label === 'first' ? 1 : 2);
    await record(label);
  });
  wrapped.test.skipIf(false)('conditional', { retry: 1 }, async () => {
    await record('conditional');
  });
  wrapped.it.skipIf(true)('must remain skipped', () => {
    throw new Error('skipped body ran');
  });
  wrapped.it.runIf(true)(
    'enabled',
    async () => {
      await record('enabled');
    },
    1_000,
  );
  wrapped.it.each`
    label       | count
    ${'tagged'} | ${3}
  `('tagged $label', { timeout: 1_000 }, async ({ label, count }) => {
    vitest.expect(count).toBe(3);
    await record(label);
  });
  wrapped.test.extend({ fixture: 42 })('fixture dependency', async ({ fixture }) => {
    vitest.expect(fixture).toBe(42);
    await record('fixture');
  });
  wrapped.test.for([7])('for context', async (value, { task }) => {
    vitest.expect(value).toBe(7);
    vitest.expect(task.name).toBe('for context');
    await record('for');
  });
  const builder = wrapped.test.extend('base', 41).extend('answer', ({ base }) => base + 1);
  builder.describe('builder fixtures', () => {
    builder.override('answer', 43)('builder fixture', async ({ answer }) => {
      vitest.expect(answer).toBe(43);
      await record('builder');
    });
  });
});

tracedDescribe.each(['standalone'])('%s suite', (label) => {
  tracedIt.each([label])('%s each', async (value) => {
    await record(value);
  });
});

const gate = Promise.withResolvers<void>();
const concurrentSpans = new Set<unknown>();
let entered = 0;
wrapped.it.concurrent.each([1, 2])('concurrent %i', async () => {
  const ctx = tracer.useTestSpan();
  concurrentSpans.add(ctx);
  if (++entered === 2) gate.resolve();
  await gate.promise;
  vitest.expect(tracer.useTestSpan()).toBe(ctx);
  vitest.expect(concurrentSpans.size).toBe(2);
});

let attempts = 0;
wrapped.it('retry still observes assertion rejection', { retry: 1 }, () => {
  // The original harness swallowed errors beginning with expect( and bypassed retry.
  if (++attempts === 1) throw new Error('expect(retry) must fail the first attempt');
});

vitest.it('all registered variants execute and preserve the installed tracer', () => {
  vitest
    .expect(seen.sort())
    .toEqual(['builder', 'conditional', 'enabled', 'first', 'fixture', 'for', 'second', 'standalone', 'tagged']);
  vitest.expect(attempts).toBe(2);
  vitest.expect(getTracer()).toBe(tracer.getTracer());
  vitest.expect(getTracer()).toBeInstanceOf(TestTracer);
});
