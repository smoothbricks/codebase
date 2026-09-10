import { describe, expect, it } from 'bun:test';
import { defineOpContext } from '../../defineOpContext.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import { TestTracer } from '../../tracers/TestTracer.js';
import {
  getTracer,
  initTraceTestRun,
  installVitestTestTracing,
  makeVitestTestTracer,
  type VitestModuleShape,
} from '../vitest-harness.js';

const vitestBinding = defineOpContext({
  logSchema: defineLogSchema({ test_field: S.category() }),
});

describe('vitest harness tracer defaults', () => {
  it('defaults to an in-memory TestTracer when sqlite and verbose are off', () => {
    const tracer = makeVitestTestTracer({ binding: vitestBinding });
    tracer.initTraceTestRun();

    expect(tracer.getTracer()).toBeInstanceOf(TestTracer);
  });

  it('uses the installed suite tracer for the global tracer accessor', () => {
    const tracer = makeVitestTestTracer({ binding: vitestBinding });
    installVitestTestTracing(tracer);

    // Global accessor erases the concrete binding generic; assert identity directly.
    expect(Object.is(getTracer(), tracer.getTracer())).toBe(true);
  });

  it('routes initTraceTestRun through the active suite tracer path', () => {
    initTraceTestRun(vitestBinding);

    expect(getTracer()).toBeInstanceOf(TestTracer);
  });
});

/** What the harness handed the underlying runner: the label and vitest's timeout/options slot. */
type Registration = { name: string; options: unknown };

function createRecordingVitestModule(registrations: Registration[]): VitestModuleShape {
  const record = (name: string, fn: () => unknown, options?: unknown) => {
    registrations.push({ name, options });
    return fn();
  };
  const branch = Object.assign(record, {
    skip: record,
    only: record,
    todo: record,
    each: (...args: unknown[]) => args,
    skipIf: () => record,
  });
  return { describe: branch, it: branch, test: branch };
}

// Argument-forwarding assertions, not end-to-end timeout expiry: driving a real expiry would need a
// test slower than the ambient `testTimeout`. The seam asserted here is the exact call the harness
// makes into vitest's runner, which is where a dropped timeout becomes an ignored timeout.
describe('vitest harness per-test options forwarding', () => {
  it('hands vitest the timeout slot it was given', async () => {
    const tracer = makeVitestTestTracer({ binding: vitestBinding });
    tracer.initTraceTestRun();

    const registrations: Registration[] = [];
    const wrapped = tracer.createVitestMock(createRecordingVitestModule(registrations));

    await wrapped.it('bare millisecond timeout', () => undefined, 300_000);
    await wrapped.test('full options object', () => undefined, { timeout: 250, retry: 2 });
    await wrapped.it('caller passed nothing', () => undefined);
    wrapped.describe('suite timeout', () => undefined, 120_000);

    expect(registrations).toEqual([
      { name: 'bare millisecond timeout', options: 300_000 },
      { name: 'full options object', options: { timeout: 250, retry: 2 } },
      { name: 'caller passed nothing', options: undefined },
      { name: 'suite timeout', options: 120_000 },
    ]);
  });
});
