import { describe, expect, it } from 'bun:test';
import { defineOpContext } from '../../defineOpContext.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import { TestTracer } from '../../tracers/TestTracer.js';
import { getTracer, initTraceTestRun, installVitestTestTracing, makeVitestTestTracer } from '../vitest-harness.js';

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

    expect(getTracer()).toBe(tracer.getTracer());
  });

  it('routes initTraceTestRun through the active suite tracer path', () => {
    initTraceTestRun(vitestBinding);

    expect(getTracer()).toBeInstanceOf(TestTracer);
  });
});
