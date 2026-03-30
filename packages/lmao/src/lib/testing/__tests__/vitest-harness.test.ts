import { describe, expect, it } from 'bun:test';
import { defineOpContext } from '../../defineOpContext.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import { TestTracer } from '../../tracers/TestTracer.js';
import { makeVitestTestTracer } from '../vitest-harness.js';

const vitestBinding = defineOpContext({
  logSchema: defineLogSchema({ test_field: S.category() }),
});

describe('vitest harness tracer defaults', () => {
  it('defaults to an in-memory TestTracer when sqlite and verbose are off', () => {
    const tracer = makeVitestTestTracer({ binding: vitestBinding });
    tracer.initTraceTestRun();

    expect(tracer.getTracer()).toBeInstanceOf(TestTracer);
  });
});
