import { describe, expect, it } from 'bun:test';
import { defineOpContext } from '../../defineOpContext.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import { makeBunTestSuiteTracer, makeTestTracer } from '../bun-harness.js';

type BunTestModuleLike = {
  it: (name: string, fn: () => unknown | Promise<unknown>) => unknown;
  describe: (name: string, fn: () => void) => unknown;
  test: (name: string, fn: () => unknown | Promise<unknown>) => unknown;
};

function createImmediateIt() {
  const run = ((_: string, fn: () => unknown | Promise<unknown>) => fn()) as BunTestModuleLike['it'] & {
    skip: (...args: unknown[]) => unknown;
    only: BunTestModuleLike['it'];
    todo: (...args: unknown[]) => unknown;
    each: unknown;
    skipIf: (condition: boolean) => unknown;
    if: (condition: boolean) => unknown;
  };
  run.skip = () => undefined;
  run.only = run;
  run.todo = () => undefined;
  run.each = run;
  run.skipIf = (condition: boolean) => (condition ? run.skip : run);
  run.if = (condition: boolean) => (condition ? run : run.skip);
  return run;
}

function createImmediateDescribe() {
  const run = ((_: string, fn: () => void) => fn()) as BunTestModuleLike['describe'] & {
    skip: (...args: unknown[]) => unknown;
    only: BunTestModuleLike['describe'];
    todo: (...args: unknown[]) => unknown;
    each: unknown;
    skipIf: (condition: boolean) => unknown;
    if: (condition: boolean) => unknown;
  };
  run.skip = () => undefined;
  run.only = run;
  run.todo = () => undefined;
  run.each = run;
  run.skipIf = (condition: boolean) => (condition ? run.skip : run);
  run.if = (condition: boolean) => (condition ? run : run.skip);
  return run;
}

function createImmediateBunTestModule(): BunTestModuleLike {
  const itFn = createImmediateIt();
  return {
    it: itFn,
    describe: createImmediateDescribe(),
    test: itFn,
  };
}

const baseBinding = defineOpContext({
  logSchema: defineLogSchema({ base_field: S.category() }),
});

const suiteWithExtension = makeBunTestSuiteTracer(baseBinding, {
  testLogSchema: {
    test_metric: S.number(),
  },
});

type ExtendedSpan = ReturnType<typeof suiteWithExtension.useTestSpan>;
type HasExtendedTag = ExtendedSpan['tag'] extends { test_metric: (value: number) => unknown } ? true : false;
type HasExtendedLog =
  ReturnType<ExtendedSpan['log']['info']> extends { test_metric: (value: number) => unknown } ? true : false;
const hasExtendedTag: HasExtendedTag = true;
const hasExtendedLog: HasExtendedLog = true;

describe('bun harness test log schema extension', () => {
  it('includes extension fields in suite span typing', () => {
    expect(hasExtendedTag).toBe(true);
    expect(hasExtendedLog).toBe(true);
  });

  it('writes extension fields through tag and log APIs', async () => {
    const tracer = makeTestTracer(baseBinding, {
      testLogSchema: {
        test_metric: S.number(),
        test_note: S.text(),
      },
    });
    tracer.setup();

    const wrappedModule = tracer.createBunTestMock(createImmediateBunTestModule()) as BunTestModuleLike;
    await wrappedModule.it('writes test-only fields', async () => {
      const span = tracer.useTestSpan();
      span.tag.test_metric(123);
      span.log.info('row-1').test_metric(456).test_note('hello');

      const buffer = span.buffer;
      expect(buffer.test_metric_values[0]).toBe(123);
      expect(buffer.test_metric_values[2]).toBe(456);
      expect(buffer.test_note_values[2]).toBe('hello');
    });
  });
});
