import { describe, expect, it } from 'bun:test';
import { defineOpContext } from '../../defineOpContext.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import { type BunTestModuleShape, makeBunTestSuiteTracer, makeTestTracer } from '../bun-harness.js';

type BunTestModuleLike = BunTestModuleShape;

function createImmediateTestFn(): BunTestModuleLike['it'] {
  const runBase = (_name: string, fn: () => unknown | Promise<unknown>) => fn();
  return Object.assign(runBase, it, {
    only: runBase,
    skipIf: (condition: boolean) => (condition ? it.skip : runBase),
    if: (condition: boolean) => (condition ? runBase : it.skip),
  });
}

function createImmediateDescribeFn(): BunTestModuleLike['describe'] {
  const runBase = (_name: string, fn: () => void) => fn();
  return Object.assign(runBase, describe, {
    only: runBase,
    skipIf: (condition: boolean) => (condition ? describe.skip : runBase),
    if: (condition: boolean) => (condition ? runBase : describe.skip),
  });
}

function createImmediateIt() {
  return createImmediateTestFn();
}

function createImmediateDescribe() {
  return createImmediateDescribeFn();
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

    const wrappedModule = tracer.createBunTestMock(createImmediateBunTestModule());
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
