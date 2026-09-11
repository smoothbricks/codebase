import { describe, expect, it } from 'bun:test';
import { defineOpContext } from '../../defineOpContext.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import { TestTracer } from '../../tracers/TestTracer.js';
import { type BunTestModuleShape, makeBunTestSuiteTracer, makeTestTracer } from '../bun-harness.js';

type BunTestModuleLike = BunTestModuleShape;

/** What the harness handed the underlying runner: the label and bun's timeout/options slot. */
type Registration = { name: string; options: unknown };

function createImmediateTestFn(registrations?: Registration[]): BunTestModuleLike['it'] {
  const runBase = (name: string, fn: () => unknown | Promise<unknown>, options?: unknown) => {
    registrations?.push({ name, options });
    return fn();
  };
  return Object.assign(runBase, it, {
    only: runBase,
    skipIf: (condition: boolean) => (condition ? it.skip : runBase),
    if: (condition: boolean) => (condition ? runBase : it.skip),
  });
}

function createImmediateDescribeFn(registrations?: Registration[]): BunTestModuleLike['describe'] {
  const runBase = (name: string, fn: () => void) => {
    registrations?.push({ name, options: undefined });
    return fn();
  };
  return Object.assign(runBase, describe, {
    only: runBase,
    skipIf: (condition: boolean) => (condition ? describe.skip : runBase),
    if: (condition: boolean) => (condition ? runBase : describe.skip),
  });
}

function createImmediateBunTestModule(registrations?: Registration[]): BunTestModuleLike {
  const itFn = createImmediateTestFn(registrations);
  return {
    it: itFn,
    describe: createImmediateDescribeFn(registrations),
    test: itFn,
  };
}

const baseBinding = defineOpContext({
  logSchema: defineLogSchema({ base_field: S.category() }),
});

const suiteWithExtension = makeBunTestSuiteTracer(baseBinding, {
  extraTestColumns: {
    test_metric: S.number(),
  },
});

const suiteWithoutExtension = makeBunTestSuiteTracer(baseBinding);
type BaseSpan = ReturnType<typeof suiteWithoutExtension.useTestSpan>;
type ExtendedSpan = ReturnType<typeof suiteWithExtension.useTestSpan>;
type HasExtendedTag = ExtendedSpan['tag'] extends { test_metric: (value: number) => unknown } ? true : false;
type HasExtendedLog =
  ReturnType<ExtendedSpan['log']['info']> extends { test_metric: (value: number) => unknown } ? true : false;
const hasExtendedTag: HasExtendedTag = true;
const hasExtendedLog: HasExtendedLog = true;

function assertHarnessResultInference(base: BaseSpan, extended: ExtendedSpan): void {
  const baseValue: number = base.ok(1).base_field('base').describe('suite').value;
  const extendedValue: string = extended.ok(1).test_metric(200).map(String).base_field('result').value;
  const extendedError: string = extended.err('failed').test_metric(500).error;
  // @ts-expect-error - a base-only harness must not acquire extension setters
  base.ok(1).test_metric(200);
  // @ts-expect-error - extension fields retain their declared value types
  extended.ok(1).test_metric('200');
  void baseValue;
  void extendedValue;
  void extendedError;
}
void assertHarnessResultInference;

describe('bun harness test log schema extension', () => {
  it('includes extension fields in suite span typing', () => {
    expect(hasExtendedTag).toBe(true);
    expect(hasExtendedLog).toBe(true);
  });

  it('writes extension fields through tag, log, and result APIs', async () => {
    const tracer = makeTestTracer(baseBinding, {
      extraTestColumns: {
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
      const result = span.ok('done').test_metric(789).test_note('result');
      expect(result.value).toBe('done');
      expect(buffer.test_metric_values[1]).toBe(789);
      expect(buffer.test_note_values[1]).toBe('result');
    });
  });

  it('defaults to an in-memory TestTracer when sqlite and verbose are off', () => {
    const tracer = makeTestTracer(baseBinding);
    tracer.setup();

    expect(tracer.getTracer()).toBeInstanceOf(TestTracer);
  });

  it('rejects duplicate extra test columns instead of silently erasing them', () => {
    const tracer = makeTestTracer(baseBinding, {
      extraTestColumns: {
        base_field: S.category(),
      },
    });

    expect(() => tracer.setup()).toThrow(
      "Test harness schema column 'base_field' already exists in the bound log schema",
    );
  });
});

// Argument-forwarding assertions, not end-to-end timeout expiry: driving a real expiry would need a
// test slower than the ambient `--timeout`. The seam asserted here is the exact call the harness
// makes into bun's runner, which is where a dropped timeout becomes an ignored timeout.
describe('bun harness per-test options forwarding', () => {
  it('hands bun the timeout slot it was given', async () => {
    const tracer = makeTestTracer(baseBinding);
    tracer.setup();

    const registrations: Registration[] = [];
    const wrapped = tracer.createBunTestMock(createImmediateBunTestModule(registrations));

    await wrapped.it('bare millisecond timeout', () => undefined, 300_000);
    await wrapped.test('full options object', () => undefined, { timeout: 250, retry: 2 });
    await wrapped.it('caller passed nothing', () => undefined);

    expect(registrations).toEqual([
      { name: 'bare millisecond timeout', options: 300_000 },
      { name: 'full options object', options: { timeout: 250, retry: 2 } },
      { name: 'caller passed nothing', options: undefined },
    ]);
  });
});
