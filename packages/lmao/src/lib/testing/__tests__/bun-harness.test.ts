import * as bunTest from 'bun:test';
import { describe, expect, it } from 'bun:test';
import { defineOpContext } from '../../defineOpContext.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import { TestTracer } from '../../tracers/TestTracer.js';
import { type BunTestModuleShape, makeBunTestSuiteTracer, makeTestTracer } from '../bun-harness.js';
import { spanException } from '../facts.js';
import { querySpan } from '../queryable-span.js';

type BunTestModuleLike = BunTestModuleShape;

/** What the harness handed the underlying runner: the label, bun's timeout/options slot, and which slot held it. */
type Registration = { name: string; options: unknown; optionsFirst?: true };

type ImmediateBody = () => unknown | Promise<unknown>;

function createImmediateTestFn(registrations?: Registration[]): BunTestModuleLike['it'] {
  const runBase = (name: string, fnOrOptions: ImmediateBody | unknown, optionsOrFn?: ImmediateBody | unknown) => {
    if (typeof fnOrOptions === 'function') {
      registrations?.push({ name, options: optionsOrFn });
      return fnOrOptions();
    }
    if (typeof optionsOrFn !== 'function') throw new TypeError(`${name}: no test function`);
    registrations?.push({ name, options: fnOrOptions, optionsFirst: true });
    return optionsOrFn();
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
  it('defaults to an in-memory TestTracer, then hands bun the timeout slot it was given', async () => {
    const tracer = makeTestTracer(baseBinding);
    tracer.setup();
    // sqlite and verbose are off.
    expect(tracer.getTracer()).toBeInstanceOf(TestTracer);

    const registrations: Registration[] = [];
    const wrapped = tracer.createBunTestMock(createImmediateBunTestModule(registrations));

    await wrapped.it('bare millisecond timeout', () => undefined, 300_000);
    await wrapped.test('full options object', () => undefined, { timeout: 250, retry: 2 });
    await wrapped.it('caller passed nothing', () => undefined);
    let ranOptionsFirst = false;
    await wrapped.it('options before the function', { timeout: 125 }, () => {
      ranOptionsFirst = true;
    });

    expect(registrations).toEqual([
      { name: 'bare millisecond timeout', options: 300_000 },
      { name: 'full options object', options: { timeout: 250, retry: 2 } },
      { name: 'caller passed nothing', options: undefined },
      { name: 'options before the function', options: { timeout: 125 }, optionsFirst: true },
    ]);
    expect(ranOptionsFirst).toBe(true);
  });
});

// The immediate module returns what the harness hands it, so the test awaits the harness's own settlement.
describe('bun harness done callbacks', () => {
  it('settles a done-style test when it calls done, failing with the value it passes', async () => {
    const tracer = makeTestTracer(baseBinding);
    tracer.setup();
    const wrapped = tracer.createBunTestMock(createImmediateBunTestModule());
    const spans: ReturnType<typeof tracer.useTestSpan>[] = [];

    const settled = Promise.resolve(
      wrapped.it('calls done with an error after returning', (done) => {
        spans.push(tracer.useTestSpan());
        queueMicrotask(() => done(new Error('late')));
      }),
    );

    await expect(settled).rejects.toThrow('late');
    expect(
      spans.map((span) =>
        querySpan(span.buffer).facts().has(spanException('calls done with an error after returning', 'late')),
      ),
    ).toEqual([true]);
  });
});

// Each case takes plain Bun's verdict for the same callback (Bun 1.4.3 and 1.4.4, measured): a case Bun passes
// registers with `it` and a case Bun fails registers with `it.failing`, so a harness that settles a done-style callback
// differently fails.
// A failing case's span must record the same failure: Bun also fails a test on a rejection nobody handles, so the
// verdict alone cannot tell a harness that settled on done from one that leaked done's rejection.
describe('bun harness done callbacks settle as Bun settles them', () => {
  const tracer = makeTestTracer(baseBinding);
  tracer.setup();
  const wrapped = tracer.createBunTestMock(bunTest);
  const failures: { span: ReturnType<typeof tracer.useTestSpan>; fact: ReturnType<typeof spanException> }[] = [];
  const expectFailure = (label: string, message: string) =>
    failures.push({ span: tracer.useTestSpan(), fact: spanException(label, message) });

  wrapped.it('keeps the first done call when it passes', (done) => {
    done();
    done(new Error('after the first done'));
  });
  wrapped.it.failing('keeps the first done call when it fails', (done) => {
    expectFailure('keeps the first done call when it fails', 'first done');
    done(new Error('first done'));
    done();
  });
  wrapped.it('settles on the returned promise when done is never called', (_done) => Promise.resolve());
  wrapped.it.failing('fails on done(error) before a resolved promise is returned', (done) => {
    expectFailure('fails on done(error) before a resolved promise is returned', 'before return');
    done(new Error('before return'));
    return Promise.resolve();
  });
  wrapped.it.failing('fails on done(error) a microtask after a resolved promise is returned', (done) => {
    expectFailure('fails on done(error) a microtask after a resolved promise is returned', 'in a microtask');
    queueMicrotask(() => done(new Error('in a microtask')));
    return Promise.resolve();
  });
  wrapped.it.failing('fails on a rejected promise even after done()', (done) => {
    expectFailure('fails on a rejected promise even after done()', 'rejected');
    done();
    return Promise.reject(new Error('rejected'));
  });
  wrapped.it.failing('fails on a promise rejected with no reason', (_done) => {
    expectFailure('fails on a promise rejected with no reason', 'undefined: undefined');
    return Promise.reject();
  });
  wrapped.it('records each failing case as an exception, not ok', () => {
    expect(failures.map(({ span, fact }) => querySpan(span.buffer).facts().has(fact))).toEqual([
      true,
      true,
      true,
      true,
      true,
    ]);
  });
});

// Rows register through the real bun:test `each`, which formats every row's title itself and never re-enters a wrapped
// `it`. Bigint rows stay exact: the harness hands the runner each row untouched and names the row's span by the label
// template.
describe('bun harness each rows', () => {
  const tracer = makeTestTracer(baseBinding);
  tracer.setup();
  const wrapped = tracer.createBunTestMock(bunTest);

  wrapped.it.each([[1n, 9_007_199_254_740_993n]])('tuple row %p and %p', async (small, beyondF64) => {
    const span = tracer.useTestSpan();
    await Promise.resolve();
    expect(tracer.useTestSpan()).toBe(span);
    expect(querySpan(span.buffer).name).toBe('tuple row %p and %p');
    expect([small, beyondF64]).toEqual([1n, 9_007_199_254_740_993n]);
  });

  wrapped.test.each([{ until: -9_223_372_036_854_775_807n }])('object row $until', ({ until }) => {
    expect(querySpan(tracer.useTestSpan().buffer).name).toBe('object row $until');
    expect(until).toBe(-9_223_372_036_854_775_807n);
  });

  // A row callback that declares more parameters than its row has values takes `done`, as under plain Bun.
  wrapped.it.each([[1n]])('done row %p', (value, done) => {
    expect(querySpan(tracer.useTestSpan().buffer).name).toBe('done row %p');
    expect(value).toBe(1n);
    done();
  });

  wrapped.describe.each([['alpha']])('suite %s', (label) => {
    wrapped.it('records the row suite in its describe path', () => {
      expect(tracer.useTestSpan().buffer.describe_values[0]).toBe('suite %s');
      expect(label).toBe('alpha');
    });
  });
});

// Modifiers register through the real bun:test members; each one's callbacks run traced like a plain it().
describe('bun harness modifiers', () => {
  const tracer = makeTestTracer(baseBinding);
  tracer.setup();
  const wrapped = tracer.createBunTestMock(bunTest);

  wrapped.it.concurrent.each([[1n], [2n]])('concurrent row %p', async () => {
    const span = tracer.useTestSpan();
    await Promise.resolve();
    expect(tracer.useTestSpan()).toBe(span);
    expect(querySpan(span.buffer).name).toBe('concurrent row %p');
  });

  // A `.failing` row passes when its body fails. This row fails only through the done it calls after returning, so it
  // passes only if the harness settles it on done, and its span must record that failure.
  const failingRows: ReturnType<typeof tracer.useTestSpan>[] = [];
  wrapped.it.failing.each([[1n]])('failing done row %p', (_value, done) => {
    failingRows.push(tracer.useTestSpan());
    queueMicrotask(() => done(new Error('late')));
  });
  wrapped.it('records the failing done row as an exception, not ok', () => {
    expect(
      failingRows.map((span) => querySpan(span.buffer).facts().has(spanException('failing done row %p', 'late'))),
    ).toEqual([true]);
  });

  wrapped.describe(makeTestTracer, () => {
    wrapped.it('records a function label by its name', () => {
      expect(tracer.useTestSpan().buffer.describe_values[0]).toBe('makeTestTracer');
    });
  });
});
