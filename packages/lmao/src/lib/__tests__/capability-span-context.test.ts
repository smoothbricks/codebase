import { describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import { defineOpContext } from '../defineOpContext.js';
import type { OpContext, OpContextOf, SpanContext } from '../opContext/types.js';
import { Ok, defineCodeError } from '../result.js';
import { resolveMessage } from '../resolveMessage.js';
import {
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_CAPABILITIES_MASK,
  RUNTIME_HINT_DEPS,
  RUNTIME_HINT_FF,
  RUNTIME_HINT_FULL_CAPABILITIES,
  RUNTIME_HINT_LOG,
  RUNTIME_HINT_RESERVED_MASK,
  RUNTIME_HINT_RESULT,
  RUNTIME_HINT_SCOPE,
  RUNTIME_HINT_SPAN,
  RUNTIME_HINT_TAG,
} from '../runtimeHint.js';
import { S } from '../schema/builder.js';
import { defineFeatureFlags } from '../schema/defineFeatureFlags.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { InMemoryFlagEvaluator, type SpanContextWithoutFf } from '../schema/evaluator.js';
import { TestTracer } from '../tracers/TestTracer.js';
import { iterateSpanChildren } from '../traceTopology.js';
import { createTestTracerOptions } from './test-helpers.js';

const schema = defineLogSchema({ marker: S.category() });
const flags = defineFeatureFlags({ enabled: S.boolean().default(false).sync() });
const context = defineOpContext({
  logSchema: schema,
  flags: flags.schema,
  ctx: { requestId: 'default-request', tenant: { region: 'iad' } },
});
type Context = OpContextOf<typeof context>;
type CapturedContext = SpanContext<Context>;

const TEST_FAILURE = defineCodeError('TEST_FAILURE')<{ reason: string }>();
const CAPABILITY_NAMES = ['tag', 'log', 'ff', 'span', 'spanSync', 'ok', 'err', 'setScope', 'deps'];
const RESULT_ONLY_KEYS = [
  '_buffer',
  '_schema',
  '_logBinding',
  '_physicalLayoutPlan',
  'requestId',
  'tenant',
  'ok',
  'err',
];
const LOG_RESULT_KEYS = [
  '_buffer',
  '_schema',
  '_logBinding',
  '_physicalLayoutPlan',
  '_spanLogger',
  'log',
  'requestId',
  'tenant',
  'ok',
  'err',
];
const LOG_SPAN_RESULT_KEYS = [
  '_buffer',
  '_schema',
  '_logBinding',
  '_physicalLayoutPlan',
  '_spanLogger',
  'log',
  'requestId',
  'tenant',
  'ok',
  'err',
  'span',
  'spanSync',
];
const FULL_FALLBACK_KEYS = [
  '_buffer',
  '_schema',
  '_logBinding',
  '_physicalLayoutPlan',
  '_spanLogger',
  'tag',
  'log',
  'deps',
  'ff',
  'requestId',
  'tenant',
  'setScope',
  'ok',
  'err',
  'span',
  'spanSync',
];

function requireCaptured(value: CapturedContext | undefined, label: string): CapturedContext {
  if (!value) throw new Error(`missing ${label} context`);
  return value;
}

function constructorOf(value: object): Function {
  const prototype = Object.getPrototypeOf(value);
  const constructor = Reflect.get(prototype, 'constructor');
  if (typeof constructor !== 'function') throw new Error('context prototype has no constructor');
  return constructor;
}


async function captureRoot(runtimeHint: number, name: string): Promise<CapturedContext> {
  let captured: CapturedContext | undefined;
  const op = context.defineOp(
    name,
    (ctx) => {
      captured = ctx;
      return new Ok(name);
    },
    undefined,
    { runtimeHint },
  );
  const tracer = new TestTracer(context, createTestTracerOptions());
  await tracer.trace(name, { requestId: `${name}-request` }, op);
  return requireCaptured(captured, name);
}

function expectedCapabilityNames(mask: number): string[] {
  const names: string[] = [];
  if ((mask & RUNTIME_HINT_TAG) !== 0) names.push('tag');
  if ((mask & RUNTIME_HINT_LOG) !== 0) names.push('log');
  if ((mask & RUNTIME_HINT_FF) !== 0) names.push('ff');
  if ((mask & RUNTIME_HINT_SPAN) !== 0) names.push('span', 'spanSync');
  if ((mask & RUNTIME_HINT_RESULT) !== 0) names.push('ok', 'err');
  if ((mask & RUNTIME_HINT_SCOPE) !== 0) names.push('setScope');
  if ((mask & RUNTIME_HINT_DEPS) !== 0) names.push('deps');
  return names;
}

function capabilitySnapshot(ctx: CapturedContext): string[] {
  return CAPABILITY_NAMES.filter((name) => Object.hasOwn(ctx, name));
}

function bufferSnapshot(tracer: TestTracer<typeof context>) {
  const root = tracer.rootBuffers[0];
  if (!root) throw new Error('missing completed root buffer');
  const [child] = iterateSpanChildren(root);
  if (!child) throw new Error('missing completed child buffer');
  return {
    rootEntryTypes: Array.from(root.entry_type.subarray(0, root._writeIndex)),
    rootMessages: Array.from({ length: root._writeIndex }, (_, row) => resolveMessage(root, row)),
    rootMarkers: root.marker_values.slice(0, root._writeIndex),
    rootErrorCodes: root.error_code_values.slice(0, root._writeIndex),
    childEntryTypes: Array.from(child.entry_type.subarray(0, child._writeIndex)),
    childMessages: Array.from({ length: child._writeIndex }, (_, row) => resolveMessage(child, row)),
  };
}

describe('capability-specialized SpanContext shapes', () => {
  it('reuses constructors and exact own-key order for identical analyzed signatures', async () => {
    const hint = RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_RESULT | 4;
    const first = await captureRoot(hint, 'same-a');
    const second = await captureRoot(hint | 31, 'same-b');

    expect(constructorOf(first)).toBe(constructorOf(second));
    expect(Reflect.ownKeys(first)).toEqual(RESULT_ONLY_KEYS);
    expect(Reflect.ownKeys(second)).toEqual(RESULT_ONLY_KEYS);
    expect(capabilitySnapshot(first)).toEqual(['ok', 'err']);
    expect(capabilitySnapshot(second)).toEqual(['ok', 'err']);
  });

  it('creates distinct constructors and own shapes for distinct capability masks', async () => {
    const resultOnly = await captureRoot(RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_RESULT | 2, 'result-only');
    const loggingResult = await captureRoot(
      RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_LOG | RUNTIME_HINT_RESULT | 2,
      'logging-result',
    );

    expect(constructorOf(resultOnly)).not.toBe(constructorOf(loggingResult));
    expect(Reflect.ownKeys(resultOnly)).toEqual(RESULT_ONLY_KEYS);
    expect(Reflect.ownKeys(loggingResult)).toEqual(LOG_RESULT_KEYS);
    expect(capabilitySnapshot(resultOnly)).toEqual(['ok', 'err']);
    expect(capabilitySnapshot(loggingResult)).toEqual(['log', 'ok', 'err']);
  });

  it('covers every capability mask without allocating unused capability properties', async () => {
    const everyMask = Array.from({ length: 0x80 }, (_, maskIndex) => maskIndex);
    await fc.assert(
      fc.asyncProperty(
        fc.shuffledSubarray(everyMask, { minLength: everyMask.length, maxLength: everyMask.length }),
        async (maskIndexes) => {
          for (const maskIndex of maskIndexes) {
            const mask = maskIndex << 16;
            const captured = await captureRoot(RUNTIME_HINT_ANALYZED_VALID | mask | 2, `mask-${maskIndex}`);
            expect(capabilitySnapshot(captured)).toEqual(expectedCapabilityNames(mask));
            expect(mask & ~RUNTIME_HINT_CAPABILITIES_MASK).toBe(0);
          }
        },
      ),
      { numRuns: 1 },
    );
  });

  it('uses one full canonical fallback constructor for every invalid or unanalysed hint', async () => {
    const fallbacks: CapturedContext[] = [];
    for (const [index, hint] of [
      0,
      RUNTIME_HINT_RESULT,
      RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_RESERVED_MASK,
      -1,
      0x1_0000_0000,
      RUNTIME_HINT_ANALYZED_VALID + 0.5,
    ].entries()) {
      fallbacks.push(await captureRoot(hint, `fallback-${index}`));
    }

    const first = fallbacks[0];
    if (!first) throw new Error('missing canonical fallback');
    for (const fallback of fallbacks) {
      expect(constructorOf(fallback)).toBe(constructorOf(first));
      expect(Reflect.ownKeys(fallback)).toEqual(FULL_FALLBACK_KEYS);
      expect(capabilitySnapshot(fallback)).toEqual(CAPABILITY_NAMES);
    }
  });

  it('preserves inferred overrides while reusing the Op CallsitePlan', async () => {
    const capturedRequests: string[] = [];
    const capturedRegions: string[] = [];
    const capturedContexts: CapturedContext[] = [];
    const op = context.defineOp(
      'typed-user-context',
      (ctx) => {
        capturedContexts.push(ctx);
        capturedRequests.push(ctx.requestId.toUpperCase());
        capturedRegions.push(ctx.tenant.region.toUpperCase());
        return ctx.ok(null);
      },
      undefined,
      { runtimeHint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_RESULT | 2 },
    );
    const plan = op.callsitePlan;
    const tracer = new TestTracer(context, createTestTracerOptions());
    await tracer.trace('typed-user-context-1', { requestId: 'request-7', tenant: { region: 'ord' } }, op);
    await tracer.trace('typed-user-context-2', { requestId: 'request-8', tenant: { region: 'dfw' } }, op);

    expect(capturedRequests).toEqual(['REQUEST-7', 'REQUEST-8']);
    expect(capturedRegions).toEqual(['ORD', 'DFW']);
    expect(op.callsitePlan).toBe(plan);
    expect(capturedContexts).toHaveLength(2);
    for (const captured of capturedContexts) {
      expect(captured).toBeInstanceOf(plan.SpanContextClass);
      expect(Reflect.ownKeys(captured)).toEqual(RESULT_ONLY_KEYS);
    }
  });
});

describe('specialized SpanContext runtime semantics', () => {
  class CapturingEvaluator extends InMemoryFlagEvaluator {
    readonly contexts: SpanContextWithoutFf<OpContext>[] = [];

    override getSync<K extends string>(ctx: SpanContextWithoutFf<OpContext>, flag: K) {
      this.contexts.push(ctx);
      return super.getSync(ctx, flag);
    }
  }

  it('binds repeated feature evaluations to contexts created by one CallsitePlan', async () => {
    const evaluator = new CapturingEvaluator(flags.schema, { enabled: true });
    const childContexts: CapturedContext[] = [];
    const child = context.defineOp(
      'flag-child',
      (ctx) => {
        childContexts.push(ctx);
        const enabled = ctx.ff.enabled;
        if (!enabled) throw new Error('enabled feature flag was not bound');
        expect(enabled.value).toBe(true);
        return ctx.ok('child');
      },
      undefined,
      {
        runtimeHint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_FF | RUNTIME_HINT_RESULT | 2,
      },
    );
    const plan = child.callsitePlan;
    const parent = context.defineOp(
      'flag-parent',
      async (ctx) => {
        await ctx.span('flag-child-1', child);
        await ctx.span('flag-child-2', child);
        return ctx.ok('parent');
      },
      undefined,
      {
        runtimeHint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_SPAN | RUNTIME_HINT_RESULT | 2,
      },
    );
    const tracer = new TestTracer(context, { ...createTestTracerOptions(), flagEvaluator: evaluator });
    await tracer.trace('flag-parent', parent);

    expect(child.callsitePlan).toBe(plan);
    expect(childContexts).toHaveLength(2);
    expect(childContexts.every((captured) => captured instanceof plan.SpanContextClass)).toBe(true);
    expect(evaluator.contexts).toEqual(childContexts);
  });

  it('keeps exact own-key order stable across root, child, and overflowing contexts', async () => {
    const hint = RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_LOG | RUNTIME_HINT_SPAN | RUNTIME_HINT_RESULT | 3;
    let childContext: CapturedContext | undefined;
    const child = context.defineOp(
      'overflow-child',
      (ctx) => {
        childContext = ctx;
        ctx.log.info('first');
        ctx.log.info('second');
        return ctx.ok('child');
      },
      undefined,
      { runtimeHint: hint },
    );
    let rootContext: CapturedContext | undefined;
    const root = context.defineOp(
      'overflow-root',
      async (ctx) => {
        rootContext = ctx;
        ctx.log.info('root-first');
        ctx.log.info('root-second');
        await ctx.span('overflow-child', child);
        return ctx.ok('root');
      },
      undefined,
      { runtimeHint: hint },
    );
    const tracer = new TestTracer(context, createTestTracerOptions());
    await tracer.trace('overflow-root', root);

    const capturedRoot = requireCaptured(rootContext, 'root');
    const capturedChild = requireCaptured(childContext, 'overflow child');
    const repeated = await captureRoot(hint, 'overflow-repeat');
    expect(Reflect.ownKeys(capturedRoot)).toEqual(LOG_SPAN_RESULT_KEYS);
    expect(Reflect.ownKeys(capturedChild)).toEqual(LOG_SPAN_RESULT_KEYS);
    expect(Reflect.ownKeys(repeated)).toEqual(LOG_SPAN_RESULT_KEYS);
    expect(constructorOf(capturedRoot)).toBe(constructorOf(capturedChild));
    expect(constructorOf(capturedChild)).toBe(constructorOf(repeated));
    const overflowRoot = tracer.rootBuffers[0];
    if (!overflowRoot) throw new Error('missing overflow root buffer');
    const [overflowChild] = iterateSpanChildren(overflowRoot);
    if (!overflowChild) throw new Error('missing overflow child buffer');
    expect(overflowChild._overflow).toBeDefined();
  });

  it('matches ok, err, log, tag, span, and scope semantics between specialized and fallback contexts', async () => {
    async function run(runtimeHint: number, name: string) {
      let childSucceeded = false;
      const op = context.defineOp(
        name,
        async (ctx) => {
          ctx.setScope({ marker: 'scoped' });
          ctx.tag.marker('tagged');
          ctx.log.info('logged');
          const ok = ctx.ok('probe');
          expect(ok.success).toBe(true);
          const child = await ctx.span('nested', (childCtx) => childCtx.ok('child'));
          childSucceeded = child.success;
          return ctx.err(TEST_FAILURE({ reason: 'expected' }));
        },
        undefined,
        { runtimeHint },
      );
      const tracer = new TestTracer(context, createTestTracerOptions());
      const result = await tracer.trace(name, op);
      if (result.success) throw new Error(`${name} unexpectedly succeeded`);
      return {
        childSucceeded,
        errorCode: result.error.code,
        errorReason: result.error.reason,
        buffer: bufferSnapshot(tracer),
      };
    }

    const specialized = await run(
      RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_FULL_CAPABILITIES | 8,
      'semantic-parity',
    );
    const fallback = await run(0, 'semantic-parity');
    expect(specialized).toEqual(fallback);
  });
});
