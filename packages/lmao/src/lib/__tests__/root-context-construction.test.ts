import { describe, expect, it } from 'bun:test';
import './test-helpers.js';
import { defineOpContext } from '../defineOpContext.js';
import type { OpContext, OpContextOf, SpanContext } from '../opContext/types.js';
import { Ok } from '../result.js';
import {
  RUNTIME_HINT_ANALYZED_VALID,
  RUNTIME_HINT_DEPS,
  RUNTIME_HINT_FF,
  RUNTIME_HINT_RESULT,
  RUNTIME_HINT_SPAN,
} from '../runtimeHint.js';
import { S } from '../schema/builder.js';
import { defineFeatureFlags } from '../schema/defineFeatureFlags.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { InMemoryFlagEvaluator, type SpanContextWithoutFf } from '../schema/evaluator.js';
import { TestTracer } from '../tracers/TestTracer.js';
import { createTraceId } from '../traceId.js';
import { createTestTracerOptions } from './test-helpers.js';

const schema = defineLogSchema({ marker: S.category() });
const flags = defineFeatureFlags({ enabled: S.boolean().default(false).sync() });

const dependencyContext = defineOpContext({ logSchema: defineLogSchema({}) });
const dependencyOps = dependencyContext.defineOps({
  probe: (ctx) => ctx.ok('dependency'),
});

interface RuntimeEnv {
  REGION: string;
}

const rootDefaults: {
  requestId: string;
  optional: string | undefined;
  env: RuntimeEnv | null;
  nullable: string | null;
  config: { mode: string };
} = {
  requestId: 'default-request',
  optional: undefined,
  env: null,
  nullable: 'default-nullable',
  config: { mode: 'safe' },
};

const rootContext = defineOpContext({
  logSchema: schema,
  flags: flags.schema,
  deps: { dependency: dependencyOps },
  ctx: rootDefaults,
});

type RootContext = OpContextOf<typeof rootContext>;
type RootSpanContext = SpanContext<RootContext>;

const ROOT_HINT =
  RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_FF | RUNTIME_HINT_DEPS | RUNTIME_HINT_SPAN | RUNTIME_HINT_RESULT | 4;
const ROOT_KEYS = [
  '_spanBuffer',
  '_buffer',
  '_appendLogEntry',
  '_schema',
  '_logBinding',
  '_physicalLayoutPlan',
  '_spanLogger',
  'deps',
  'ff',
  'config',
  'env',
  'nullable',
  'optional',
  'requestId',
  'ok',
  'err',
  'span',
  'spanSync',
];
const RESULT_ONLY_KEYS = [
  '_spanBuffer',
  '_buffer',
  '_appendLogEntry',
  '_schema',
  '_logBinding',
  '_physicalLayoutPlan',
  'config',
  'env',
  'nullable',
  'optional',
  'requestId',
  'ok',
  'err',
];

function requireContext(value: RootSpanContext | undefined, label: string): RootSpanContext {
  if (!value) throw new Error(`missing ${label}`);
  return value;
}

class CapturingEvaluator extends InMemoryFlagEvaluator {
  readonly contexts: SpanContextWithoutFf<OpContext>[] = [];

  override getSync<K extends string>(ctx: SpanContextWithoutFf<OpContext>, flag: K) {
    this.contexts.push(ctx);
    return super.getSync(ctx, flag);
  }
}

describe('root SpanContext direct construction', () => {
  it('passes defaults and overrides directly without enumerable carrier copies', () => {
    let defaultOwnKeysCalls = 0;
    let overrideOwnKeysCalls = 0;
    const defaultsTarget: { env: RuntimeEnv | null; requestId: string } = {
      env: null,
      requestId: 'default-request',
    };
    const defaults = new Proxy(defaultsTarget, {
      ownKeys(target) {
        defaultOwnKeysCalls += 1;
        return Reflect.ownKeys(target);
      },
    });
    const directContext = defineOpContext({ logSchema: schema, ctx: defaults });
    const traceId = createTraceId('direct-context-sources');
    const directOp = directContext.defineOp(
      'direct-context-sources',
      (ctx) => {
        if (!ctx.env) throw new Error('missing direct env');
        expect(ctx.buffer.trace_id).toBe(traceId);
        expect(ctx.env.REGION).toBe('nrt');
        expect(ctx.requestId).toBe('override-request');
        return ctx.ok(null);
      },
      undefined,
      { runtimeHint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_RESULT | 2 },
    );
    const tracer = new TestTracer(directContext, createTestTracerOptions());
    const overridesTarget = {
      env: { REGION: 'nrt' },
      requestId: 'override-request',
      trace_id: traceId,
    };
    const overrides = new Proxy(overridesTarget, {
      ownKeys(target) {
        overrideOwnKeysCalls += 1;
        return Reflect.ownKeys(target);
      },
    });

    // Ignore factory/class setup. The trace path must not enumerate either record
    // to validate or to build temporary spread/copy carriers.
    defaultOwnKeysCalls = 0;
    overrideOwnKeysCalls = 0;
    const result = tracer.trace('direct-context-sources', overrides, directOp);

    expect(result).toBeInstanceOf(Ok);
    expect(defaultOwnKeysCalls).toBe(0);
    expect(overrideOwnKeysCalls).toBe(0);
  });

  it('uses the selected constructor once for sync, async, and child contexts with canonical keys', async () => {
    const evaluator = new CapturingEvaluator(flags.schema, { enabled: true });
    const observed: Array<{
      config: string;
      dependencyBound: boolean;
      enabled: boolean;
      env: string;
      nullable: string | null;
      optional: string | undefined;
      requestId: string;
    }> = [];
    let syncRoot: RootSpanContext | undefined;
    let syncChild: RootSpanContext | undefined;
    let asyncRoot: RootSpanContext | undefined;
    let asyncChild: RootSpanContext | undefined;

    function observe(ctx: RootSpanContext) {
      if (!ctx.env) throw new Error('missing observed env');
      const enabled = ctx.ff.enabled;
      if (!enabled) throw new Error('missing enabled flag');
      observed.push({
        config: ctx.config.mode,
        dependencyBound: ctx.deps.dependency.probe === dependencyOps.probe,
        enabled: enabled.value,
        env: ctx.env.REGION,
        nullable: ctx.nullable,
        optional: ctx.optional,
        requestId: ctx.requestId,
      });
    }

    const syncChildOp = rootContext.defineOp(
      'direct-sync-child',
      (ctx) => {
        syncChild = ctx;
        observe(ctx);
        return ctx.ok('sync-child');
      },
      undefined,
      { runtimeHint: ROOT_HINT },
    );
    const asyncChildOp = rootContext.defineOp(
      'direct-async-child',
      async (ctx) => {
        asyncChild = ctx;
        observe(ctx);
        await Promise.resolve();
        return ctx.ok('async-child');
      },
      undefined,
      { runtimeHint: ROOT_HINT | 27 },
    );

    const syncOp = rootContext.defineOp(
      'direct-root-sync',
      (ctx) => {
        syncRoot = ctx;
        observe(ctx);
        const childResult = ctx.spanSync('direct-sync-child', syncChildOp);
        expect(childResult.success).toBe(true);
        return ctx.ok('sync-root');
      },
      undefined,
      { runtimeHint: ROOT_HINT },
    );
    const asyncOp = rootContext.defineOp(
      'direct-root-async',
      async (ctx) => {
        asyncRoot = ctx;
        observe(ctx);
        const childResult = await ctx.span('direct-async-child', asyncChildOp);
        expect(childResult.success).toBe(true);
        return ctx.ok('async-root');
      },
      undefined,
      { runtimeHint: ROOT_HINT | 27 },
    );

    expect(syncOp.callsitePlan.SpanContextClass).toBe(asyncOp.callsitePlan.SpanContextClass);
    expect(syncChildOp.callsitePlan.SpanContextClass).toBe(syncOp.callsitePlan.SpanContextClass);
    expect(asyncChildOp.callsitePlan.SpanContextClass).toBe(syncOp.callsitePlan.SpanContextClass);

    const tracer = new TestTracer(rootContext, { ...createTestTracerOptions(), flagEvaluator: evaluator });
    const syncResult = tracer.trace(
      'direct-root-sync',
      { env: { REGION: 'iad' }, nullable: null },
      syncOp,
    );
    expect(syncResult).toBeInstanceOf(Ok);

    const asyncResultPromise = tracer.trace(
      'direct-root-async',
      {
        config: { mode: 'fast' },
        env: { REGION: 'fra' },
        optional: 'present',
        requestId: 'override-request',
      },
      asyncOp,
    );
    expect(asyncResultPromise).toBeInstanceOf(Promise);
    const asyncResult = await asyncResultPromise;
    expect(asyncResult).toBeInstanceOf(Ok);

    const contexts = [
      requireContext(syncRoot, 'sync root'),
      requireContext(syncChild, 'sync child'),
      requireContext(asyncRoot, 'async root'),
      requireContext(asyncChild, 'async child'),
    ];
    for (const ctx of contexts) {
      expect(ctx).toBeInstanceOf(syncOp.callsitePlan.SpanContextClass);
      expect(Object.keys(ctx)).toEqual(ROOT_KEYS);
    }
    expect(evaluator.contexts).toEqual(contexts);
    expect(new Set(evaluator.contexts).size).toBe(contexts.length);
    expect(observed).toEqual([
      {
        config: 'safe',
        dependencyBound: true,
        enabled: true,
        env: 'iad',
        nullable: null,
        optional: undefined,
        requestId: 'default-request',
      },
      {
        config: 'safe',
        dependencyBound: true,
        enabled: true,
        env: 'iad',
        nullable: null,
        optional: undefined,
        requestId: 'default-request',
      },
      {
        config: 'fast',
        dependencyBound: true,
        enabled: true,
        env: 'fra',
        nullable: 'default-nullable',
        optional: 'present',
        requestId: 'override-request',
      },
      {
        config: 'fast',
        dependencyBound: true,
        enabled: true,
        env: 'fra',
        nullable: 'default-nullable',
        optional: 'present',
        requestId: 'override-request',
      },
    ]);
  });

  it('separates root constructors for capability and user-context layouts', () => {
    let fullRoot: RootSpanContext | undefined;
    let resultOnlyRoot: RootSpanContext | undefined;
    const fullOp = rootContext.defineOp(
      'full-layout-root',
      (ctx) => {
        fullRoot = ctx;
        return ctx.ok(null);
      },
      undefined,
      { runtimeHint: ROOT_HINT },
    );
    const resultOnlyOp = rootContext.defineOp(
      'result-layout-root',
      (ctx) => {
        resultOnlyRoot = ctx;
        return ctx.ok(null);
      },
      undefined,
      { runtimeHint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_RESULT | 4 },
    );

    const alternateDefaults: { accountId: string; env: RuntimeEnv | null } = {
      accountId: 'default-account',
      env: null,
    };
    const alternateContext = defineOpContext({
      logSchema: schema,
      flags: flags.schema,
      deps: { dependency: dependencyOps },
      ctx: alternateDefaults,
    });
    let alternateRoot: SpanContext<OpContextOf<typeof alternateContext>> | undefined;
    const alternateOp = alternateContext.defineOp(
      'alternate-layout-root',
      (ctx) => {
        if (!ctx.env) throw new Error('missing alternate env');
        alternateRoot = ctx;
        expect(ctx.accountId).toBe('account-7');
        expect(ctx.env.REGION).toBe('syd');
        return ctx.ok(null);
      },
      undefined,
      { runtimeHint: ROOT_HINT },
    );

    const tracer = new TestTracer(rootContext, createTestTracerOptions());
    tracer.trace('full-layout-root', { env: { REGION: 'ord' } }, fullOp);
    tracer.trace('result-layout-root', { env: { REGION: 'ord' } }, resultOnlyOp);
    const alternateTracer = new TestTracer(alternateContext, createTestTracerOptions());
    alternateTracer.trace(
      'alternate-layout-root',
      { accountId: 'account-7', env: { REGION: 'syd' } },
      alternateOp,
    );

    const capturedFull = requireContext(fullRoot, 'full root');
    const capturedResultOnly = requireContext(resultOnlyRoot, 'result-only root');
    if (!alternateRoot) throw new Error('missing alternate root');

    expect(fullOp.callsitePlan.SpanContextClass).not.toBe(resultOnlyOp.callsitePlan.SpanContextClass);
    expect(fullOp.callsitePlan.SpanContextClass).not.toBe(alternateOp.callsitePlan.SpanContextClass);
    expect(capturedFull).toBeInstanceOf(fullOp.callsitePlan.SpanContextClass);
    expect(capturedResultOnly).toBeInstanceOf(resultOnlyOp.callsitePlan.SpanContextClass);
    expect(alternateRoot).toBeInstanceOf(alternateOp.callsitePlan.SpanContextClass);
    expect(Object.keys(capturedFull)).toEqual(ROOT_KEYS);
    expect(Object.keys(capturedResultOnly)).toEqual(RESULT_ONLY_KEYS);
    expect(Object.keys(alternateRoot)).toEqual([
      '_spanBuffer',
      '_buffer',
      '_appendLogEntry',
      '_schema',
      '_logBinding',
      '_physicalLayoutPlan',
      '_spanLogger',
      'deps',
      'ff',
      'accountId',
      'env',
      'ok',
      'err',
      'span',
      'spanSync',
    ]);
  });

  it('rejects missing and null required root values before construction', () => {
    const op = rootContext.defineOp(
      'required-root-env',
      (ctx) => {
        if (!ctx.env) throw new Error('missing required env');
        return ctx.ok(ctx.env.REGION);
      },
      undefined,
      { runtimeHint: RUNTIME_HINT_ANALYZED_VALID | RUNTIME_HINT_RESULT | 2 },
    );
    const tracer = new TestTracer(rootContext, createTestTracerOptions());

    expect(() => Reflect.apply(tracer.trace, tracer, ['missing-root-env', op])).toThrow(
      "Required context parameter 'env' must be provided",
    );
    expect(() => Reflect.apply(tracer.trace, tracer, ['null-root-env', { env: null }, op])).toThrow(
      "Required context parameter 'env' must be provided",
    );
  });
});
