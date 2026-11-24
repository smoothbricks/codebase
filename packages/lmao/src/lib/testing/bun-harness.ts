/// <reference types="bun" />

/**
 * bun:test integration for LMAO trace-testing.
 *
 * Transparently wraps bun:test's it()/test() via mock.module so every test
 * automatically creates a trace span — no import changes needed in test files.
 *
 * Architecture:
 * - initTraceTestRun() in preload sets up a lifecycle tracer pipeline
 * - mock.module('bun:test', ...) in preload patches it()/test() with trace spans
 * - One root trace per test run (tracer.trace('test-run', ...))
 * - Each it()/test() creates a child span of the root (rootCtx.span(name, fn)), whatever modifier registered it
 *   (.only, .concurrent, .serial, .failing, .todo, the *If forms, .each); an each row's span takes its label template,
 *   because Bun formats each row's title itself
 * - describe() and its modifiers are wrapped to track nesting — path written to `describe` schema column
 * - After all tests: flush tracer outputs (SQLite, stdio, or both)
 *
 * mock.module MUST be called from the preload file itself — bun only intercepts
 * subsequent imports when the mock is registered from the entry module context.
 *
 * @example
 * ```typescript
 * // test-setup.ts (preload)
 * import { mock } from 'bun:test';
 * import * as bunTest from 'bun:test';
 * import { DEFAULT_TRACE_DB_PATH, initTraceTestRun, createBunTestMock } from '@smoothbricks/lmao/testing/bun';
 * import { myOpContext } from './src/opContext.js';
 *
 * initTraceTestRun(myOpContext, { sqlite: { dbPath: DEFAULT_TRACE_DB_PATH } });
 * mock.module('bun:test', () => createBunTestMock(bunTest));
 *
 * // my-test.test.ts — uses bun:test directly, no import changes needed
 * import { describe, it, expect } from 'bun:test';
 *
 * describe('Order Processing', () => {
 *   it('validates order', () => {
 *     // This it() is automatically wrapped in a child span of the root
 *   });
 * });
 * ```
 *
 * @module testing/bun
 */

import { Database } from 'bun:sqlite';
import * as bunTest from 'bun:test';
import { afterAll as _afterAll, describe as _describe, it as _it, mock } from 'bun:test';
import { AsyncLocalStorage } from 'node:async_hooks';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { cleanupDebug, cleanupDebugActiveHandles } from '../cleanupDiagnostics.js';
import { defineOpContext } from '../defineOpContext.js';
import { JsBufferStrategy } from '../JsBufferStrategy.js';
import type { SpanContext } from '../opContext/spanContextTypes.js';
import type { DepsConfig, FeatureFlagSchema, OpContext, OpContextBinding, OpContextOf } from '../opContext/types.js';
import { S } from '../schema/builder.js';
import { LogSchema } from '../schema/LogSchema.js';
import type { SchemaFields } from '../schema/types.js';
import { isSpanContext } from '../spanContext.js';
import { openWalDatabase } from '../sqlite/sqlite-wal.js';
import type { SQLiteWriterConfig } from '../sqlite/sqlite-writer.js';
import { DEFAULT_TRACE_DB_PATH, TRACE_DB_DIRECTORY, TRACE_DB_FILENAME } from '../sqlite/trace-db-path.js';
import { createTraceRoot } from '../traceRoot.universal.js';
import type { Tracer } from '../tracer.js';
import { CompositeTracer } from '../tracers/CompositeTracer.js';
import { SQLiteTracer } from '../tracers/SQLiteTracer.js';
import { StdioTracer } from '../tracers/StdioTracer.js';
import { TestTracer as InMemoryTestTracer } from '../tracers/TestTracer.js';
import type { AnySpanBuffer } from '../types.js';

// Preloads wire `sqlite.dbPath` from here rather than spelling the sink path, which is only safe under a directory
// project walkers and watchers ignore.
export { DEFAULT_TRACE_DB_PATH, TRACE_DB_DIRECTORY, TRACE_DB_FILENAME } from '../sqlite/trace-db-path.js';

/** bun:test expect() errors start with 'expect(received).' — distinguishes assertion failures from other throws */
function isExpectError(error: unknown): boolean {
  return error instanceof Error && error.message.startsWith('expect(');
}

function writeDescribeTag(tag: unknown, describePath: string | null): void {
  if (!describePath || typeof tag !== 'object' || tag === null) {
    return;
  }

  const describeWriter = Reflect.get(tag, 'describe');
  if (typeof describeWriter === 'function') {
    describeWriter.call(tag, describePath);
    return;
  }

  const batchWriter = Reflect.get(tag, 'with');
  if (typeof batchWriter === 'function') {
    batchWriter.call(tag, { describe: describePath });
  }
}

function formatTestName(ctx: { readonly buffer: AnySpanBuffer; readonly spanName: string }): string {
  const describeNulls = ctx.buffer.getNullsIfAllocated('describe');
  const describeValues =
    describeNulls === undefined || ((describeNulls[0] ?? 0) & 1) !== 0
      ? ctx.buffer.getColumnIfAllocated('describe')
      : undefined;
  const describePath =
    Array.isArray(describeValues) && typeof describeValues[0] === 'string' && describeValues[0].length > 0
      ? describeValues[0]
      : undefined;
  return describePath !== undefined ? `${describePath} > ${ctx.spanName}` : ctx.spanName;
}

type HarnessSpanContext<B extends OpContextBinding> = SpanContext<OpContextOf<B>>;
type BunTestHarnessBuiltins = { describe: ReturnType<typeof S.category> };
type HarnessSchema<TExt extends SchemaFields> = BunTestHarnessBuiltins & TExt;
type BunDescribe = typeof _describe;
type BunIt = typeof _it;

const ciOnlyDisabledMessage =
  '.only is disabled in CI environments to prevent accidentally skipping tests. To override, set the environment variable CI=false.';

export interface BunTestModuleShape {
  describe: BunDescribe;
  it: BunIt;
  test: BunIt;
  [key: string]: unknown;
}

// WHY: Package-local `nx test` runs Bun from that package directory, so the
// shared preload must not eagerly import unrelated packages' tracer modules just
// to find one opContext. A tiny fallback binding keeps tracing active for
// packages that only need SQLite-backed test spans and do not define their own
// `src/test-suite-tracer.ts`.
const defaultAutoSetupOpContext = defineOpContext({ logSchema: new LogSchema({}) });

type ExtendBindingLogSchema<B extends OpContextBinding, TExt extends SchemaFields> =
  B extends OpContextBinding<infer T, infer FF, infer Deps, infer UserCtx>
    ? OpContextBinding<T extends LogSchema<infer Fields> ? LogSchema<Fields & TExt> : never, FF, Deps, UserCtx>
    : never;

export type BunTestSetupOptions<TExt extends SchemaFields = Record<never, never>> = {
  /**
   * Optional SQLite output path.
   *
   * Omit this to run traced tests without DB persistence.
   */
  sqlite?: SQLiteWriterConfig;
  /** Optional verbose stdout tracing; defaults to env flag detection. */
  verbose?: boolean;
  /** Optional test-only schema columns merged into span tag/log methods. */
  extraTestColumns?: TExt;
};

function buildHarnessSchemaExtensions(binding: OpContextBinding, extraTestColumns: undefined): BunTestHarnessBuiltins;
function buildHarnessSchemaExtensions<TExt extends SchemaFields>(
  binding: OpContextBinding,
  extraTestColumns: TExt,
): HarnessSchema<TExt>;
function buildHarnessSchemaExtensions<TExt extends SchemaFields>(
  binding: OpContextBinding,
  extraTestColumns: TExt | undefined,
): BunTestHarnessBuiltins | HarnessSchema<TExt> {
  const extensionBase: BunTestHarnessBuiltins = { describe: S.category() };
  const baseColumnNames = new Set(binding.logBinding.logSchema._columnNames);
  for (const fieldName of Object.keys(extraTestColumns ?? {})) {
    if (baseColumnNames.has(fieldName)) {
      throw new Error(`Test harness schema column '${fieldName}' already exists in the bound log schema`);
    }
  }

  return extraTestColumns ? { ...extensionBase, ...extraTestColumns } : extensionBase;
}

function extendBindingLogSchema<
  B extends OpContextBinding<LogSchema<TFields>, FF, Deps, UserCtx>,
  TFields extends SchemaFields,
  FF extends FeatureFlagSchema,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
  TExt extends SchemaFields,
>(binding: B, extension: TExt): OpContextBinding<LogSchema<TFields & TExt>, FF, Deps, UserCtx> {
  const extendedBinding: OpContextBinding<LogSchema<TFields & TExt>, FF, Deps, UserCtx> = {
    logBinding: {
      ...binding.logBinding,
      logSchema: binding.logBinding.logSchema.extend(extension),
    },
    flags: binding.flags,
    ctxDefaults: binding.ctxDefaults,
    deps: binding.deps,
  };

  return extendedBinding;
}

type RuntimeHarnessBinding<
  TFields extends SchemaFields,
  FF extends FeatureFlagSchema,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
  TExt extends SchemaFields,
> = OpContextBinding<LogSchema<TFields & (BunTestHarnessBuiltins | HarnessSchema<TExt>)>, FF, Deps, UserCtx>;

function createRuntimeHarnessBinding<
  TFields extends SchemaFields,
  FF extends FeatureFlagSchema,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
  TExt extends SchemaFields,
>(
  binding: OpContextBinding<LogSchema<TFields>, FF, Deps, UserCtx>,
  options: BunTestSetupOptions<TExt> | undefined,
): RuntimeHarnessBinding<TFields, FF, Deps, UserCtx, TExt> {
  const harnessSchema: BunTestHarnessBuiltins | HarnessSchema<TExt> =
    options?.extraTestColumns === undefined
      ? buildHarnessSchemaExtensions(binding, undefined)
      : buildHarnessSchemaExtensions(binding, options.extraTestColumns);
  return extendBindingLogSchema<
    OpContextBinding<LogSchema<TFields>, FF, Deps, UserCtx>,
    TFields,
    FF,
    Deps,
    UserCtx,
    BunTestHarnessBuiltins | HarnessSchema<TExt>
  >(binding, harnessSchema);
}

function isTruthyEnvFlag(value: unknown): boolean {
  return value === true || value === '1' || value === 'true';
}

function isSchemaFieldsRecord(value: unknown): value is SchemaFields {
  return typeof value === 'object' && value !== null;
}

function isOpContextBindingLike(value: unknown): value is OpContextBinding {
  return typeof value === 'object' && value !== null && typeof Reflect.get(value, 'logBinding') === 'object';
}

function isSpanContextForBinding<B extends OpContextBinding>(
  value: unknown,
  binding: B,
): value is SpanContext<OpContextOf<B>> {
  return isSpanContext<OpContextOf<B>>(value) && Reflect.get(value, '_schema') === binding.logBinding.logSchema;
}

function isTracerInstance(value: unknown): value is Tracer<OpContextBinding> {
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof Reflect.get(value, 'trace') === 'function' &&
    typeof Reflect.get(value, 'flush') === 'function'
  );
}

function isVerboseTraceEnabled(explicitVerbose: boolean | undefined): boolean {
  if (explicitVerbose !== undefined) {
    return explicitVerbose;
  }

  const globalVerbose = Reflect.get(globalThis, '__LMAO_TEST_TRACE_VERBOSE__');
  if (isTruthyEnvFlag(globalVerbose)) {
    return true;
  }

  return isTruthyEnvFlag(process.env.LMAO_TEST_TRACE_VERBOSE);
}

type TracerFactoryOptions<B extends OpContextBinding> = {
  binding: B;
  sqlite: SQLiteWriterConfig | undefined;
  verbose: boolean;
};

function createRootTracer<B extends OpContextBinding>({
  binding,
  sqlite,
  verbose,
}: TracerFactoryOptions<B>): Tracer<B> {
  const tracerOptions = {
    bufferStrategy: new JsBufferStrategy<B['logBinding']['logSchema']>(),
    createTraceRoot,
  } as const;

  if (sqlite) {
    // Every test process opens this one sink at once; it must already be WAL when its path appears.
    const db = openWalDatabase(sqlite.dbPath ?? DEFAULT_TRACE_DB_PATH, (path) => new Database(path));
    // liveChildRows: a test's start row is in the sink before its body runs, so a killed run names the test that hung.
    const sqliteTracer = new SQLiteTracer(binding, {
      ...tracerOptions,
      db,
      liveChildRows: true,
    });

    if (verbose) {
      const stdioTracer = new StdioTracer(binding, {
        ...tracerOptions,
      });

      return new CompositeTracer(binding, {
        ...tracerOptions,
        delegates: [stdioTracer, sqliteTracer],
      });
    }

    return sqliteTracer;
  }

  if (verbose) {
    return new StdioTracer(binding, {
      ...tracerOptions,
    });
  }

  return new InMemoryTestTracer(binding, {
    ...tracerOptions,
  });
}

async function closeTracer<B extends OpContextBinding>(tracer: Tracer<B>): Promise<void> {
  const close = Reflect.get(tracer, 'close');
  if (typeof close === 'function') {
    cleanupDebug('tracer.close:start', { tracer: tracer.constructor.name });
    await close.call(tracer);
    cleanupDebug('tracer.close:end', { tracer: tracer.constructor.name });
  }
}

/** Where a run's trace is written, for the lines that report on it. */
interface TraceSink {
  readonly path: string;
  readonly traceId: string;
}

/** One open test run: its tracer, its root `test-run` span, and the handle that ends that span. */
interface ActiveTestRun<B extends OpContextBinding> {
  readonly tracer: Tracer<B>;
  readonly rootCtx: HarnessSpanContext<B>;
  readonly rootTrace: Promise<unknown>;
  readonly resolve: () => void;
}

/** Opens a new test run: constructs its tracer and starts a fresh `test-run` root span. */
function startTestRun<B extends OpContextBinding>(options: TracerFactoryOptions<B>): ActiveTestRun<B> {
  const tracer = createRootTracer(options);
  const state: {
    rootCtx: HarnessSpanContext<B> | null;
    resolve: (() => void) | null;
  } = { rootCtx: null, resolve: null };
  const rootTrace = tracer.trace('test-run', (ctx) => {
    state.rootCtx = ctx;
    return new Promise<void>((resolve) => {
      state.resolve = resolve;
    });
  });
  if (state.rootCtx === null || state.resolve === null) {
    throw new Error('Test harness tracer did not open its test-run root span');
  }
  return {
    tracer,
    rootCtx: state.rootCtx,
    rootTrace,
    resolve: state.resolve,
  };
}

/**
 * Ends a test run: lets the root `test-run` span end, flushes the tracer and always closes it.
 *
 * A persisting tracer writes the whole run's trace when the root span ends, so a sink that cannot take it fails the
 * `await rootTrace`, not the later `flush()`. Either failure is reported with its sink and trace id, then thrown: the
 * run must not pass with its trace lost (the Vitest harness holds the same contract), and the tracer is closed either
 * way. `scope` only names the diagnostics of the entry that owns the run.
 */
async function endTestRun<B extends OpContextBinding>(
  scope: 'instance' | 'global',
  run: ActiveTestRun<B>,
  sqlite: SQLiteWriterConfig | undefined,
): Promise<void> {
  cleanupDebug(`${scope}.afterAll:start`);
  cleanupDebugActiveHandles(`${scope}.afterAll:handles:start`);
  const sink: TraceSink | undefined = sqlite
    ? { path: sqlite.dbPath ?? DEFAULT_TRACE_DB_PATH, traceId: run.rootCtx.buffer.trace_id }
    : undefined;
  try {
    cleanupDebug(`${scope}.rootTrace:resolve`);
    run.resolve();
    cleanupDebug(`${scope}.rootTrace:await:start`);
    await run.rootTrace;
    cleanupDebug(`${scope}.rootTrace:await:end`);
    cleanupDebug(`${scope}.tracer.flush:start`, { tracer: run.tracer.constructor.name });
    await run.tracer.flush();
    cleanupDebug(`${scope}.tracer.flush:end`, { tracer: run.tracer.constructor.name });
    if (sink) console.log(`\n[trace] trace_id: ${sink.traceId} -> ${sink.path}`);
  } catch (error) {
    const where = sink ? ` trace_id: ${sink.traceId} -> ${sink.path}` : '';
    console.error(`[lmao/testing] SQLite flush error:${where}`, error);
    throw error;
  } finally {
    await closeTracer(run.tracer);
    cleanupDebugActiveHandles(`${scope}.afterAll:handles:end`);
    cleanupDebug(`${scope}.afterAll:end`);
  }
}

export interface BunTestTracerInstance<B extends OpContextBinding> {
  setup(): void;
  useTestSpan(): HarnessSpanContext<B>;
  currentTestName(): string | undefined;
  getTracer(): Tracer<B>;
  createBunTestMock<TModule extends BunTestModuleShape>(bunTestModule: TModule): TModule;
}

export interface BunTestSuiteTracer<B extends OpContextBinding> {
  useTestTracer: BunTestTracerInstance<B>;
  useTestSpan(): HarnessSpanContext<B>;
  currentTestName(): string | undefined;
  setupBunTestSuiteTracing(): void;
}

export type BunTestSuiteUseTestTracer<
  B extends OpContextBinding,
  TExt extends SchemaFields = Record<never, never>,
> = BunTestTracerInstance<ExtendBindingLogSchema<B, HarnessSchema<TExt>>>;

export type TestTracer<
  B extends OpContextBinding,
  TExt extends SchemaFields = Record<never, never>,
> = BunTestSuiteUseTestTracer<B, TExt>;

export interface ActiveBunTestTracer {
  setup(): void;
  useTestSpan(): unknown;
  currentTestName(): string | undefined;
  getTracer(): unknown;
  createBunTestMock<TModule extends BunTestModuleShape>(bunTestModule: TModule): TModule;
}

let _activeSuiteTracer: ActiveBunTestTracer | null = null;

/** Returns the active suite tracer set by installBunTestTracing(), or null if none is active. */
export function getActiveSuiteTracer(): ActiveBunTestTracer | null {
  return _activeSuiteTracer;
}

export function installBunTestTracing(tracer: ActiveBunTestTracer): void {
  _activeSuiteTracer = tracer;
  tracer.setup();
  mock.module('bun:test', () => tracer.createBunTestMock(bunTest));
}

export function makeBunTestSuiteTracer<B extends OpContextBinding>(
  binding: B,
  options?: Omit<BunTestSetupOptions, 'extraTestColumns'>,
): BunTestSuiteTracer<ExtendBindingLogSchema<B, BunTestHarnessBuiltins>>;
export function makeBunTestSuiteTracer<B extends OpContextBinding, TExt extends SchemaFields = Record<never, never>>(
  binding: B,
  options?: BunTestSetupOptions<TExt>,
): BunTestSuiteTracer<ExtendBindingLogSchema<B, HarnessSchema<TExt>>>;
export function makeBunTestSuiteTracer<B extends OpContextBinding, TExt extends SchemaFields = Record<never, never>>(
  binding: B,
  options?: BunTestSetupOptions<TExt>,
):
  | BunTestSuiteTracer<ExtendBindingLogSchema<B, BunTestHarnessBuiltins>>
  | BunTestSuiteTracer<ExtendBindingLogSchema<B, HarnessSchema<TExt>>> {
  // Keep the caller's binding and extension through the public typed factory.
  // Inferring the lower-level factory here widens B to its schema constraint.
  const useTestTracer = makeTestTracer<B, TExt>(binding, options);
  return {
    useTestTracer,
    useTestSpan: () => useTestTracer.useTestSpan(),
    currentTestName: () => useTestTracer.currentTestName(),
    setupBunTestSuiteTracing: () => installBunTestTracing(useTestTracer),
  };
}

/**
 * Define a per-package test tracer — metadata for preload discovery + typed span accessor.
 *
 * Returns plain data that `autoSetupBunTestTracing` reads at discovery time, plus a
 * `useTestSpan()` that threads the package-local binding through the generic
 * `useTestSpan<B>()` helper.
 *
 * WHY generic threading: `autoSetupBunTestTracing` creates the active tracer from the
 * same `opContext` binding passed here, so the exported helper can preserve the real
 * span type without a local assertion bridge.
 */
export function defineTestTracer<B extends OpContextBinding>(
  binding: B,
  options?: { extraTestColumns?: SchemaFields },
): {
  useTestSpan: () => SpanContext<OpContextOf<B>>;
  opContext: B;
  extraTestColumns: SchemaFields | undefined;
} {
  return {
    opContext: binding,
    extraTestColumns: options?.extraTestColumns,
    useTestSpan: () => useTestSpan<B>(),
  };
}

/**
 * Create an instance-scoped Bun test tracer harness.
 *
 * Unlike initTraceTestRun(), this keeps tracer/root/ALS state isolated per instance.
 */
export function makeTestTracer<B extends OpContextBinding>(
  binding: B,
  options?: Omit<BunTestSetupOptions, 'extraTestColumns'>,
): BunTestTracerInstance<ExtendBindingLogSchema<B, BunTestHarnessBuiltins>>;
export function makeTestTracer<B extends OpContextBinding, TExt extends SchemaFields = Record<never, never>>(
  binding: B,
  options?: BunTestSetupOptions<TExt>,
): BunTestTracerInstance<ExtendBindingLogSchema<B, HarnessSchema<TExt>>>;
export function makeTestTracer<B extends OpContextBinding, TExt extends SchemaFields = Record<never, never>>(
  binding: B,
  options?: BunTestSetupOptions<TExt>,
) {
  return createTestTracerInstance(binding, options);
}
function createTestTracerInstance<
  TFields extends SchemaFields,
  FF extends FeatureFlagSchema,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
  TExt extends SchemaFields = Record<never, never>,
>(
  binding: OpContextBinding<LogSchema<TFields>, FF, Deps, UserCtx>,
  options?: BunTestSetupOptions<TExt>,
): BunTestTracerInstance<RuntimeHarnessBinding<TFields, FF, Deps, UserCtx, TExt>> {
  type InstanceBinding = RuntimeHarnessBinding<TFields, FF, Deps, UserCtx, TExt>;
  let runConfig: TracerFactoryOptions<InstanceBinding> | null = null;
  let activeRun: ActiveTestRun<InstanceBinding> | null = null;

  const als = new AsyncLocalStorage<HarnessSpanContext<InstanceBinding>>();

  function ensureRun(missingSetupMessage: string): ActiveTestRun<InstanceBinding> {
    if (runConfig === null) {
      throw new Error(missingSetupMessage);
    }
    activeRun ??= startTestRun(runConfig);
    return activeRun;
  }

  function assertRootCtx(): HarnessSpanContext<InstanceBinding> {
    return ensureRun('Call setup() before wiring bun:test wrappers').rootCtx;
  }

  function assertTracer(): Tracer<InstanceBinding> {
    return ensureRun('Call setup() in preload before tests').tracer;
  }

  return {
    setup(): void {
      if (runConfig !== null) {
        return;
      }

      const extendedBinding = createRuntimeHarnessBinding(binding, options);
      const verboseTrace = isVerboseTraceEnabled(options?.verbose);
      runConfig = {
        binding: extendedBinding,
        sqlite: options?.sqlite,
        verbose: verboseTrace,
      };

      activeRun = startTestRun(runConfig);

      _afterAll(async () => {
        const run = activeRun;
        activeRun = null;
        if (run !== null) {
          await endTestRun('instance', run, runConfig?.sqlite);
        }
      });
    },

    useTestSpan(): HarnessSpanContext<InstanceBinding> {
      const ctx = als.getStore();
      if (!ctx || !runConfig || !isSpanContextForBinding(ctx, runConfig.binding)) {
        throw new Error('useTestSpan() called outside of a traced it()');
      }
      return ctx;
    },

    currentTestName(): string | undefined {
      const ctx = als.getStore();
      return ctx ? formatTestName(ctx) : undefined;
    },

    getTracer(): Tracer<InstanceBinding> {
      return assertTracer();
    },
    createBunTestMock<TModule extends BunTestModuleShape>(bunTestModule: TModule): TModule {
      const describeStack: string[] = [];
      const wrappedIt = wrapBunRegistration(bunTestModule.it, traceTests(describeStack, assertRootCtx, als));
      return {
        ...bunTestModule,
        describe: wrapBunRegistration(bunTestModule.describe, trackDescribes(describeStack)),
        it: wrappedIt,
        test: wrappedIt,
      };
    },
  };
}

// Global singleton — setup config survives across per-file runs in a --no-isolate worker;
// the active run is cleared on preload afterAll and reopened by the next file's first traced test.
let _globalRunConfig: TracerFactoryOptions<OpContextBinding> | null = null;
let _globalActiveRun: ActiveTestRun<OpContextBinding> | null = null;
let _globalAfterAllRegistered = false;

// AsyncLocalStorage for propagating SpanContext to test bodies.
// Each it() runs in its own async context so concurrent tests don't collide.
const _als = new AsyncLocalStorage<SpanContext<OpContext>>();

function ensureGlobalRun(missingInitMessage: string): ActiveTestRun<OpContextBinding> {
  if (_globalRunConfig === null) {
    throw new Error(missingInitMessage);
  }
  _globalActiveRun ??= startTestRun(_globalRunConfig);
  return _globalActiveRun;
}

/** Initialize the root tracer for the entire bun test run. Call once in preload. */
export function initTraceTestRun<B extends OpContextBinding>(opContext: B, options?: BunTestSetupOptions): void {
  _activeSuiteTracer = null;

  // Extend user's schema with `describe` column for test grouping
  const harnessSchema =
    options?.extraTestColumns === undefined
      ? buildHarnessSchemaExtensions(opContext, undefined)
      : buildHarnessSchemaExtensions(opContext, options.extraTestColumns);
  const extendedSchema = opContext.logBinding.logSchema.extend(harnessSchema);
  const extendedBinding: OpContextBinding = {
    ...opContext,
    logBinding: { ...opContext.logBinding, logSchema: extendedSchema },
  };

  const verbose = isVerboseTraceEnabled(options?.verbose);
  _globalRunConfig = {
    binding: extendedBinding,
    sqlite: options?.sqlite,
    verbose,
  };
  _globalActiveRun = startTestRun(_globalRunConfig);

  // Register global teardown once — each preload afterAll ends the current run and clears it so the next
  // file's first traced test opens a fresh root and tracer.
  if (!_globalAfterAllRegistered) {
    _globalAfterAllRegistered = true;
    _afterAll(async () => {
      const run = _globalActiveRun;
      _globalActiveRun = null;
      if (run !== null) {
        await endTestRun('global', run, _globalRunConfig?.sqlite);
      }
    });
  }
}

/**
 * Create a bun:test mock replacement that wraps it()/test() in trace spans.
 *
 * Call this from the preload file and pass the result to mock.module:
 * ```typescript
 * mock.module('bun:test', () => createBunTestMock(bunTest));
 * ```
 *
 * @param bunTestModule - The original bun:test namespace (`import * as bunTest from 'bun:test'`)
 */
export function createBunTestMock<TModule extends BunTestModuleShape>(bunTestModule: TModule): TModule {
  if (_globalRunConfig === null) {
    if (_activeSuiteTracer) {
      return _activeSuiteTracer.createBunTestMock(bunTestModule);
    }
    throw new Error('Call initTraceTestRun() before createBunTestMock()');
  }

  const describeStack: string[] = [];

  const wrappedIt = wrapBunRegistration(
    bunTestModule.it,
    traceTests(
      describeStack,
      () => ensureGlobalRun('Call initTraceTestRun() before createBunTestMock()').rootCtx,
      _als,
    ),
  );
  return {
    ...bunTestModule,
    describe: wrapBunRegistration(bunTestModule.describe, trackDescribes(describeStack)),
    it: wrappedIt,
    test: wrappedIt,
  };
}

/**
 * Get the current span context from AsyncLocalStorage (inside an it() block).
 *
 * Package-local tracer modules pass their own exported span-context type here so
 * `useTestSpan()` preserves the suite-local schema extension instead of falling
 * back to the erased global singleton shape.
 */
export function useTestSpan<B extends OpContextBinding>(): SpanContext<OpContextOf<B>>;
export function useTestSpan(): unknown {
  if (_activeSuiteTracer) {
    const ctx = _activeSuiteTracer.useTestSpan();
    if (!isSpanContext<OpContextOf<OpContextBinding>>(ctx)) {
      throw new TypeError('Active test tracer returned an invalid span context');
    }
    return ctx;
  }

  const ctx = _als.getStore();
  if (!ctx) throw new Error('useTestSpan() called outside of a traced it()');
  return ctx;
}

/**
 * Return the current traced test's identity (`describePath > label`, or `label` alone at top level),
 * or `undefined` outside a traced `it()`. Never throws.
 */
export function currentTestName(): string | undefined {
  if (_activeSuiteTracer) {
    return _activeSuiteTracer.currentTestName();
  }

  const ctx = _als.getStore();
  return ctx ? formatTestName(ctx) : undefined;
}

/**
 * Get the root tracer instance.
 *
 * The optional type parameter narrows the return type for consumers that know
 * the concrete binding (e.g. `getTracer<typeof opContext>()`). The underlying
 * tracer is always the one created by `installBunTestTracing()` or `initTraceTestRun()`.
 */
export function getTracer(): Tracer<OpContextBinding> {
  if (_activeSuiteTracer) {
    const tracer = _activeSuiteTracer.getTracer();
    if (!isTracerInstance(tracer)) {
      throw new TypeError('Active test tracer returned an invalid tracer');
    }
    return tracer;
  }

  return ensureGlobalRun('Call initTraceTestRun() in preload before tests').tracer;
}

/** Runs one registered callback, given the values Bun passed, the callback's declared parameter count, and the call. */
type CallbackRun = (values: unknown[], arity: number, call: (args: unknown[]) => unknown) => unknown;

/**
 * Wraps a bun:test registration (`describe`, `it`/`test`, or any member chain of them) so every callback it registers
 * runs through `wrapCallback(label)`, the vitest harness's pattern (vitest-harness.ts `wrapVitestRegistration`). The
 * modifiers (`.only`, `.skip`, `.todo`, `.failing`, `.concurrent`, `.serial`) are wrapped the same way, and the
 * factories (`.each` and the `*If` forms) have the registration they return wrapped. Bun keeps its own argument checks
 * and title formatting, so an each row's label is its template. A function label is named by its `name`.
 */
function wrapBunRegistration<F extends object>(
  registration: F,
  wrapCallback: (label: string) => CallbackRun,
  thisArg?: object,
): F {
  return new Proxy(registration, {
    apply(target, _receiver: unknown, args: unknown[]) {
      if (typeof target !== 'function') throw new TypeError('bun:test registration is not callable');
      // Bun takes `(label, fn, options?)` or the options-first `(label, options, fn)`; `describe(fn)` has no label.
      const index = typeof args[1] === 'function' ? 1 : 2;
      const callback = args[index];
      if (typeof callback === 'function') {
        const label = args[0];
        const run = wrapCallback(typeof label === 'function' ? label.name : String(label));
        // A plain function, not a Proxy: Bun's each refuses a Proxy ("test.each() expects a function as the second
        // argument"). It declares no parameters, so Bun never passes `done`; settleTestCallback stands in for it.
        args[index] = function (this: unknown, ...values: unknown[]): unknown {
          return run(values, callback.length, (callArgs) => Reflect.apply(callback, this, callArgs));
        };
      }
      return Reflect.apply(target, thisArg, args);
    },
    get(target, key) {
      const member: unknown = Reflect.get(target, key);
      if (typeof member !== 'function') return member;
      switch (key) {
        case 'only':
          return process.env.CI ? disabledOnly : wrapBunRegistration(member, wrapCallback, target);
        case 'skip':
        case 'todo':
        case 'failing':
        case 'concurrent':
        case 'serial':
          return wrapBunRegistration(member, wrapCallback, target);
        case 'each':
        case 'if':
        case 'skipIf':
        case 'todoIf':
        case 'failingIf':
        case 'concurrentIf':
        case 'serialIf':
          return new Proxy(member, {
            apply(factory, _factoryReceiver: unknown, factoryArgs: unknown[]) {
              const chosen: unknown = Reflect.apply(factory, target, factoryArgs);
              if (typeof chosen !== 'function') {
                throw new TypeError(`bun:test ${key}() returned no registration function`);
              }
              return wrapBunRegistration(chosen, wrapCallback, target);
            },
          });
        default:
          return member;
      }
    },
  });
}

/**
 * Runs each test callback in its own child span of `rootCtx()`, named by the label it registered, under the describe
 * path it registered in, and holds the span in `als` for useTestSpan(). An assertion failure records span-err and any
 * other failure span-exception; both still fail the test.
 */
function traceTests<Ctx extends OpContext>(
  describeStack: readonly string[],
  rootCtx: () => SpanContext<Ctx>,
  als: AsyncLocalStorage<SpanContext<Ctx>>,
): (label: string) => CallbackRun {
  return (label) => {
    const describePath = describeStack.length > 0 ? describeStack.join(' > ') : null;
    return (values, arity, call) =>
      rootCtx().span(label, async (ctx) => {
        writeDescribeTag(ctx.tag, describePath);
        try {
          await als.run(ctx, () => settleTestCallback(values, arity, call));
          return ctx.ok(undefined); // pass → span-ok
        } catch (error) {
          if (isExpectError(error)) ctx.err(error); // assertion failure → span-err
          throw error; // re-throw so bun:test sees the failure; any other throw → span-exception
        }
      });
  };
}

/** Runs each describe callback with its registered ancestry on the describe stack, so its tests record the full path. */
function trackDescribes(describeStack: string[]): (label: string) => CallbackRun {
  return (label) => {
    // Capture enclosing describes at registration: Bun may run a nested describe's callback after its parent returns.
    const registeredPath = [...describeStack, label];
    return (values, _arity, call) => {
      const previous = describeStack.splice(0, describeStack.length, ...registeredPath);
      try {
        return call(values);
      } finally {
        describeStack.splice(0, describeStack.length, ...previous);
      }
    };
  };
}

/**
 * Settles a test callback as plain Bun settles it (Bun 1.4.3 and 1.4.4, measured). Bun never passes `done` to the
 * harness's callback, which declares no parameters, so a callback that declares more parameters than the values Bun
 * passed gets the harness's `done`: `done()` and `done(null)` pass, and any other value fails the test with that value.
 * - A callback that returns no promise settles on its first `done` call; later calls change nothing.
 * - A callback that returns a promise settles on the promise, and a rejection fails the test whatever its reason,
 *   `undefined` included. `done()` changes nothing, and a `done(error)` before the promise resolves fails the test. A
 *   `done(error)` after it resolves throws, which Bun reports as an error between tests; Bun alone still fails the test
 *   when that call lands in the microtask drain right after the resolution.
 * Every rejection here settles the returned promise, so none goes unhandled.
 */
function settleTestCallback(values: unknown[], arity: number, call: (args: unknown[]) => unknown): unknown {
  if (arity <= values.length) return call(values);
  const { promise, resolve, reject } = Promise.withResolvers<void>();
  // `calling` until the callback returns, because whether it returns a promise decides what a done call means.
  let phase: 'calling' | 'awaiting-done' | 'awaiting-promise' | 'resolved-by-promise' | 'settled' = 'calling';
  let firstCall: { readonly error: unknown } | undefined;
  let firstFailure: { readonly error: unknown } | undefined;
  const settle = (error: unknown): void => {
    phase = 'settled';
    if (error === undefined || error === null) resolve();
    else reject(error);
  };
  const result = call([
    ...values,
    (error?: unknown): void => {
      const failed = error !== undefined && error !== null;
      switch (phase) {
        case 'calling':
          firstCall ??= { error };
          if (failed) firstFailure ??= { error };
          return;
        case 'awaiting-done':
          settle(error);
          return;
        case 'awaiting-promise':
          if (failed) settle(error);
          return;
        case 'resolved-by-promise':
          if (failed) throw error;
          return;
        case 'settled':
          return;
      }
    },
  ]);
  if (result instanceof Promise) {
    phase = 'awaiting-promise';
    result.then(
      () => {
        if (phase !== 'awaiting-promise') return;
        phase = 'resolved-by-promise';
        resolve();
      },
      (reason: unknown) => {
        if (phase !== 'awaiting-promise') return;
        // A rejection fails the test whatever its reason, `undefined` included; only done's value rule passes it.
        phase = 'settled';
        reject(reason);
      },
    );
    if (firstFailure) settle(firstFailure.error);
  } else if (firstCall) {
    settle(firstCall.error);
  } else {
    phase = 'awaiting-done';
  }
  return promise;
}

function disabledOnly(): never {
  throw new Error(ciOnlyDisabledMessage);
}

// =============================================================================
// Auto-discovery preload
// =============================================================================

export type AutoSetupTarget =
  | {
      /** Root of the single consumer package whose tests are running. */
      readonly packageRoot: string;
    }
  | {
      /** Workspace root for a cross-package run, with its member package roots. */
      readonly workspaceRoot: string;
      readonly packageRoots: readonly string[];
    };

/**
 * Create the root tracer for the current bun test run from a resolved consumer
 * target (see `resolveConsumerTarget` in `./consumer-package.js`).
 *
 * A package target imports that package's own `src/test-suite-tracer.ts` (if
 * present) and writes the sink under the package root. A workspace-root target
 * scans the member packages' tracer modules so cross-package runs keep the
 * merged-schema behavior, writing the sink under the workspace root.
 *
 * Per-package `useTestSpan()` delegates to the global `useTestSpan()` from this
 * module which checks `_activeSuiteTracer` — set by the root preload via
 * `installBunTestTracing()`.
 *
 * @example
 * ```ts
 * // bun trace preload
 * const target = resolveConsumerTarget(process.cwd());
 * if (target.kind === 'package') await autoSetupBunTestTracing({ packageRoot: target.packageRoot });
 * ```
 */
export async function autoSetupBunTestTracing(target: AutoSetupTarget): Promise<boolean> {
  const installSuite = (opContext: OpContextBinding, mergedSchema: Record<string, unknown>, dbDir: string): boolean => {
    const hasCustomSchema = Object.keys(mergedSchema).length > 0;
    const suite = makeBunTestSuiteTracer(opContext, {
      sqlite: { dbPath: join(dbDir, TRACE_DB_DIRECTORY, TRACE_DB_FILENAME) },
      extraTestColumns: hasCustomSchema && isSchemaFieldsRecord(mergedSchema) ? mergedSchema : undefined,
    });

    suite.setupBunTestSuiteTracing();
    return true;
  };

  if ('packageRoot' in target) {
    const tracerPath = join(target.packageRoot, 'src', 'test-suite-tracer.ts');

    if (!existsSync(tracerPath)) {
      return installSuite(defaultAutoSetupOpContext, {}, target.packageRoot);
    }

    const mod = await import(tracerPath);
    const mergedSchema = isSchemaFieldsRecord(mod.extraTestColumns) ? mod.extraTestColumns : {};
    const opContext = isOpContextBindingLike(mod.opContext) ? mod.opContext : defaultAutoSetupOpContext;

    return installSuite(opContext, mergedSchema, target.packageRoot);
  }

  const mergedSchema: Record<string, unknown> = {};
  let opContext: OpContextBinding | null = null;

  for (const packageRoot of target.packageRoots) {
    const tracerPath = join(packageRoot, 'src', 'test-suite-tracer.ts');
    if (!existsSync(tracerPath)) continue;

    const mod = await import(tracerPath);

    // Collect custom schema columns from each package
    if (mod.extraTestColumns && typeof mod.extraTestColumns === 'object') {
      Object.assign(mergedSchema, mod.extraTestColumns);
    }

    // Use the first available opContext (they all bind the same opContext)
    if (!opContext && isOpContextBindingLike(mod.opContext)) {
      opContext = mod.opContext;
    }
  }

  return installSuite(opContext ?? defaultAutoSetupOpContext, mergedSchema, target.workspaceRoot);
}
