/* biome-ignore-all lint/complexity/noArguments: hot-path overload dispatch avoids rest-arg allocations */

/**
 * SpanContext - Context provided to op functions during span execution
 *
 * Per specs/lmao/01c_context_flow_and_op_wrappers.md and specs/lmao/01l_op_context_pattern.md:
 * - SpanContext combines built-in properties with user-extensible Extra
 * - Uses prototype-based inheritance for V8 hidden class optimization
 * - Child spans inherit scoped attributes from parents
 *
 * ## Class-Based Design
 *
 * SpanContext is implemented as a class factory pattern:
 * - `createSpanContextClass()` returns a class that closes over schema/logBinding
 * - Child contexts are constructed directly from the frozen CallsitePlan
 * - User properties are copied from the parent only when an override context is requested
 *
 * ## Method Binding for Destructuring
 *
 * Methods that users commonly destructure (span, ok, err, setScope) are defined
 * as arrow function properties. This means:
 * - They're own properties on each instance (slight memory overhead)
 * - But destructuring works without explicit .bind() calls
 * - const { span, ok, err } = ctx; // Just works!
 */

import type { TagWriter, WriterState } from './codegen/fixedPositionWriterGenerator.js';
import {
  createSpanLogger as createSpanLoggerFromGenerator,
  type FluentLogEntry,
  type ScopeUpdate,
  type SpanLoggerImpl,
} from './codegen/spanLoggerGenerator.js';
import type { RetryPolicy } from './errors/retry-policy.js';
import { TransientError } from './errors/Transient.js';
import type { Op } from './op.js';
import type { OpContext, OpMetadata, SpanContext, SpanFn, SpanLogger, SpanSyncFn } from './opContext/types.js';
import type { CallsitePlan, PhysicalLayoutPlan } from './physicalLayoutPlan.js';
import { Err, hasErrorCode, Ok, type Result } from './result.js';
import {
  RUNTIME_HINT_DEPS,
  RUNTIME_HINT_FF,
  RUNTIME_HINT_FULL_CAPABILITIES,
  RUNTIME_HINT_LOG,
  RUNTIME_HINT_RESULT,
  RUNTIME_HINT_SCOPE,
  RUNTIME_HINT_SPAN,
  RUNTIME_HINT_TAG,
} from './runtimeHint.js';
import type { FeatureFlagEvaluator, FlagEvaluator, InferFeatureFlagsWithContext } from './schema/evaluator.js';
import {
  ENTRY_TYPE_SPAN_ERR,
  ENTRY_TYPE_SPAN_EXCEPTION,
  ENTRY_TYPE_SPAN_OK,
  ENTRY_TYPE_SPAN_RETRY,
} from './schema/systemSchema.js';
import type { InferSchema, LogSchema } from './schema/types.js';
import type { AnySpanBuffer, LogBinding, SpanBuffer } from './types.js';

// Note: TraceRoot.writeSpanStart() is used instead of direct timestamp writes.
// The platform-specific TraceRoot (traceRoot.es.ts or traceRoot.node.ts) handles
// timestamp calculation and writes to buffer._system columns.

// =============================================================================
// SpanContext Symbol Marker
// =============================================================================

/**
 * Internal symbol to mark span contexts
 * Used for type guards and prototype chain detection
 */
export const SPAN_CONTEXT_MARKER = Symbol.for('lmao.SpanContext');

// =============================================================================
// Type Exports
// =============================================================================

//#region smoo/lmao!n/codegen-spanlogger.ff
/**
 * Internal SpanLogger type with FF methods and internal methods exposed.
 * Used by FeatureFlagEvaluator and internal span management code.
 *
 * The 01g "SpanLoggerInternal Type Pattern": the public SpanLogger<T> hides
 * ffAccess/ffUsage; the evaluator casts to this internal type to reach them.
 * The methods themselves are generated in codegen/spanLoggerGenerator.ts.
 */
export type SpanLoggerInternal<T extends LogSchema> = SpanLoggerImpl<T> & {
  /**
   * Write feature flag access entry (internal, not on public type).
   * Called by FeatureFlagEvaluator to log flag access.
   */
  ffAccess(flagName: string, value: unknown): void;
  /**
   * Write feature flag usage entry (internal, not on public type).
   * Called by FeatureFlagEvaluator to log flag usage.
   */
  ffUsage(flagName: string, context?: Record<string, unknown>): FluentLogEntry<T>;
};
//#endregion smoo/lmao!n/codegen-spanlogger.ff

type SpanDispatchFn<Ctx extends OpContext, Args extends unknown[] = unknown[], S = unknown, E = unknown> = (
  ctx: SpanContext<Ctx>,
  ...args: Args
) => Result<S, E> | Promise<Result<S, E>>;

type SpanDispatchTarget<Ctx extends OpContext> = {
  readonly callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>;
  readonly opMetadata: OpMetadata;
  readonly fn: SpanDispatchFn<Ctx>;
};

function isOpLike(value: unknown): value is Op<OpContext, unknown[], unknown, unknown> {
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof Reflect.get(value, 'fn') === 'function' &&
    typeof Reflect.get(value, 'callsitePlan') === 'object' &&
    typeof Reflect.get(value, 'metadata') === 'object'
  );
}

function isOpInstance<Ctx extends OpContext>(value: unknown): value is Op<Ctx, unknown[], unknown, unknown> {
  return isOpLike(value);
}

// WHY: resolveSpanTarget() accepts either an Op instance or a raw span
// function. TypeScript needs an explicit predicate here to preserve the call
// signature when the non-Op branch passes `value` through as `fn`.
function isSpanDispatchFn<Ctx extends OpContext>(value: unknown): value is SpanDispatchFn<Ctx> {
  return typeof value === 'function';
}

function isSpanContextInstance<Ctx extends OpContext>(value: unknown): value is SpanContextInstance<Ctx> {
  return (
    isSpanContext<Ctx>(value) &&
    Reflect.has(value, '_spanBuffer') &&
    Reflect.has(value, '_buffer') &&
    Reflect.has(value, '_schema') &&
    Reflect.has(value, '_logBinding') &&
    Reflect.has(value, '_physicalLayoutPlan')
  );
}

function asSpanContextInstance<Ctx extends OpContext>(value: object): SpanContextInstance<Ctx> {
  if (!isSpanContextInstance<Ctx>(value)) {
    throw new TypeError('Callsite plan created an invalid child span context');
  }
  return value;
}

function readStringArgument(args: IArguments, index: number, label: string): string {
  const value = args[index];
  if (typeof value !== 'string') {
    throw new TypeError(`${label} must be a string`);
  }
  return value;
}

function resolveSpanTarget<Ctx extends OpContext>(
  value: unknown,
  fallbackPlan: CallsitePlan<Ctx['logSchema'], Ctx>,
  fallbackMetadata: OpMetadata,
): SpanDispatchTarget<Ctx> {
  if (isOpInstance<Ctx>(value)) {
    return {
      callsitePlan: value.callsitePlan,
      opMetadata: value.metadata,
      fn: value.fn,
    };
  }

  if (!isSpanDispatchFn<Ctx>(value)) {
    throw new TypeError('span() expects an Op or function');
  }

  return {
    callsitePlan: fallbackPlan,
    opMetadata: fallbackMetadata,
    fn: value,
  };
}

function isSynchronousResult<S, E>(value: Result<S, E> | PromiseLike<Result<S, E>>): value is Result<S, E> {
  return value instanceof Ok || value instanceof Err;
}

function getRetryableError<S, E>(result: Result<S, E>, attempt: number): TransientError<string, unknown> | undefined {
  if (!(result instanceof Err) || !(result.error instanceof TransientError)) return undefined;
  return attempt < result.error.policy.maxAttempts ? result.error : undefined;
}

// =============================================================================
// Helper Functions
// =============================================================================

//#region smoo/lmao!n/lmao-entry-span-lifecycle-entry-types.start
/**
 * Write span-start entry to buffer at row 0 (fixed layout)
 * Pre-initialize row 1 as span-exception (will be overwritten by ok/err)
 * Set writeIndex to 2 (events start after reserved rows)
 *
 * Per specs/lmao/01h_entry_types_and_logging_primitives.md:
 * - Row 0: span-start (written here)
 * - Row 1: span-end (pre-initialized as exception, overwritten by ok/err)
 * - Row 2+: events (ctx.log.* appends here)
 *
 * @param buffer - SpanBuffer to write to
 * @param spanName - Name for this span
 */
export function writeSpanStart<T extends LogSchema>(buffer: SpanBuffer<T>, spanName: string): void {
  // Delegate to TraceRoot - platform-specific implementation handles timestamps
  buffer._traceRoot._writeSpanStart(buffer._traceRoot, buffer, spanName);
}
//#endregion smoo/lmao!n/lmao-entry-span-lifecycle-entry-types.start

/** Create a SpanLogger bound to an existing SpanContext writer state. */
export function createSpanLogger<T extends LogSchema>(schema: T, state: WriterState): SpanLoggerImpl<T> {
  return createSpanLoggerFromGenerator(schema, state);
}

//#region smoo/lmao!n/op-retry.delay
/**
 * Calculate delay for retry based on policy and attempt number.
 *
 * @param policy - RetryPolicy from TransientError
 * @param attempt - Current attempt number (1-indexed)
 * @returns Delay in milliseconds before next retry
 */
function calculateDelay(policy: RetryPolicy, attempt: number): number {
  let delay: number;

  switch (policy.backoff) {
    case 'exponential':
      // Delay doubles each attempt: baseDelay * 2^(attempt-1)
      delay = Math.min(policy.baseDelayMs * 2 ** (attempt - 1), policy.maxDelayMs ?? 30000);
      break;
    case 'linear':
      // Delay increases linearly: baseDelay * attempt
      delay = Math.min(policy.baseDelayMs * attempt, policy.maxDelayMs ?? 30000);
      break;
    case 'fixed':
      // Same delay between each attempt
      delay = policy.baseDelayMs;
      break;
    default:
      delay = policy.baseDelayMs;
  }

  // Apply jitter if enabled (default true) - random value between 0 and delay
  if (policy.jitter !== false) {
    delay = Math.floor(Math.random() * delay);
  }

  return delay;
}
//#endregion smoo/lmao!n/op-retry.delay

//#region smoo/lmao!n/lmao-entry-span-lifecycle-entry-types.retry
/**
 * Write span-retry entry to buffer.
 *
 * Per specs/lmao/01h_entry_types_and_logging_primitives.md:
 * - span-retry appends to Row 2+ in parent span buffer (like log entries)
 * - Trace-only entry (NOT written to event log)
 * - Contains: attempt number, error code, and delay until next attempt
 *
 * @param buffer - SpanBuffer to write to (appends at _writeIndex)
 * @param attempt - Attempt number (1-indexed)
 * @param error - TransientError that triggered the retry
 * @param delayMs - Delay before next attempt
 */
function writeRetryEntry<T extends LogSchema>(
  buffer: SpanBuffer<T>,
  attempt: number,
  error: TransientError<string, unknown>,
  delayMs: number,
): void {
  // Get current write index and advance for next write
  const index = buffer._writeIndex;

  const traceRoot = buffer._traceRoot;
  const appendLogEntry = buffer._opMetadata._physicalLayoutPlan?.appendLogEntry ?? traceRoot._appendLogEntry;
  appendLogEntry(traceRoot, buffer, ENTRY_TYPE_SPAN_RETRY);

  // Write retry-specific message: retry:op:{opName} for prefix-based querying
  const opName = buffer._opMetadata?.name ?? 'unknown';
  buffer.message(index, `retry:op:${opName}`);

  // Write error code if available
  if (error.code) {
    buffer.error_code(index, error.code);
  }

  // Retry metadata fields for direct queryability in Arrow output
  buffer.retry_attempt(index, attempt);
  buffer.retry_delay_ms(index, delayMs);
}
//#endregion smoo/lmao!n/lmao-entry-span-lifecycle-entry-types.retry

//#region smoo/lmao!n/op-retry.loop
/**
 * Sleep for the specified duration.
 *
 * @param ms - Duration in milliseconds
 * @returns Promise that resolves after the delay
 */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function executeWithRetry0<Ctx extends OpContext, S, E>(
  buffer: SpanBuffer<Ctx['logSchema']>,
  fn: (ctx: SpanContext<Ctx>) => Result<S, E> | Promise<Result<S, E>>,
  ctx: SpanContext<Ctx>,
): Promise<Result<S, E>> {
  let attempt = 0;
  while (true) {
    attempt++;
    const result = await fn(ctx);
    if (!(result instanceof Err) || !(result.error instanceof TransientError)) return result;
    if (attempt >= result.error.policy.maxAttempts) return result;
    const delay = calculateDelay(result.error.policy, attempt);
    writeRetryEntry(buffer, attempt, result.error, delay);
    await sleep(delay);
  }
}

async function executeWithRetry1<Ctx extends OpContext, S, E, A1>(
  buffer: SpanBuffer<Ctx['logSchema']>,
  fn: (ctx: SpanContext<Ctx>, a1: A1) => Result<S, E> | Promise<Result<S, E>>,
  ctx: SpanContext<Ctx>,
  a1: A1,
): Promise<Result<S, E>> {
  let attempt = 0;
  while (true) {
    attempt++;
    const result = await fn(ctx, a1);
    if (!(result instanceof Err) || !(result.error instanceof TransientError)) return result;
    if (attempt >= result.error.policy.maxAttempts) return result;
    const delay = calculateDelay(result.error.policy, attempt);
    writeRetryEntry(buffer, attempt, result.error, delay);
    await sleep(delay);
  }
}

async function executeWithRetry2<Ctx extends OpContext, S, E, A1, A2>(
  buffer: SpanBuffer<Ctx['logSchema']>,
  fn: (ctx: SpanContext<Ctx>, a1: A1, a2: A2) => Result<S, E> | Promise<Result<S, E>>,
  ctx: SpanContext<Ctx>,
  a1: A1,
  a2: A2,
): Promise<Result<S, E>> {
  let attempt = 0;
  while (true) {
    attempt++;
    const result = await fn(ctx, a1, a2);
    if (!(result instanceof Err) || !(result.error instanceof TransientError)) return result;
    if (attempt >= result.error.policy.maxAttempts) return result;
    const delay = calculateDelay(result.error.policy, attempt);
    writeRetryEntry(buffer, attempt, result.error, delay);
    await sleep(delay);
  }
}

async function executeWithRetry3<Ctx extends OpContext, S, E, A1, A2, A3>(
  buffer: SpanBuffer<Ctx['logSchema']>,
  fn: (ctx: SpanContext<Ctx>, a1: A1, a2: A2, a3: A3) => Result<S, E> | Promise<Result<S, E>>,
  ctx: SpanContext<Ctx>,
  a1: A1,
  a2: A2,
  a3: A3,
): Promise<Result<S, E>> {
  let attempt = 0;
  while (true) {
    attempt++;
    const result = await fn(ctx, a1, a2, a3);
    if (!(result instanceof Err) || !(result.error instanceof TransientError)) return result;
    if (attempt >= result.error.policy.maxAttempts) return result;
    const delay = calculateDelay(result.error.policy, attempt);
    writeRetryEntry(buffer, attempt, result.error, delay);
    await sleep(delay);
  }
}

async function executeWithRetry4<Ctx extends OpContext, S, E, A1, A2, A3, A4>(
  buffer: SpanBuffer<Ctx['logSchema']>,
  fn: (ctx: SpanContext<Ctx>, a1: A1, a2: A2, a3: A3, a4: A4) => Result<S, E> | Promise<Result<S, E>>,
  ctx: SpanContext<Ctx>,
  a1: A1,
  a2: A2,
  a3: A3,
  a4: A4,
): Promise<Result<S, E>> {
  let attempt = 0;
  while (true) {
    attempt++;
    const result = await fn(ctx, a1, a2, a3, a4);
    if (!(result instanceof Err) || !(result.error instanceof TransientError)) return result;
    if (attempt >= result.error.policy.maxAttempts) return result;
    const delay = calculateDelay(result.error.policy, attempt);
    writeRetryEntry(buffer, attempt, result.error, delay);
    await sleep(delay);
  }
}

async function executeWithRetry5<Ctx extends OpContext, S, E, A1, A2, A3, A4, A5>(
  buffer: SpanBuffer<Ctx['logSchema']>,
  fn: (ctx: SpanContext<Ctx>, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5) => Result<S, E> | Promise<Result<S, E>>,
  ctx: SpanContext<Ctx>,
  a1: A1,
  a2: A2,
  a3: A3,
  a4: A4,
  a5: A5,
): Promise<Result<S, E>> {
  let attempt = 0;
  while (true) {
    attempt++;
    const result = await fn(ctx, a1, a2, a3, a4, a5);
    if (!(result instanceof Err) || !(result.error instanceof TransientError)) return result;
    if (attempt >= result.error.policy.maxAttempts) return result;
    const delay = calculateDelay(result.error.policy, attempt);
    writeRetryEntry(buffer, attempt, result.error, delay);
    await sleep(delay);
  }
}

async function executeWithRetry6<Ctx extends OpContext, S, E, A1, A2, A3, A4, A5, A6>(
  buffer: SpanBuffer<Ctx['logSchema']>,
  fn: (ctx: SpanContext<Ctx>, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6) => Result<S, E> | Promise<Result<S, E>>,
  ctx: SpanContext<Ctx>,
  a1: A1,
  a2: A2,
  a3: A3,
  a4: A4,
  a5: A5,
  a6: A6,
): Promise<Result<S, E>> {
  let attempt = 0;
  while (true) {
    attempt++;
    const result = await fn(ctx, a1, a2, a3, a4, a5, a6);
    if (!(result instanceof Err) || !(result.error instanceof TransientError)) return result;
    if (attempt >= result.error.policy.maxAttempts) return result;
    const delay = calculateDelay(result.error.policy, attempt);
    writeRetryEntry(buffer, attempt, result.error, delay);
    await sleep(delay);
  }
}

async function executeWithRetry7<Ctx extends OpContext, S, E, A1, A2, A3, A4, A5, A6, A7>(
  buffer: SpanBuffer<Ctx['logSchema']>,
  fn: (
    ctx: SpanContext<Ctx>,
    a1: A1,
    a2: A2,
    a3: A3,
    a4: A4,
    a5: A5,
    a6: A6,
    a7: A7,
  ) => Result<S, E> | Promise<Result<S, E>>,
  ctx: SpanContext<Ctx>,
  a1: A1,
  a2: A2,
  a3: A3,
  a4: A4,
  a5: A5,
  a6: A6,
  a7: A7,
): Promise<Result<S, E>> {
  let attempt = 0;
  while (true) {
    attempt++;
    const result = await fn(ctx, a1, a2, a3, a4, a5, a6, a7);
    if (!(result instanceof Err) || !(result.error instanceof TransientError)) return result;
    if (attempt >= result.error.policy.maxAttempts) return result;
    const delay = calculateDelay(result.error.policy, attempt);
    writeRetryEntry(buffer, attempt, result.error, delay);
    await sleep(delay);
  }
}

async function executeWithRetry8<Ctx extends OpContext, S, E, A1, A2, A3, A4, A5, A6, A7, A8>(
  buffer: SpanBuffer<Ctx['logSchema']>,
  fn: (
    ctx: SpanContext<Ctx>,
    a1: A1,
    a2: A2,
    a3: A3,
    a4: A4,
    a5: A5,
    a6: A6,
    a7: A7,
    a8: A8,
  ) => Result<S, E> | Promise<Result<S, E>>,
  ctx: SpanContext<Ctx>,
  a1: A1,
  a2: A2,
  a3: A3,
  a4: A4,
  a5: A5,
  a6: A6,
  a7: A7,
  a8: A8,
): Promise<Result<S, E>> {
  let attempt = 0;
  while (true) {
    attempt++;
    const result = await fn(ctx, a1, a2, a3, a4, a5, a6, a7, a8);
    if (!(result instanceof Err) || !(result.error instanceof TransientError)) return result;
    if (attempt >= result.error.policy.maxAttempts) return result;
    const delay = calculateDelay(result.error.policy, attempt);
    writeRetryEntry(buffer, attempt, result.error, delay);
    await sleep(delay);
  }
}
//#endregion smoo/lmao!n/op-retry.loop

//#region smoo/lmao!n/lmao-entry-span-lifecycle-entry-types.end
/**
 * Write span-end entry to buffer at row 1.
 *
 * Called by span methods after fn() returns to write:
 * - entry_type (SPAN_OK or SPAN_ERR based on result instanceof)
 * - timestamp
 * - error_code (for Err only)
 * - result row attributes already written eagerly by the fixed-position ResultWriter
 *
 * @param buffer - SpanBuffer to write to
 * @param result - The result returned by the span function
 */
function writeSpanEndEntry<T extends LogSchema>(buffer: SpanBuffer<T>, entryType: number): void {
  const appenders = buffer._opMetadata._physicalLayoutPlan?.appenders;
  if (appenders === undefined) throw new TypeError('SpanBuffer metadata is missing physical lifecycle appenders');
  appenders.writeSpanEnd(buffer, entryType);
}

export function writeSpanEnd<T extends LogSchema, S, E>(buffer: SpanBuffer<T>, result: Result<S, E>): void {
  if (result instanceof Ok) {
    writeSpanEndEntry(buffer, ENTRY_TYPE_SPAN_OK);
  } else if (result instanceof Err) {
    writeSpanEndEntry(buffer, ENTRY_TYPE_SPAN_ERR);
    // Write error_code if the error has a code property
    if (hasErrorCode(result.error)) {
      buffer.error_code(1, result.error.code);
    }
  }
}
//#endregion smoo/lmao!n/lmao-entry-span-lifecycle-entry-types.end

// =============================================================================
// SpanContext Class Types
// =============================================================================

//#region smoo/lmao!n/spancontext-type
/**
 * Constructor type for SpanContext classes created by createSpanContextClass.
 * The instance itself becomes the writer state after its core fields are assigned.
 */
export type SpanContextClass<Ctx extends OpContext = OpContext> = new (
  buffer: SpanBuffer<Ctx['logSchema']>,
  schema: Ctx['logSchema'],
  callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
  contextSource?: Record<string, unknown>,
  contextOverrides?: Record<string, unknown>,
  deps?: Record<string, unknown>,
  ffSource?: FlagEvaluator<Ctx> | (FeatureFlagEvaluator<Ctx> & InferFeatureFlagsWithContext<Ctx>),
) => SpanContextInstance<Ctx>;

/**
 * Instance type of SpanContext class - what ops actually receive.
 * This is a type alias that extends the public SpanContext with internal properties.
 */
export type SpanContextInstance<Ctx extends OpContext> = SpanContext<Ctx> &
  WriterState & {
    _spanBuffer: SpanBuffer<Ctx['logSchema']>;
    _schema: Ctx['logSchema'];
    _spanLogger: SpanLoggerImpl<Ctx['logSchema']>;
    _logBinding: LogBinding;
    _physicalLayoutPlan: CallsitePlan<Ctx['logSchema'], Ctx>;

    // Internal methods
    _newCtx0(): SpanContextInstance<Ctx>;
    _newCtx1(overrides: object): SpanContextInstance<Ctx>;
    _spanPre(
      childCtx: SpanContextInstance<Ctx>,
      line: number,
      name: string | number,
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      opMetadata: OpMetadata,
    ): SpanContextInstance<Ctx>;
    _spanException(buffer: SpanBuffer<Ctx['logSchema']>, error: unknown): void;

    spanSync0<S, E>(
      line: number,
      name: string | number,
      childCtx: SpanContextInstance<Ctx>,
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      fn: (ctx: SpanContext<Ctx>) => Result<S, E> | Promise<Result<S, E>>,
    ): Result<S, E>;
    span0<S, E>(
      line: number,
      name: string | number,
      childCtx: SpanContextInstance<Ctx>,
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      fn: (ctx: SpanContext<Ctx>) => Result<S, E> | Promise<Result<S, E>>,
    ): Promise<Result<S, E>>;
    span1<S, E, A1>(
      line: number,
      name: string | number,
      childCtx: SpanContextInstance<Ctx>,
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      fn: (ctx: SpanContext<Ctx>, a1: A1) => Result<S, E> | Promise<Result<S, E>>,
      a1: A1,
    ): Promise<Result<S, E>>;
    span2<S, E, A1, A2>(
      line: number,
      name: string | number,
      childCtx: SpanContextInstance<Ctx>,
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      fn: (ctx: SpanContext<Ctx>, a1: A1, a2: A2) => Result<S, E> | Promise<Result<S, E>>,
      a1: A1,
      a2: A2,
    ): Promise<Result<S, E>>;
    span3<S, E, A1, A2, A3>(
      line: number,
      name: string | number,
      childCtx: SpanContextInstance<Ctx>,
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      fn: (ctx: SpanContext<Ctx>, a1: A1, a2: A2, a3: A3) => Result<S, E> | Promise<Result<S, E>>,
      a1: A1,
      a2: A2,
      a3: A3,
    ): Promise<Result<S, E>>;
    span4<S, E, A1, A2, A3, A4>(
      line: number,
      name: string | number,
      childCtx: SpanContextInstance<Ctx>,
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      fn: (ctx: SpanContext<Ctx>, a1: A1, a2: A2, a3: A3, a4: A4) => Result<S, E> | Promise<Result<S, E>>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
    ): Promise<Result<S, E>>;
    span5<S, E, A1, A2, A3, A4, A5>(
      line: number,
      name: string | number,
      childCtx: SpanContextInstance<Ctx>,
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      fn: (ctx: SpanContext<Ctx>, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5) => Result<S, E> | Promise<Result<S, E>>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
      a5: A5,
    ): Promise<Result<S, E>>;
    span6<S, E, A1, A2, A3, A4, A5, A6>(
      line: number,
      name: string | number,
      childCtx: SpanContextInstance<Ctx>,
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      fn: (
        ctx: SpanContext<Ctx>,
        a1: A1,
        a2: A2,
        a3: A3,
        a4: A4,
        a5: A5,
        a6: A6,
      ) => Result<S, E> | Promise<Result<S, E>>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
      a5: A5,
      a6: A6,
    ): Promise<Result<S, E>>;
    span7<S, E, A1, A2, A3, A4, A5, A6, A7>(
      line: number,
      name: string | number,
      childCtx: SpanContextInstance<Ctx>,
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      fn: (
        ctx: SpanContext<Ctx>,
        a1: A1,
        a2: A2,
        a3: A3,
        a4: A4,
        a5: A5,
        a6: A6,
        a7: A7,
      ) => Result<S, E> | Promise<Result<S, E>>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
      a5: A5,
      a6: A6,
      a7: A7,
    ): Promise<Result<S, E>>;
    span8<S, E, A1, A2, A3, A4, A5, A6, A7, A8>(
      line: number,
      name: string | number,
      childCtx: SpanContextInstance<Ctx>,
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      fn: (
        ctx: SpanContext<Ctx>,
        a1: A1,
        a2: A2,
        a3: A3,
        a4: A4,
        a5: A5,
        a6: A6,
        a7: A7,
        a8: A8,
      ) => Result<S, E, Ctx['logSchema']> | Promise<Result<S, E, Ctx['logSchema']>>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
      a5: A5,
      a6: A6,
      a7: A7,
      a8: A8,
    ): Promise<Result<S, E, Ctx['logSchema']>>;
  };
//#endregion smoo/lmao!n/spancontext-type

// =============================================================================
// SpanContext Class Factory
// =============================================================================

const spanContextClassCaches = new WeakMap<LogBinding, Map<string, unknown>>();

function isSpanContextClass<Ctx extends OpContext>(value: unknown): value is SpanContextClass<Ctx> {
  return typeof value === 'function';
}

export function isPhysicalLayoutPlanForContext<Ctx extends OpContext>(
  value: unknown,
  schema: Ctx['logSchema'],
): value is PhysicalLayoutPlan<Ctx['logSchema'], Ctx> {
  return (
    typeof value === 'object' &&
    value !== null &&
    Reflect.get(value, 'schema') === schema &&
    isSpanContextClass<Ctx>(Reflect.get(value, 'SpanContextClass'))
  );
}

function isFlagEvaluatorForContext<Ctx extends OpContext>(value: unknown): value is FlagEvaluator<Ctx> {
  return typeof value === 'object' && value !== null && typeof Reflect.get(value, 'forContext') === 'function';
}

/**
 * Create a SpanContext class for the given schema and logBinding.
 *
 * The returned class:
 * - Closes over schema/logBinding (available to all methods)
 * - Constructor takes buffer, spanLogger, tag directly (no temp object allocation)
 * - Uses prototype methods for internal methods (_spanPre, _spanException, span0-span8)
 * - Reuses the parent context as the zero-override source without allocating a carrier object
 * - Context overrides use the plan's schema-specific context factory
 *
 * @param schemaOnly - Tag attribute schema
 * @param logBinding - LogBinding with schema and stats
 * @returns Class constructor for SpanContext instances
 */
export function createSpanContextClass<Ctx extends OpContext>(
  _schemaOnly: Ctx['logSchema'],
  logBinding: LogBinding,
  capabilities = RUNTIME_HINT_FULL_CAPABILITIES,
  userContextKeys: readonly string[] = [],
): SpanContextClass<Ctx> {
  const canonicalUserContextKeys = [...userContextKeys].sort();
  const layoutKey = canonicalUserContextKeys.join('\u0000');
  const cacheKey = `${capabilities}:${layoutKey}`;
  let cache = spanContextClassCaches.get(logBinding);
  if (!cache) {
    cache = new Map();
    spanContextClassCaches.set(logBinding, cache);
  }
  const cached = cache.get(cacheKey);
  if (isSpanContextClass<Ctx>(cached)) return cached;

  const hasTag = (capabilities & RUNTIME_HINT_TAG) !== 0;
  const hasLog = (capabilities & RUNTIME_HINT_LOG) !== 0;
  const hasFf = (capabilities & RUNTIME_HINT_FF) !== 0;
  const hasSpan = (capabilities & RUNTIME_HINT_SPAN) !== 0;
  const hasResult = (capabilities & RUNTIME_HINT_RESULT) !== 0;
  const hasScope = (capabilities & RUNTIME_HINT_SCOPE) !== 0;
  const hasDeps = (capabilities & RUNTIME_HINT_DEPS) !== 0;
  const needsLogger = hasLog || hasFf || hasScope;
  /**
   * SpanContext implementation class.
   *
   * Constructor takes core values directly so arrow functions can close over them,
   * avoiding both temp object allocation and property lookups in hot paths.
   */

  class SpanContextImpl implements WriterState {
    declare readonly _spanBuffer: SpanBuffer<Ctx['logSchema']>;
    declare _buffer: AnySpanBuffer;
    declare readonly _appendLogEntry: WriterState['_appendLogEntry'];
    declare _schema: Ctx['logSchema'];
    declare _spanLogger: SpanLoggerImpl<Ctx['logSchema']>;
    declare _logBinding: LogBinding<Ctx['logSchema']>;
    declare _physicalLayoutPlan: CallsitePlan<Ctx['logSchema'], Ctx>;

    declare tag: TagWriter<Ctx['logSchema']>;
    declare log: SpanLogger<Ctx['logSchema']>;
    declare ff: FeatureFlagEvaluator<Ctx> & InferFeatureFlagsWithContext<Ctx>;
    declare deps: Record<string, unknown>;

    [key: string]: unknown;

    declare setScope: (attributes: ScopeUpdate<Ctx['logSchema']> | null) => void;
    declare ok: <V>(value: V) => Ok<V, Ctx['logSchema']>;
    declare err: <E>(error: E) => Err<E, Ctx['logSchema']>;
    declare span: SpanFn<Ctx>;
    declare spanSync: SpanSyncFn<Ctx>;

    constructor(
      buffer: SpanBuffer<Ctx['logSchema']>,
      schema: Ctx['logSchema'],
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      contextSource?: Record<string, unknown>,
      contextOverrides?: Record<string, unknown>,
      deps?: Record<string, unknown>,
      ffSource?: FlagEvaluator<Ctx> | (FeatureFlagEvaluator<Ctx> & InferFeatureFlagsWithContext<Ctx>),
    ) {
      //#region smoo/lmao!n/codegen-destructured-context
      // Destructured-context assembly (01g): every property an op destructures —
      // tag, log, scope (setScope), ok, err, span — is wired here as a closed-over
      // field/closure so `op(({ tag, log, span, ok, err }) => ...)` needs no ctx drilling.
      this._spanBuffer = buffer;
      this._buffer = buffer;
      this._appendLogEntry = callsitePlan.appendLogEntry;
      this._schema = schema;
      this._logBinding = logBinding;
      this._physicalLayoutPlan = callsitePlan;
      const spanLogger = callsitePlan.newSpanLogger?.(this);
      const tag = callsitePlan.newTagWriter?.(this);
      if (needsLogger) {
        if (!spanLogger) throw new TypeError('SpanContext capability requires a logger');
        this._spanLogger = spanLogger;
      }
      if (hasTag) {
        if (!tag) throw new TypeError('SpanContext tag capability requires a tag writer');
        this.tag = tag;
      }
      if (hasLog && spanLogger) this.log = spanLogger;
      if (hasDeps) this.deps = deps ?? {};
      if (hasFf && ffSource) this.ff = ffSource.forContext(this);
      for (const key of canonicalUserContextKeys) {
        let value = contextSource?.[key];
        if (contextOverrides && Object.prototype.propertyIsEnumerable.call(contextOverrides, key)) {
          const overrideValue = contextOverrides[key];
          if (overrideValue !== undefined) value = overrideValue;
        }
        this[key] = value;
      }

      // Regular functions close over constructor args directly - no property lookups
      // Using regular function (not arrow) allows destructuring while closing over args
      if (hasScope) {
        if (!spanLogger) throw new TypeError('SpanContext scope capability requires a logger');
        this.setScope = (attributes: ScopeUpdate<Ctx['logSchema']> | null): void => {
          spanLogger._setScope(attributes ?? {});
        };
      }

      if (hasResult) {
        this.ok = <V>(value: V): Ok<V, Ctx['logSchema']> => new Ok<V, Ctx['logSchema']>(value, this);
        this.err = <E>(error: E): Err<E, Ctx['logSchema']> => new Err<E, Ctx['logSchema']>(error, this);
      }
      //#endregion smoo/lmao!n/codegen-destructured-context

      //#region smoo/lmao!n/context-flow-span-promise
      // 01c "Why span() Is Promise-Based" + "Child Span Creation via span()" + "spanSync()".
      // span() is the variadic, line-number-aware Promise-based dispatcher: it detects the
      // (line?, name, overrides?, op|fn, ...args) pattern via arguments.length (no rest-spread
      // allocation) and routes to the monomorphic span0-span8. spanSync() is the sync-only
      // sibling (returns Result, no Promise, no async retry loop) for guaranteed-sync callbacks.
      // span uses regular function to access `arguments` (no ...rest spread allocation)
      // Closes over `self` for calling prototype methods
      // Named function for better stack traces
      if (hasSpan) {
        const self = this;
        const span: SpanFn<Ctx> = function span(
          nameOrLine: string | number,
          _arg1?: unknown,
          _arg2?: unknown,
          _arg3?: unknown,
          _arg4?: unknown,
          _arg5?: unknown,
          _arg6?: unknown,
          _arg7?: unknown,
          _arg8?: unknown,
          _arg9?: unknown,
          _arg10?: unknown,
          _arg11?: unknown,
        ): Promise<Result<unknown, unknown>> {
          // Use arguments.length instead of ...rest to avoid array allocation
          const len = arguments.length;

          // Detect call pattern:
          // - span(name, fn) - len=2, typeof arg0 === 'string'
          // - span(line, name, fn) - len=3, typeof arg0 === 'number'
          // - span(name, overrides, fn) - len=3, typeof arg0 === 'string', typeof arg1 === 'object'
          // - span(line, name, overrides, fn) - len=4, typeof arg0 === 'number', typeof arg2 === 'object'
          // Plus additional args for span arguments

          const hasLine = typeof nameOrLine === 'number';
          const spanLine = hasLine ? nameOrLine : 0;
          const spanName = hasLine ? readStringArgument(arguments, 1, 'span name') : nameOrLine;

          // Index where fn/op or overrides starts
          const checkIdx = hasLine ? 2 : 1;
          const maybeOverrides = arguments[checkIdx];

          // Check if it's an overrides object (not Op, not function)
          const hasOverrides =
            maybeOverrides !== null &&
            typeof maybeOverrides === 'object' &&
            !isOpLike(maybeOverrides) &&
            typeof maybeOverrides !== 'function';

          const fnIdx = checkIdx + (hasOverrides ? 1 : 0);
          const target = resolveSpanTarget<Ctx>(
            arguments[fnIdx],
            self._physicalLayoutPlan,
            self._spanBuffer._opMetadata,
          );
          const childCtx = asSpanContextInstance<Ctx>(
            hasOverrides ? target.callsitePlan.newCtx1(self, maybeOverrides) : target.callsitePlan.newCtx0(self),
          );

          // Dispatch to monomorphic methods with all Op properties extracted
          const argCount = len - fnIdx - 1;
          switch (argCount) {
            case 0:
              return self.span0(spanLine, spanName, childCtx, target.callsitePlan, target.fn);
            case 1:
              return self.span1(spanLine, spanName, childCtx, target.callsitePlan, target.fn, arguments[fnIdx + 1]);
            case 2:
              return self.span2(
                spanLine,
                spanName,
                childCtx,
                target.callsitePlan,
                target.fn,
                arguments[fnIdx + 1],
                arguments[fnIdx + 2],
              );
            case 3:
              return self.span3(
                spanLine,
                spanName,
                childCtx,
                target.callsitePlan,
                target.fn,
                arguments[fnIdx + 1],
                arguments[fnIdx + 2],
                arguments[fnIdx + 3],
              );
            case 4:
              return self.span4(
                spanLine,
                spanName,
                childCtx,
                target.callsitePlan,
                target.fn,
                arguments[fnIdx + 1],
                arguments[fnIdx + 2],
                arguments[fnIdx + 3],
                arguments[fnIdx + 4],
              );
            case 5:
              return self.span5(
                spanLine,
                spanName,
                childCtx,
                target.callsitePlan,
                target.fn,
                arguments[fnIdx + 1],
                arguments[fnIdx + 2],
                arguments[fnIdx + 3],
                arguments[fnIdx + 4],
                arguments[fnIdx + 5],
              );
            case 6:
              return self.span6(
                spanLine,
                spanName,
                childCtx,
                target.callsitePlan,
                target.fn,
                arguments[fnIdx + 1],
                arguments[fnIdx + 2],
                arguments[fnIdx + 3],
                arguments[fnIdx + 4],
                arguments[fnIdx + 5],
                arguments[fnIdx + 6],
              );
            case 7:
              return self.span7(
                spanLine,
                spanName,
                childCtx,
                target.callsitePlan,
                target.fn,
                arguments[fnIdx + 1],
                arguments[fnIdx + 2],
                arguments[fnIdx + 3],
                arguments[fnIdx + 4],
                arguments[fnIdx + 5],
                arguments[fnIdx + 6],
                arguments[fnIdx + 7],
              );
            case 8:
              return self.span8(
                spanLine,
                spanName,
                childCtx,
                target.callsitePlan,
                target.fn,
                arguments[fnIdx + 1],
                arguments[fnIdx + 2],
                arguments[fnIdx + 3],
                arguments[fnIdx + 4],
                arguments[fnIdx + 5],
                arguments[fnIdx + 6],
                arguments[fnIdx + 7],
                arguments[fnIdx + 8],
              );
            default:
              throw new Error(`span() supports up to 8 arguments, got ${argCount}`);
          }
        };
        this.span = span;

        this.spanSync = function spanSync<S, E>(name: string, op: Op<Ctx, [], S, E>): Result<S, E> {
          const callsitePlan = op.callsitePlan;
          const childCtx = asSpanContextInstance<Ctx>(callsitePlan.newCtx0(self));
          return self.spanSync0(0, name, childCtx, callsitePlan, op.fn);
        };
      }
      //#endregion smoo/lmao!n/context-flow-span-promise
    }

    // =========================================================================
    // Getters (on prototype, inherited by children)
    // =========================================================================

    get [SPAN_CONTEXT_MARKER](): true {
      return true;
    }

    get module(): import('./opContext/opTypes.js').OpMetadata {
      return this._spanBuffer._opMetadata;
    }

    get callee_package(): string {
      return this._spanBuffer._opMetadata.package_name;
    }

    get callee_file(): string {
      return this._spanBuffer._opMetadata.package_file;
    }

    get callee_line(): number {
      // Access the line_values array directly to read line number at row 0
      // line_values is a Float64Array per the system schema (LazyNumberSchema)
      return this._spanBuffer.line_values[0];
    }

    get callee_git_sha(): string {
      return this._spanBuffer._opMetadata.git_sha;
    }

    get buffer(): SpanBuffer<Ctx['logSchema']> {
      return this._spanBuffer;
    }

    //#region smoo/lmao!n/scope-attributes.read
    // 01i read-only scope view: the immutable _scopeValues snapshot (setScope is wired in the
    // destructured-context region; the immutable merge is _setScope in spanLoggerGenerator).
    get scope(): Readonly<Partial<InferSchema<Ctx['logSchema']>>> {
      return this._spanBuffer._scopeValues;
    }
    //#endregion smoo/lmao!n/scope-attributes.read

    // =========================================================================
    // Internal methods (on prototype)
    // =========================================================================

    _appendWriterEntry(entryType: number): number {
      if (this._buffer._writeIndex >= this._buffer._capacity) {
        this._buffer = this._buffer.getOrCreateOverflow();
        this._spanLogger._prefillScopedAttributesOn(this._buffer);
      }
      const traceRoot = this._buffer._traceRoot;
      return this._appendLogEntry(traceRoot, this._buffer, entryType);
    }

    /**
     * Create a new child context with prototype chain inheritance.
     * Called by transformer for spans without overrides.
     *
     * Transformer emits: ctx.span0(line, name, ctx._newCtx0(), fn)
     */
    _newCtx0(): SpanContextInstance<Ctx> {
      return asSpanContextInstance<Ctx>(this._physicalLayoutPlan.newCtx0(this));
    }

    /**
     * Create a new child context with prototype chain inheritance + user overrides.
     * Called by transformer for spans with overrides.
     *
     * Transformer emits: ctx.span0(line, name, ctx._newCtx1({ requestId }), fn)
     */
    _newCtx1(overrides: object): SpanContextInstance<Ctx> {
      return asSpanContextInstance<Ctx>(this._physicalLayoutPlan.newCtx1(this, overrides));
    }

    /**
     * Complete child span context setup - creates buffer, registers with parent.
     * Called by span0-span8 after _newCtx0/_newCtx1 has created the prototype chain.
     *
     * Properties are set in HOT→COLD order per specs/lmao/01b1_buffer_performance_optimizations.md
     * to ensure frequently-accessed properties get V8 in-object slots.
     *
     * @param childCtx - Child context created by _newCtx0 or _newCtx1
     * @param line - Source line number (0 if not provided by transformer)
     * @param name - Child span name
     * @param SpanBufferClass - Buffer class to use (Op's class or parent's class for plain functions)
     * @param remapDescriptor - Optional immutable cold-path output mapping
     * @param opMetadata - Metadata for the executing op (Op's metadata or parent's for plain functions)
     */
    //#region smoo/lmao!n/context-flow-child-span
    // 01c "Child Span Creation via span()" / "How span() Works" and 01e "Op's Responsibility:
    // _spanPre creates the child through the planned schema/capacity, attaches the immutable
    // cold-path remap descriptor to the raw buffer, registers it, and writes row 0.
    _spanPre(
      childCtx: SpanContextInstance<Ctx>,
      line: number,
      name: string | number,
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      opMetadata: OpMetadata,
    ): SpanContextInstance<Ctx> {
      const childSchema = callsitePlan.schema;
      const createdBuffer = this._spanBuffer._traceRoot.tracer.bufferStrategy.createChildSpanBuffer(
        this._spanBuffer,
        this._spanBuffer._opMetadata,
        opMetadata,
        callsitePlan.capacityTier,
        childSchema,
        callsitePlan.SpanBufferClass,
      );
      const childBuffer = createdBuffer;
      childBuffer._remapDescriptor = callsitePlan.remapDescriptor ?? undefined;
      callsitePlan.appenders.writeSpanStart(childBuffer, name);
      childBuffer.line(0, line);

      const childFfSource = Object.hasOwn(this, 'ff')
        ? this.ff
        : this._spanBuffer._traceRoot.tracer.getFlagEvaluatorForContext();
      if (!isFlagEvaluatorForContext<Ctx>(childFfSource)) {
        throw new TypeError('Span context requires a context evaluator');
      }
      return new callsitePlan.SpanContextClass(
        childBuffer,
        childSchema,
        callsitePlan,
        childCtx,
        undefined,
        this.deps,
        childFfSource,
      );
    }
    //#endregion smoo/lmao!n/context-flow-child-span

    //#region smoo/lmao!n/lmao-entry-span-lifecycle-entry-types.exception
    /**
     * Handle span exception - writes exception details to row 1.
     * Called in catch blocks of span methods.
     */
    _spanException(childBuffer: SpanBuffer<Ctx['logSchema']>, error: unknown): void {
      writeSpanEndEntry(childBuffer, ENTRY_TYPE_SPAN_EXCEPTION);
      const errorMessage = error instanceof Error ? error.message : String(error);
      const errorStack = error instanceof Error ? error.stack : undefined;
      childBuffer.message(1, errorMessage);
      if (errorStack) {
        childBuffer.exception_stack(1, errorStack);
      }
    }
    //#endregion smoo/lmao!n/lmao-entry-span-lifecycle-entry-types.exception

    private _spanAutoPre<Args extends unknown[], S, E>(
      line: number,
      name: string,
      op: Op<Ctx, Args, S, E>,
    ): SpanContextInstance<Ctx> {
      const child = asSpanContextInstance<Ctx>(op.callsitePlan.newCtx0(this));
      return this._spanPre(child, line, name, op.callsitePlan, op.metadata);
    }

    spanAuto0<S, E>(line: number, name: string, op: Op<Ctx, [], S, E>): Result<S, E> | Promise<Result<S, E>> {
      const ctx = this._spanAutoPre(line, name, op);
      const buffer = ctx._spanBuffer;
      buffer._traceRoot.tracer.onSpanStart(buffer);
      let isAsync = false;
      try {
        const result = op.fn(ctx);
        if (!isSynchronousResult(result) || getRetryableError(result, 1) !== undefined) {
          isAsync = true;
          return this._spanAutoAsync0(ctx, buffer, op, result);
        }
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        if (!isAsync) {
          buffer._traceRoot.tracer.onSpanEnd(buffer);
        }
      }
    }

    private async _spanAutoAsync0<S, E>(
      ctx: SpanContextInstance<Ctx>,
      buffer: SpanBuffer<Ctx['logSchema']>,
      op: Op<Ctx, [], S, E>,
      first: Result<S, E> | PromiseLike<Result<S, E>>,
    ): Promise<Result<S, E>> {
      try {
        let attempt = 1;
        let result = await first;
        while (true) {
          const retryError = getRetryableError(result, attempt);
          if (!retryError) break;
          const delay = calculateDelay(retryError.policy, attempt);
          writeRetryEntry(buffer, attempt, retryError, delay);
          await sleep(delay);
          attempt++;
          result = await op.fn(ctx);
        }
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        buffer._traceRoot.tracer.onSpanEnd(buffer);
      }
    }

    spanAuto1<S, E, A1>(
      line: number,
      name: string,
      op: Op<Ctx, [A1], S, E>,
      a1: A1,
    ): Result<S, E> | Promise<Result<S, E>> {
      const ctx = this._spanAutoPre(line, name, op);
      const buffer = ctx._spanBuffer;
      buffer._traceRoot.tracer.onSpanStart(buffer);
      let isAsync = false;
      try {
        const result = op.fn(ctx, a1);
        if (!isSynchronousResult(result) || getRetryableError(result, 1) !== undefined) {
          isAsync = true;
          return this._spanAutoAsync1(ctx, buffer, op, result, a1);
        }
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        if (!isAsync) {
          buffer._traceRoot.tracer.onSpanEnd(buffer);
        }
      }
    }

    private async _spanAutoAsync1<S, E, A1>(
      ctx: SpanContextInstance<Ctx>,
      buffer: SpanBuffer<Ctx['logSchema']>,
      op: Op<Ctx, [A1], S, E>,
      first: Result<S, E> | PromiseLike<Result<S, E>>,
      a1: A1,
    ): Promise<Result<S, E>> {
      try {
        let attempt = 1;
        let result = await first;
        while (true) {
          const retryError = getRetryableError(result, attempt);
          if (!retryError) break;
          const delay = calculateDelay(retryError.policy, attempt);
          writeRetryEntry(buffer, attempt, retryError, delay);
          await sleep(delay);
          attempt++;
          result = await op.fn(ctx, a1);
        }
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        buffer._traceRoot.tracer.onSpanEnd(buffer);
      }
    }

    spanAuto2<S, E, A1, A2>(
      line: number,
      name: string,
      op: Op<Ctx, [A1, A2], S, E>,
      a1: A1,
      a2: A2,
    ): Result<S, E> | Promise<Result<S, E>> {
      const ctx = this._spanAutoPre(line, name, op);
      const buffer = ctx._spanBuffer;
      buffer._traceRoot.tracer.onSpanStart(buffer);
      let isAsync = false;
      try {
        const result = op.fn(ctx, a1, a2);
        if (!isSynchronousResult(result) || getRetryableError(result, 1) !== undefined) {
          isAsync = true;
          return this._spanAutoAsync2(ctx, buffer, op, result, a1, a2);
        }
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        if (!isAsync) {
          buffer._traceRoot.tracer.onSpanEnd(buffer);
        }
      }
    }

    private async _spanAutoAsync2<S, E, A1, A2>(
      ctx: SpanContextInstance<Ctx>,
      buffer: SpanBuffer<Ctx['logSchema']>,
      op: Op<Ctx, [A1, A2], S, E>,
      first: Result<S, E> | PromiseLike<Result<S, E>>,
      a1: A1,
      a2: A2,
    ): Promise<Result<S, E>> {
      try {
        let attempt = 1;
        let result = await first;
        while (true) {
          const retryError = getRetryableError(result, attempt);
          if (!retryError) break;
          const delay = calculateDelay(retryError.policy, attempt);
          writeRetryEntry(buffer, attempt, retryError, delay);
          await sleep(delay);
          attempt++;
          result = await op.fn(ctx, a1, a2);
        }
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        buffer._traceRoot.tracer.onSpanEnd(buffer);
      }
    }

    spanAuto3<S, E, A1, A2, A3>(
      line: number,
      name: string,
      op: Op<Ctx, [A1, A2, A3], S, E>,
      a1: A1,
      a2: A2,
      a3: A3,
    ): Result<S, E> | Promise<Result<S, E>> {
      const ctx = this._spanAutoPre(line, name, op);
      const buffer = ctx._spanBuffer;
      buffer._traceRoot.tracer.onSpanStart(buffer);
      let isAsync = false;
      try {
        const result = op.fn(ctx, a1, a2, a3);
        if (!isSynchronousResult(result) || getRetryableError(result, 1) !== undefined) {
          isAsync = true;
          return this._spanAutoAsync3(ctx, buffer, op, result, a1, a2, a3);
        }
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        if (!isAsync) {
          buffer._traceRoot.tracer.onSpanEnd(buffer);
        }
      }
    }

    private async _spanAutoAsync3<S, E, A1, A2, A3>(
      ctx: SpanContextInstance<Ctx>,
      buffer: SpanBuffer<Ctx['logSchema']>,
      op: Op<Ctx, [A1, A2, A3], S, E>,
      first: Result<S, E> | PromiseLike<Result<S, E>>,
      a1: A1,
      a2: A2,
      a3: A3,
    ): Promise<Result<S, E>> {
      try {
        let attempt = 1;
        let result = await first;
        while (true) {
          const retryError = getRetryableError(result, attempt);
          if (!retryError) break;
          const delay = calculateDelay(retryError.policy, attempt);
          writeRetryEntry(buffer, attempt, retryError, delay);
          await sleep(delay);
          attempt++;
          result = await op.fn(ctx, a1, a2, a3);
        }
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        buffer._traceRoot.tracer.onSpanEnd(buffer);
      }
    }

    spanAuto4<S, E, A1, A2, A3, A4>(
      line: number,
      name: string,
      op: Op<Ctx, [A1, A2, A3, A4], S, E>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
    ): Result<S, E> | Promise<Result<S, E>> {
      const ctx = this._spanAutoPre(line, name, op);
      const buffer = ctx._spanBuffer;
      buffer._traceRoot.tracer.onSpanStart(buffer);
      let isAsync = false;
      try {
        const result = op.fn(ctx, a1, a2, a3, a4);
        if (!isSynchronousResult(result) || getRetryableError(result, 1) !== undefined) {
          isAsync = true;
          return this._spanAutoAsync4(ctx, buffer, op, result, a1, a2, a3, a4);
        }
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        if (!isAsync) {
          buffer._traceRoot.tracer.onSpanEnd(buffer);
        }
      }
    }

    private async _spanAutoAsync4<S, E, A1, A2, A3, A4>(
      ctx: SpanContextInstance<Ctx>,
      buffer: SpanBuffer<Ctx['logSchema']>,
      op: Op<Ctx, [A1, A2, A3, A4], S, E>,
      first: Result<S, E> | PromiseLike<Result<S, E>>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
    ): Promise<Result<S, E>> {
      try {
        let attempt = 1;
        let result = await first;
        while (true) {
          const retryError = getRetryableError(result, attempt);
          if (!retryError) break;
          const delay = calculateDelay(retryError.policy, attempt);
          writeRetryEntry(buffer, attempt, retryError, delay);
          await sleep(delay);
          attempt++;
          result = await op.fn(ctx, a1, a2, a3, a4);
        }
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        buffer._traceRoot.tracer.onSpanEnd(buffer);
      }
    }

    spanAuto5<S, E, A1, A2, A3, A4, A5>(
      line: number,
      name: string,
      op: Op<Ctx, [A1, A2, A3, A4, A5], S, E>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
      a5: A5,
    ): Result<S, E> | Promise<Result<S, E>> {
      const ctx = this._spanAutoPre(line, name, op);
      const buffer = ctx._spanBuffer;
      buffer._traceRoot.tracer.onSpanStart(buffer);
      let isAsync = false;
      try {
        const result = op.fn(ctx, a1, a2, a3, a4, a5);
        if (!isSynchronousResult(result) || getRetryableError(result, 1) !== undefined) {
          isAsync = true;
          return this._spanAutoAsync5(ctx, buffer, op, result, a1, a2, a3, a4, a5);
        }
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        if (!isAsync) {
          buffer._traceRoot.tracer.onSpanEnd(buffer);
        }
      }
    }

    private async _spanAutoAsync5<S, E, A1, A2, A3, A4, A5>(
      ctx: SpanContextInstance<Ctx>,
      buffer: SpanBuffer<Ctx['logSchema']>,
      op: Op<Ctx, [A1, A2, A3, A4, A5], S, E>,
      first: Result<S, E> | PromiseLike<Result<S, E>>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
      a5: A5,
    ): Promise<Result<S, E>> {
      try {
        let attempt = 1;
        let result = await first;
        while (true) {
          const retryError = getRetryableError(result, attempt);
          if (!retryError) break;
          const delay = calculateDelay(retryError.policy, attempt);
          writeRetryEntry(buffer, attempt, retryError, delay);
          await sleep(delay);
          attempt++;
          result = await op.fn(ctx, a1, a2, a3, a4, a5);
        }
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        buffer._traceRoot.tracer.onSpanEnd(buffer);
      }
    }

    spanAuto6<S, E, A1, A2, A3, A4, A5, A6>(
      line: number,
      name: string,
      op: Op<Ctx, [A1, A2, A3, A4, A5, A6], S, E>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
      a5: A5,
      a6: A6,
    ): Result<S, E> | Promise<Result<S, E>> {
      const ctx = this._spanAutoPre(line, name, op);
      const buffer = ctx._spanBuffer;
      buffer._traceRoot.tracer.onSpanStart(buffer);
      let isAsync = false;
      try {
        const result = op.fn(ctx, a1, a2, a3, a4, a5, a6);
        if (!isSynchronousResult(result) || getRetryableError(result, 1) !== undefined) {
          isAsync = true;
          return this._spanAutoAsync6(ctx, buffer, op, result, a1, a2, a3, a4, a5, a6);
        }
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        if (!isAsync) {
          buffer._traceRoot.tracer.onSpanEnd(buffer);
        }
      }
    }

    private async _spanAutoAsync6<S, E, A1, A2, A3, A4, A5, A6>(
      ctx: SpanContextInstance<Ctx>,
      buffer: SpanBuffer<Ctx['logSchema']>,
      op: Op<Ctx, [A1, A2, A3, A4, A5, A6], S, E>,
      first: Result<S, E> | PromiseLike<Result<S, E>>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
      a5: A5,
      a6: A6,
    ): Promise<Result<S, E>> {
      try {
        let attempt = 1;
        let result = await first;
        while (true) {
          const retryError = getRetryableError(result, attempt);
          if (!retryError) break;
          const delay = calculateDelay(retryError.policy, attempt);
          writeRetryEntry(buffer, attempt, retryError, delay);
          await sleep(delay);
          attempt++;
          result = await op.fn(ctx, a1, a2, a3, a4, a5, a6);
        }
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        buffer._traceRoot.tracer.onSpanEnd(buffer);
      }
    }

    spanAuto7<S, E, A1, A2, A3, A4, A5, A6, A7>(
      line: number,
      name: string,
      op: Op<Ctx, [A1, A2, A3, A4, A5, A6, A7], S, E>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
      a5: A5,
      a6: A6,
      a7: A7,
    ): Result<S, E> | Promise<Result<S, E>> {
      const ctx = this._spanAutoPre(line, name, op);
      const buffer = ctx._spanBuffer;
      buffer._traceRoot.tracer.onSpanStart(buffer);
      let isAsync = false;
      try {
        const result = op.fn(ctx, a1, a2, a3, a4, a5, a6, a7);
        if (!isSynchronousResult(result) || getRetryableError(result, 1) !== undefined) {
          isAsync = true;
          return this._spanAutoAsync7(ctx, buffer, op, result, a1, a2, a3, a4, a5, a6, a7);
        }
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        if (!isAsync) {
          buffer._traceRoot.tracer.onSpanEnd(buffer);
        }
      }
    }

    private async _spanAutoAsync7<S, E, A1, A2, A3, A4, A5, A6, A7>(
      ctx: SpanContextInstance<Ctx>,
      buffer: SpanBuffer<Ctx['logSchema']>,
      op: Op<Ctx, [A1, A2, A3, A4, A5, A6, A7], S, E>,
      first: Result<S, E> | PromiseLike<Result<S, E>>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
      a5: A5,
      a6: A6,
      a7: A7,
    ): Promise<Result<S, E>> {
      try {
        let attempt = 1;
        let result = await first;
        while (true) {
          const retryError = getRetryableError(result, attempt);
          if (!retryError) break;
          const delay = calculateDelay(retryError.policy, attempt);
          writeRetryEntry(buffer, attempt, retryError, delay);
          await sleep(delay);
          attempt++;
          result = await op.fn(ctx, a1, a2, a3, a4, a5, a6, a7);
        }
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        buffer._traceRoot.tracer.onSpanEnd(buffer);
      }
    }

    spanAuto8<S, E, A1, A2, A3, A4, A5, A6, A7, A8>(
      line: number,
      name: string,
      op: Op<Ctx, [A1, A2, A3, A4, A5, A6, A7, A8], S, E>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
      a5: A5,
      a6: A6,
      a7: A7,
      a8: A8,
    ): Result<S, E> | Promise<Result<S, E>> {
      const ctx = this._spanAutoPre(line, name, op);
      const buffer = ctx._spanBuffer;
      buffer._traceRoot.tracer.onSpanStart(buffer);
      let isAsync = false;
      try {
        const result = op.fn(ctx, a1, a2, a3, a4, a5, a6, a7, a8);
        if (!isSynchronousResult(result) || getRetryableError(result, 1) !== undefined) {
          isAsync = true;
          return this._spanAutoAsync8(ctx, buffer, op, result, a1, a2, a3, a4, a5, a6, a7, a8);
        }
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        if (!isAsync) {
          buffer._traceRoot.tracer.onSpanEnd(buffer);
        }
      }
    }

    private async _spanAutoAsync8<S, E, A1, A2, A3, A4, A5, A6, A7, A8>(
      ctx: SpanContextInstance<Ctx>,
      buffer: SpanBuffer<Ctx['logSchema']>,
      op: Op<Ctx, [A1, A2, A3, A4, A5, A6, A7, A8], S, E>,
      first: Result<S, E> | PromiseLike<Result<S, E>>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
      a5: A5,
      a6: A6,
      a7: A7,
      a8: A8,
    ): Promise<Result<S, E>> {
      try {
        let attempt = 1;
        let result = await first;
        while (true) {
          const retryError = getRetryableError(result, attempt);
          if (!retryError) break;
          const delay = calculateDelay(retryError.policy, attempt);
          writeRetryEntry(buffer, attempt, retryError, delay);
          await sleep(delay);
          attempt++;
          result = await op.fn(ctx, a1, a2, a3, a4, a5, a6, a7, a8);
        }
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        buffer._traceRoot.tracer.onSpanEnd(buffer);
      }
    }

    // =========================================================================
    // Monomorphic span methods (for transformer output)
    // Transformer emits: ctx.span0(line, name, plan.newCtx0(ctx), plan, fn)
    // The frozen plan supplies the exact context source factory for each callsite.
    // User overrides are materialized by plan.newCtx1 without prototype allocation.
    // =========================================================================

    spanSync0<S, E>(
      line: number,
      name: string | number,
      childCtx: SpanContextInstance<Ctx>,
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      fn: (ctx: SpanContext<Ctx>) => Result<S, E> | Promise<Result<S, E>>,
    ): Result<S, E> {
      const ctx = this._spanPre(childCtx, line, name, callsitePlan, callsitePlan.metadata);
      const buffer = ctx._spanBuffer;
      buffer._traceRoot.tracer.onSpanStart(buffer);
      try {
        const result = fn(ctx);
        if (!isSynchronousResult(result)) {
          throw new TypeError('spanSync operation returned a Promise');
        }
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        buffer._traceRoot.tracer.onSpanEnd(buffer);
      }
    }

    async span0<S, E>(
      line: number,
      name: string | number,
      childCtx: SpanContextInstance<Ctx>,
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      fn: (ctx: SpanContext<Ctx>) => Result<S, E> | Promise<Result<S, E>>,
    ): Promise<Result<S, E>> {
      const ctx = this._spanPre(childCtx, line, name, callsitePlan, callsitePlan.metadata);
      const buffer = ctx._spanBuffer;
      buffer._traceRoot.tracer.onSpanStart(buffer);
      try {
        // Execute with retry loop - TransientError triggers retry, BlockedError returns immediately
        const result = await executeWithRetry0(buffer, fn, ctx);
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        buffer._traceRoot.tracer.onSpanEnd(buffer);
      }
    }

    async span1<S, E, A1>(
      line: number,
      name: string | number,
      childCtx: SpanContextInstance<Ctx>,
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      fn: (ctx: SpanContext<Ctx>, a1: A1) => Result<S, E> | Promise<Result<S, E>>,
      a1: A1,
    ): Promise<Result<S, E>> {
      const ctx = this._spanPre(childCtx, line, name, callsitePlan, callsitePlan.metadata);
      const buffer = ctx._spanBuffer;
      buffer._traceRoot.tracer.onSpanStart(buffer);
      try {
        // Execute with retry loop - TransientError triggers retry, BlockedError returns immediately
        const result = await executeWithRetry1(buffer, fn, ctx, a1);
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        buffer._traceRoot.tracer.onSpanEnd(buffer);
      }
    }

    async span2<S, E, A1, A2>(
      line: number,
      name: string | number,
      childCtx: SpanContextInstance<Ctx>,
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      fn: (ctx: SpanContext<Ctx>, a1: A1, a2: A2) => Result<S, E> | Promise<Result<S, E>>,
      a1: A1,
      a2: A2,
    ): Promise<Result<S, E>> {
      const ctx = this._spanPre(childCtx, line, name, callsitePlan, callsitePlan.metadata);
      const buffer = ctx._spanBuffer;
      buffer._traceRoot.tracer.onSpanStart(buffer);
      try {
        // Execute with retry loop - TransientError triggers retry, BlockedError returns immediately
        const result = await executeWithRetry2(buffer, fn, ctx, a1, a2);
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        buffer._traceRoot.tracer.onSpanEnd(buffer);
      }
    }

    async span3<S, E, A1, A2, A3>(
      line: number,
      name: string | number,
      childCtx: SpanContextInstance<Ctx>,
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      fn: (ctx: SpanContext<Ctx>, a1: A1, a2: A2, a3: A3) => Result<S, E> | Promise<Result<S, E>>,
      a1: A1,
      a2: A2,
      a3: A3,
    ): Promise<Result<S, E>> {
      const ctx = this._spanPre(childCtx, line, name, callsitePlan, callsitePlan.metadata);
      const buffer = ctx._spanBuffer;
      buffer._traceRoot.tracer.onSpanStart(buffer);
      try {
        // Execute with retry loop - TransientError triggers retry, BlockedError returns immediately
        const result = await executeWithRetry3(buffer, fn, ctx, a1, a2, a3);
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        buffer._traceRoot.tracer.onSpanEnd(buffer);
      }
    }

    async span4<S, E, A1, A2, A3, A4>(
      line: number,
      name: string | number,
      childCtx: SpanContextInstance<Ctx>,
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      fn: (ctx: SpanContext<Ctx>, a1: A1, a2: A2, a3: A3, a4: A4) => Result<S, E> | Promise<Result<S, E>>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
    ): Promise<Result<S, E>> {
      const ctx = this._spanPre(childCtx, line, name, callsitePlan, callsitePlan.metadata);
      const buffer = ctx._spanBuffer;
      buffer._traceRoot.tracer.onSpanStart(buffer);
      try {
        // Execute with retry loop - TransientError triggers retry, BlockedError returns immediately
        const result = await executeWithRetry4(buffer, fn, ctx, a1, a2, a3, a4);
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        buffer._traceRoot.tracer.onSpanEnd(buffer);
      }
    }

    async span5<S, E, A1, A2, A3, A4, A5>(
      line: number,
      name: string | number,
      childCtx: SpanContextInstance<Ctx>,
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      fn: (ctx: SpanContext<Ctx>, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5) => Result<S, E> | Promise<Result<S, E>>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
      a5: A5,
    ): Promise<Result<S, E>> {
      const ctx = this._spanPre(childCtx, line, name, callsitePlan, callsitePlan.metadata);
      const buffer = ctx._spanBuffer;
      buffer._traceRoot.tracer.onSpanStart(buffer);
      try {
        // Execute with retry loop - TransientError triggers retry, BlockedError returns immediately
        const result = await executeWithRetry5(buffer, fn, ctx, a1, a2, a3, a4, a5);
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        buffer._traceRoot.tracer.onSpanEnd(buffer);
      }
    }

    async span6<S, E, A1, A2, A3, A4, A5, A6>(
      line: number,
      name: string | number,
      childCtx: SpanContextInstance<Ctx>,
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      fn: (
        ctx: SpanContext<Ctx>,
        a1: A1,
        a2: A2,
        a3: A3,
        a4: A4,
        a5: A5,
        a6: A6,
      ) => Result<S, E> | Promise<Result<S, E>>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
      a5: A5,
      a6: A6,
    ): Promise<Result<S, E>> {
      const ctx = this._spanPre(childCtx, line, name, callsitePlan, callsitePlan.metadata);
      const buffer = ctx._spanBuffer;
      buffer._traceRoot.tracer.onSpanStart(buffer);
      try {
        // Execute with retry loop - TransientError triggers retry, BlockedError returns immediately
        const result = await executeWithRetry6(buffer, fn, ctx, a1, a2, a3, a4, a5, a6);
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        buffer._traceRoot.tracer.onSpanEnd(buffer);
      }
    }

    async span7<S, E, A1, A2, A3, A4, A5, A6, A7>(
      line: number,
      name: string | number,
      childCtx: SpanContextInstance<Ctx>,
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      fn: (
        ctx: SpanContext<Ctx>,
        a1: A1,
        a2: A2,
        a3: A3,
        a4: A4,
        a5: A5,
        a6: A6,
        a7: A7,
      ) => Result<S, E> | Promise<Result<S, E>>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
      a5: A5,
      a6: A6,
      a7: A7,
    ): Promise<Result<S, E>> {
      const ctx = this._spanPre(childCtx, line, name, callsitePlan, callsitePlan.metadata);
      const buffer = ctx._spanBuffer;
      buffer._traceRoot.tracer.onSpanStart(buffer);
      try {
        // Execute with retry loop - TransientError triggers retry, BlockedError returns immediately
        const result = await executeWithRetry7(buffer, fn, ctx, a1, a2, a3, a4, a5, a6, a7);
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        buffer._traceRoot.tracer.onSpanEnd(buffer);
      }
    }

    async span8<S, E, A1, A2, A3, A4, A5, A6, A7, A8>(
      line: number,
      name: string | number,
      childCtx: SpanContextInstance<Ctx>,
      callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>,
      fn: (
        ctx: SpanContext<Ctx>,
        a1: A1,
        a2: A2,
        a3: A3,
        a4: A4,
        a5: A5,
        a6: A6,
        a7: A7,
        a8: A8,
      ) => Result<S, E, Ctx['logSchema']> | Promise<Result<S, E, Ctx['logSchema']>>,
      a1: A1,
      a2: A2,
      a3: A3,
      a4: A4,
      a5: A5,
      a6: A6,
      a7: A7,
      a8: A8,
    ): Promise<Result<S, E, Ctx['logSchema']>> {
      const ctx = this._spanPre(childCtx, line, name, callsitePlan, callsitePlan.metadata);
      const buffer = ctx._spanBuffer;
      buffer._traceRoot.tracer.onSpanStart(buffer);
      try {
        // Execute with retry loop - TransientError triggers retry, BlockedError returns immediately
        const result = await executeWithRetry8(buffer, fn, ctx, a1, a2, a3, a4, a5, a6, a7, a8);
        writeSpanEnd(buffer, result);
        return result;
      } catch (error) {
        this._spanException(buffer, error);
        throw error;
      } finally {
        buffer._traceRoot.tracer.onSpanEnd(buffer);
      }
    }
  }

  const SpanContextCtor: SpanContextClass<Ctx> = SpanContextImpl;
  cache.set(cacheKey, SpanContextCtor);
  return SpanContextCtor;
}

// =============================================================================
// Type Guard
// =============================================================================

/**
 * Type guard to check if a value is a SpanContext
 */
export function isSpanContext<Ctx extends OpContext>(value: unknown): value is SpanContext<Ctx> {
  return (
    typeof value === 'object' &&
    value !== null &&
    SPAN_CONTEXT_MARKER in value &&
    Reflect.get(value, SPAN_CONTEXT_MARKER) === true
  );
}

// =============================================================================
// Re-export public types from opContext/types
// =============================================================================

export type { FluentLogEntry, SpanContext, SpanFn, SpanLogger } from './opContext/types.js';
