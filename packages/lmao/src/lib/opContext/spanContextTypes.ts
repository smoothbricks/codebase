/**
 * SpanContext Types for Op-Centric API
 *
 * Core types for span-level operations within the trace logging system.
 * These types define the context passed to Op functions for:
 * - Tag writing (row 0 attributes)
 * - Structured logging (rows 2+)
 * - Result handling (ok/err)
 * - Child span creation
 * - Feature flag access
 * - Dependency access
 *
 * ## Dependency Layer
 * This module is at Layer 2 in the type hierarchy:
 * - Depends on: result (Layer 1), types (Layer 3), featureFlagTypes (Layer 1)
 * - Depended on by: opTypes (Layer 3)
 *
 * SpanFn now uses Op<Ctx, Args, S, E> directly (no structural typing).
 */

import type { FluentLogEntry as FluentLogEntryFromGenerator } from '../codegen/spanLoggerGenerator.js';
import type { FluentErr, FluentOk, Result } from '../result.js';
import type { FeatureFlagEvaluator, InferFeatureFlagsWithContext } from '../schema/evaluator.js';
import type { LogSchema } from '../schema/LogSchema.js';
import type { InferSchema } from '../schema/types.js';
import type { SpanBuffer } from '../types.js';
import type { Op } from './opTypes.js';
import type { OpContext } from './types.js';

// =============================================================================
// DEPENDENCY TYPES (structural to avoid circular imports)
// =============================================================================

/**
 * DepsConfig - structural type to avoid importing from opGroupTypes.
 * Represents a record of dependency groups.
 */
// biome-ignore lint/suspicious/noExplicitAny: Structural placeholder - actual type in opGroupTypes
export type DepsConfig = Record<string, any>;

/**
 * ResolvedDeps - structural type to avoid importing from opGroupTypes.
 * At runtime, deps are resolved OpGroups with their ops accessible.
 */
// biome-ignore lint/suspicious/noExplicitAny: Structural placeholder - actual type in opGroupTypes
export type ResolvedDeps<_D extends DepsConfig> = Record<string, any>;

// =============================================================================
// TAG & LOG WRITER TYPES
// =============================================================================

/**
 * Tag writer for setting span attributes (row 0)
 * Generated at runtime with typed methods per schema field.
 *
 * Per specs/01h_entry_types_and_logging_primitives.md:
 * - Writes to row 0 (fixed layout for span-start attributes)
 * - All methods return TagWriter<T> for fluent chaining
 * - Generated at runtime via createTagWriter()
 */
export type TagWriter<T extends LogSchema> = {
  [K in keyof InferSchema<T>]: (value: InferSchema<T>[K]) => TagWriter<T>;
} & {
  /** Set multiple attributes at once */
  with(attributes: Partial<InferSchema<T>>): TagWriter<T>;
};

/**
 * SpanLogger - public API surface for structured logging.
 *
 * Includes core methods (info/debug/warn/error) from BaseSpanLogger
 * and schema-specific methods from ColumnWriter.
 * Internal methods (_setScope, _buffer, etc.) are hidden from this type.
 *
 * Per specs/01h_entry_types_and_logging_primitives.md:
 * - Each level (info/debug/warn/error) creates a log entry
 * - Returns FluentLogEntry for chaining field setters
 * - Generated at runtime via createSpanLoggerFromGenerator()
 */
export interface SpanLogger<T extends LogSchema> {
  /** Log at info level */
  info(message: string): FluentLogEntry<T>;
  /** Log at debug level */
  debug(message: string): FluentLogEntry<T>;
  /** Log at warn level */
  warn(message: string): FluentLogEntry<T>;
  /** Log at error level */
  error(message: string): FluentLogEntry<T>;
}

/**
 * Fluent log entry - chainable attribute setters after log level.
 *
 * Re-exported from spanLoggerGenerator.ts to avoid circular dependencies.
 * The implementation in spanLoggerGenerator.ts is the source of truth.
 *
 * Allows setting attributes after choosing a log level:
 * ctx.log.info('message').userId('u1').requestId('r1')
 *
 * All attributes are optional, chainable, and return FluentLogEntry for continuation.
 *
 * Includes system schema fields for direct access (no information hiding):
 * - line(n) - Source line number (0-65535)
 * - error_code(code) - Error code string
 * - exception_stack(stack) - Exception stack trace
 * - ff_value(value) - Feature flag value
 * - uint64_value(value) - BigInt value
 *
 * Per specs/01c_context_flow_and_op_wrappers.md "Line Number System":
 * - TypeScript transformer injects line() calls at compile time
 * - No runtime overhead - just a method call with literal number
 */
export type FluentLogEntry<T extends LogSchema> = FluentLogEntryFromGenerator<T>;

// =============================================================================
// SPAN FUNCTION TYPES
// =============================================================================

/**
 * SpanFn - overloaded span creation function.
 *
 * Per specs/01l_module_builder_pattern.md lines 474-511 and 01o lines 34-72:
 * Creates child spans with type-safe Op invocation or inline closures, with/without line numbers.
 *
 * The transformer injects line numbers at compile time, but runtime supports both patterns for fallback.
 * Uses Op<Ctx, Args, S, E> and returns Result<S, E> (no generic R).
 */
export type SpanFn<Ctx extends OpContext> = {
  /**
   * Create child span with line number and Op invocation.
   *
   * Per spec 01o lines 46-49: Monomorphic span_op for transformer-injected line numbers.
   *
   * @param line - Source code line number (injected by transformer at compile time)
   * @param name - Child span name (overrides Op's default name)
   * @param op - Op to invoke (type-safe with Args, Success, Error types)
   * @param args - Arguments to pass to Op function
   * @returns Promise resolving to Result<S, E>
   *
   * @example
   * const result = await ctx.span(42, 'fetch-user', fetchUserOp, userId);
   */
  <S, E, Args extends unknown[]>(
    line: number,
    name: string,
    op: Op<Ctx, Args, S, E>,
    ...args: Args
  ): Promise<Result<S, E>>;

  /**
   * Create child span with line number and inline closure.
   *
   * Per spec 01o lines 51-53: Monomorphic span_fn for transformer-injected line numbers.
   *
   * @param line - Source code line number (injected by transformer at compile time)
   * @param name - Child span name
   * @param fn - Async function to execute in child span (receives SpanContext<Ctx>)
   * @returns Promise resolving to Result<S, E>
   *
   * @example
   * const result = await ctx.span(42, 'validate', async (childCtx) => {
   *   childCtx.tag.step('validation');
   *   return childCtx.ok({ valid: true });
   * });
   */
  <S, E>(
    line: number,
    name: string,
    fn: (ctx: SpanContext<Ctx>) => Result<S, E> | Promise<Result<S, E>>,
  ): Promise<Result<S, E>>;

  /**
   * Create child span with Op invocation (most common - no line number).
   *
   * Per spec 01o lines 55-71: Polymorphic span dispatcher for fallback when transformer didn't inject line number.
   *
   * @param name - Child span name (overrides Op's default name)
   * @param op - Op to invoke (type-safe with Args, Success, Error types)
   * @param args - Arguments to pass to Op function
   * @returns Promise resolving to Result<S, E>
   *
   * @example
   * const result = await ctx.span('fetch-user', fetchUserOp, userId);
   */
  <S, E, Args extends unknown[]>(name: string, op: Op<Ctx, Args, S, E>, ...args: Args): Promise<Result<S, E>>;

  /**
   * Create child span with inline closure (most common - no line number).
   *
   * Per spec 01o lines 55-71: Polymorphic span dispatcher for fallback when transformer didn't inject line number.
   *
   * @param name - Child span name
   * @param fn - Async function to execute in child span (receives SpanContext<Ctx>)
   * @returns Promise resolving to Result<S, E>
   *
   * @example
   * const result = await ctx.span('validate', async (childCtx) => {
   *   childCtx.tag.step('validation');
   *   return childCtx.ok({ valid: true });
   * });
   */
  <S, E>(name: string, fn: (ctx: SpanContext<Ctx>) => Result<S, E> | Promise<Result<S, E>>): Promise<Result<S, E>>;
};

// =============================================================================
// SPAN CONTEXT TYPE
// =============================================================================

/**
 * SpanContext - what op functions receive.
 *
 * Per specs/01c_context_flow_and_op_wrappers.md and 01l:
 * - Passed to Op functions as first argument
 * - Combines built-in properties with user-extensible UserCtx via intersection
 * - Child spans inherit scoped attributes from parent (frozen copy for safety)
 * - User properties from Ctx['userCtx'] are spread via intersection at end
 *
 * @template Ctx - Bundled context (logSchema, flags, deps, userCtx)
 *
 * @example
 * ```typescript
 * const createUser = op('create-user', async (ctx, userData) => {
 *   ctx.setScope({ userId: userData.id });
 *   ctx.tag.operation('INSERT');
 *   ctx.log.info('Creating user').line(42);
 *   return ctx.ok(user);
 * });
 * ```
 */
export type SpanContext<Ctx extends OpContext> = {
  /**
   * The underlying SpanBuffer for this context.
   *
   * Useful for Arrow table conversion after task completion.
   * The buffer contains all trace data written during this span's execution.
   *
   * @example
   * ```typescript
   * const result = await myTask(requestCtx, args);
   * const table = convertToArrowTable(ctx.buffer);
   * ```
   */
  buffer: SpanBuffer<Ctx['logSchema']>;

  /**
   * Tag writer for span attributes (row 0).
   *
   * Per specs/01h_entry_types_and_logging_primitives.md:
   * - Writes to row 0 (fixed layout for span-start attributes)
   * - All methods return TagWriter for fluent chaining
   *
   * @example
   * ctx.tag.userId('u1').requestId('r1').operation('INSERT');
   */
  tag: TagWriter<Ctx['logSchema']>;

  /**
   * Structured logger (rows 2+).
   *
   * Per specs/01h_entry_types_and_logging_primitives.md:
   * - info/debug/warn/error create log entries
   * - Returns FluentLogEntry for chaining field setters
   * - Entries are appended after row 1
   *
   * @example
   * ctx.log.info('Processing request').userId('u1').line(42);
   */
  log: SpanLogger<Ctx['logSchema']>;

  /**
   * Feature flags (logs access to current span).
   *
   * Per specs/01p_feature_flags.md:
   * - Access via ff.flagName() or ff['flagName']()
   * - Logs flag access to span automatically
   * - Type-safe evaluation with context
   */
  ff: FeatureFlagEvaluator<Ctx> & InferFeatureFlagsWithContext<Ctx>;

  /**
   * Dependencies - can be destructured!
   *
   * Per spec 01l lines 757-758:
   * - Plain object references to dependency modules/ops
   * - Zero-allocation deps (not closures)
   *
   * @example
   * const { retry, auth } = deps;
   * await span('retry', retry, 1);
   */
  deps: ResolvedDeps<Ctx['deps']>;

  /**
   * Create a success result with optional attributes.
   *
   * Writes span-ok entry to row 1 (span-end).
   * Supports fluent chaining with .with() and .message().
   *
   * @param value - The success value
   * @returns Fluent result builder
   *
   * @example
   * return ctx.ok(user).with({ userId: user.id });
   */
  ok<S>(value: S): FluentOk<S, Ctx['logSchema']>;

  /**
   * Create an error result with optional attributes.
   *
   * Writes span-err entry to row 1 (span-end).
   * Supports fluent chaining with .with() and .message().
   *
   * @param code - Error code string
   * @param details - Error details
   * @returns Fluent result builder
   *
   * @example
   * return ctx.err('NOT_FOUND', { userId }).message('User not found');
   */
  err<E>(code: string, details: E): FluentErr<E, Ctx['logSchema']>;

  /**
   * Create a child span with its own buffer.
   *
   * Child spans inherit scoped attributes from the parent.
   * The child function receives a new SpanContext.
   *
   * Per specs/01i_span_scope_attributes.md:
   * - Child inherits parent's scope by reference (safe because immutable)
   * - Child can override scope values with setScope()
   * - Scope is frozen at each level to prevent race conditions
   *
   * @example
   * const result = await ctx.span('validate', async (childCtx) => {
   *   childCtx.tag.step('validation');
   *   return childCtx.ok({ valid: true });
   * });
   */
  span: SpanFn<Ctx>;

  /**
   * Set scoped attributes that auto-propagate to all subsequent log entries.
   *
   * Scoped attributes are automatically included in all log entries
   * and inherited by child spans. Uses merge semantics - pass null to clear a value.
   *
   * Per specs/01i_span_scope_attributes.md:
   * - Creates a NEW frozen object (never mutates existing scope)
   * - setScope({ x: null }) removes x from scope
   * - Child spans get frozen snapshot at creation (async-safe)
   *
   * @param attributes - Attributes to scope to this span (null clears a value)
   *
   * @example
   * ctx.setScope({ requestId: req.id, userId: req.user?.id });
   * ctx.log.info('Processing'); // Includes requestId and userId
   * ctx.setScope({ userId: null }); // Clear userId from scope
   */
  setScope(values: Partial<InferSchema<Ctx['logSchema']>> | null): void;

  /**
   * Read-only view of current scoped attributes.
   *
   * Returns the frozen scope object containing all currently scoped values.
   * Use setScope() to modify scope values.
   *
   * Per specs/01i_span_scope_attributes.md:
   * - Always a frozen object, never undefined
   * - Initialized to EMPTY_SCOPE, only replaced with new frozen objects
   * - Safe to read from async code without race conditions
   *
   * @example
   * ctx.setScope({ requestId: 'r1' });
   * console.log(ctx.scope.requestId); // 'r1'
   */
  scope: Readonly<Partial<InferSchema<Ctx['logSchema']>>>;
} & Ctx['userCtx'];
