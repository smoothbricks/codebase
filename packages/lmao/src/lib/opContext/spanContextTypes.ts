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
 * - Depends on: contextTypes (Layer 1), featureFlagTypes (Layer 1)
 * - Depended on by: opTypes (Layer 3)
 *
 * To avoid circular dependencies, SpanFn uses a structural type for the Op
 * parameter rather than importing the Op type directly.
 */

import type { FluentErrorResult, FluentSuccessResult } from '../result.js';
import type { LogSchema } from '../schema/LogSchema.js';
import type { InferSchema } from '../schema/types.js';
import type { SpanBuffer } from '../types.js';
import type { ResolvedContext } from './contextTypes.js';
import type { BoundFeatureFlags, FeatureFlagSchema } from './featureFlagTypes.js';

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

/**
 * Invocable operation - structural type for SpanFn's Op parameter.
 * This avoids importing Op from opTypes which would create a circular dependency.
 *
 * Any object matching this shape can be passed to span() as an Op.
 */
export interface InvocableOp<Args extends unknown[], R> {
  /** Op name (for span naming) */
  readonly name: string;
  /** Internal invoke method */
  // biome-ignore lint/suspicious/noExplicitAny: Accepts any SpanContext-like object
  _invoke(ctx: any, ...args: Args): Promise<R>;
}

// =============================================================================
// TAG & LOG WRITER TYPES
// =============================================================================

/**
 * Tag writer for setting span attributes (row 0)
 * Generated at runtime with typed methods per schema field
 */
export type TagWriter<T extends LogSchema> = {
  [K in keyof InferSchema<T>]: (value: InferSchema<T>[K]) => TagWriter<T>;
} & {
  /** Set multiple attributes at once */
  with(attributes: Partial<InferSchema<T>>): TagWriter<T>;
};

/**
 * Span logger for structured logging (rows 2+)
 * Generated at runtime with typed methods per schema field
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
 * Fluent log entry - chainable attribute setters after log level
 */
export type FluentLogEntry<T extends LogSchema> = {
  [K in keyof InferSchema<T>]: (value: InferSchema<T>[K]) => FluentLogEntry<T>;
} & {
  /** Set source line number */
  line(n: number): FluentLogEntry<T>;
  /** Set multiple attributes at once */
  with(attributes: Partial<InferSchema<T>>): FluentLogEntry<T>;
};

// =============================================================================
// SPAN FUNCTION TYPES
// =============================================================================

/**
 * Span function signature for creating child spans
 */
export interface SpanFn<
  T extends LogSchema,
  FF extends FeatureFlagSchema,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
> {
  /**
   * Create child span with inline function
   *
   * @param name - Child span name
   * @param fn - Function to execute in child span
   * @param line - Source line (injected by transformer)
   */
  <R>(name: string, fn: (ctx: SpanContext<T, FF, Deps, UserCtx>) => Promise<R>, line?: number): Promise<R>;

  /**
   * Create child span invoking an Op
   *
   * Uses structural typing (InvocableOp) to accept any Op without importing
   * the Op type, which would create a circular dependency.
   *
   * @param name - Child span name (overrides Op's default name)
   * @param op - Op to invoke (any object with name and _invoke)
   * @param args - Arguments to pass to Op
   */
  <Args extends unknown[], R>(name: string, op: InvocableOp<Args, R>, ...args: Args): Promise<R>;
}

// =============================================================================
// SPAN CONTEXT TYPE
// =============================================================================

/**
 * Full SpanContext passed to Op functions
 *
 * Combines:
 * - Core span operations (tag, log, ok, err, span)
 * - Feature flags (ff)
 * - Dependencies (deps) - typed per wired OpGroups
 * - User context properties (resolved - nulls become required values)
 */
export type SpanContext<
  T extends LogSchema,
  FF extends FeatureFlagSchema,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
> = ResolvedContext<UserCtx> & {
  /** Feature flag accessor */
  readonly ff: BoundFeatureFlags<FF>;

  /** Dependencies - wired OpGroups */
  readonly deps: ResolvedDeps<Deps>;

  /** Span attribute writer (row 0) */
  readonly tag: TagWriter<T>;

  /** Structured logger (rows 2+) */
  readonly log: SpanLogger<T>;

  /** Read-only view of current scoped attributes */
  readonly scope: Readonly<Partial<InferSchema<T>>>;

  /** Set scoped attributes that propagate to child spans */
  setScope(attributes: Partial<InferSchema<T>> | null): void;

  /** Create success result */
  ok<V>(value: V): FluentSuccessResult<V, T>;

  /** Create error result */
  err<E>(code: string, error: E): FluentErrorResult<E, T>;

  /** Create child span */
  readonly span: SpanFn<T, FF, Deps, UserCtx>;

  /** Access underlying buffer (for Arrow conversion) */
  readonly buffer: SpanBuffer<T>;
};
