/**
 * Op Type Definitions
 *
 * Core type definitions for Ops - the first-class citizens of the Op-centric API.
 * Ops wrap functions with metadata and enable composition.
 *
 * ## Dependency Layer
 * This module is at Layer 3 in the type hierarchy:
 * - Depends on: spanContextTypes (Layer 2), featureFlagTypes (Layer 1)
 * - Depended on by: opGroupTypes (Layer 4)
 */

import type { PreEncodedEntry } from '@smoothbricks/arrow-builder';
import type { LogSchema } from '../schema/LogSchema.js';
import type { LogBinding } from '../types.js';
import type { FeatureFlagSchema } from './featureFlagTypes.js';
import type { DepsConfig, SpanContext } from './spanContextTypes.js';

// =============================================================================
// OP METADATA
// =============================================================================

/**
 * Metadata injected by the transformer into each Op.
 * These values are determined at compile time from the source location.
 *
 * Includes pre-encoded UTF-8 entries for Arrow dictionary building -
 * encode once at Op definition time, reuse for every span conversion.
 */
export interface OpMetadata {
  /** Package name from package.json */
  readonly package_name: string;
  /** File path relative to package root */
  readonly package_file: string;
  /** Git SHA at build time */
  readonly git_sha: string;
  /** Line number where defineOp was called */
  readonly line: number;

  /** Pre-encoded package_name for Arrow dictionary building */
  readonly package_name_entry: PreEncodedEntry;
  /** Pre-encoded package_file for Arrow dictionary building */
  readonly package_file_entry: PreEncodedEntry;
  /** Pre-encoded git_sha for Arrow dictionary building */
  readonly git_sha_entry: PreEncodedEntry;
}

// =============================================================================
// OP FUNCTION & INTERFACE
// =============================================================================

/**
 * Op function signature - what the user provides to defineOp
 *
 * @template T - LogSchema type
 * @template FF - Feature flag schema
 * @template Deps - Dependencies config
 * @template UserCtx - User context properties
 * @template Args - Function arguments (after ctx)
 * @template R - Return type
 */
export type OpFn<
  T extends LogSchema,
  FF extends FeatureFlagSchema,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
  Args extends unknown[],
  R,
> = (ctx: SpanContext<T, FF, Deps, UserCtx>, ...args: Args) => Promise<R>;

/**
 * Op instance - wraps a function with metadata
 *
 * Ops are the first-class citizens of the Op-centric API. Each Op:
 * - Has a name for metrics and span naming
 * - Carries metadata injected by the transformer (package, file, git sha, line)
 * - References the LogSchema it writes to
 * - Holds a LogBinding with logging infrastructure (schema, capacity stats)
 * - Can be invoked via span() or directly via _invoke()
 *
 * @template T - LogSchema type
 * @template FF - Feature flag schema
 * @template Deps - Dependencies config
 * @template UserCtx - User context properties
 * @template Args - Function arguments (after ctx)
 * @template R - Return type
 */
export interface Op<
  T extends LogSchema,
  FF extends FeatureFlagSchema,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
  Args extends unknown[],
  R,
> {
  /** Op name (for metrics, span naming) */
  readonly name: string;

  /** Metadata injected by transformer */
  readonly metadata: OpMetadata;

  /** LogBinding with logging infrastructure (schema, capacity stats, optional prefix view) */
  readonly logBinding: LogBinding;

  /** Internal: invoke the op (called by span()) */
  _invoke(ctx: SpanContext<T, FF, Deps, UserCtx>, ...args: Args): Promise<R>;
}

// =============================================================================
// OP RECORD TRANSFORMATION
// =============================================================================

/**
 * Transform a record of Op functions into Ops
 *
 * Used by defineOps for batch definition. Takes a record where values are either:
 * - Existing Op instances (passed through)
 * - Op function signatures (wrapped into Ops)
 *
 * @template T - LogSchema type
 * @template FF - Feature flag schema
 * @template Deps - Dependencies config
 * @template UserCtx - User context properties
 * @template Defs - Record of Op definitions (functions or existing Ops)
 */
export type OpsFromRecord<
  T extends LogSchema,
  FF extends FeatureFlagSchema,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
  Defs extends Record<
    string,
    Op<T, FF, Deps, UserCtx, unknown[], unknown> | OpFn<T, FF, Deps, UserCtx, unknown[], unknown>
  >,
> = {
  [K in keyof Defs]: Defs[K] extends Op<T, FF, Deps, UserCtx, infer Args, infer R>
    ? Op<T, FF, Deps, UserCtx, Args, R>
    : Defs[K] extends (ctx: SpanContext<T, FF, Deps, UserCtx>, ...args: infer Args) => Promise<infer R>
      ? Op<T, FF, Deps, UserCtx, Args, R>
      : never;
};
