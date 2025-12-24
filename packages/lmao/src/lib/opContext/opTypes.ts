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
import type { Result } from '../result.js';
import type { LogBinding } from '../types.js';
import type { SpanContext } from './spanContextTypes.js';
import type { OpContext } from './types.js';

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
 * Op function signature - what the user provides to defineOp.
 * MUST return Result<S, E> or Promise<Result<S, E>>.
 *
 * @template Ctx - Bundled OpContext (logSchema, flags, deps, userCtx)
 * @template Args - Function arguments (after ctx)
 * @template S - Success type
 * @template E - Error type
 */
export type OpFn<Ctx extends OpContext, Args extends unknown[], S, E> = (
  ctx: SpanContext<Ctx>,
  ...args: Args
) => Result<S, E> | Promise<Result<S, E>>;

/**
 * Op instance - wraps a function with metadata.
 * MUST return Result<S, E> or Promise<Result<S, E>>.
 *
 * @template Ctx - Bundled OpContext (logSchema, flags, deps, userCtx)
 * @template Args - Function arguments (after ctx)
 * @template S - Success type
 * @template E - Error type
 */
export interface Op<Ctx extends OpContext, Args extends unknown[], S, E> {
  readonly name: string;
  readonly metadata: OpMetadata;
  readonly logBinding: LogBinding;
  readonly fn: (ctx: SpanContext<Ctx>, ...args: Args) => Result<S, E> | Promise<Result<S, E>>;
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
 * @template Ctx - Bundled OpContext (logSchema, flags, deps, userCtx)
 * @template Defs - Record of Op definitions (functions or existing Ops)
 */
export type OpsFromRecord<
  Ctx extends OpContext,
  Defs extends Record<string, Op<Ctx, unknown[], unknown, unknown> | OpFn<Ctx, unknown[], unknown, unknown>>,
> = {
  [K in keyof Defs]: Defs[K] extends Op<Ctx, infer Args, infer S, infer E>
    ? Op<Ctx, Args, S, E>
    : Defs[K] extends (ctx: SpanContext<Ctx>, ...args: infer Args) => infer R
      ? R extends Result<infer S, infer E> | Promise<Result<infer S, infer E>>
        ? Op<Ctx, Args, S, E>
        : never
      : never;
};
