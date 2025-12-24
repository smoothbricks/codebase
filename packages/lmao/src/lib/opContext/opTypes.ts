/**
 * Op Type Definitions
 *
 * Core type definitions for Ops - the first-class citizens of the Op-centric API.
 * Ops wrap functions with metadata and enable composition.
 *
 * The Op class and OpMetadata interface are defined in ../op.ts (single source of truth)
 * and re-exported here for convenience.
 *
 * ## Dependency Layer
 * This module is at Layer 3 in the type hierarchy:
 * - Depends on: spanContextTypes (Layer 2), featureFlagTypes (Layer 1)
 * - Depended on by: opGroupTypes (Layer 4)
 */

import type { ExtractError, ExtractSuccess, Result } from '../result.js';
import type { SpanContext } from './spanContextTypes.js';
import type { OpContext } from './types.js';

// =============================================================================
// OP CLASS & METADATA RE-EXPORTS
// =============================================================================

// Re-export the Op class and OpMetadata from op.ts - single source of truth
// The class is defined in op.ts to be the canonical type used by both
// defineOp (creates instances) and Tracer (checks instanceof)
export { Op, type OpMetadata } from '../op.js';

// =============================================================================
// OP FUNCTION TYPE
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

// =============================================================================
// OP RECORD TRANSFORMATION
// =============================================================================

// Import Op class for use in OpsFromRecord type
import type { Op } from '../op.js';

/**
 * Convert a single definition (Op or OpFn) to an Op type.
 * Uses ExtractSuccess/ExtractError from result.ts to infer types from return value.
 */
type DefToOp<Ctx extends OpContext, Def> = Def extends Op<Ctx, infer Args, infer S, infer E>
  ? Op<Ctx, Args, S, E>
  : Def extends (ctx: SpanContext<Ctx>, ...args: infer Args) => infer R
    ? Op<Ctx, Args, ExtractSuccess<R>, ExtractError<R>>
    : never;

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
  [K in keyof Defs]: DefToOp<Ctx, Defs[K]>;
};
