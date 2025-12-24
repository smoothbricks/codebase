/**
 * Op - Traced operation class
 *
 * Wraps a user function with automatic span tracking.
 * Created by defineOp() from the Op-centric API (defineOpContext).
 *
 * Per specs/01l_op_context_pattern.md and specs/01c_context_flow_and_op_wrappers.md:
 * - Captures OpMetadata for attribution (package_name, package_file, git_sha, line)
 * - fn is called by span_op() after span_op creates the SpanBuffer/SpanContext
 * - Op name is used for metrics tracking (separate from span names)
 */

import type { OpMetadata } from './opContext/opTypes.js';
import type { LogBinding } from './types.js';

// =============================================================================
// Op Class
// =============================================================================

/**
 * Op - Traced operation
 *
 * Per spec 01l lines 436-453 and 01c lines 292-362:
 * - Wraps a user function with automatic span tracking
 * - Captures module metadata for attribution
 * - fn is public and called by span_op() after buffer/context creation
 *
 * Type parameter order matches function signature: (ctx: Ctx, ...args: Args) => Result
 *
 * Op has TWO names:
 * - `name`: The Op's name for metrics (invocations, errors, duration tracking)
 * - Span names are provided at CALL SITE: `await span('contextual-name', myOp, args)`
 *
 * Op captures the metadata for source attribution:
 * - When span() invokes this Op, the Op's metadata becomes buffer._opMetadata (for rows 1+)
 * - The caller's metadata becomes buffer._callsiteMetadata (for row 0)
 *
 * @typeParam Ctx - Required context type (contravariant position)
 * @typeParam Args - Tuple of argument types (excluding ctx)
 * @typeParam Result - Return type (can be sync T or async Promise<T>)
 */
export class Op<Ctx, Args extends unknown[], Result> {
  constructor(
    /** The Op's name for metrics (invocations, errors, duration) */
    readonly name: string,
    /** Metadata injected by transformer: package_name, package_file, git_sha, line */
    readonly metadata: OpMetadata,
    /** LogBinding with logging infrastructure (schema, capacity stats, optional prefix view) */
    readonly logBinding: LogBinding,
    /** The user function to execute - called by span_op after context creation (can be sync or async) */
    readonly fn: (ctx: Ctx, ...args: Args) => Result,
  ) {}
}
