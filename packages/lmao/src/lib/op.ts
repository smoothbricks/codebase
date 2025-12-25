/**
 * Op - Traced operation class and OpMetadata
 *
 * This is the single source of truth for the Op class and OpMetadata interface.
 * Other modules re-export these types for convenience.
 *
 * Wraps a user function with automatic span tracking.
 * Created by defineOp() from the Op-centric API (defineOpContext).
 *
 * Per specs/01l_op_context_pattern.md and specs/01c_context_flow_and_op_wrappers.md:
 * - Captures OpMetadata for attribution (package_name, package_file, git_sha, line)
 * - fn is called by span_op() after span_op creates the SpanBuffer/SpanContext
 * - Op name is used for metrics tracking (separate from span names)
 */

import type { PreEncodedEntry } from '@smoothbricks/arrow-builder';
import type { RemappedViewConstructor } from './logBinding.js';
import type { SpanContext } from './opContext/spanContextTypes.js';
import type { OpContext } from './opContext/types.js';
import type { Result } from './result.js';
import type { SpanBufferConstructor } from './spanBuffer.js';

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
  /** Op name for metrics tracking (distinct from span names which are provided at call sites) */
  readonly name: string;
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
// Op Class
// =============================================================================

/**
 * Op - Traced operation (Phase 2 architecture)
 *
 * Per spec 01l lines 436-453 and 01c lines 292-362:
 * - Wraps a user function with automatic span tracking
 * - Captures module metadata for attribution
 * - fn is public and called by span_op() after buffer/context creation
 *
 * Type parameter order matches function signature: (ctx: SpanContext<Ctx>, ...args: Args) => Result<S, E>
 *
 * Op has TWO names:
 * - `name`: The Op's name for metrics (invocations, errors, duration tracking)
 * - Span names are provided at CALL SITE: `await span('contextual-name', myOp, args)`
 *
 * Op captures the metadata for source attribution:
 * - When span() invokes this Op, the Op's metadata becomes buffer._opMetadata (for rows 1+)
 * - The caller's metadata becomes buffer._callsiteMetadata (for row 0)
 *
 * Phase 2 architecture:
 * - SpanBufferClass carries static schema + stats (shared by all ops from same defineOpContext)
 * - remappedViewClass only set for prefixed ops (wraps child buffers before adding to _children)
 * - No LogBinding - stats accessed via SpanBufferClass.stats
 *
 * @typeParam Ctx - OpContext with deps, ff, env (contravariant position)
 * @typeParam Args - Tuple of argument types (excluding ctx)
 * @typeParam S - Success type of the Result
 * @typeParam E - Error type of the Result
 */
export class Op<Ctx extends OpContext, Args extends unknown[], S, E> {
  constructor(
    /** Op name for metrics, telemetry, and debugging */
    readonly metadata: OpMetadata,
    /** Op owns buffer creation - class has static schema + stats shared by all instances */
    readonly SpanBufferClass: SpanBufferConstructor,
    /** The user function to execute - called by span_op after context creation (can be sync or async) */
    readonly fn: (ctx: SpanContext<Ctx>, ...args: Args) => Result<S, E> | Promise<Result<S, E>>,
    /** Only set for prefixed ops - wraps child buffers during registration */
    readonly remappedViewClass?: RemappedViewConstructor,
  ) {}
}
