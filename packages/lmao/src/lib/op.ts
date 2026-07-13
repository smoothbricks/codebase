/**
 * Op - Traced operation class and OpMetadata
 *
 * This is the single source of truth for the Op class and OpMetadata interface.
 * Other modules re-export these types for convenience.
 *
 * Wraps a user function with automatic span tracking.
 * Created by defineOp() from the Op-centric API (defineOpContext).
 *
 * Per specs/lmao/01l_op_context_pattern.md and specs/lmao/01c_context_flow_and_op_wrappers.md:
 * - Captures OpMetadata for attribution (package_name, package_file, git_sha, line)
 * - fn is called by span_op() after span_op creates the SpanBuffer/SpanContext
 * - Op name is used for metrics tracking (separate from span names)
 */

import type { PreEncodedEntry } from '@smoothbricks/arrow-builder';
import type { RemapDescriptor } from './logBinding.js';
import type { SpanContext } from './opContext/spanContextTypes.js';
import type { OpContext, OpContextBinding } from './opContext/types.js';
import { getPhysicalLayoutPlan, type PhysicalLayoutPlan } from './physicalLayoutPlan.js';
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
//#region smoo/lmao!n/opcontext-hierarchy
// 01j "ModuleContext", renamed: per-Op source-attribution metadata
// (package_name/package_file/git_sha) + pre-encoded UTF-8 entries for Arrow
// dictionary building. One per defineOp; SpanBuffer/SpanContext are the other
// two levels of the 01j Context Hierarchy.
export interface OpCompileMetadata {
  readonly runtimeHint: number;
  readonly logTemplateIds: readonly string[];
}

export const EMPTY_LOG_TEMPLATE_IDS: readonly string[] = Object.freeze([]);

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
  /** Op-local compile-time log templates; ID n resolves at index n - 1. */
  readonly logTemplateIds: readonly string[];
  /** Startup-resolved physical layout used by span setup. */
  readonly _physicalLayoutPlan?: PhysicalLayoutPlan;
}
//#endregion smoo/lmao!n/opcontext-hierarchy

// =============================================================================
// Op Class
// =============================================================================

//#region smoo/lmao!n/op-class
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
 * - remapDescriptor is immutable cold-path metadata for prefixed/mapped ops
 * - No LogBinding - stats accessed via SpanBufferClass.stats
 *
 * @typeParam Ctx - OpContext with deps, ff, env (contravariant position)
 * @typeParam Args - Tuple of argument types (excluding ctx)
 * @typeParam S - Success type of the Result
 * @typeParam E - Error type of the Result
 */
export class Op<Ctx extends OpContext, Args extends unknown[], S, E> {
  readonly metadata: OpMetadata;
  readonly physicalLayoutPlan: PhysicalLayoutPlan<Ctx['logSchema']>;
  readonly SpanBufferClass: SpanBufferConstructor<Ctx['logSchema']>;
  readonly fn: (ctx: SpanContext<Ctx>, ...args: Args) => Result<S, E> | Promise<Result<S, E>>;
  readonly remapDescriptor?: RemapDescriptor;
  readonly _opContextBinding?: OpContextBinding;
  readonly runtimeHint: number;

  constructor(
    metadata: OpMetadata,
    SpanBufferClass: SpanBufferConstructor<Ctx['logSchema']>,
    fn: (ctx: SpanContext<Ctx>, ...args: Args) => Result<S, E> | Promise<Result<S, E>>,
    remapDescriptor?: RemapDescriptor,
    opContextBinding?: OpContextBinding,
    runtimeHint = 0,
  ) {
    const physicalLayoutPlan = getPhysicalLayoutPlan(SpanBufferClass, runtimeHint, remapDescriptor);
    this.physicalLayoutPlan = physicalLayoutPlan;
    this.metadata = Object.freeze({ ...metadata, _physicalLayoutPlan: physicalLayoutPlan });
    this.SpanBufferClass = physicalLayoutPlan.SpanBufferClass;
    this.fn = fn;
    this.remapDescriptor = remapDescriptor;
    this._opContextBinding = opContextBinding;
    this.runtimeHint = physicalLayoutPlan.runtimeHint;
  }
}
//#endregion smoo/lmao!n/op-class
