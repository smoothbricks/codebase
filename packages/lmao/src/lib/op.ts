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
import { getPhysicalLayoutPlan, sealCallsitePlan } from './physicalLayoutPlan.js';
import type {
  ArrowExposurePlan,
  CallsitePlan,
  EagerColumnDescriptor,
  PhysicalAppenders,
} from './physicalLayoutPlan.js';
import type { LogSchema } from './schema/LogSchema.js';
import type { SpanContext } from './opContext/spanContextTypes.js';
import type { OpContext, OpContextBinding } from './opContext/types.js';
import type { Result } from './result.js';
import { decodeRuntimeHint } from './runtimeHint.js';
import { createSpanContextClass } from './spanContext.js';
import type { SpanBufferConstructor } from './spanBuffer.js';
import type { TimestampAppendPrimitive } from './traceRoot.js';
import type { VocabularyGeneration } from './vocabularyRegistry.js';
import type { WasmLayoutTemplate } from './wasm/wasmPhysicalLayout.js';

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
  readonly eagerColumns?: readonly string[];
  readonly localMessageDictionary?: readonly number[];
}


export interface OpMetadata<T extends LogSchema = LogSchema> {
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
  /** Startup-resolved physical layout used by span setup; absent only on raw fallback metadata. */
  readonly _physicalLayoutPlan?: {
    readonly SpanBufferClass: SpanBufferConstructor<T>;
    readonly eagerColumns: EagerColumnDescriptor;
    readonly vocabularyGeneration: VocabularyGeneration;
    readonly localMessageDictionary: readonly number[];
    readonly encodeLocalMessage: (globalDenseIndex: number) => number;
    readonly wasmLayout: WasmLayoutTemplate;
    readonly appendLogEntry: TimestampAppendPrimitive;
    readonly arrowExposure: ArrowExposurePlan;
    readonly appenders: PhysicalAppenders;
  };
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
 * - remapDescriptor is an immutable cold-path binding for prefixed/mapped ops
 * - No LogBinding - stats accessed via SpanBufferClass.stats
 *
 * @typeParam Ctx - OpContext with deps, ff, env (contravariant position)
 * @typeParam Args - Tuple of argument types (excluding ctx)
 * @typeParam S - Success type of the Result
 * @typeParam E - Error type of the Result
 */
export class Op<Ctx extends OpContext, Args extends unknown[], S, E> {
  readonly metadata: OpMetadata;
  readonly callsitePlan: CallsitePlan<Ctx['logSchema'], Ctx>;
  readonly fn: (ctx: SpanContext<Ctx>, ...args: Args) => Result<S, E> | Promise<Result<S, E>>;
  readonly _opContextBinding?: OpContextBinding<Ctx['logSchema'], Ctx['flags'], Ctx['deps'], Ctx['userCtx']>;

  constructor(
    metadata: OpMetadata,
    SpanBufferClass: SpanBufferConstructor<Ctx['logSchema']>,
    fn: (ctx: SpanContext<Ctx>, ...args: Args) => Result<S, E> | Promise<Result<S, E>>,
    remapDescriptor?: RemapDescriptor,
    opContextBinding?: OpContextBinding<Ctx['logSchema'], Ctx['flags'], Ctx['deps'], Ctx['userCtx']>,
    runtimeHint = 0,
    eagerColumns: readonly string[] = [],
    localMessageDictionary: readonly number[] = Object.freeze([]),
  ) {
    const userContextKeys = Object.keys(opContextBinding?.ctxDefaults ?? {}).sort();
    const layoutKey = userContextKeys.join('\u0000');
    const capabilities = decodeRuntimeHint(runtimeHint).capabilities;
    const SpanContextClass = createSpanContextClass<Ctx>(
      SpanBufferClass.schema,
      opContextBinding?.logBinding ?? { logSchema: SpanBufferClass.schema },
      capabilities,
      userContextKeys,
    );
    const physicalLayoutPlan = getPhysicalLayoutPlan<Ctx['logSchema'], Ctx>(
      SpanBufferClass,
      runtimeHint,
      SpanContextClass,
      remapDescriptor,
      'strategy-selected',
      layoutKey,
      eagerColumns,
      localMessageDictionary,
    );
    const resolvedMetadata = Object.freeze({ ...metadata, _physicalLayoutPlan: physicalLayoutPlan });
    this.callsitePlan = sealCallsitePlan(physicalLayoutPlan, resolvedMetadata);
    this.metadata = resolvedMetadata;
    this.fn = fn;
    this._opContextBinding = opContextBinding;
  }



}
//#endregion smoo/lmao!n/op-class
