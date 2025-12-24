/**
 * Op Definition Functions
 *
 * Factory functions for defining Ops and OpGroups. These are used internally
 * by OpContextFactory to create type-safe Op definitions.
 *
 * @module opContext/defineOp
 */

import { intern, PreEncodedEntry } from '@smoothbricks/arrow-builder';
import { Op as OpClass } from '../op.js';
import type { LogSchema } from '../schema/LogSchema.js';
import type { LogBinding } from '../types.js';
import type { FeatureFlagSchema } from './featureFlagTypes.js';
import type { DepsConfig, OpGroup } from './opGroupTypes.js';
import type { Op, OpFn, OpMetadata, OpsFromRecord } from './opTypes.js';
import type { SpanContext } from './spanContextTypes.js';

// =============================================================================
// METADATA HELPERS
// =============================================================================

/**
 * Create OpMetadata with pre-encoded entries for Arrow dictionary building.
 * Uses global interner to deduplicate UTF-8 bytes across all Ops with same strings.
 */
export function createOpMetadata(
  package_name: string,
  package_file: string,
  git_sha: string,
  line: number,
): OpMetadata {
  return {
    package_name,
    package_file,
    git_sha,
    line,
    // Use global interner for deduplication - same package_name across Ops shares UTF-8 bytes
    package_name_entry: new PreEncodedEntry(package_name, intern(package_name)),
    package_file_entry: new PreEncodedEntry(package_file, intern(package_file)),
    git_sha_entry: new PreEncodedEntry(git_sha, intern(git_sha)),
  };
}

// =============================================================================
// DEFAULT METADATA
// =============================================================================

/**
 * Default metadata values for Ops when not injected by transformer.
 * These are placeholder values that will be replaced at compile time.
 */
export const DEFAULT_METADATA: OpMetadata = createOpMetadata('unknown', 'unknown', 'unknown', 0);

// =============================================================================
// STACK TRACE METADATA EXTRACTION
// =============================================================================

/**
 * Extracts metadata from stack trace when transformer is not installed.
 * Provides useful debugging info (file path, line number) even without build-time injection.
 *
 * Stack frame format varies by runtime:
 * - V8 (Node/Chrome): "    at functionName (file:///path/to/file.ts:10:5)"
 * - V8 anonymous:     "    at file:///path/to/file.ts:10:5"
 * - JSC (Safari):     "functionName@file:///path/to/file.ts:10:5"
 *
 * @param skipFrames - Number of stack frames to skip (to get caller's location)
 * @returns OpMetadata with extracted file path and line number
 */
export function extractMetadataFromStack(skipFrames = 2): OpMetadata {
  const err = new Error();
  const stack = err.stack;

  if (!stack) {
    return DEFAULT_METADATA;
  }

  const lines = stack.split('\n');

  // Skip "Error" line + specified frames to get to caller
  // Frame 0: Error
  // Frame 1: extractMetadataFromStack
  // Frame 2: defineOpImpl (or caller)
  // Frame 3: actual defineOp call site
  const targetFrame = lines[skipFrames + 1];

  if (!targetFrame) {
    return DEFAULT_METADATA;
  }

  // Try V8 format: "    at functionName (file:///path/to/file.ts:10:5)"
  // or anonymous: "    at file:///path/to/file.ts:10:5"
  let match = targetFrame.match(/at\s+(?:.*?\s+\()?(.+?):(\d+):\d+\)?$/);

  // Try JSC format: "functionName@file:///path/to/file.ts:10:5"
  if (!match) {
    match = targetFrame.match(/@(.+?):(\d+):\d+$/);
  }

  if (!match) {
    return DEFAULT_METADATA;
  }

  let filePath = match[1];
  const lineNumber = Number.parseInt(match[2], 10);

  // Clean up file:// protocol if present
  if (filePath.startsWith('file://')) {
    filePath = filePath.slice(7);
  }

  // Extract package name from path (last directory component before src/)
  // e.g., "/path/to/my-package/src/ops.ts" -> "my-package"
  let packageName = 'unknown';
  const srcIndex = filePath.lastIndexOf('/src/');
  if (srcIndex > 0) {
    const beforeSrc = filePath.slice(0, srcIndex);
    const lastSlash = beforeSrc.lastIndexOf('/');
    if (lastSlash >= 0) {
      packageName = beforeSrc.slice(lastSlash + 1);
    }
  } else {
    // Fallback: use filename without extension
    const lastSlash = filePath.lastIndexOf('/');
    const fileName = lastSlash >= 0 ? filePath.slice(lastSlash + 1) : filePath;
    const dotIndex = fileName.lastIndexOf('.');
    packageName = dotIndex > 0 ? fileName.slice(0, dotIndex) : fileName;
  }

  return createOpMetadata(packageName, filePath, 'runtime', lineNumber);
}

// =============================================================================
// FACTORY CONFIG
// =============================================================================

/**
 * Configuration passed to createDefineOp and createDefineOps factories.
 */
interface DefineOpFactoryConfig<T extends LogSchema, FF extends FeatureFlagSchema> {
  readonly logSchema: T;
  readonly flags: FF;
  readonly logBinding: LogBinding;
}

/**
 * Factory function type for creating OpGroups.
 * Used by createDefineOps to delegate OpGroup creation.
 */
type CreateOpGroupFn<T extends LogSchema, FF extends FeatureFlagSchema, UserCtx extends Record<string, unknown>> = (
  logSchema: T,
  flags: FF,
  // biome-ignore lint/suspicious/noExplicitAny: Ops stored with any deps for flexibility
  ops: Record<string, Op<T, FF, any, UserCtx, unknown[], unknown>>,
) => OpGroup<T, FF, UserCtx>;

// =============================================================================
// CREATE DEFINE OP
// =============================================================================

/**
 * Creates a defineOp function bound to a specific factory config.
 *
 * The returned function creates Op objects with:
 * - name: the provided name
 * - metadata: merged with defaults
 * - logBinding: from factoryConfig (contains logSchema, capacity stats, optional prefix view)
 * - fn(ctx, ...args): the user function to execute
 *
 * @template T - LogSchema type
 * @template FF - Feature flag schema
 * @template Deps - Dependencies config
 * @template UserCtx - User context properties
 * @param factoryConfig - Configuration containing logSchema and flags
 * @returns A defineOp function for creating Op instances
 */
export function createDefineOp<
  T extends LogSchema,
  FF extends FeatureFlagSchema,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
>(
  factoryConfig: DefineOpFactoryConfig<T, FF>,
): <Args extends unknown[], R>(
  name: string,
  fn: OpFn<T, FF, Deps, UserCtx, Args, R>,
  metadata?: Partial<OpMetadata>,
) => Op<T, FF, Deps, UserCtx, Args, R> {
  return function defineOpImpl<Args extends unknown[], R>(
    name: string,
    fn: OpFn<T, FF, Deps, UserCtx, Args, R>,
    metadata?: Partial<OpMetadata>,
  ): Op<T, FF, Deps, UserCtx, Args, R> {
    // When transformer is not installed, extract metadata from stack trace
    // This provides useful file/line info for debugging even without build-time injection
    // skipFrames=3: Error -> extractMetadataFromStack -> defineOpImpl -> caller
    const baseMetadata = metadata?.package_file ? DEFAULT_METADATA : extractMetadataFromStack(3);
    const finalMetadata = { ...baseMetadata, ...metadata };

    // Use the Op class which handles all span/buffer management:
    // - Creates SpanBuffer for the op
    // - Registers child buffers with parent
    // - Sets _opMetadata and _callsiteMetadata
    // - Writes span-start/span-ok/span-err entries
    // - Handles exceptions with span-exception
    return new OpClass(
      name,
      finalMetadata,
      factoryConfig.logBinding,
      fn as (ctx: SpanContext<T, FF, Deps, UserCtx>, ...args: Args) => Promise<R>,
    ) as unknown as Op<T, FF, Deps, UserCtx, Args, R>;
  };
}

// =============================================================================
// CREATE DEFINE OPS
// =============================================================================

/**
 * Type guard to check if a definition is an existing Op instance.
 */
function isOp<
  T extends LogSchema,
  FF extends FeatureFlagSchema,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
>(def: unknown): def is Op<T, FF, Deps, UserCtx, unknown[], unknown> {
  return def instanceof OpClass;
}

/**
 * Creates a defineOps function bound to a specific factory config.
 *
 * The returned function:
 * - Iterates over Object.entries(definitions)
 * - For each [name, def]:
 *   - If def is an Op instance, use it as-is
 *   - Else wrap with defineOpImpl(name, def)
 * - Stores all ops in a record
 * - Creates OpGroup using createOpGroup(logSchema, flags, ops)
 * - Returns Object.assign(opGroup, ops) to get both OpGroup methods and individual ops
 *
 * @template T - LogSchema type
 * @template FF - Feature flag schema
 * @template Deps - Dependencies config
 * @template UserCtx - User context properties
 * @param factoryConfig - Configuration containing logSchema and flags
 * @param createOpGroup - Factory function for creating OpGroups
 * @returns A defineOps function for creating OpGroups
 */
export function createDefineOps<
  T extends LogSchema,
  FF extends FeatureFlagSchema,
  Deps extends DepsConfig,
  UserCtx extends Record<string, unknown>,
>(
  factoryConfig: DefineOpFactoryConfig<T, FF>,
  createOpGroup: CreateOpGroupFn<T, FF, UserCtx>,
): <
  Defs extends Record<
    string,
    Op<T, FF, Deps, UserCtx, unknown[], unknown> | OpFn<T, FF, Deps, UserCtx, unknown[], unknown>
  >,
>(
  definitions: Defs & ThisType<OpsFromRecord<T, FF, Deps, UserCtx, Defs>>,
) => OpGroup<T, FF, UserCtx> & OpsFromRecord<T, FF, Deps, UserCtx, Defs> {
  // Get the defineOp function for wrapping raw functions
  const defineOp = createDefineOp<T, FF, Deps, UserCtx>(factoryConfig);

  return function defineOpsImpl<
    Defs extends Record<
      string,
      Op<T, FF, Deps, UserCtx, unknown[], unknown> | OpFn<T, FF, Deps, UserCtx, unknown[], unknown>
    >,
  >(
    definitions: Defs & ThisType<OpsFromRecord<T, FF, Deps, UserCtx, Defs>>,
  ): OpGroup<T, FF, UserCtx> & OpsFromRecord<T, FF, Deps, UserCtx, Defs> {
    // Build the ops record by processing each definition
    // biome-ignore lint/suspicious/noExplicitAny: Ops stored with any deps for flexibility
    const ops: Record<string, Op<T, FF, any, UserCtx, unknown[], unknown>> = {};

    for (const [name, def] of Object.entries(definitions)) {
      if (isOp<T, FF, Deps, UserCtx>(def)) {
        // Already an Op instance, use as-is
        ops[name] = def;
      } else {
        // Raw function, wrap with defineOp
        ops[name] = defineOp(name, def as OpFn<T, FF, Deps, UserCtx, unknown[], unknown>);
      }
    }

    // Create the OpGroup
    const opGroup = createOpGroup(factoryConfig.logSchema, factoryConfig.flags, ops);

    // Return OpGroup merged with individual ops for direct access
    return Object.assign(opGroup, ops) as OpGroup<T, FF, UserCtx> & OpsFromRecord<T, FF, Deps, UserCtx, Defs>;
  };
}
