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
import { getSpanBufferClass } from '../spanBuffer.js';
import type { OpGroup } from './opGroupTypes.js';
import type { Op, OpFn, OpMetadata, OpsFromRecord } from './opTypes.js';
import type { OpContext } from './types.js';

// =============================================================================
// METADATA HELPERS
// =============================================================================

/**
 * Create OpMetadata with pre-encoded entries for Arrow dictionary building.
 * Uses global interner to deduplicate UTF-8 bytes across all Ops with same strings.
 */
export function createOpMetadata(
  name: string,
  package_name: string,
  package_file: string,
  git_sha: string,
  line: number,
): OpMetadata {
  return {
    name,
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
export const DEFAULT_METADATA: OpMetadata = createOpMetadata('unknown', 'unknown', 'unknown', 'unknown', 0);

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

  return createOpMetadata('unknown', packageName, filePath, 'runtime', lineNumber);
}

// =============================================================================
// FACTORY CONFIG
// =============================================================================

/**
 * Configuration using OpContext bundle.
 * Simplifies factory config by carrying all context types in one parameter.
 */
interface DefineOpFactoryConfig<Ctx extends OpContext> {
  readonly logSchema: Ctx['logSchema'];
  readonly flags: Ctx['flags'];
}

/**
 * Factory function type for creating OpGroups.
 * Used by createDefineOps to delegate OpGroup creation.
 */
type CreateOpGroupFn<Ctx extends OpContext> = (
  logSchema: Ctx['logSchema'],
  flags: Ctx['flags'],
  ops: Record<string, Op<Ctx, unknown[], unknown, unknown>>,
) => OpGroup<Ctx>;

// =============================================================================
// CREATE DEFINE OP
// =============================================================================

/**
 * Creates a defineOp function bound to a specific factory config.
 *
 * The returned function creates Op objects with:
 * - metadata: includes name + merged with defaults
 * - SpanBufferClass: from getSpanBufferClass(logSchema) - has static schema + stats
 * - fn(ctx, ...args): the user function to execute
 * - remappedViewClass: undefined for original ops (set by .prefix() method)
 *
 * @template Ctx - Bundled OpContext (logSchema, flags, deps, userCtx)
 * @param factoryConfig - Configuration containing logSchema and flags
 * @returns A defineOp function for creating Op instances
 */
export function createDefineOp<Ctx extends OpContext>(
  factoryConfig: DefineOpFactoryConfig<Ctx>,
): <Args extends unknown[], S, E>(
  name: string,
  fn: OpFn<Ctx, Args, S, E>,
  metadata?: Partial<OpMetadata>,
) => Op<Ctx, Args, S, E> {
  // Why get class from schema: Class has static schema + stats shared by all instances
  const SpanBufferClass = getSpanBufferClass(factoryConfig.logSchema);

  return function defineOpImpl<Args extends unknown[], S, E>(
    name: string,
    fn: OpFn<Ctx, Args, S, E>,
    metadata?: Partial<OpMetadata>,
  ): Op<Ctx, Args, S, E> {
    // When transformer is not installed, extract metadata from stack trace
    // This provides useful file/line info for debugging even without build-time injection
    // skipFrames=3: Error -> extractMetadataFromStack -> defineOpImpl -> caller
    const baseMetadata = metadata?.package_file ? DEFAULT_METADATA : extractMetadataFromStack(3);
    // Use name from: 1) explicit metadata.name (transformer), 2) defineOp('name', fn), 3) baseMetadata
    const finalMetadata = { ...baseMetadata, ...metadata, name: metadata?.name ?? name };

    // Use the Op class which handles all span/buffer management:
    // - Creates SpanBuffer for the op
    // - Registers child buffers with parent
    // - Sets _opMetadata and _callsiteMetadata
    // - Writes span-start/span-ok/span-err entries
    // - Handles exceptions with span-exception
    // Why SpanBufferClass: All ops from same defineOpContext share this class for stats coordination
    // Why undefined for remappedViewClass: Only prefixed ops need remapping - set by .prefix() method
    return new OpClass(finalMetadata, SpanBufferClass, fn, undefined);
  };
}

// =============================================================================
// CREATE DEFINE OPS
// =============================================================================

/**
 * Type guard to check if a definition is an existing Op instance.
 */
function isOp<Ctx extends OpContext>(def: unknown): def is Op<Ctx, unknown[], unknown, unknown> {
  return def instanceof OpClass;
}

function hasAllDefinedOps<
  Ctx extends OpContext,
  Defs extends Record<string, Op<Ctx, unknown[], unknown, unknown> | OpFn<Ctx, unknown[], unknown, unknown>>,
>(definitions: Defs, ops: Record<string, Op<Ctx, unknown[], unknown, unknown>>): ops is OpsFromRecord<Ctx, Defs> {
  for (const name in definitions) {
    if (!(name in ops)) {
      return false;
    }
  }
  return true;
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
 * @template Ctx - Bundled OpContext (logSchema, flags, deps, userCtx)
 * @param factoryConfig - Configuration containing logSchema and flags
 * @param createOpGroup - Factory function for creating OpGroups
 * @returns A defineOps function for creating OpGroups
 */
export function createDefineOps<Ctx extends OpContext>(
  factoryConfig: DefineOpFactoryConfig<Ctx>,
  createOpGroup: CreateOpGroupFn<Ctx>,
): <Defs extends Record<string, Op<Ctx, unknown[], unknown, unknown> | OpFn<Ctx, unknown[], unknown, unknown>>>(
  definitions: Defs & ThisType<OpsFromRecord<Ctx, Defs>>,
) => OpGroup<Ctx> & OpsFromRecord<Ctx, Defs> {
  // Get the defineOp function for wrapping raw functions
  const defineOp = createDefineOp<Ctx>(factoryConfig);

  return function defineOpsImpl<
    Defs extends Record<string, Op<Ctx, unknown[], unknown, unknown> | OpFn<Ctx, unknown[], unknown, unknown>>,
  >(definitions: Defs & ThisType<OpsFromRecord<Ctx, Defs>>): OpGroup<Ctx> & OpsFromRecord<Ctx, Defs> {
    // Build the ops record by processing each definition.
    // Keep the runtime container broad, then narrow once we've populated every key.
    const ops: Record<string, Op<Ctx, unknown[], unknown, unknown>> = {};

    for (const name in definitions) {
      const def = definitions[name];
      ops[name] = isOp<Ctx>(def) ? def : defineOp(name, def);
    }

    if (!hasAllDefinedOps(definitions, ops)) {
      throw new Error('defineOps failed to normalize all op definitions');
    }

    // Create the OpGroup
    const opGroup = createOpGroup(factoryConfig.logSchema, factoryConfig.flags, ops);

    // Return OpGroup merged with individual ops for direct access
    return Object.assign(opGroup, ops);
  };
}
