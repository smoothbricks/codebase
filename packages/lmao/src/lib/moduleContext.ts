/**
 * ModuleContext - Shared context for all tasks in a module.
 *
 * @module moduleContext
 */

import { DEFAULT_BUFFER_CAPACITY, intern, PreEncodedEntry } from '@smoothbricks/arrow-builder';
import { LogSchema } from './schema/LogSchema.js';
import type { SchemaFields } from './schema/types.js';
import type { SpanBuffer } from './types.js';

/**
 * Module context shared across all tasks in the same module.
 */
export class ModuleContext {
  // ============================================================================
  // Self-tuning buffer capacity stats (sb_ prefix for V8 optimization)
  // Direct property access is faster than nested object access.
  // ============================================================================

  /** Current tuned buffer capacity */
  sb_capacity = DEFAULT_BUFFER_CAPACITY;

  /** Total writes since last tuning */
  sb_totalWrites = 0;

  /** Writes that caused buffer overflow */
  sb_overflows = 0;

  /** Entries written to overflow buffers (count of writes, not overflow events) */
  sb_overflowWrites = 0;

  /** Number of buffers created */
  sb_totalCreated = 0;

  /** The log schema for this module */
  public readonly logSchema: LogSchema;

  /** Pre-encoded entry for package (for Arrow dictionary building) */
  readonly package_name_entry: PreEncodedEntry;

  /** Pre-encoded entry for package_file (for Arrow dictionary building) */
  readonly package_file_entry: PreEncodedEntry;

  /** Pre-encoded entry for git_sha (for Arrow dictionary building) */
  readonly git_sha_entry: PreEncodedEntry;

  /**
   * RemappedBufferView class for library prefix support.
   *
   * V8 OPTIMIZATION: Always present (not optional) for consistent hidden class shape.
   * Set to undefined for unprefixed modules, set to generated class for prefixed modules.
   *
   * This ensures all ModuleContext instances share the same hidden class, allowing V8 to:
   * - Inline property access after first check
   * - Optimize the truthy check `if (this._module.remappedViewClass)` to near-zero cost
   *
   * Per specs/01e_library_integration_pattern.md and 01c_context_flow_and_op_wrappers.md
   */
  remappedViewClass: (new (buffer: SpanBuffer) => SpanBuffer) | undefined = undefined;

  constructor(
    public readonly git_sha: string,
    /** npm package name (e.g., '@smoothbricks/lmao') */
    public readonly package_name: string,
    /** Path within package, relative to package.json (e.g., 'src/services/user.ts') */
    public readonly package_file: string,
    schema: SchemaFields | LogSchema,
  ) {
    // Wrap plain schema objects in LogSchema if needed
    this.logSchema = schema instanceof LogSchema ? schema : new LogSchema(schema);
    // Pre-encode and store as reusable entries for Arrow dictionary building
    // Using class instances for V8 optimization (consistent hidden class shape)
    this.package_name_entry = new PreEncodedEntry(package_name, intern(package_name));
    this.package_file_entry = new PreEncodedEntry(package_file, intern(package_file));
    this.git_sha_entry = new PreEncodedEntry(git_sha, intern(git_sha));
  }
}
