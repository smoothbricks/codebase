/**
 * LogBinding - Logging infrastructure binding for ops and buffers
 *
 * Represents the binding between a set of operations and their logging infrastructure.
 * Contains the schema and optional remapping support for prefixed modules.
 *
 * Note: Buffer stats (self-tuning capacity) are stored on SpanBufferClass.stats as static
 * properties, NOT on LogBinding. See agent-todo/opgroup-refactor.md for rationale.
 *
 * Per specs:
 * - 01b2_buffer_self_tuning.md - Self-tuning via SpanBufferClass.stats
 * - 01e_library_integration_pattern.md - Library prefix/remapping support
 */

import type { LogSchema } from './schema/LogSchema.js';

// Forward declare SpanBuffer type to avoid circular import with types.ts
// The actual SpanBuffer type is defined in types.ts, which re-exports RemappedViewConstructor
declare type SpanBuffer = any;

/**
 * RemappedViewConstructor - Constructor type for RemappedBufferView classes.
 *
 * Generated at module composition time (cold path) by `generateRemappedBufferViewClass()`.
 * Used by prefixed modules to wrap SpanBuffer instances for Arrow conversion when
 * the module's schema columns have been renamed with a prefix.
 *
 * The constructor takes a raw SpanBuffer and returns a view that remaps column access
 * from prefixed names to unprefixed names for tree traversal during Arrow conversion.
 *
 * Per specs/lmao/01e_library_integration_pattern.md:
 * - Hot path: Library writes directly to unprefixed columns (zero overhead)
 * - Cold path: Arrow conversion uses RemappedBufferView to access via prefixed names
 */
export type RemappedViewConstructor = new (buffer: SpanBuffer) => SpanBuffer;

/**
 * LogBinding - Everything needed for a group of ops to write logs.
 *
 * Represents the binding between a set of operations and their logging infrastructure.
 * Contains the schema and optional remapping support for prefixed modules.
 *
 * Note: Buffer stats (self-tuning capacity) are stored on SpanBufferClass.stats as static
 * properties, NOT on LogBinding. This avoids sync bugs from having two stat objects and
 * reduces per-instance memory. See agent-todo/opgroup-refactor.md for rationale.
 *
 * Per specs:
 * - 01b2_buffer_self_tuning.md - Self-tuning via SpanBufferClass.stats
 * - 01e_library_integration_pattern.md - Library prefix/remapping support
 *
 * @property logSchema - The unprefixed schema defining what columns can be written.
 *   This is the canonical schema. Column names in remappedViewClass will be prefixed versions
 *   of these field names when a prefix or column mapping has been applied.
 *
 * @property remappedViewClass - Optional RemappedBufferView class for prefixed/remapped buffers.
 *   - When present: the module's ops write to unprefixed columns, but buffers must be wrapped
 *     in this view for Arrow conversion to access them by prefixed names
 *   - When absent: buffers can be used directly without remapping
 *   - Set during module composition when prefix() or mapColumns() is applied (cold path)
 *   - Used when registering child spans to parent's tree (see op.ts)
 */
export interface LogBinding<T extends LogSchema = LogSchema> {
  /** The unprefixed schema defining what columns exist */
  readonly logSchema: T;

  /** Optional RemappedBufferView class for prefixed/remapped buffers */
  readonly remappedViewClass?: RemappedViewConstructor;
}
