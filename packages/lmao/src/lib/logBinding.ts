/**
 * LogBinding - Logging infrastructure binding for ops and buffers
 *
 * Represents the binding between a set of operations and their logging infrastructure.
 * Contains the schema, optional remapping support, and mutable self-tuning statistics.
 *
 * Per specs:
 * - 01b2_buffer_self_tuning.md - Self-tuning capacity learning
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
 * Per specs/01e_library_integration_pattern.md:
 * - Hot path: Library writes directly to unprefixed columns (zero overhead)
 * - Cold path: Arrow conversion uses RemappedBufferView to access via prefixed names
 */
export type RemappedViewConstructor = new (buffer: SpanBuffer) => SpanBuffer;

/**
 * LogBinding - Everything needed for a group of ops to write logs.
 *
 * Represents the binding between a set of operations and their logging infrastructure.
 * Mutable buffer stats enable self-tuning: as operations execute, stats are updated
 * to track capacity usage and overflow behavior, allowing the framework to adapt
 * buffer sizes to actual workload patterns.
 *
 * Per specs/01b2_buffer_self_tuning.md:
 * - `sb_capacity`: Current buffer capacity (grows when overflow occurs)
 * - `sb_totalWrites`: Total entries written across all buffers for this binding
 * - `sb_overflowWrites`: How many times buffers overflowed (new buffer created)
 * - `sb_totalCreated`: Total buffers created (root + children + chains)
 * - `sb_overflows`: Number of times overflow occurred (buffer count = 1 + sb_overflows)
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
 *
 * @property sb_capacity - Current buffer capacity for new buffers created by this binding.
 *   Starts at initial capacity, grows when overflow occurs. Updated during flush/creation.
 *
 * @property sb_totalWrites - Total entries written across all buffers.
 *   Incremented each time SpanLogger writes an entry (hot path via writeIndex increment).
 *
 * @property sb_overflowWrites - How many times an overflow occurred.
 *   Incremented when a buffer fills and a new buffer must be created.
 *
 * @property sb_totalCreated - Total buffers created for this binding.
 *   Includes root buffer + child span buffers + chained overflow buffers.
 *
 * @property sb_overflows - Number of overflow events.
 *   Derived: buffer count = 1 + sb_overflows (always at least 1 buffer).
 */
export interface LogBinding {
  /** The unprefixed schema defining what columns exist */
  readonly logSchema: LogSchema;

  /** Optional RemappedBufferView class for prefixed/remapped buffers */
  readonly remappedViewClass?: RemappedViewConstructor;

  /** Current buffer capacity for new buffers created by this binding */
  sb_capacity: number;

  /** Total entries written across all buffers for this binding */
  sb_totalWrites: number;

  /** How many times an overflow occurred (buffer count = 1 + sb_overflows) */
  sb_overflowWrites: number;

  /** Total buffers created (root + children + chains) */
  sb_totalCreated: number;

  /** Number of overflow events (incremented when buffer fills) */
  sb_overflows: number;
}
