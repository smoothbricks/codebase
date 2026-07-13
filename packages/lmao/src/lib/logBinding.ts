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

/** Dense cold-path remap entry with schema and canonical storage property metadata. */
export type RemappedColumn = readonly [
  outputName: string,
  sourceName: string,
  schema: unknown,
  valuesProperty: `${string}_values`,
  nullsProperty: `${string}_nulls`,
];

/**
 * Immutable metadata interpreted only by cold Arrow conversion.
 * Child SpanBuffers retain their canonical schema and storage layout.
 */
export interface RemapDescriptor {
  /** Fully composed output-name to canonical source-name lookup. */
  readonly sourceNames: Readonly<Record<string, string>>;
  /** Dense output/source/schema entries in deterministic composition order. */
  readonly columns: readonly RemappedColumn[];
}

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
 * @property remapDescriptor - Optional immutable output-to-source metadata for
 *   prefixed/remapped buffers. Ops attach the descriptor pointer to raw child
 *   buffers; cold Arrow traversal interprets it without wrapping the buffer.
 */
export interface LogBinding<T extends LogSchema = LogSchema> {
  /** The unprefixed schema defining what columns exist */
  readonly logSchema: T;

  /** Optional immutable remapping metadata for cold Arrow conversion. */
  readonly remapDescriptor?: RemapDescriptor;
}
