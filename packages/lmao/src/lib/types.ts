/**
 * LMAO-specific types for trace logging
 *
 * ## Type System
 *
 * - `AnySpanBuffer`: NO index signatures, accepts any schema (for generic processing)
 * - `SpanBuffer<T>`: WITH index signatures, typed for specific schema T (T is REQUIRED)
 *
 * This mirrors arrow-builder's AnyColumnBuffer/ColumnBuffer<T> pattern.
 */

import type { AnyColumnBuffer, ColumnBuffer, ColumnValueType, TypedArray } from '@smoothbricks/arrow-builder';
import type { OpMetadata } from './opContext/opTypes.js';
import type { LogSchema } from './schema/LogSchema.js';
import type { SpanBufferStats } from './spanBufferStats.js';
import type { TraceId, TraceRoot } from './traceId.js';

// Re-export infrastructure types
export type { LogBinding, RemappedViewConstructor } from './logBinding.js';
export type { OpMetadata } from './opContext/opTypes.js';

// Tracer interface for lifecycle hook calls from span lifecycle events
// The Tracer provides these methods that are called during span execution
export interface TracerLifecycleHooks {
  onTraceStart(buffer: AnySpanBuffer): void;
  onTraceEnd(buffer: AnySpanBuffer): void;
  onSpanStart(buffer: AnySpanBuffer): void;
  onSpanEnd(buffer: AnySpanBuffer): void;
  /**
   * Called before stats are reset during capacity tuning.
   * Allows tracer to capture stats for observability before they're lost.
   *
   * The buffer provides all necessary context:
   * - buffer._stats → SpanBufferStats about to be reset
   * - buffer._opMetadata → which Op/module these stats belong to
   * - buffer.constructor → SpanBufferClass (schema info)
   *
   * @param buffer - The buffer that triggered overflow
   */
  onStatsWillResetFor(buffer: AnySpanBuffer): void;
}

// Re-export arrow-builder types for convenience
export type { AnyColumnBuffer, ColumnBuffer, ColumnValueType, TypedArray };

// ============================================================================
// AnySpanBuffer - Base type WITHOUT index signatures
// ============================================================================

/**
 * AnySpanBuffer - Core buffer API for Arrow conversion and generic processing.
 *
 * Per specs/01b5_spanbuffer_memory_layout.md:
 * SpanBuffer is the per-span columnar storage that holds all trace data for a single span.
 * It uses cache-aligned TypedArrays for V8 optimization and zero-copy Arrow conversion.
 *
 * This interface has NO index signatures, making it compatible with any SpanBuffer<T>
 * regardless of schema. Use this type when you need to accept any buffer:
 * - Arrow conversion (convertToArrowTable)
 * - Tree walking (traverseSpanTree)
 * - Generic buffer processing utilities
 *
 * ## Memory Layout (per specs/01b5)
 *
 * ```
 * _system ArrayBuffer (64-byte aligned):
 * ├─ timestamp: BigInt64Array (8 bytes × capacity) - nanosecond precision
 * └─ entry_type: Uint8Array (1 byte × capacity) - enum from ENTRY_TYPE_*
 *
 * _identity Uint8Array (48 bytes, immutable after creation):
 * ├─ trace_id: bytes 0-15 (128-bit UUID)
 * ├─ thread_id: bytes 16-23 (64-bit BigInt)
 * ├─ span_id: bytes 24-27 (32-bit counter)
 * ├─ parent_thread_id: bytes 28-35 (64-bit BigInt, 0 if root)
 * └─ parent_span_id: bytes 36-39 (32-bit counter, 0 if root)
 * ```
 *
 * ## Fixed Row Layout (per specs/01h)
 *
 * - Row 0: span-start (written by writeSpanStart)
 * - Row 1: span-end (span-ok, span-err, or span-exception)
 * - Rows 2+: log entries (info, debug, warn, error, tag, ff-access)
 *
 * @see SpanBuffer<T> for schema-typed version with index signatures
 */
export interface AnySpanBuffer extends AnyColumnBuffer {
  // ===========================================================================
  // System Memory (cache-aligned, per specs/01b1)
  // ===========================================================================

  /**
   * System ArrayBuffer containing timestamp and entry_type columns.
   * 64-byte aligned for CPU cache optimization and SIMD operations.
   */
  readonly _system: ArrayBuffer;

  /**
   * Identity bytes (48 bytes) containing trace_id, thread_id, span_id, parent info.
   * Immutable after buffer creation - identity never changes.
   */
  readonly _identity: Uint8Array;

  // ===========================================================================
  // System Columns (always eager, never lazy - per specs/01b6)
  // ===========================================================================

  /**
   * Nanosecond timestamps for each entry (BigInt64Array).
   *
   * Per specs/01b3_high_precision_timestamps.md:
   * - Uses performance.now() anchored to epoch for nanosecond precision
   * - Row 0: span-start timestamp
   * - Row 1: span-end timestamp (ok/err/exception)
   * - Rows 2+: log entry timestamps
   */
  readonly timestamp: BigInt64Array;

  /**
   * Entry type enum for each row (Uint8Array, 1 byte per entry).
   *
   * Per specs/01h_entry_types_and_logging_primitives.md:
   * Values are ENTRY_TYPE_* constants (span-start=1, span-ok=2, etc.)
   */
  readonly entry_type: Uint8Array;

  // ===========================================================================
  // Buffer State
  // ===========================================================================

  /**
   * Current write position (next row to write).
   * Starts at 2 after span-start (row 0) and span-end placeholder (row 1).
   */
  _writeIndex: number;

  /**
   * Maximum number of rows this buffer can hold.
   * Per specs/01b2_buffer_self_tuning.md: Capacity adapts based on workload.
   */
  readonly _capacity: number;

  /**
   * Next buffer in overflow chain (created when _writeIndex >= _capacity).
   * Per specs/01b2: Buffers chain rather than resize for predictable allocation.
   */
  _overflow?: AnySpanBuffer;

  // ===========================================================================
  // Identity Getters (read from _identity bytes)
  // ===========================================================================

  /**
   * Span ID - 32-bit counter unique within this trace.
   * Per specs/01b4_span_identity.md: Monotonic counter, not globally unique.
   */
  readonly span_id: number;

  /**
   * Thread ID - 64-bit identifier for this execution thread/context.
   * Per specs/01b4: Combines worker ID + sequence for uniqueness.
   */
  readonly thread_id: bigint;

  /**
   * Trace ID - 128-bit UUID identifying the entire trace.
   * Per specs/01b4: Propagated to all child spans and buffers.
   */
  readonly trace_id: TraceId;

  /**
   * Whether this span has a parent (false for root spans).
   */
  readonly _hasParent: boolean;

  /**
   * Parent's span ID (0 if root span).
   */
  readonly parent_span_id: number;

  /**
   * Parent's thread ID (0n if root span).
   */
  readonly parent_thread_id: bigint;

  // ===========================================================================
  // Schema & Stats Access (convenience getters for static properties)
  // ===========================================================================

  /**
   * Schema for this buffer (accessed via constructor.schema).
   * All buffers from same defineOpContext share the same schema.
   * Underscore prefix since this is an internal property.
   */
  readonly _logSchema: LogSchema;

  /**
   * Column entries for Arrow conversion iteration.
   * For SpanBuffer: delegates to _logSchema._columns (unprefixed names)
   * For RemappedBufferView: built in constructor with prefixed names from mapping
   *
   * This enables Arrow conversion to iterate column names correctly regardless of
   * whether the buffer is wrapped in a RemappedBufferView.
   */
  readonly _columns: ReadonlyArray<[string, unknown]>;

  /**
   * Shared stats for all buffers from same defineOpContext (accessed via constructor.stats).
   * Enables self-tuning capacity learning across all ops in the same context.
   * Underscore prefix since this is an internal property.
   */
  readonly _stats: SpanBufferStats;

  // ===========================================================================
  // Tree Structure (for span hierarchy)
  // ===========================================================================

  /**
   * Child spans created via ctx.span().
   * Per specs/01c: Parent-child relationship for trace tree.
   */
  _children: AnySpanBuffer[];

  /**
   * Parent span buffer (undefined for root).
   * Set during createChildSpanBuffer().
   */
  _parent?: AnySpanBuffer;

  // ===========================================================================
  // Context (metadata and scope)
  // ===========================================================================

  /**
   * Root trace context - shared by all buffers in a trace.
   * Contains anchorEpochNanos/anchorPerfNow for timestamp calculation.
   */
  _traceRoot: TraceRoot;

  /**
   * Op metadata for this span - identifies WHICH OP IS EXECUTING.
   * Per specs/01j and opgroup-refactor.md:
   * - Used for rows 1+ (log entries, tags, span-ok/err) attribution
   * - For Op calls: set to op.metadata
   * - For plain functions: inherited from parent's _opMetadata
   * - Different from _callsiteMetadata which identifies WHO CALLED span()
   */
  _opMetadata: OpMetadata;

  /**
   * Span name - written to row 0's message column.
   */
  _spanName: string;

  /**
   * Callsite metadata - where ctx.span() was called from.
   * Different from _opMetadata when calling into another op.
   * Per specs/01j: Row 0 gets callsite, rows 1+ get op metadata.
   */
  _callsiteMetadata?: OpMetadata;

  /**
   * Scoped attribute values - inherited by child spans.
   *
   * Per specs/01i_span_scope_attributes.md:
   * - Frozen object (never mutated, replaced on setScope)
   * - Child spans inherit by reference (safe because frozen)
   * - Applied to all rows at Arrow conversion via TypedArray.fill()
   */
  _scopeValues: Readonly<Record<string, unknown>>;

  // ===========================================================================
  // System Column Arrays (per specs/01h - known system schema fields)
  // These are explicitly typed here since they're always present from systemSchema.
  // ===========================================================================

  /**
   * Message column values - category strings (eager, always allocated).
   * Contains span names, log messages, exception messages, etc.
   * String arrays don't use null bitmaps - undefined/null in the array itself.
   */
  readonly message_values: string[];

  /**
   * Line number column values - Float64Array (lazy but always present).
   * Contains source code line numbers injected by transformer.
   * TypedArrays use null bitmaps for sparse data.
   */
  readonly line_values: Float64Array;
  readonly line_nulls: Uint8Array;

  /**
   * Error code column values - category strings (lazy but always present).
   * String arrays don't use null bitmaps.
   */
  readonly error_code_values: string[];

  /**
   * Exception stack trace column values - text strings (lazy but always present).
   * String arrays don't use null bitmaps.
   */
  readonly exception_stack_values: string[];

  /**
   * Feature flag value column - category strings (lazy but always present).
   * String arrays don't use null bitmaps.
   */
  readonly ff_value_values: string[];

  /**
   * Uint64 value column - BigUint64Array (lazy but always present).
   * TypedArrays use null bitmaps for sparse data.
   */
  readonly uint64_value_values: BigUint64Array;
  readonly uint64_value_nulls: Uint8Array;

  // ===========================================================================
  // System Column Setters (per specs/01h - always eager)
  // ===========================================================================

  /**
   * Set message for a row (span name for row 0, log message for rows 2+).
   * @returns this buffer for chaining (returns AnySpanBuffer for variance)
   */
  message(pos: number, val: string): AnySpanBuffer;

  /**
   * Set source line number for a row.
   * Per specs/01o: Injected by transformer at compile time.
   */
  line(pos: number, val: number): AnySpanBuffer;

  /**
   * Set error code for a row (used by span-err and error log entries).
   */
  error_code(pos: number, val: string): AnySpanBuffer;

  /**
   * Set exception stack trace for a row (used by span-exception).
   */
  exception_stack(pos: number, val: string): AnySpanBuffer;

  /**
   * Set feature flag value for a row (used by ff-access entries).
   */
  ff_value(pos: number, val: string): AnySpanBuffer;

  /**
   * Set uint64 value for a row (general-purpose bigint storage).
   */
  uint64_value(pos: number, val: bigint): AnySpanBuffer;

  // ===========================================================================
  // Methods
  // ===========================================================================

  /**
   * Check if this span is a parent of another span.
   * Used for tree traversal and validation.
   */
  isParentOf(other: AnySpanBuffer): boolean;

  /**
   * Check if this span is a child of another span.
   * Used for tree traversal and validation.
   */
  isChildOf(other: AnySpanBuffer): boolean;

  /**
   * Copy this span's thread_id bytes to a destination array.
   * Used for Arrow column building (zero-copy when possible).
   */
  copyThreadIdTo(dest: Uint8Array, offset: number): void;

  /**
   * Copy this span's parent_thread_id bytes to a destination array.
   * Used for Arrow column building (zero-copy when possible).
   */
  copyParentThreadIdTo(dest: Uint8Array, offset: number): void;
}

// ============================================================================
// Schema Type Helpers
// ============================================================================

type SetterValueType<S> = S extends { __schema_type: 'enum' }
  ? number
  : S extends { __schema_type: 'category' | 'text' }
    ? string
    : S extends { __schema_type: 'number' }
      ? number
      : S extends { __schema_type: 'bigUint64' }
        ? bigint
        : S extends { __schema_type: 'boolean' }
          ? boolean
          : unknown;

type ValuesArrayType<S> = S extends { __schema_type: 'enum' }
  ? Uint8Array
  : S extends { __schema_type: 'category' | 'text' }
    ? string[]
    : S extends { __schema_type: 'number' }
      ? Float64Array
      : S extends { __schema_type: 'bigUint64' }
        ? BigUint64Array
        : S extends { __schema_type: 'boolean' }
          ? Uint8Array
          : TypedArray | string[];

type FilterSchemaFields<T> = {
  [K in keyof T as T[K] extends (...args: unknown[]) => unknown ? never : K]: T[K];
};

// ============================================================================
// SpanBuffer<T> - Typed buffer WITH index signatures
// ============================================================================

/**
 * SpanBuffer<T> - Fully typed buffer for a specific schema.
 *
 * Extends AnySpanBuffer with:
 * - Schema-specific _values/_nulls properties
 * - Schema-specific setter methods
 * - Index signatures for dynamic access
 *
 * T is REQUIRED - there is no default. Use AnySpanBuffer for generic processing.
 */
export type SpanBuffer<T extends LogSchema> = AnySpanBuffer & {
  // Override tree structure with typed versions
  _children: SpanBuffer<T>[];
  _parent?: SpanBuffer<T>;
  _overflow?: SpanBuffer<T>;

  // Override methods with typed versions
  isParentOf(other: SpanBuffer<T>): boolean;
  isChildOf(other: SpanBuffer<T>): boolean;

  // Index signatures for dynamic access
  [key: `${string}_values`]: TypedArray | string[] | undefined;
  [key: `${string}_nulls`]: Uint8Array | undefined;
} & {
  // Schema-specific column data
  readonly [K in keyof FilterSchemaFields<T['fields']> as `${K & string}_values`]: ValuesArrayType<
    FilterSchemaFields<T['fields']>[K]
  >;
} & {
  readonly [K in keyof FilterSchemaFields<T['fields']> as `${K & string}_nulls`]: Uint8Array;
} & {
  // Schema-specific setter methods (return this for method chaining)
  [K in keyof FilterSchemaFields<T['fields']>]: (
    pos: number,
    val: SetterValueType<FilterSchemaFields<T['fields']>[K]>,
  ) => SpanBuffer<T>;
};
