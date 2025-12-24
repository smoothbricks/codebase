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
import type { LogBinding } from './logBinding.js';
import type { OpMetadata } from './opContext/opTypes.js';
import type { LogSchema } from './schema/LogSchema.js';
import type { TraceId, TraceRoot } from './traceId.js';

// Re-export infrastructure types
export type { LogBinding, RemappedViewConstructor } from './logBinding.js';
export type { OpMetadata } from './opContext/opTypes.js';

// Legacy alias - ModuleContext was renamed to LogBinding
export type ModuleContext = LogBinding;

// Re-export arrow-builder types for convenience
export type { AnyColumnBuffer, ColumnBuffer, ColumnValueType, TypedArray };

// ============================================================================
// AnySpanBuffer - Base type WITHOUT index signatures
// ============================================================================

/**
 * AnySpanBuffer - Core buffer API for Arrow conversion and generic processing.
 *
 * This interface has NO index signatures, making it compatible with
 * any SpanBuffer<T> regardless of schema. Use this type when you need
 * to accept any buffer (e.g., Arrow conversion, tree walking).
 */
export interface AnySpanBuffer extends AnyColumnBuffer {
  // System ArrayBuffer
  readonly _system: ArrayBuffer;
  readonly _identity: Uint8Array;

  // System Columns
  readonly timestamp: BigInt64Array;
  readonly entry_type: Uint8Array;

  // Buffer state
  _writeIndex: number;
  readonly _capacity: number;
  _overflow?: AnySpanBuffer;

  // Identity Getters
  readonly span_id: number;
  readonly thread_id: bigint;
  readonly trace_id: TraceId;
  readonly _hasParent: boolean;
  readonly parent_span_id: number;
  readonly parent_thread_id: bigint;

  // Tree Structure
  _children: AnySpanBuffer[];
  _parent?: AnySpanBuffer;

  // Context
  _traceRoot: TraceRoot;
  _logBinding: LogBinding;
  _opMetadata: OpMetadata;
  _spanName: string;
  _callsiteMetadata?: OpMetadata;
  _scopeValues: Readonly<Record<string, unknown>>;

  // System Column Setters (return AnySpanBuffer for variance)
  message(pos: number, val: string): AnySpanBuffer;
  line(pos: number, val: number): AnySpanBuffer;
  error_code(pos: number, val: string): AnySpanBuffer;
  exception_stack(pos: number, val: string): AnySpanBuffer;
  ff_value(pos: number, val: string): AnySpanBuffer;
  uint64_value(pos: number, val: bigint): AnySpanBuffer;

  // Methods
  isParentOf(other: AnySpanBuffer): boolean;
  isChildOf(other: AnySpanBuffer): boolean;
  copyThreadIdTo(dest: Uint8Array, offset: number): void;
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
  // Schema-specific setter methods (return AnySpanBuffer for variance)
  [K in keyof FilterSchemaFields<T['fields']>]: (
    pos: number,
    val: SetterValueType<FilterSchemaFields<T['fields']>[K]>,
  ) => AnySpanBuffer;
};
