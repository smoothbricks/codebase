/**
 * Test helpers for creating test buffers using the new Op-centric API.
 *
 * These helpers create LogBinding instances and SpanBuffers for testing
 * without requiring full Op/trace context setup.
 */

import { DEFAULT_BUFFER_CAPACITY } from '@smoothbricks/arrow-builder';
import { createSpanLogger, type SpanLoggerImpl } from '../codegen/spanLoggerGenerator.js';
import { defineOpContext } from '../defineOpContext.js';
import { JsBufferStrategy } from '../JsBufferStrategy.js';
import { createOpMetadata, DEFAULT_METADATA } from '../opContext/defineOp.js';
import type {
  ColumnMapping,
  MappedOpGroup,
  MappedOpGroupInternal,
  OpGroup,
  OpGroupInternal,
  SchemaFieldsOf,
} from '../opContext/opGroupTypes.js';
import type { OpMetadata } from '../opContext/opTypes.js';
import type { OpContext } from '../opContext/types.js';
import { LogSchema } from '../schema/LogSchema.js';
import { mergeWithSystemSchema } from '../schema/systemSchema.js';
import type { SchemaFields } from '../schema/types.js';
import { createSpanBuffer } from '../spanBuffer.js';
import { createTraceId, type TraceId } from '../traceId.js';
import type { ITraceRoot, TracerLifecycleHooks } from '../traceRoot.js';
import { createTraceRoot } from '../traceRoot.node.js';
import type { TracerOptions } from '../tracer.js';
import { TestTracer } from '../tracers/TestTracer.js';
import type { LogBinding, SpanBuffer } from '../types.js';

// Create a minimal OpContextBinding for shared test trace roots.
const minimalOpContext = defineOpContext({ logSchema: new LogSchema({}) });

function requireLogSchema(value: unknown, label: string): LogSchema {
  if (!(value instanceof LogSchema)) {
    throw new Error(`Expected ${label} to be a LogSchema`);
  }
  return value;
}

function assertColumnMapping<LibSchema extends SchemaFields>(
  value: unknown,
  label: string,
): asserts value is ColumnMapping<LibSchema> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error(`Expected ${label} to be a column mapping record`);
  }
  for (const [key, mappingValue] of Object.entries(value)) {
    if (typeof mappingValue !== 'string' && mappingValue !== null) {
      throw new Error(`Expected column mapping '${key}' in ${label} to be a string or null`);
    }
  }
}

function assertOpGroupInternals<Ctx extends OpContext>(
  opGroup: OpGroup<Ctx>,
  label: string,
): asserts opGroup is OpGroupInternal<Ctx> {
  requireLogSchema(Reflect.get(opGroup, '_logSchema'), `${label}._logSchema`);
}

function assertMappedOpGroupInternals<Ctx extends OpContext, ContributedSchema extends SchemaFields>(
  opGroup: MappedOpGroup<Ctx, ContributedSchema>,
  label: string,
): asserts opGroup is MappedOpGroupInternal<Ctx, ContributedSchema> {
  requireLogSchema(Reflect.get(opGroup, '_logSchema'), `${label}._logSchema`);
  const columnMapping = Reflect.get(opGroup, '_columnMapping');
  assertColumnMapping<SchemaFieldsOf<Ctx['logSchema']>>(columnMapping, `${label}._columnMapping`);
}

export function requireStringArray(value: unknown, label: string): string[] {
  if (!Array.isArray(value) || value.some((entry) => typeof entry !== 'string')) {
    throw new Error(`Expected ${label} to be a string[]`);
  }
  return value;
}

export function requireFloat64Array(value: unknown, label: string): Float64Array {
  if (!(value instanceof Float64Array)) {
    throw new Error(`Expected ${label} to be a Float64Array`);
  }
  return value;
}

export function requireUint8Array(value: unknown, label: string): Uint8Array {
  if (!(value instanceof Uint8Array)) {
    throw new Error(`Expected ${label} to be a Uint8Array`);
  }
  return value;
}

/**
 * Default TracerOptions for tests.
 * Includes JsBufferStrategy and createTraceRoot.
 */
export function createTestTracerOptions<T extends LogSchema>(): TracerOptions<T> {
  return {
    bufferStrategy: new JsBufferStrategy<T>(),
    createTraceRoot,
  };
}

/**
 * Shared TestTracer instance for tests that create buffers directly.
 * Typed as TracerLifecycleHooks because that's what TraceRoot expects.
 */
export const TEST_TRACER: TracerLifecycleHooks = new TestTracer(minimalOpContext, createTestTracerOptions());

type TestSpanBufferBundle<T extends SchemaFields> = {
  logBinding: LogBinding;
  spanBuffer: SpanBuffer<LogSchema<T>>;
};

type TestLoggerBundle<T extends LogSchema> = {
  buffer: SpanBuffer<T>;
  logger: SpanLoggerImpl<T>;
  logBinding: LogBinding;
};

/**
 * Create a TraceRoot for testing.
 *
 * TraceRoot contains trace_id, timestamp anchors, and tracer reference.
 * This helper creates one with sensible defaults for tests.
 *
 * @param traceId - Optional trace ID (defaults to 'test-trace')
 * @param tracer - Optional tracer (defaults to TEST_TRACER)
 */
export function createTestTraceRoot(traceId?: TraceId | string, tracer?: TracerLifecycleHooks): ITraceRoot {
  // Accept string or TraceId - convert string to TraceId
  const resolvedTraceId = traceId
    ? typeof traceId === 'string'
      ? createTraceId(traceId)
      : traceId
    : createTraceId('test-trace');

  return createTraceRoot(resolvedTraceId, tracer ?? TEST_TRACER);
}

/**
 * Create a test OpMetadata with pre-encoded entries.
 * Uses createOpMetadata to ensure pre-encoded entries match the string values.
 */
export function createTestOpMetadata(overrides: Partial<OpMetadata> = {}): OpMetadata {
  const name = overrides.name ?? DEFAULT_METADATA.name;
  const package_name = overrides.package_name ?? DEFAULT_METADATA.package_name;
  const package_file = overrides.package_file ?? DEFAULT_METADATA.package_file;
  const git_sha = overrides.git_sha ?? DEFAULT_METADATA.git_sha;
  const line = overrides.line ?? DEFAULT_METADATA.line;
  return createOpMetadata(name, package_name, package_file, git_sha, line);
}

/**
 * Create a LogSchema with system fields merged for testing.
 * Use this when calling createSpanBuffer directly in tests.
 */
export function createTestSchema<T extends SchemaFields>(fields: T): LogSchema<T> {
  const merged = mergeWithSystemSchema(fields);
  return new LogSchema(merged as T);
}

/**
 * Create a LogBinding for testing.
 *
 * LogBinding is the infrastructure object that ops use to create buffers.
 * It contains the schema and optional remapping class.
 *
 * NOTE: Stats are NO LONGER on LogBinding - they are on SpanBufferClass.stats (static property).
 * See agent-todo/opgroup-refactor.md lines 58-70, 525-547 for rationale.
 *
 * @param schema - LogSchema or SchemaFields to use (will be wrapped in LogSchema if needed)
 * @returns LogBinding ready for use with createSpanBuffer
 */
export function createTestLogBinding<T extends SchemaFields>(schema: LogSchema<T>): LogBinding;
export function createTestLogBinding<T extends SchemaFields>(schema: T): LogBinding;
export function createTestLogBinding<T extends SchemaFields>(schema: T | LogSchema<T>): LogBinding {
  // Wrap in LogSchema if plain SchemaFields provided
  const logSchema = schema instanceof LogSchema ? schema : createTestSchema(schema);

  return {
    logSchema,
    remappedViewClass: undefined,
  };
}

/**
 * Test-only helper that creates a LogBinding and SpanBuffer in one call.
 *
 * This is a convenience helper for tests that need both objects.
 * Creates a root span buffer (no parent).
 *
 * @param schema - LogSchema or SchemaFields to use
 * @param options - Configuration for the buffer
 * @returns Object with logBinding and spanBuffer
 */
export function createTestSpanBuffer<T extends SchemaFields>(
  schema: LogSchema<T>,
  options?: {
    trace_id?: TraceId;
    capacity?: number;
  },
): TestSpanBufferBundle<T>;
export function createTestSpanBuffer<T extends SchemaFields>(
  schema: T,
  options?: {
    trace_id?: TraceId;
    capacity?: number;
  },
): TestSpanBufferBundle<T>;
export function createTestSpanBuffer<T extends SchemaFields>(
  schema: T | LogSchema<T>,
  options: {
    trace_id?: TraceId;
    capacity?: number;
  } = {},
) {
  // Wrap in LogSchema if plain SchemaFields provided
  const logSchema: LogSchema<T> = schema instanceof LogSchema ? schema : createTestSchema(schema);

  // Create LogBinding (stats are on SpanBufferClass.stats, not LogBinding)
  const logBinding = createTestLogBinding(logSchema);

  // Create TraceRoot with provided or default trace_id
  const traceRoot = createTestTraceRoot(options.trace_id);

  // Create SpanBuffer using the Phase 2 API (no LogBinding parameter)
  const spanBuffer = createSpanBuffer(
    logSchema,
    traceRoot,
    DEFAULT_METADATA,
    options.capacity ?? DEFAULT_BUFFER_CAPACITY,
  );

  return { logBinding, spanBuffer };
}

/**
 * Create a properly typed SpanLogger for testing.
 *
 * Returns buffer and logger so tests can verify buffer contents directly.
 * Uses the real createSpanLogger API - no mocks.
 *
 * @param schema - LogSchema to use
 * @returns Object with buffer and logger, both properly typed
 */
export function createTestLogger<T extends LogSchema>(schema: T): TestLoggerBundle<T> {
  const logBinding = createTestLogBinding(schema);
  const traceRoot = createTestTraceRoot();
  const buffer = createSpanBuffer(schema, traceRoot, DEFAULT_METADATA);
  const logger = createSpanLogger(schema, buffer);
  return { buffer, logger, logBinding };
}

// =============================================================================
// OPGROUP INTERNAL ACCESS HELPERS
// =============================================================================

/**
 * Get internal properties from an OpGroup.
 *
 * OpGroup's internal properties (_logSchema, _flags) are hidden from
 * intellisense but accessible at runtime. This helper provides typed
 * access for tests that need to verify internal state.
 *
 * @param opGroup - The OpGroup to access
 * @returns Internal interface with _logSchema and _flags
 */
export function getOpGroupInternals<Ctx extends OpContext>(
  opGroup: OpGroup<Ctx>,
): Pick<OpGroupInternal<Ctx>, '_logSchema'> {
  assertOpGroupInternals(opGroup, 'OpGroup');
  return { _logSchema: opGroup._logSchema };
}

/**
 * Get internal properties from a MappedOpGroup.
 *
 * MappedOpGroup's internal properties (_logSchema, _flags, _columnMapping,
 * _contributedSchema) are hidden from intellisense but accessible at runtime.
 * This helper provides typed access for tests.
 *
 * @param opGroup - The MappedOpGroup to access
 * @returns Internal interface with all internal properties
 */
export function getMappedOpGroupInternals<Ctx extends OpContext, ContributedSchema extends SchemaFields>(
  opGroup: MappedOpGroup<Ctx, ContributedSchema>,
): Pick<MappedOpGroupInternal<Ctx, ContributedSchema>, '_logSchema' | '_columnMapping'> {
  assertMappedOpGroupInternals(opGroup, 'MappedOpGroup');
  return {
    _logSchema: opGroup._logSchema,
    _columnMapping: opGroup._columnMapping,
  };
}

/**
 * Convenience type alias for accessing column mapping from a MappedOpGroup.
 */
export type { ColumnMapping, SchemaFieldsOf };

// Re-export createTraceId for tests that need it
export { createTraceId };

/**
 * Simple buffer creation helper for tests.
 *
 * This provides a simpler API than createSpanBuffer by using defaults for
 * trace_id and opMetadata. Tests that need specific values should use
 * createSpanBuffer directly.
 *
 * @param schema - LogSchema to use
 * @param capacity - Buffer capacity (optional, uses default from class stats)
 */
export function createBuffer<T extends LogSchema>(schema: T, capacity?: number): SpanBuffer<T> {
  return createSpanBuffer(schema, createTestTraceRoot(), DEFAULT_METADATA, capacity);
}
