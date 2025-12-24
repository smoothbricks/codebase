/**
 * Test helpers for creating test buffers using the new Op-centric API.
 *
 * These helpers create LogBinding instances and SpanBuffers for testing
 * without requiring full Op/trace context setup.
 */

import { DEFAULT_BUFFER_CAPACITY } from '@smoothbricks/arrow-builder';
import { createOpMetadata, DEFAULT_METADATA } from '../opContext/defineOp.js';
import type { OpMetadata } from '../opContext/opTypes.js';
import { LogSchema } from '../schema/LogSchema.js';
import { mergeWithSystemSchema } from '../schema/systemSchema.js';
import type { SchemaFields } from '../schema/types.js';
import { createSpanBuffer } from '../spanBuffer.js';
import type { LogBinding, SpanBuffer } from '../types.js';

/**
 * Create a test OpMetadata with pre-encoded entries.
 * Uses createOpMetadata to ensure pre-encoded entries match the string values.
 */
export function createTestOpMetadata(overrides: Partial<OpMetadata> = {}): OpMetadata {
  const package_name = overrides.package_name ?? DEFAULT_METADATA.package_name;
  const package_file = overrides.package_file ?? DEFAULT_METADATA.package_file;
  const git_sha = overrides.git_sha ?? DEFAULT_METADATA.git_sha;
  const line = overrides.line ?? DEFAULT_METADATA.line;
  return createOpMetadata(package_name, package_file, git_sha, line);
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
 * It contains the schema, capacity stats, and optional remapping class.
 *
 * @param schema - LogSchema or SchemaFields to use (will be wrapped in LogSchema if needed)
 * @param options - Optional capacity and stats overrides
 * @returns LogBinding ready for use with createSpanBuffer
 */
export function createTestLogBinding<T extends SchemaFields>(
  schema: SchemaFields | LogSchema<T>,
  options: {
    capacity?: number;
    sb_totalWrites?: number;
    sb_overflowWrites?: number;
    sb_totalCreated?: number;
    sb_overflows?: number;
  } = {},
): LogBinding {
  // Wrap in LogSchema if plain SchemaFields provided
  const logSchema = schema instanceof LogSchema ? schema : createTestSchema(schema);

  return {
    logSchema,
    remappedViewClass: undefined,
    sb_capacity: options.capacity ?? DEFAULT_BUFFER_CAPACITY,
    sb_totalWrites: options.sb_totalWrites ?? 0,
    sb_overflowWrites: options.sb_overflowWrites ?? 0,
    sb_totalCreated: options.sb_totalCreated ?? 0,
    sb_overflows: options.sb_overflows ?? 0,
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
  schema: SchemaFields | LogSchema<T>,
  options: {
    spanName?: string;
    trace_id?: string;
    capacity?: number;
  } = {},
): { logBinding: LogBinding; spanBuffer: SpanBuffer<LogSchema<T>> } {
  // Wrap in LogSchema if plain SchemaFields provided
  const logSchema = schema instanceof LogSchema ? (schema as LogSchema<T>) : createTestSchema(schema);

  // Create LogBinding
  const logBinding = createTestLogBinding(logSchema, {
    capacity: options.capacity,
  });

  // Create SpanBuffer using the new API
  const spanBuffer = createSpanBuffer(
    logSchema,
    logBinding,
    options.spanName ?? 'test-span',
    options.trace_id,
    options.capacity ?? DEFAULT_BUFFER_CAPACITY,
  );

  // Set _opMetadata for Arrow conversion (tests bypass tracer which normally sets this)
  // This is necessary because createSpanBuffer doesn't set _opMetadata - that's done by tracer.ts
  spanBuffer._opMetadata = DEFAULT_METADATA;

  return { logBinding, spanBuffer };
}
