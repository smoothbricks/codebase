/**
 * Test helpers for creating ModuleContext instances.
 */

import { DEFAULT_BUFFER_CAPACITY } from '@smoothbricks/arrow-builder';
import { ModuleContext } from '../moduleContext.js';
import { LogSchema } from '../schema/LogSchema.js';
import { mergeWithSystemSchema } from '../schema/systemSchema.js';
import type { SchemaFields } from '../schema/types.js';
import { createSpanBuffer as realCreateSpanBuffer } from '../spanBuffer.js';
import { createTraceId, generateTraceId } from '../traceId.js';
import type { SpanBuffer } from '../types.js';

/**
 * Create a LogSchema with system fields merged for testing.
 * Use this when calling createSpanBuffer directly in tests.
 */
export function createTestSchema<T extends SchemaFields>(fields: T): LogSchema<T> {
  const merged = mergeWithSystemSchema(fields);
  return new LogSchema(merged as T);
}

/**
 * Create a ModuleContext for testing.
 * Accepts either LogSchema or plain SchemaFields.
 */
export function createTestModuleContext(
  schema: SchemaFields | LogSchema,
  options: {
    git_sha?: string;
    package_name?: string;
    package_file?: string;
  } = {},
): ModuleContext {
  return new ModuleContext(
    options.git_sha ?? 'test-sha',
    options.package_name ?? '@test/package',
    options.package_file ?? 'src/test.ts',
    schema,
  );
}

/**
 * Test-only helper that creates a ModuleContext and SpanBuffer in one call.
 *
 * @returns Object with module and spanBuffer
 */
export function createTestSpanBuffer(
  schema: SchemaFields | LogSchema,
  options: {
    git_sha?: string;
    package_name?: string;
    package_file?: string;
    spanName?: string;
    trace_id?: string;
    capacity?: number;
  } = {},
): { __module: ModuleContext; spanBuffer: SpanBuffer<LogSchema> } {
  const module = createTestModuleContext(schema, options);
  const spanName = options.spanName ?? 'test-span';
  const traceId = options.trace_id ? createTraceId(options.trace_id) : generateTraceId();
  const capacity = options.capacity ?? DEFAULT_BUFFER_CAPACITY;

  const logSchema = schema instanceof LogSchema ? schema : module.logSchema;
  const spanBuffer = realCreateSpanBuffer(logSchema, module, spanName, traceId, capacity);

  return { module, spanBuffer };
}
