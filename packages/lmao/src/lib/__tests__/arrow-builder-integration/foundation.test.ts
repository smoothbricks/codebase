import { describe, expect, it } from 'bun:test';
import { ModuleContext } from '../../moduleContext.js';
import { S } from '../../schema/builder.js';
import { defineTagAttributes } from '../../schema/defineTagAttributes.js';
import type { TagAttributeSchema } from '../../schema/types.js';
import { createSpanBuffer } from '../../spanBuffer.js';
import { TaskContext } from '../../taskContext.js';
import { createTraceId } from '../../traceId.js';

/**
 * Type helper to extract schema fields from ExtendedSchema
 * Removes the validation and extension methods, leaving only Sury schemas
 */
type ExtractSchemaFields<T> = Omit<T, 'validate' | 'parse' | 'safeParse' | 'extend'>;

describe('Buffer Foundation', () => {
  // Helper to create a test task context
  function createTestTaskContext(): TaskContext {
    const schema = defineTagAttributes({
      userId: S.category(), // Category: user IDs repeat
      count: S.number(),
    });

    // Extract just the schema fields (exclude methods like validate, parse, etc.)
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const tagAttributes = schemaFields as ExtractSchemaFields<typeof schema> & TagAttributeSchema;

    const moduleContext = new ModuleContext(1, 'abc123', 'test.ts', tagAttributes);
    return new TaskContext(moduleContext, 1, 10);
  }

  it('creates SpanBuffer with TypedArrays', () => {
    const taskContext = createTestTaskContext();
    const schema = taskContext.module.tagAttributes;
    const traceId = createTraceId('trace-123');

    const buf = createSpanBuffer(schema, taskContext, traceId, 64);

    // Span identity assertions (unified memory layout)
    expect(typeof buf.spanId).toBe('number');
    expect(buf.spanId).toBeGreaterThan(0);
    expect(buf.hasParent).toBe(false);
    expect(buf.traceId).toBe(traceId);

    // Check TypedArrays are created
    expect(buf.timestamps).toBeInstanceOf(BigInt64Array);
    expect(buf.operations).toBeInstanceOf(Uint8Array);

    // Check null bitmaps exist for each attribute (Arrow format: 1 Uint8Array per column)
    expect(buf.userId_nulls).toBeInstanceOf(Uint8Array);
    expect(buf.count_nulls).toBeInstanceOf(Uint8Array);

    // Check attribute columns exist for each schema field (using _values suffix)
    // category columns now use string[] arrays for zero-cost hot path writes
    expect(Array.isArray(buf.userId_values)).toBe(true); // category → string[]
    expect(buf.count_values).toBeInstanceOf(Float64Array); // number → Float64Array

    // Metadata
    expect(buf.children).toBeInstanceOf(Array);
    expect(buf.writeIndex).toBe(0);
    expect(buf.capacity).toBe(64);
    expect(buf.task).toBe(taskContext);
  });

  it('creates root SpanBuffer with createSpanBuffer', () => {
    const taskContext = createTestTaskContext();
    const schema = taskContext.module.tagAttributes;

    const buf = createSpanBuffer(schema, taskContext, createTraceId('trace-999'));

    expect(buf.spanId).toBeGreaterThan(0);
    expect(buf.hasParent).toBe(false);
    expect(buf.parent).toBeUndefined();
    expect(buf.children).toHaveLength(0);
  });

  it('tracks buffer creation in capacity stats', () => {
    const taskContext = createTestTaskContext();
    const schema = taskContext.module.tagAttributes;

    const initialCount = taskContext.module.spanBufferCapacityStats.totalBuffersCreated;

    createSpanBuffer(schema, taskContext, createTraceId('trace-456'), 64);

    expect(taskContext.module.spanBufferCapacityStats.totalBuffersCreated).toBe(initialCount + 1);
  });

  it('handles different schema sizes', () => {
    // Define a larger schema
    const largeSchema = defineTagAttributes({
      field1: S.category(), // Category string
      field2: S.number(),
      field3: S.boolean(),
      field4: S.text(), // Text string
      field5: S.number(),
    });

    // Extract just the schema fields (exclude methods)
    const { validate, parse, safeParse, extend, ...schemaFields } = largeSchema;
    const tagAttributes = schemaFields as ExtractSchemaFields<typeof largeSchema> & TagAttributeSchema;

    // Create task context with larger schema
    const moduleContext = new ModuleContext(2, 'abc123', 'test.ts', tagAttributes);
    const taskContext = new TaskContext(moduleContext, 1, 10);

    const buf = createSpanBuffer(tagAttributes, taskContext, createTraceId('trace-789'), 64);

    // Should have TypedArray columns for all 5 attributes (each has _values and _nulls)
    // Note: Columns are lazy-allocated via getters, so Object.keys() won't find them
    // Access them directly to trigger allocation and verify they exist
    expect(Array.isArray(buf['field1_values'])).toBe(true); // category (raw strings)
    expect(buf['field1_nulls']).toBeInstanceOf(Uint8Array); // null bitmap
    expect(buf['field2_values']).toBeInstanceOf(Float64Array); // number
    expect(buf['field2_nulls']).toBeInstanceOf(Uint8Array);
    expect(buf['field3_values']).toBeInstanceOf(Uint8Array); // boolean
    expect(buf['field3_nulls']).toBeInstanceOf(Uint8Array);
    expect(Array.isArray(buf['field4_values'])).toBe(true); // text (raw strings)
    expect(buf['field4_nulls']).toBeInstanceOf(Uint8Array);
    expect(buf['field5_values']).toBeInstanceOf(Float64Array); // number
    expect(buf['field5_nulls']).toBeInstanceOf(Uint8Array);
  });
});
