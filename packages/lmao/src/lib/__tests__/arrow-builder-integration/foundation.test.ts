import { describe, expect, it } from 'bun:test';
import type { TagAttributeSchema } from '@smoothbricks/lmao';
import { createEmptySpanBuffer, createSpanBuffer, defineTagAttributes, S } from '@smoothbricks/lmao';
import type { TaskContext } from '../../types.js';

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

    return {
      module: {
        moduleId: 1,
        gitSha: 'abc123',
        filePath: 'test.ts',
        tagAttributes,
        spanBufferCapacityStats: {
          currentCapacity: 64,
          totalWrites: 0,
          overflowWrites: 0,
          totalBuffersCreated: 0,
        },
      },
      spanNameId: 1,
      lineNumber: 10,
    };
  }

  it('creates empty SpanBuffer with TypedArrays', () => {
    const taskContext = createTestTaskContext();
    const schema = taskContext.module.tagAttributes;
    const threadId = BigInt('0x123456789ABCDEF0');

    const buf = createEmptySpanBuffer(1, threadId, 'trace-123', schema, taskContext, undefined, 64);

    expect(buf.localSpanId).toBe(1);
    expect(buf.threadId).toBe(threadId);
    expect(buf.traceId).toBe('trace-123');

    // Check TypedArrays are created
    expect(buf.timestamps).toBeInstanceOf(Float64Array);
    expect(buf.operations).toBeInstanceOf(Uint8Array);

    // Check null bitmaps exist for each attribute (Arrow format: 1 Uint8Array per column)
    expect(buf.attr_userId_nulls).toBeInstanceOf(Uint8Array);
    expect(buf.attr_count_nulls).toBeInstanceOf(Uint8Array);

    // Check attribute columns exist for each schema field (using _values suffix)
    // category columns now use string[] arrays for zero-cost hot path writes
    expect(Array.isArray(buf.attr_userId_values)).toBe(true); // category → string[]
    expect(buf.attr_count_values).toBeInstanceOf(Float64Array); // number → Float64Array

    // Metadata
    expect(buf.children).toBeInstanceOf(Array);
    expect(buf.writeIndex).toBe(0);
    expect(buf.capacity).toBe(64);
    expect(buf.task).toBe(taskContext);
  });

  it('creates root SpanBuffer with createSpanBuffer', () => {
    const taskContext = createTestTaskContext();
    const schema = taskContext.module.tagAttributes;

    const buf = createSpanBuffer(schema, taskContext, 'trace-999');

    expect(buf.localSpanId).toBeGreaterThan(0);
    expect(buf.threadId).toBeDefined();
    expect(buf.parent).toBeUndefined();
    expect(buf.children).toHaveLength(0);
  });

  it('tracks buffer creation in capacity stats', () => {
    const taskContext = createTestTaskContext();
    const schema = taskContext.module.tagAttributes;
    const threadId = BigInt('0x123456789ABCDEF0');

    const initialCount = taskContext.module.spanBufferCapacityStats.totalBuffersCreated;

    createEmptySpanBuffer(1, threadId, 'trace-456', schema, taskContext, undefined, 64);

    expect(taskContext.module.spanBufferCapacityStats.totalBuffersCreated).toBe(initialCount + 1);
  });

  it('handles different schema sizes', () => {
    const taskContext = createTestTaskContext();
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

    const threadId = BigInt('0x123456789ABCDEF0');
    const buf = createEmptySpanBuffer(1, threadId, 'trace-789', tagAttributes, taskContext, undefined, 64);

    // Should have TypedArray columns for all 5 attributes (each has _values and _nulls)
    // Note: Columns are lazy-allocated via getters, so Object.keys() won't find them
    // Access them directly to trigger allocation and verify they exist
    expect(Array.isArray(buf['attr_field1_values'])).toBe(true); // category (raw strings)expect(buf['attr_field1_nulls']).toBeInstanceOf(Uint8Array); // null bitmap
    expect(buf['attr_field2_values']).toBeInstanceOf(Float64Array); // number
    expect(buf['attr_field2_nulls']).toBeInstanceOf(Uint8Array);
    expect(buf['attr_field3_values']).toBeInstanceOf(Uint8Array); // boolean
    expect(buf['attr_field3_nulls']).toBeInstanceOf(Uint8Array);
    expect(Array.isArray(buf['attr_field4_values'])).toBe(true); // category (raw strings)expect(buf['attr_field4_nulls']).toBeInstanceOf(Uint8Array);
    expect(buf['attr_field5_values']).toBeInstanceOf(Float64Array); // number
    expect(buf['attr_field5_nulls']).toBeInstanceOf(Uint8Array);
  });
});
