import { describe, it, expect } from 'bun:test';
import { createEmptySpanBuffer, createSpanBuffer } from '../createSpanBuffer.js';
import { defineTagAttributes, S } from '@smoothbricks/lmao';
import type { TaskContext } from '../types.js';
import type { TagAttributeSchema } from '@smoothbricks/lmao';

/**
 * Type helper to extract schema fields from ExtendedSchema
 * Removes the validation and extension methods, leaving only Sury schemas
 */
type ExtractSchemaFields<T> = Omit<T, 'validate' | 'parse' | 'safeParse' | 'extend'>;

describe('Buffer Foundation', () => {
  // Helper to create a test task context
  function createTestTaskContext(): TaskContext {
    const schema = defineTagAttributes({
      userId: S.category(),  // Category: user IDs repeat
      count: S.number()
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
          totalBuffersCreated: 0
        }
      },
      spanNameId: 1,
      lineNumber: 10
    };
  }

  it('creates empty SpanBuffer with TypedArrays', () => {
    const taskContext = createTestTaskContext();
    const schema = taskContext.module.tagAttributes;
    
    const buf = createEmptySpanBuffer(1, schema, taskContext, undefined, 64);

    expect(buf.spanId).toBe(1);
    expect(buf.traceId).toBeDefined();
    
    // Check TypedArrays are created
    expect(buf.timestamps).toBeInstanceOf(Float64Array);
    expect(buf.operations).toBeInstanceOf(Uint8Array);
    expect(buf.nullBitmap).toBeDefined();
    
    // Check attribute columns exist for each schema field
    expect(buf['attr_userId']).toBeInstanceOf(Uint32Array); // category → Uint32Array
    expect(buf['attr_count']).toBeInstanceOf(Float64Array);  // number → Float64Array

    // Metadata
    expect(buf.children).toBeInstanceOf(Array);
    expect(buf.writeIndex).toBe(0);
    expect(buf.capacity).toBe(64);
    expect(buf.task).toBe(taskContext);
  });

  it('creates root SpanBuffer with createSpanBuffer', () => {
    const taskContext = createTestTaskContext();
    const schema = taskContext.module.tagAttributes;
    
    const buf = createSpanBuffer(schema, taskContext);
    
    expect(buf.spanId).toBeGreaterThan(0);
    expect(buf.parent).toBeUndefined();
    expect(buf.children).toHaveLength(0);
  });

  it('tracks buffer creation in capacity stats', () => {
    const taskContext = createTestTaskContext();
    const schema = taskContext.module.tagAttributes;
    
    const initialCount = taskContext.module.spanBufferCapacityStats.totalBuffersCreated;
    
    createEmptySpanBuffer(1, schema, taskContext, undefined, 64);
    
    expect(taskContext.module.spanBufferCapacityStats.totalBuffersCreated).toBe(initialCount + 1);
  });

  it('handles different schema sizes', () => {
    const taskContext = createTestTaskContext();
    // Define a larger schema
    const largeSchema = defineTagAttributes({
      field1: S.category(),  // Category string
      field2: S.number(),
      field3: S.boolean(),
      field4: S.text(),      // Text string
      field5: S.number(),
    });
    
    // Extract just the schema fields (exclude methods)
    const { validate, parse, safeParse, extend, ...schemaFields } = largeSchema;
    const tagAttributes = schemaFields as ExtractSchemaFields<typeof largeSchema> & TagAttributeSchema;
    
    const buf = createEmptySpanBuffer(1, tagAttributes, taskContext, undefined, 64);
    
    // Should have TypedArray columns for all 5 attributes
    // Get all attr_ keys from the buffer
    const attrKeys = Object.keys(buf).filter(k => k.startsWith('attr_'));
    expect(attrKeys).toHaveLength(5);
    expect(buf['attr_field1']).toBeInstanceOf(Uint32Array); // category
    expect(buf['attr_field4']).toBeInstanceOf(Uint32Array); // text
  });
});
