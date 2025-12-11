import { describe, expect, it } from 'bun:test';
import type { TagAttributeSchema } from '@smoothbricks/lmao';
import { createSpanBuffer, defineTagAttributes, S } from '@smoothbricks/lmao';
import { createAttributeColumns } from '../createBuilders.js';
import type { TaskContext } from '../types.js';

/**
 * Type helper to extract schema fields from ExtendedSchema
 * Removes the validation and extension methods, leaving only Sury schemas
 */
type ExtractSchemaFields<T> = Omit<T, 'validate' | 'parse' | 'safeParse' | 'extend'>;

describe('Buffer Integration', () => {
  // Helper to create a test task context
  function createTestTaskContext(schema: TagAttributeSchema): TaskContext {
    return {
      module: {
        moduleId: 1,
        gitSha: 'abc123',
        filePath: 'test.ts',
        tagAttributes: schema,
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

  it('generates TypedArray columns with proper names for defined schema', () => {
    const schema = defineTagAttributes({
      userId: S.category(), // Category: user IDs repeat
      isActive: S.boolean(),
      score: S.number(),
    });

    // Extract just the schema fields (exclude methods)
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const tagAttributes = schemaFields as ExtractSchemaFields<typeof schema> & TagAttributeSchema;

    const capacity = 64;
    const columns = createAttributeColumns(tagAttributes, capacity);

    // Keys - should have attr_ prefix
    expect(columns).toHaveProperty('attr_userId');
    expect(columns).toHaveProperty('attr_isActive');
    expect(columns).toHaveProperty('attr_score');

    // All columns should be TypedArrays
    expect(columns.attr_userId).toBeInstanceOf(Uint32Array); // category
    expect(columns.attr_isActive).toBeInstanceOf(Uint8Array); // boolean
    expect(columns.attr_score).toBeInstanceOf(Float64Array); // number
  });

  it('creates a SpanBuffer with core and attribute TypedArray columns', () => {
    const schema = defineTagAttributes({
      userId: S.category(), // Category: user IDs repeat
      score: S.number(),
    });

    // Extract just the schema fields (exclude methods)
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const tagAttributes = schemaFields as ExtractSchemaFields<typeof schema> & TagAttributeSchema;

    const taskContext = createTestTaskContext(tagAttributes);
    const capacity = 64;
    const buf = createSpanBuffer(tagAttributes, taskContext, capacity);

    // Core TypedArrays exist
    expect(buf.timestamps).toBeInstanceOf(Float64Array);
    expect(buf.operations).toBeInstanceOf(Uint8Array);

    // Attribute columns exist with correct types
    expect(buf['attr_userId']).toBeInstanceOf(Uint32Array); // category
    expect(buf['attr_score']).toBeInstanceOf(Float64Array); // number

    // Metadata
    expect(buf.capacity).toBe(capacity);
    expect(buf.writeIndex).toBe(0);
    expect(buf.children).toHaveLength(0);
  });

  it('integrates schema definition with buffer creation', () => {
    // Define schema with defineTagAttributes
    const schema = defineTagAttributes({
      requestId: S.category(), // Category: request IDs repeat
      httpStatus: S.number(),
      operation: S.enum(['GET', 'POST', 'PUT', 'DELETE']), // Enum: known HTTP methods
    });

    // Extract just the schema fields (exclude methods)
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const tagAttributes = schemaFields as ExtractSchemaFields<typeof schema> & TagAttributeSchema;

    // Create buffer with schema
    const taskContext = createTestTaskContext(tagAttributes);
    const buffer = createSpanBuffer(tagAttributes, taskContext);

    // Verify all attribute columns created as TypedArrays with correct types
    expect(buffer['attr_requestId']).toBeInstanceOf(Uint32Array); // category
    expect(buffer['attr_httpStatus']).toBeInstanceOf(Float64Array); // number
    expect(buffer['attr_operation']).toBeInstanceOf(Uint8Array); // enum

    // Verify task context is set
    expect(buffer.task).toBe(taskContext);
    expect(buffer.task.module.tagAttributes).toBe(tagAttributes);
  });

  it('handles optional fields in schema', () => {
    const schema = defineTagAttributes({
      required: S.category(), // Category string
      optional: S.optional(S.number()),
    });

    // Extract just the schema fields (exclude methods)
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const tagAttributes = schemaFields as ExtractSchemaFields<typeof schema> & TagAttributeSchema;

    const taskContext = createTestTaskContext(tagAttributes);
    const buffer = createSpanBuffer(tagAttributes, taskContext);

    // Both should have TypedArray columns
    expect(buffer['attr_required']).toBeInstanceOf(Uint32Array);
    expect(buffer['attr_optional']).toBeInstanceOf(Uint32Array);
  });

  it('handles masked fields in schema', () => {
    const schema = defineTagAttributes({
      userId: S.masked('hash'),
      email: S.masked('email'),
      plainText: S.text(), // Text: unmasked plain text
    });

    // Extract just the schema fields (exclude methods)
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const tagAttributes = schemaFields as ExtractSchemaFields<typeof schema> & TagAttributeSchema;

    const taskContext = createTestTaskContext(tagAttributes);
    const buffer = createSpanBuffer(tagAttributes, taskContext);

    // All should have TypedArray columns (masking is applied during serialization, not buffer creation)
    expect(buffer['attr_userId']).toBeInstanceOf(Uint32Array);
    expect(buffer['attr_email']).toBeInstanceOf(Uint32Array);
    expect(buffer['attr_plainText']).toBeInstanceOf(Uint32Array); // text
  });

  it('selects correct enum TypedArray size based on value count', () => {
    // Small enum (<256 values) should use Uint8Array
    const smallEnumSchema = defineTagAttributes({
      operation: S.enum(['GET', 'POST', 'PUT', 'DELETE']), // 4 values → Uint8Array
    });

    const { validate: v1, parse: p1, safeParse: s1, extend: e1, ...smallFields } = smallEnumSchema;
    const smallAttrs = smallFields as ExtractSchemaFields<typeof smallEnumSchema> & TagAttributeSchema;

    const smallContext = createTestTaskContext(smallAttrs);
    const smallBuffer = createSpanBuffer(smallAttrs, smallContext);

    // Should use Uint8Array for enums with ≤255 values
    expect(smallBuffer['attr_operation']).toBeInstanceOf(Uint8Array);
  });
});
