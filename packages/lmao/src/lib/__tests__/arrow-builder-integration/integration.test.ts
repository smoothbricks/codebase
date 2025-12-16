import { describe, expect, it } from 'bun:test';
import { createAttributeColumns } from '@smoothbricks/arrow-builder';
import type { TagAttributeSchema } from '@smoothbricks/lmao';
import { createSpanBuffer, defineTagAttributes, S } from '@smoothbricks/lmao';
import { createTestTaskContext } from '../test-helpers.js';

/**
 * Type helper to extract schema fields from ExtendedSchema
 * Removes the validation and extension methods, leaving only Sury schemas
 */
type ExtractSchemaFields<T> = Omit<T, 'validate' | 'parse' | 'safeParse' | 'extend'>;

describe('Buffer Integration', () => {
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

    // Keys - should have  prefix
    expect(columns).toHaveProperty('userId');
    expect(columns).toHaveProperty('isActive');
    expect(columns).toHaveProperty('score');

    // All columns should be TypedArrays
    expect(columns.userId).toBeInstanceOf(Uint32Array); // category
    expect(columns.isActive).toBeInstanceOf(Uint8Array); // boolean
    expect(columns.score).toBeInstanceOf(Float64Array); // number
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
    expect(buf.timestamps).toBeInstanceOf(BigInt64Array);
    expect(buf.operations).toBeInstanceOf(Uint8Array);

    // Attribute columns exist with correct types
    expect(Array.isArray(buf['userId'])).toBe(true); // category (raw strings)
    expect(buf['score']).toBeInstanceOf(Float64Array); // number

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
    expect(Array.isArray(buffer['requestId'])).toBe(true); // category (raw strings)
    expect(buffer['httpStatus']).toBeInstanceOf(Float64Array); // number
    expect(buffer['operation']).toBeInstanceOf(Uint8Array); // enum

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
    expect(Array.isArray(buffer['required'])).toBe(true); // category (raw strings)
    // Note: S.optional() wraps the inner schema, losing __schema_type metadata
    // This falls back to Uint32Array (default)
    expect(buffer['optional']).toBeInstanceOf(Uint32Array);
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
    // Note: S.masked() creates a transformed Sury schema without __schema_type metadata
    // This falls back to Uint32Array (default)
    expect(buffer['userId']).toBeInstanceOf(Uint32Array); // masked hash (no metadata)
    expect(buffer['email']).toBeInstanceOf(Uint32Array); // masked email (no metadata)
    expect(Array.isArray(buffer['plainText'])).toBe(true); // text (raw strings)
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
    expect(smallBuffer['operation']).toBeInstanceOf(Uint8Array);
  });
});
