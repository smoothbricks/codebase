import { describe, it, expect } from 'bun:test';
import { createAttributeBuilders } from '../createBuilders.js';
import { createSpanBuffer } from '../createSpanBuffer.js';
import { defineTagAttributes } from '../../schema/defineTagAttributes.js';
import { S } from '../../schema/builder.js';
import type { TaskContext } from '../types.js';
import type { TagAttributeSchema } from '../../schema/types.js';

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
          totalBuffersCreated: 0
        }
      },
      spanNameId: 1,
      lineNumber: 10
    };
  }

  it('generates Arrow builders with proper names for defined schema', () => {
    const schema = defineTagAttributes({
      userId: S.string(),
      isActive: S.boolean(),
      score: S.number(),
    });

    // Extract just the schema fields (exclude methods)
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const tagAttributes = schemaFields as ExtractSchemaFields<typeof schema> & TagAttributeSchema;

    const capacity = 64;
    const builders = createAttributeBuilders(tagAttributes, capacity);

    // Keys - should have attr_ prefix
    expect(builders).toHaveProperty('attr_userId');
    expect(builders).toHaveProperty('attr_isActive');
    expect(builders).toHaveProperty('attr_score');

    // All builders should be defined
    expect(builders.attr_userId).toBeDefined();
    expect(builders.attr_isActive).toBeDefined();
    expect(builders.attr_score).toBeDefined();
  });

  it('creates a SpanBuffer with core and attribute Arrow builders', () => {
    const schema = defineTagAttributes({
      userId: S.string(),
      score: S.number(),
    });

    // Extract just the schema fields (exclude methods)
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const tagAttributes = schemaFields as ExtractSchemaFields<typeof schema> & TagAttributeSchema;

    const taskContext = createTestTaskContext(tagAttributes);
    const capacity = 64;
    const buf = createSpanBuffer(tagAttributes, taskContext, capacity);

    // Core builders exist
    expect(buf.timestampBuilder).toBeDefined();
    expect(buf.operationBuilder).toBeDefined();
    
    // Attribute builders exist
    expect(buf.attributeBuilders).toHaveProperty('attr_userId');
    expect(buf.attributeBuilders).toHaveProperty('attr_score');

    // Metadata
    expect(buf.capacity).toBe(capacity);
    expect(buf.writeIndex).toBe(0);
    expect(buf.children).toHaveLength(0);
  });

  it('integrates schema definition with buffer creation', () => {
    // Define schema with defineTagAttributes
    const schema = defineTagAttributes({
      requestId: S.string(),
      httpStatus: S.number(),
      operation: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
    });

    // Extract just the schema fields (exclude methods)
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const tagAttributes = schemaFields as ExtractSchemaFields<typeof schema> & TagAttributeSchema;

    // Create buffer with schema
    const taskContext = createTestTaskContext(tagAttributes);
    const buffer = createSpanBuffer(tagAttributes, taskContext);

    // Verify all attribute columns created
    expect(buffer.attributeBuilders['attr_requestId']).toBeDefined();
    expect(buffer.attributeBuilders['attr_httpStatus']).toBeDefined();
    expect(buffer.attributeBuilders['attr_operation']).toBeDefined();

    // Verify task context is set
    expect(buffer.task).toBe(taskContext);
    expect(buffer.task.module.tagAttributes).toBe(tagAttributes);
  });

  it('handles optional fields in schema', () => {
    const schema = defineTagAttributes({
      required: S.string(),
      optional: S.optional(S.number()),
    });

    // Extract just the schema fields (exclude methods)
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const tagAttributes = schemaFields as ExtractSchemaFields<typeof schema> & TagAttributeSchema;

    const taskContext = createTestTaskContext(tagAttributes);
    const buffer = createSpanBuffer(tagAttributes, taskContext);

    // Both should have builders
    expect(buffer.attributeBuilders['attr_required']).toBeDefined();
    expect(buffer.attributeBuilders['attr_optional']).toBeDefined();
  });

  it('handles masked fields in schema', () => {
    const schema = defineTagAttributes({
      userId: S.masked('hash'),
      email: S.masked('email'),
      plainText: S.string(),
    });

    // Extract just the schema fields (exclude methods)
    const { validate, parse, safeParse, extend, ...schemaFields } = schema;
    const tagAttributes = schemaFields as ExtractSchemaFields<typeof schema> & TagAttributeSchema;

    const taskContext = createTestTaskContext(tagAttributes);
    const buffer = createSpanBuffer(tagAttributes, taskContext);

    // All should have builders (masking is applied during serialization, not buffer creation)
    expect(buffer.attributeBuilders['attr_userId']).toBeDefined();
    expect(buffer.attributeBuilders['attr_email']).toBeDefined();
    expect(buffer.attributeBuilders['attr_plainText']).toBeDefined();
  });
});
