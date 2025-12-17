import { beforeEach, describe, expect, it } from 'bun:test';
import { DEFAULT_BUFFER_CAPACITY } from '@smoothbricks/arrow-builder';
import type { TagAttributeSchema } from '@smoothbricks/lmao';
import { createNextBuffer, createSpanBuffer, defineTagAttributes, S } from '@smoothbricks/lmao';
import type { TaskContext } from '../../types.js';
import { createTestTaskContext } from '../test-helpers.js';

/**
 * Type helper to extract schema fields from ExtendedSchema
 */
type ExtractSchemaFields<T> = Omit<T, 'validate' | 'parse' | 'safeParse' | 'extend'>;

describe('Buffer Chaining', () => {
  let taskContext: TaskContext;
  let schema: TagAttributeSchema;

  beforeEach(() => {
    const schemaDefinition = defineTagAttributes({
      userId: S.category(),
      requestId: S.category(),
      operation: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
      duration: S.number(),
    });

    const { validate, parse, safeParse, extend, ...schemaFields } = schemaDefinition;
    schema = schemaFields as ExtractSchemaFields<typeof schemaDefinition> & TagAttributeSchema;

    taskContext = createTestTaskContext(schema, { lineNumber: 10 });
  });

  describe('createNextBuffer', () => {
    it('should create a chained buffer with same spanId and traceId', () => {
      const buffer = createSpanBuffer(schema, taskContext);
      const nextBuffer = createNextBuffer(buffer);

      // Should inherit spanId and traceId
      expect(nextBuffer.spanId).toBe(buffer.spanId);
      expect(nextBuffer.traceId).toBe(buffer.traceId);

      // Should be linked via next property
      expect(buffer.next).toBe(nextBuffer);

      // Should have same parent
      expect(nextBuffer.parent).toBe(buffer.parent);

      // Should have same task context
      expect(nextBuffer.task).toBe(buffer.task);
    });

    it('should create buffer with current capacity from stats', () => {
      const buffer = createSpanBuffer(schema, taskContext);

      // Update capacity stats
      taskContext.module.spanBufferCapacityStats.currentCapacity = 128;

      const nextBuffer = createNextBuffer(buffer);

      // Should use updated capacity
      expect(nextBuffer.capacity).toBe(128);
    });

    it('should create independent writeIndex for chained buffer', () => {
      const buffer = createSpanBuffer(schema, taskContext);

      // Write some data to original buffer
      buffer.writeIndex = 50;

      const nextBuffer = createNextBuffer(buffer);

      // Chained buffer should start at 0
      expect(nextBuffer.writeIndex).toBe(0);
      // Uses currentCapacity from stats (default)
      expect(nextBuffer.capacity).toBe(DEFAULT_BUFFER_CAPACITY);
    });

    it('should maintain schema structure in chained buffer', () => {
      const buffer = createSpanBuffer(schema, taskContext);
      const nextBuffer = createNextBuffer(buffer);

      // Should have same attribute columns (use _values suffix to access storage)
      expect(Array.isArray(nextBuffer['userId_values'])).toBe(true); // category (raw strings)
      expect(Array.isArray(nextBuffer['requestId_values'])).toBe(true); // category (raw strings)
      expect(nextBuffer['operation_values']).toBeInstanceOf(Uint8Array);
      expect(nextBuffer['duration_values']).toBeInstanceOf(Float64Array);

      // Should have core columns
      expect(nextBuffer.timestamps).toBeInstanceOf(BigInt64Array);
      expect(nextBuffer.operations).toBeInstanceOf(Uint8Array);
    });

    it('should handle multiple chained buffers', () => {
      const buffer1 = createSpanBuffer(schema, taskContext);
      const buffer2 = createNextBuffer(buffer1);
      const buffer3 = createNextBuffer(buffer2);

      // All should have same spanId and traceId
      expect(buffer2.spanId).toBe(buffer1.spanId);
      expect(buffer3.spanId).toBe(buffer1.spanId);
      expect(buffer2.traceId).toBe(buffer1.traceId);
      expect(buffer3.traceId).toBe(buffer1.traceId);

      // Should be properly linked
      expect(buffer1.next).toBe(buffer2);
      expect(buffer2.next).toBe(buffer3);
    });

    it('should increment totalBuffersCreated stat', () => {
      const buffer = createSpanBuffer(schema, taskContext);
      const initialCount = taskContext.module.spanBufferCapacityStats.totalBuffersCreated;

      createNextBuffer(buffer);

      expect(taskContext.module.spanBufferCapacityStats.totalBuffersCreated).toBe(initialCount + 1);
    });

    it('should handle buffer with parent correctly', () => {
      const parentBuffer = createSpanBuffer(schema, taskContext);

      const childTaskContext = createTestTaskContext(schema, { spanName: 'child-span' });

      const childBuffer = createSpanBuffer(schema, childTaskContext);
      childBuffer.parent = parentBuffer;
      parentBuffer.children.push(childBuffer);

      const nextChildBuffer = createNextBuffer(childBuffer);

      // Should maintain parent relationship
      expect(nextChildBuffer.parent).toBe(parentBuffer);

      // Should NOT be added to parent's children (it's a continuation, not a new span)
      expect(parentBuffer.children).toHaveLength(1);
      expect(parentBuffer.children[0]).toBe(childBuffer);
    });

    it('should create empty children array for chained buffer', () => {
      const buffer = createSpanBuffer(schema, taskContext);

      // Add a child to original buffer
      const childTaskContext = createTestTaskContext(schema, { spanName: 'child-span' });
      const childBuffer = createSpanBuffer(schema, childTaskContext);
      buffer.children.push(childBuffer);

      const nextBuffer = createNextBuffer(buffer);

      // Chained buffer should have empty children array
      expect(nextBuffer.children).toHaveLength(0);
    });
  });

  describe('Buffer Chaining Edge Cases', () => {
    it('should handle buffer at exact capacity', () => {
      const buffer = createSpanBuffer(schema, taskContext, undefined, 10);
      buffer.writeIndex = 10; // At exact capacity

      const nextBuffer = createNextBuffer(buffer);

      expect(nextBuffer.writeIndex).toBe(0);
      expect(nextBuffer.capacity).toBe(taskContext.module.spanBufferCapacityStats.currentCapacity);
    });

    it('should preserve null bitmaps structure in chained buffer', () => {
      const buffer = createSpanBuffer(schema, taskContext);
      const nextBuffer = createNextBuffer(buffer);

      // Should have null bitmaps for each attribute (direct properties)
      expect(nextBuffer.userId_nulls).toBeInstanceOf(Uint8Array);
      expect(nextBuffer.requestId_nulls).toBeInstanceOf(Uint8Array);
      expect(nextBuffer.operation_nulls).toBeInstanceOf(Uint8Array);
      expect(nextBuffer.duration_nulls).toBeInstanceOf(Uint8Array);
    });

    it('should handle capacity changes between chained buffers', () => {
      const buffer1 = createSpanBuffer(schema, taskContext);
      expect(buffer1.capacity).toBe(DEFAULT_BUFFER_CAPACITY);

      // Simulate capacity tuning - double it
      taskContext.module.spanBufferCapacityStats.currentCapacity = DEFAULT_BUFFER_CAPACITY * 2;

      const buffer2 = createNextBuffer(buffer1);
      expect(buffer2.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2);

      // Change capacity again - double again
      taskContext.module.spanBufferCapacityStats.currentCapacity = DEFAULT_BUFFER_CAPACITY * 4;

      const buffer3 = createNextBuffer(buffer2);
      expect(buffer3.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 4);
    });
  });
});
