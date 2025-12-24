import { beforeEach, describe, expect, it } from 'bun:test';
import { DEFAULT_BUFFER_CAPACITY } from '@smoothbricks/arrow-builder';
import { createChildSpanBuffer, createOverflowBuffer, createSpanBuffer, createTraceId, S } from '@smoothbricks/lmao';
import { DEFAULT_METADATA } from '../../opContext/defineOp.js';
import type { LogSchema } from '../../schema/LogSchema.js';
import type { LogBinding, SpanBuffer } from '../../types.js';
import { createTestLogBinding, createTestSchema } from '../test-helpers.js';

describe('Buffer Chaining', () => {
  let module: LogBinding;
  let schema: LogSchema<any>;

  beforeEach(() => {
    // Use LogSchema directly - createSpanBuffer requires LogSchema, not plain object
    schema = createTestSchema({
      userId: S.category(),
      requestId: S.category(),
      operation: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
      duration: S.number(),
    });

    module = createTestLogBinding(schema);
  });

  describe('createOverflowBuffer', () => {
    it('should create a chained buffer with same spanId and traceId', () => {
      const buffer = createSpanBuffer(schema, module, 'test-span');
      const nextBuffer = createOverflowBuffer(buffer);

      // Should inherit spanId and traceId
      expect(nextBuffer.span_id).toBe(buffer.span_id);
      expect(nextBuffer.trace_id).toBe(buffer.trace_id);

      // Should be linked via next property
      expect(buffer._overflow).toBe(nextBuffer);

      // Should have same parent
      expect(nextBuffer._parent).toBe(buffer._parent);

      // Should have same task context
      expect(nextBuffer.task).toBe(buffer.task);
    });

    it('should create buffer with current capacity from stats', () => {
      const buffer = createSpanBuffer(schema, module, 'test-span');

      // Update capacity stats
      module.sb_capacity = 128;

      const nextBuffer = createOverflowBuffer(buffer);

      // Should use updated capacity
      expect(nextBuffer._capacity).toBe(128);
    });

    it('should create independent writeIndex for chained buffer', () => {
      const buffer = createSpanBuffer(schema, module, 'test-span');

      // Write some data to original buffer
      buffer._writeIndex = 50;

      const nextBuffer = createOverflowBuffer(buffer);

      // Chained buffer should start at 0
      expect(nextBuffer._writeIndex).toBe(0);
      // Uses currentCapacity from stats (default)
      expect(nextBuffer._capacity).toBe(DEFAULT_BUFFER_CAPACITY);
    });

    it('should maintain schema structure in chained buffer', () => {
      const buffer = createSpanBuffer(schema, module, 'test-span');
      const nextBuffer = createOverflowBuffer(buffer);

      // Should have same attribute columns (use _values suffix to access storage)
      expect(Array.isArray(nextBuffer.userId_values)).toBe(true); // category (raw strings)
      expect(Array.isArray(nextBuffer.requestId_values)).toBe(true); // category (raw strings)
      expect(nextBuffer.operation_values).toBeInstanceOf(Uint8Array);
      expect(nextBuffer.duration_values).toBeInstanceOf(Float64Array);

      // Should have core columns
      expect(nextBuffer.timestamp).toBeInstanceOf(BigInt64Array);
      expect(nextBuffer.entry_type).toBeInstanceOf(Uint8Array);
    });

    it('should handle multiple chained buffers', () => {
      const buffer1 = createSpanBuffer(schema, module, 'test-span');
      const buffer2 = createOverflowBuffer(buffer1);
      const buffer3 = createOverflowBuffer(buffer2);

      // All should have same spanId and traceId
      expect(buffer2.span_id).toBe(buffer1.span_id);
      expect(buffer3.span_id).toBe(buffer1.span_id);
      expect(buffer2.trace_id).toBe(buffer1.trace_id);
      expect(buffer3.trace_id).toBe(buffer1.trace_id);

      // Should be properly linked
      expect(buffer1._overflow).toBe(buffer2);
      expect(buffer2._overflow).toBe(buffer3);
    });

    it('should increment totalBuffersCreated stat', () => {
      const buffer = createSpanBuffer(schema, module, 'test-span');
      const initialCount = module.sb_totalCreated;

      createOverflowBuffer(buffer);

      expect(module.sb_totalCreated).toBe(initialCount + 1);
    });

    it('should handle buffer with parent correctly', () => {
      const parentBuffer = createSpanBuffer(schema, module, 'test-span');

      const childBuffer = createChildSpanBuffer(parentBuffer, module, 'child-span', DEFAULT_METADATA);
      parentBuffer._children.push(childBuffer);

      const nextChildBuffer = createOverflowBuffer(childBuffer);

      // Should maintain parent relationship
      expect(nextChildBuffer._parent).toBe(parentBuffer);

      // Should NOT be added to parent's children (it's a continuation, not a new span)
      expect(parentBuffer._children).toHaveLength(1);
      expect(parentBuffer._children[0]).toBe(childBuffer);
    });

    it('should create empty children array for chained buffer', () => {
      const buffer = createSpanBuffer(schema, module, 'test-span');

      // Add a child to original buffer
      const childBuffer = createChildSpanBuffer(buffer, module, 'child-span', DEFAULT_METADATA);
      buffer._children.push(childBuffer);

      const nextBuffer = createOverflowBuffer(buffer);

      // Chained buffer should have empty children array
      expect(nextBuffer._children).toHaveLength(0);
    });
  });

  describe('Buffer Chaining Edge Cases', () => {
    it('should handle buffer at exact capacity', () => {
      const buffer = createSpanBuffer(schema, module, 'test-span', undefined, 10);
      buffer._writeIndex = 10; // At exact capacity

      const nextBuffer = createOverflowBuffer(buffer);

      expect(nextBuffer._writeIndex).toBe(0);
      expect(nextBuffer._capacity).toBe(module.sb_capacity);
    });

    it('should preserve null bitmaps structure in chained buffer', () => {
      const buffer = createSpanBuffer(schema, module, 'test-span');
      const nextBuffer = createOverflowBuffer(buffer);

      // Should have null bitmaps for each attribute (direct properties)
      expect(nextBuffer.userId_nulls).toBeInstanceOf(Uint8Array);
      expect(nextBuffer.requestId_nulls).toBeInstanceOf(Uint8Array);
      expect(nextBuffer.operation_nulls).toBeInstanceOf(Uint8Array);
      expect(nextBuffer.duration_nulls).toBeInstanceOf(Uint8Array);
    });

    it('should handle capacity changes between chained buffers', () => {
      const buffer1 = createSpanBuffer(schema, module, 'test-span');
      expect(buffer1._capacity).toBe(DEFAULT_BUFFER_CAPACITY);

      // Simulate capacity tuning - double it
      module.sb_capacity = DEFAULT_BUFFER_CAPACITY * 2;

      const buffer2 = createOverflowBuffer(buffer1);
      expect(buffer2._capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2);

      // Change capacity again - double again
      module.sb_capacity = DEFAULT_BUFFER_CAPACITY * 4;

      const buffer3 = createOverflowBuffer(buffer2);
      expect(buffer3._capacity).toBe(DEFAULT_BUFFER_CAPACITY * 4);
    });
  });

  describe('Enhanced Buffer Chaining Tests', () => {
    it('should preserve data integrity across multiple buffer overflows', () => {
      const traceId = createTraceId('complex-trace');
      const rootBuffer = createSpanBuffer(schema, module, 'root-span', traceId, 4); // Small capacity

      // Write entries that will cause multiple overflows
      const testEntries = Array.from({ length: 10 }, (_, i) => ({
        userId: `user-${i % 3}`,
        requestId: `req-${i % 2}`,
        operation: (['GET', 'POST', 'PUT', 'DELETE'] as const)[i % 4],
        duration: i * 1.5,
      }));

      const operationEnumValues = ['GET', 'POST', 'PUT', 'DELETE'] as const;
      let currentBuffer = rootBuffer;
      for (let i = 0; i < testEntries.length; i++) {
        if (currentBuffer._writeIndex >= currentBuffer._capacity) {
          currentBuffer = createOverflowBuffer(currentBuffer);
        }

        const pos = currentBuffer._writeIndex;
        const entry = testEntries[i];

        currentBuffer.userId(pos, entry.userId);
        currentBuffer.requestId(pos, entry.requestId);
        // Enum setter expects index, not string
        currentBuffer.operation(pos, operationEnumValues.indexOf(entry.operation));
        currentBuffer.duration(pos, entry.duration);
        currentBuffer.timestamp[pos] = BigInt(Date.now() + i * 1000);
        currentBuffer._writeIndex++;
      }

      // Verify all data is preserved across the chain
      let verificationBuffer: SpanBuffer | undefined = rootBuffer;
      let verifiedCount = 0;

      while (verificationBuffer && verifiedCount < testEntries.length) {
        for (let i = 0; i < verificationBuffer._writeIndex && verifiedCount < testEntries.length; i++) {
          const expected = testEntries[verifiedCount];

          expect(verificationBuffer.userId_values[i]).toBe(expected.userId);
          expect(verificationBuffer.requestId_values[i]).toBe(expected.requestId);
          expect(verificationBuffer.operation_values[i]).toBe(
            ['GET', 'POST', 'PUT', 'DELETE'].indexOf(expected.operation),
          );
          expect(verificationBuffer.duration_values[i]).toBe(expected.duration);

          verifiedCount++;
        }

        verificationBuffer = verificationBuffer._overflow;
      }

      expect(verifiedCount).toBe(testEntries.length);
    });

    it('should maintain buffer topology with mixed relationships', () => {
      const traceId = createTraceId('topology-test');
      const rootBuffer = createSpanBuffer(schema, module, 'root', traceId);

      // Create first chained buffer with children
      const buffer1 = createOverflowBuffer(rootBuffer);
      const child1 = createChildSpanBuffer(buffer1, module, 'child1', DEFAULT_METADATA);
      const child2 = createChildSpanBuffer(buffer1, module, 'child2', DEFAULT_METADATA);
      buffer1._children.push(child1, child2);

      // Create second chained buffer with children
      const buffer2 = createOverflowBuffer(buffer1);
      const child3 = createChildSpanBuffer(buffer2, module, 'child3', DEFAULT_METADATA);
      buffer2._children.push(child3);

      // Verify topology
      // Overflow buffers inherit the logical parent (rootBuffer has no parent → undefined)
      expect(rootBuffer._parent).toBeUndefined();
      expect(rootBuffer._overflow).toBe(buffer1);
      expect(rootBuffer._children).toHaveLength(0);

      // buffer1 is an OVERFLOW of rootBuffer, so it inherits rootBuffer._parent (undefined)
      expect(buffer1._parent).toBeUndefined();
      expect(buffer1._overflow).toBe(buffer2);
      expect(buffer1._children).toHaveLength(2);
      expect(buffer1._children[0]).toBe(child1);
      expect(buffer1._children[1]).toBe(child2);

      // buffer2 is an OVERFLOW of buffer1, so it inherits buffer1._parent (undefined)
      expect(buffer2._parent).toBeUndefined();
      expect(buffer2._overflow).toBeUndefined();
      expect(buffer2._children).toHaveLength(1);
      expect(buffer2._children[0]).toBe(child3);

      // Verify identity sharing
      expect(rootBuffer.trace_id).toBe(traceId);
      expect(buffer1.trace_id).toBe(traceId);
      expect(buffer2.trace_id).toBe(traceId);
      expect(child1.trace_id).toBe(traceId);
      expect(child2.trace_id).toBe(traceId);
      expect(child3.trace_id).toBe(traceId);

      expect(rootBuffer.span_id).toBe(buffer1.span_id);
      expect(buffer1.span_id).toBe(buffer2.span_id);
      expect(child1.span_id).not.toBe(buffer1.span_id);
      expect(child2.span_id).not.toBe(buffer1.span_id);
      expect(child3.span_id).not.toBe(buffer2.span_id);

      // Verify child relationships
      expect(child1._parent).toBe(buffer1);
      expect(child2._parent).toBe(buffer1);
      expect(child3._parent).toBe(buffer2);
      expect(child1._overflow).toBeUndefined();
      expect(child2._overflow).toBeUndefined();
      expect(child3._overflow).toBeUndefined();
    });

    it('should track overflow statistics accurately', () => {
      const initialCreated = module.sb_totalCreated;

      const rootBuffer = createSpanBuffer(schema, module, 'stats-test', undefined, 3); // Very small capacity

      // Manually create overflow buffers (note: sb_overflows is only tracked by SpanLogger, not createOverflowBuffer)
      let currentBuffer = rootBuffer;
      for (let i = 0; i < 10; i++) {
        if (currentBuffer._writeIndex >= currentBuffer._capacity) {
          currentBuffer = createOverflowBuffer(currentBuffer);
        }

        const pos = currentBuffer._writeIndex;
        currentBuffer.userId(pos, `user-${i}`);
        currentBuffer.operation(pos, 0); // GET
        currentBuffer.duration(pos, 1.0);
        currentBuffer._writeIndex++;
      }

      // Count actual buffers created
      let bufferCount = 0;
      let countBuffer: SpanBuffer | undefined = rootBuffer;
      while (countBuffer) {
        bufferCount++;
        countBuffer = countBuffer._overflow;
      }

      // Should have created multiple buffers for 10 entries with capacity 3
      expect(bufferCount).toBeGreaterThan(1);
      // sb_totalCreated is incremented by createOverflowBuffer
      expect(module.sb_totalCreated).toBe(initialCreated + (bufferCount - 1));
      // Note: sb_overflows is only tracked by SpanLogger's _getNextBuffer(), not by createOverflowBuffer directly
      // If you need to track overflows, use SpanLogger which calls _getNextBuffer() automatically
    });

    it('should preserve schema consistency across chain boundaries', () => {
      const rootBuffer = createSpanBuffer(schema, module, 'schema-test');
      const buffer1 = createOverflowBuffer(rootBuffer);
      const buffer2 = createOverflowBuffer(buffer1);
      const buffer3 = createOverflowBuffer(buffer2);

      const buffers = [rootBuffer, buffer1, buffer2, buffer3];

      // All buffers should have identical schema structure
      for (const buf of buffers) {
        expect(Array.isArray(buf.userId_values)).toBe(true);
        expect(Array.isArray(buf.requestId_values)).toBe(true);
        expect(buf.operation_values).toBeInstanceOf(Uint8Array);
        expect(buf.duration_values).toBeInstanceOf(Float64Array);

        expect(buf.userId_nulls).toBeInstanceOf(Uint8Array);
        expect(buf.requestId_nulls).toBeInstanceOf(Uint8Array);
        expect(buf.operation_nulls).toBeInstanceOf(Uint8Array);
        expect(buf.duration_nulls).toBeInstanceOf(Uint8Array);

        expect(buf.timestamp).toBeInstanceOf(BigInt64Array);
        expect(buf.entry_type).toBeInstanceOf(Uint8Array);
      }
    });
  });
});
