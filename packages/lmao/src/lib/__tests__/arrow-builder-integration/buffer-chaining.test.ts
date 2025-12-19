import { beforeEach, describe, expect, it } from 'bun:test';
import { DEFAULT_BUFFER_CAPACITY } from '@smoothbricks/arrow-builder';
import { createChildSpanBuffer, createNextBuffer, createSpanBuffer, createTraceId, S } from '@smoothbricks/lmao';
import type { ModuleContext } from '../../moduleContext.js';
import type { SpanBuffer } from '../../types.js';
import { createTestModuleContext, createTestSchema } from '../test-helpers.js';

describe('Buffer Chaining', () => {
  let module: ModuleContext;
  let schema: DefinedLogSchema<ReturnType<typeof defineLogSchema>['fields']>;

  beforeEach(() => {
    // Use LogSchema directly - createSpanBuffer requires LogSchema, not plain object
    schema = createTestSchema({
      userId: S.category(),
      requestId: S.category(),
      operation: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
      duration: S.number(),
    });

    module = createTestModuleContext(schema);
  });

  describe('createNextBuffer', () => {
    it('should create a chained buffer with same spanId and traceId', () => {
      const buffer = createSpanBuffer(schema, module, 'test-span');
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
      const buffer = createSpanBuffer(schema, module, 'test-span');

      // Update capacity stats
      module.sb_capacity = 128;

      const nextBuffer = createNextBuffer(buffer);

      // Should use updated capacity
      expect(nextBuffer.capacity).toBe(128);
    });

    it('should create independent writeIndex for chained buffer', () => {
      const buffer = createSpanBuffer(schema, module, 'test-span');

      // Write some data to original buffer
      buffer.writeIndex = 50;

      const nextBuffer = createNextBuffer(buffer);

      // Chained buffer should start at 0
      expect(nextBuffer.writeIndex).toBe(0);
      // Uses currentCapacity from stats (default)
      expect(nextBuffer.capacity).toBe(DEFAULT_BUFFER_CAPACITY);
    });

    it('should maintain schema structure in chained buffer', () => {
      const buffer = createSpanBuffer(schema, module, 'test-span');
      const nextBuffer = createNextBuffer(buffer);

      // Should have same attribute columns (use _values suffix to access storage)
      expect(Array.isArray(nextBuffer.userId_values)).toBe(true); // category (raw strings)
      expect(Array.isArray(nextBuffer.requestId_values)).toBe(true); // category (raw strings)
      expect(nextBuffer.operation_values).toBeInstanceOf(Uint8Array);
      expect(nextBuffer.duration_values).toBeInstanceOf(Float64Array);

      // Should have core columns
      expect(nextBuffer.timestamps).toBeInstanceOf(BigInt64Array);
      expect(nextBuffer.operations).toBeInstanceOf(Uint8Array);
    });

    it('should handle multiple chained buffers', () => {
      const buffer1 = createSpanBuffer(schema, module, 'test-span');
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
      const buffer = createSpanBuffer(schema, module, 'test-span');
      const initialCount = module.sb_totalCreated;

      createNextBuffer(buffer);

      expect(module.sb_totalCreated).toBe(initialCount + 1);
    });

    it('should handle buffer with parent correctly', () => {
      const parentBuffer = createSpanBuffer(schema, module, 'test-span');

      const childBuffer = createChildSpanBuffer(parentBuffer, module, 'child-span');
      parentBuffer.children.push(childBuffer);

      const nextChildBuffer = createNextBuffer(childBuffer);

      // Should maintain parent relationship
      expect(nextChildBuffer.parent).toBe(parentBuffer);

      // Should NOT be added to parent's children (it's a continuation, not a new span)
      expect(parentBuffer.children).toHaveLength(1);
      expect(parentBuffer.children[0]).toBe(childBuffer);
    });

    it('should create empty children array for chained buffer', () => {
      const buffer = createSpanBuffer(schema, module, 'test-span');

      // Add a child to original buffer
      const childBuffer = createChildSpanBuffer(buffer, module, 'child-span');
      buffer.children.push(childBuffer);

      const nextBuffer = createNextBuffer(buffer);

      // Chained buffer should have empty children array
      expect(nextBuffer.children).toHaveLength(0);
    });
  });

  describe('Buffer Chaining Edge Cases', () => {
    it('should handle buffer at exact capacity', () => {
      const buffer = createSpanBuffer(schema, module, 'test-span', undefined, 10);
      buffer.writeIndex = 10; // At exact capacity

      const nextBuffer = createNextBuffer(buffer);

      expect(nextBuffer.writeIndex).toBe(0);
      expect(nextBuffer.capacity).toBe(module.sb_capacity);
    });

    it('should preserve null bitmaps structure in chained buffer', () => {
      const buffer = createSpanBuffer(schema, module, 'test-span');
      const nextBuffer = createNextBuffer(buffer);

      // Should have null bitmaps for each attribute (direct properties)
      expect(nextBuffer.userId_nulls).toBeInstanceOf(Uint8Array);
      expect(nextBuffer.requestId_nulls).toBeInstanceOf(Uint8Array);
      expect(nextBuffer.operation_nulls).toBeInstanceOf(Uint8Array);
      expect(nextBuffer.duration_nulls).toBeInstanceOf(Uint8Array);
    });

    it('should handle capacity changes between chained buffers', () => {
      const buffer1 = createSpanBuffer(schema, module, 'test-span');
      expect(buffer1.capacity).toBe(DEFAULT_BUFFER_CAPACITY);

      // Simulate capacity tuning - double it
      module.sb_capacity = DEFAULT_BUFFER_CAPACITY * 2;

      const buffer2 = createNextBuffer(buffer1);
      expect(buffer2.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2);

      // Change capacity again - double again
      module.sb_capacity = DEFAULT_BUFFER_CAPACITY * 4;

      const buffer3 = createNextBuffer(buffer2);
      expect(buffer3.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 4);
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
        operation: ['GET', 'POST', 'PUT', 'DELETE'][i % 4] as any,
        duration: i * 1.5,
      }));

      let currentBuffer = rootBuffer;
      for (let i = 0; i < testEntries.length; i++) {
        if (currentBuffer.writeIndex >= currentBuffer.capacity) {
          currentBuffer = createNextBuffer(currentBuffer);
        }

        const pos = currentBuffer.writeIndex;
        const entry = testEntries[i];

        currentBuffer.userId(pos, entry.userId);
        currentBuffer.requestId(pos, entry.requestId);
        currentBuffer.operation(pos, entry.operation);
        currentBuffer.duration(pos, entry.duration);
        currentBuffer.timestamps[pos] = BigInt(Date.now() + i * 1000);
        currentBuffer.writeIndex++;
      }

      // Verify all data is preserved across the chain
      let verificationBuffer: SpanBuffer | undefined = rootBuffer;
      let verifiedCount = 0;

      while (verificationBuffer && verifiedCount < testEntries.length) {
        for (let i = 0; i < verificationBuffer.writeIndex && verifiedCount < testEntries.length; i++) {
          const expected = testEntries[verifiedCount];

          expect(verificationBuffer.userId_values[i]).toBe(expected.userId);
          expect(verificationBuffer.requestId_values[i]).toBe(expected.requestId);
          expect(verificationBuffer.operation_values[i]).toBe(
            ['GET', 'POST', 'PUT', 'DELETE'].indexOf(expected.operation),
          );
          expect(verificationBuffer.duration_values[i]).toBe(expected.duration);

          verifiedCount++;
        }

        verificationBuffer = verificationBuffer.next;
      }

      expect(verifiedCount).toBe(testEntries.length);
    });

    it('should maintain buffer topology with mixed relationships', () => {
      const traceId = createTraceId('topology-test');
      const rootBuffer = createSpanBuffer(schema, module, 'root', traceId);

      // Create first chained buffer with children
      const buffer1 = createNextBuffer(rootBuffer);
      const child1 = createChildSpanBuffer(buffer1, module, 'child1');
      const child2 = createChildSpanBuffer(buffer1, module, 'child2');
      buffer1.children.push(child1, child2);

      // Create second chained buffer with children
      const buffer2 = createNextBuffer(buffer1);
      const child3 = createChildSpanBuffer(buffer2, module, 'child3');
      buffer2.children.push(child3);

      // Verify topology
      expect(rootBuffer.parent).toBeUndefined();
      expect(rootBuffer.next).toBe(buffer1);
      expect(rootBuffer.children).toHaveLength(0);

      expect(buffer1.parent).toBe(rootBuffer);
      expect(buffer1.next).toBe(buffer2);
      expect(buffer1.children).toHaveLength(2);
      expect(buffer1.children[0]).toBe(child1);
      expect(buffer1.children[1]).toBe(child2);

      expect(buffer2.parent).toBe(buffer1);
      expect(buffer2.next).toBeUndefined();
      expect(buffer2.children).toHaveLength(1);
      expect(buffer2.children[0]).toBe(child3);

      // Verify identity sharing
      expect(rootBuffer.traceId).toBe(traceId);
      expect(buffer1.traceId).toBe(traceId);
      expect(buffer2.traceId).toBe(traceId);
      expect(child1.traceId).toBe(traceId);
      expect(child2.traceId).toBe(traceId);
      expect(child3.traceId).toBe(traceId);

      expect(rootBuffer.spanId).toBe(buffer1.spanId);
      expect(buffer1.spanId).toBe(buffer2.spanId);
      expect(child1.spanId).not.toBe(buffer1.spanId);
      expect(child2.spanId).not.toBe(buffer1.spanId);
      expect(child3.spanId).not.toBe(buffer2.spanId);

      // Verify child relationships
      expect(child1.parent).toBe(buffer1);
      expect(child2.parent).toBe(buffer1);
      expect(child3.parent).toBe(buffer2);
      expect(child1.next).toBeUndefined();
      expect(child2.next).toBeUndefined();
      expect(child3.next).toBeUndefined();
    });

    it('should track overflow statistics accurately', () => {
      const initialCreated = module.sb_totalCreated;
      const initialOverflows = module.sb_overflows;

      const rootBuffer = createSpanBuffer(schema, module, 'stats-test', undefined, 3); // Very small capacity

      // Force multiple overflows by writing beyond capacity
      let currentBuffer = rootBuffer;
      for (let i = 0; i < 10; i++) {
        if (currentBuffer.writeIndex >= currentBuffer.capacity) {
          currentBuffer = createNextBuffer(currentBuffer);
        }

        const pos = currentBuffer.writeIndex;
        currentBuffer.userId(pos, `user-${i}`);
        currentBuffer.operation(pos, 0); // GET
        currentBuffer.duration(pos, 1.0);
        currentBuffer.writeIndex++;
      }

      // Count actual buffers created
      let bufferCount = 0;
      let countBuffer: SpanBuffer | undefined = rootBuffer;
      while (countBuffer) {
        bufferCount++;
        countBuffer = countBuffer.next;
      }

      // Should have created multiple buffers for 10 entries with capacity 3
      expect(bufferCount).toBeGreaterThan(1);
      expect(module.sb_totalCreated).toBe(initialCreated + (bufferCount - 1));
      expect(module.sb_overflows).toBe(initialOverflows + (bufferCount - 1));
    });

    it('should preserve schema consistency across chain boundaries', () => {
      const rootBuffer = createSpanBuffer(schema, module, 'schema-test');
      const buffer1 = createNextBuffer(rootBuffer);
      const buffer2 = createNextBuffer(buffer1);
      const buffer3 = createNextBuffer(buffer2);

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

        expect(buf.timestamps).toBeInstanceOf(BigInt64Array);
        expect(buf.operations).toBeInstanceOf(Uint8Array);
      }
    });
  });
});
