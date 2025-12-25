/**
 * Unit tests for null bitmap correctness in SpanLogger code generation
 *
 * These tests verify that the generated code sets null bitmap bits correctly
 * for single writes and log entries with scoped attributes.
 *
 * Architecture changes (new design):
 * - SpanLogger handles log entries (rows 2+), NOT tag writes (row 0)
 * - Tag writing is done via ctx.tag which is a separate API (not on SpanLogger)
 * - SpanLogger constructor: (buffer, createNextBuffer)
 * - SpanLogger._writeIndex starts at 1 (rows 0/1 are reserved for span-start/end)
 * - _setScope() stores scope values in buffer._scopeValues (immutable, frozen)
 * - Scope filling happens at Arrow conversion time, NOT during span execution
 * - Direct writes (tag/log) win over scope values on their respective rows
 */

import { describe, expect, it } from 'bun:test';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import type { LogSchema } from '../../schema/types.js';
import { createSpanBuffer, SpanBufferTestUtils } from '../../spanBuffer.js';
import type { AnySpanBuffer } from '../../types.js';
import { createSpanLoggerClass } from '../spanLoggerGenerator.js';

/**
 * Helper to check which bits are set in a null bitmap
 */
function getBitsSet(nullBitmap: Uint8Array, count: number): boolean[] {
  const result: boolean[] = [];
  for (let i = 0; i < count; i++) {
    const byteIndex = i >>> 3;
    const bitOffset = i & 7;
    result.push((nullBitmap[byteIndex] & (1 << bitOffset)) !== 0);
  }
  return result;
}

/**
 * Create a test buffer from a schema
 */
function createTestBuffer(schema: LogSchema): AnySpanBuffer {
  return createSpanBuffer(schema, 'test-span');
}

/**
 * Create a test buffer with a specific capacity
 */
function createTestBufferWithCapacity(schema: LogSchema, capacity: number): AnySpanBuffer {
  return createSpanBuffer(schema, 'test-span', undefined, capacity);
}

/**
 * Mock createNextBuffer that just returns the same buffer (for tests that don't need overflow)
 */
const mockCreateNextBuffer = (buffer: AnySpanBuffer): AnySpanBuffer => buffer;

describe('null bitmap correctness', () => {
  describe('immutable scope semantics', () => {
    it('should store scope values in buffer._scopeValues as frozen object', () => {
      const schema = defineLogSchema({
        requestId: S.category(),
      });
      const buffer = createTestBuffer(schema);

      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, mockCreateNextBuffer);

      // _setScope should store values in buffer._scopeValues (not fill buffer columns)
      logger._setScope({ requestId: 'req-123' });

      // Verify scope values are stored in buffer._scopeValues
      expect(buffer._scopeValues).toBeDefined();
      expect(buffer._scopeValues?.requestId).toBe('req-123');

      // Verify the object is frozen (immutable)
      expect(Object.isFrozen(buffer._scopeValues)).toBe(true);
    });

    it('should create new frozen object on each _setScope call (merge semantics)', () => {
      const schema = defineLogSchema({
        requestId: S.category(),
        userId: S.category(),
      });
      const buffer = createTestBuffer(schema);

      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, mockCreateNextBuffer);

      // First setScope call
      logger._setScope({ requestId: 'req-123' });
      const firstScopeValues = buffer._scopeValues;
      expect(firstScopeValues?.requestId).toBe('req-123');
      expect(firstScopeValues?.userId).toBeUndefined();

      // Second setScope call - should merge with existing values
      logger._setScope({ userId: 'user-456' });
      const secondScopeValues = buffer._scopeValues;

      // Should be a NEW object (immutable semantics)
      expect(secondScopeValues).not.toBe(firstScopeValues);

      // Should have merged values
      expect(secondScopeValues?.requestId).toBe('req-123');
      expect(secondScopeValues?.userId).toBe('user-456');

      // Both should be frozen
      expect(Object.isFrozen(firstScopeValues)).toBe(true);
      expect(Object.isFrozen(secondScopeValues)).toBe(true);
    });

    it('should clear keys when null is passed (null clears, undefined ignores)', () => {
      const schema = defineLogSchema({
        requestId: S.category(),
        userId: S.category(),
      });
      const buffer = createTestBuffer(schema);

      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, mockCreateNextBuffer);

      // Set initial values
      logger._setScope({ requestId: 'req-123', userId: 'user-456' });
      expect(buffer._scopeValues?.requestId).toBe('req-123');
      expect(buffer._scopeValues?.userId).toBe('user-456');

      // Pass null to clear requestId, undefined should be ignored
      logger._setScope({ requestId: null as unknown as string, userId: undefined });

      // requestId should be cleared (null), userId should remain (undefined ignored)
      expect(buffer._scopeValues?.requestId).toBeUndefined();
      expect(buffer._scopeValues?.userId).toBe('user-456');
    });

    it('should NOT fill buffer columns during _setScope (deferred to Arrow conversion)', () => {
      const schema = defineLogSchema({
        requestId: S.category(),
      });
      const buffer = createTestBuffer(schema);

      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, mockCreateNextBuffer);

      // _setScope should NOT fill buffer columns
      logger._setScope({ requestId: 'req-123' });

      // Check null bitmap - should NOT be allocated (lazy allocation on first write)
      const nulls = SpanBufferTestUtils.getNullBitmap(buffer, 'requestId');

      // Null bitmap should NOT be allocated since we never directly wrote to requestId
      // Scope filling happens at Arrow conversion time, not during span execution
      expect(nulls).toBeUndefined();
    });
  });

  describe('log message writes (arbitrary index)', () => {
    it('should NOT set null bitmap during info() - scope filling is deferred to Arrow conversion', () => {
      // Per specs/01i_span_scope_attributes.md:
      // - Scope values are stored in buffer._scopeValues as a plain object
      // - Scope filling happens at Arrow conversion time, NOT during span execution
      // - This is because scope values are "defaults" that fill null slots
      const schema = defineLogSchema({
        requestId: S.category(),
      });
      const buffer = createTestBuffer(schema);

      // Set scope value via buffer._scopeValues directly for test setup
      buffer._scopeValues = Object.freeze({ requestId: 'req-123' });

      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, mockCreateNextBuffer);

      // Set _writeIndex to 4 so nextRow() makes it 5
      logger._writeIndex = 4;

      // Log a message - this does NOT write scoped attributes during execution
      // Scope values are filled at Arrow conversion time
      logger.info('test message');

      // Verify that the null bitmap bit is NOT set (scope is deferred)
      const nulls = SpanBufferTestUtils.getNullBitmap(buffer, 'requestId');
      // Null bitmap might not even be allocated since we never directly wrote to requestId
      if (nulls) {
        // If allocated, the bits should be 0 (not written directly)
        expect(nulls[0] & (1 << 5)).toBe(0);
      }
    });

    it('should NOT set null bitmap at index 9 during info() - scope is deferred', () => {
      // Same as above - scope values are NOT written during span execution
      // They are stored in buffer._scopeValues and filled at Arrow conversion
      const schema = defineLogSchema({
        requestId: S.category(),
      });
      // Need capacity > 9 to test writing at index 9
      const buffer = createTestBufferWithCapacity(schema, 16);

      // Set scope value via buffer._scopeValues directly for test setup
      buffer._scopeValues = Object.freeze({ requestId: 'req-456' });

      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, mockCreateNextBuffer);

      // Set _writeIndex to 8 so nextRow() makes it 9
      logger._writeIndex = 8;

      // Log a message - does NOT write scoped attributes immediately
      logger.info('test message');

      // Verify scope values are stored correctly
      expect(buffer._scopeValues.requestId).toBe('req-456');

      // Null bitmap is not set during execution (deferred to Arrow conversion)
      const nulls = SpanBufferTestUtils.getNullBitmap(buffer, 'requestId');
      if (nulls) {
        const byteIndex = 9 >>> 3; // = 1
        const bitOffset = 9 & 7; // = 1
        // Not set - scope filling is deferred
        expect(nulls[byteIndex] & (1 << bitOffset)).toBe(0);
      }
    });
  });
});
