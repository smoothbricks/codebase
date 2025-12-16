/**
 * Unit tests for null bitmap correctness in SpanLogger code generation
 *
 * These tests verify that the generated code sets null bitmap bits correctly
 * for single writes, bulk fills, and edge cases like partial byte fills.
 *
 * Architecture changes (new design):
 * - SpanLogger handles log entries (rows 2+), NOT tag writes (row 0)
 * - Tag writing is done via ctx.tag which is a separate API (not on SpanLogger)
 * - SpanLogger constructor: (buffer, scope, createNextBuffer)
 * - SpanLogger._writeIndex starts at 1 (rows 0/1 are reserved for span-start/end)
 * - Scoped attributes are written via _setScope() which fills from _writeIndex+1 to capacity
 */

import { describe, expect, it } from 'bun:test';
import { createTestTaskContext } from '../../__tests__/test-helpers.js';
import { S } from '../../schema/builder.js';
import { defineTagAttributes } from '../../schema/defineTagAttributes.js';
import type { TagAttributeSchema } from '../../schema/types.js';
import { createNextBuffer, createSpanBuffer } from '../../spanBuffer.js';
import type { SpanBuffer } from '../../types.js';
import { createScope } from '../scopeGenerator.js';
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
 * Mock createNextBuffer that just returns the same buffer (for tests that don't need overflow)
 */
const mockCreateNextBuffer = (buffer: SpanBuffer): SpanBuffer => buffer;

describe('null bitmap correctness', () => {
  describe('scope bulk fill', () => {
    it('should correctly fill bits within a single byte (indices 2-7)', () => {
      const schema = defineTagAttributes({
        requestId: S.category(),
      }) as unknown as TagAttributeSchema;
      const buffer = createSpanBuffer(schema, createTestTaskContext(schema));
      // SpanLogger starts with _writeIndex = 1, but we want to test filling from index 2
      // So we need to simulate that writeIndex is 1 (so _writeIndex+1 = 2)

      const scopeInstance = createScope(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, scopeInstance, mockCreateNextBuffer);

      // Logger starts with _writeIndex = 1, so _setScope will fill from index 2 to capacity
      // Set scope - should fill from index 2 to capacity (64)
      (logger as any)._setScope({ requestId: 'req-123' });

      // Check null bitmap for first byte
      const nulls = (buffer as any).requestId_nulls as Uint8Array;
      const bitsSet = getBitsSet(nulls, 8);

      // Bits 0,1 should NOT be set (before _writeIndex+1)
      expect(bitsSet[0]).toBe(false);
      expect(bitsSet[1]).toBe(false);
      // Bits 2-7 should be set (filled by _setScope from _writeIndex+1)
      expect(bitsSet[2]).toBe(true);
      expect(bitsSet[3]).toBe(true);
      expect(bitsSet[4]).toBe(true);
      expect(bitsSet[5]).toBe(true);
      expect(bitsSet[6]).toBe(true);
      expect(bitsSet[7]).toBe(true);
    });

    it('should correctly fill exactly one full byte (indices 0-7)', () => {
      const schema = defineTagAttributes({
        requestId: S.category(),
      }) as unknown as TagAttributeSchema;
      // Create buffer with capacity 8
      const taskContext = createTestTaskContext(schema);
      taskContext.module.spanBufferCapacityStats.currentCapacity = 8;
      const buffer = createSpanBuffer(schema, taskContext, undefined, 8);

      const scopeInstance = createScope(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, scopeInstance, mockCreateNextBuffer);

      // Override _writeIndex to -1 so _writeIndex+1 = 0 (fill entire buffer from 0)
      (logger as any)._writeIndex = -1;

      // Set scope - should fill indices 0-7
      (logger as any)._setScope({ requestId: 'req-123' });

      // Check null bitmap - byte 0 should be 0xFF (all bits set)
      const nulls = (buffer as any).requestId_nulls as Uint8Array;
      expect(nulls[0]).toBe(0xff);
    });

    it('should correctly fill across byte boundary (indices 5-12)', () => {
      const schema = defineTagAttributes({
        requestId: S.category(),
      }) as unknown as TagAttributeSchema;
      const buffer = createSpanBuffer(schema, createTestTaskContext(schema), undefined, 16);
      (buffer as any)._capacity = 13; // End at index 12 (exclusive), so fill 5-12

      const scopeInstance = createScope(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, scopeInstance, mockCreateNextBuffer);

      // Set _writeIndex to 4 so _writeIndex+1 = 5
      (logger as any)._writeIndex = 4;

      // Set scope - should fill from index 5 to 12
      (logger as any)._setScope({ requestId: 'req-123' });

      // Check null bitmap
      const nulls = (buffer as any).requestId_nulls as Uint8Array;
      const bitsSet = getBitsSet(nulls, 16);

      // Bits 0-4 should NOT be set
      for (let i = 0; i < 5; i++) {
        expect(bitsSet[i]).toBe(false);
      }
      // Bits 5-12 should be set
      for (let i = 5; i < 13; i++) {
        expect(bitsSet[i]).toBe(true);
      }
      // Bits 13-15 should NOT be set
      for (let i = 13; i < 16; i++) {
        expect(bitsSet[i]).toBe(false);
      }
    });

    it('should correctly fill multiple full bytes (indices 0-23)', () => {
      const schema = defineTagAttributes({
        requestId: S.category(),
      }) as unknown as TagAttributeSchema;
      const buffer = createSpanBuffer(schema, createTestTaskContext(schema), undefined, 24);

      const scopeInstance = createScope(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, scopeInstance, mockCreateNextBuffer);

      // Override _writeIndex to -1 so _writeIndex+1 = 0
      (logger as any)._writeIndex = -1;

      // Set scope - should fill indices 0-23 (3 full bytes)
      (logger as any)._setScope({ requestId: 'req-123' });

      // Check null bitmap - bytes 0,1,2 should all be 0xFF
      const nulls = (buffer as any).requestId_nulls as Uint8Array;
      expect(nulls[0]).toBe(0xff);
      expect(nulls[1]).toBe(0xff);
      expect(nulls[2]).toBe(0xff);
    });

    it('should handle partial start and end bytes (indices 3-21)', () => {
      const schema = defineTagAttributes({
        requestId: S.category(),
      }) as unknown as TagAttributeSchema;
      const buffer = createSpanBuffer(schema, createTestTaskContext(schema), undefined, 24);
      (buffer as any)._capacity = 22; // End at index 21 (exclusive)

      const scopeInstance = createScope(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, scopeInstance, mockCreateNextBuffer);

      // Set _writeIndex to 2 so _writeIndex+1 = 3
      (logger as any)._writeIndex = 2;

      // Set scope - should fill indices 3-21
      (logger as any)._setScope({ requestId: 'req-123' });

      // Check null bitmap
      const nulls = (buffer as any).requestId_nulls as Uint8Array;
      const bitsSet = getBitsSet(nulls, 24);

      // Bits 0-2 should NOT be set
      expect(bitsSet[0]).toBe(false);
      expect(bitsSet[1]).toBe(false);
      expect(bitsSet[2]).toBe(false);

      // Bits 3-21 should be set
      for (let i = 3; i < 22; i++) {
        expect(bitsSet[i]).toBe(true);
      }

      // Bits 22-23 should NOT be set
      expect(bitsSet[22]).toBe(false);
      expect(bitsSet[23]).toBe(false);
    });

    it('BUG TEST: should correctly fill within single byte when start > 0 (indices 2-5)', () => {
      // This is the edge case that might be buggy:
      // When startIdx and endIdx are within the same byte, and startIdx > 0
      const schema = defineTagAttributes({
        requestId: S.category(),
      }) as unknown as TagAttributeSchema;
      const buffer = createSpanBuffer(schema, createTestTaskContext(schema), undefined, 8);
      (buffer as any)._capacity = 6; // End at index 5 (exclusive), so fill 2-5

      const scopeInstance = createScope(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, scopeInstance, mockCreateNextBuffer);

      // Set _writeIndex to 1 so _writeIndex+1 = 2
      (logger as any)._writeIndex = 1;

      // Set scope - should fill indices 2-5 only
      (logger as any)._setScope({ requestId: 'req-123' });

      // Check null bitmap
      const nulls = (buffer as any).requestId_nulls as Uint8Array;
      const bitsSet = getBitsSet(nulls, 8);

      // Bits 0,1 should NOT be set
      expect(bitsSet[0]).toBe(false);
      expect(bitsSet[1]).toBe(false);

      // Bits 2-5 should be set
      expect(bitsSet[2]).toBe(true);
      expect(bitsSet[3]).toBe(true);
      expect(bitsSet[4]).toBe(true);
      expect(bitsSet[5]).toBe(true);

      // Bits 6,7 should NOT be set
      expect(bitsSet[6]).toBe(false);
      expect(bitsSet[7]).toBe(false);
    });
  });

  describe('log message writes (arbitrary index)', () => {
    it('should set correct bit for log entry at index 5', () => {
      const schema = defineTagAttributes({
        requestId: S.category(),
      }) as unknown as TagAttributeSchema;
      const buffer = createSpanBuffer(schema, createTestTaskContext(schema));

      const scopeInstance = createScope(schema);
      // Set scope value directly
      (scopeInstance as any).requestId = 'req-123';

      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, scopeInstance, mockCreateNextBuffer);

      // Set _writeIndex to 4 so nextRow() makes it 5
      (logger as any)._writeIndex = 4;

      // Log a message - this should write scoped attributes at index 5
      logger.info('test message');

      // Check null bitmap - bit 5 should be set
      const nulls = (buffer as any).requestId_nulls as Uint8Array;
      expect(nulls[0] & (1 << 5)).toBe(1 << 5);

      // Check that bit 4 is NOT set (wasn't written)
      expect(nulls[0] & (1 << 4)).toBe(0);
    });

    it('should set correct bit for log entry at index 9 (crosses byte boundary)', () => {
      const schema = defineTagAttributes({
        requestId: S.category(),
      }) as unknown as TagAttributeSchema;
      const buffer = createSpanBuffer(schema, createTestTaskContext(schema));

      const scopeInstance = createScope(schema);
      (scopeInstance as any).requestId = 'req-456';

      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, scopeInstance, mockCreateNextBuffer);

      // Set _writeIndex to 8 so nextRow() makes it 9
      (logger as any)._writeIndex = 8;

      // Log a message - this should write scoped attributes at index 9
      logger.info('test message');

      // Check null bitmap - bit 9 should be set (byte 1, bit 1)
      const nulls = (buffer as any).requestId_nulls as Uint8Array;
      const byteIndex = 9 >>> 3; // = 1
      const bitOffset = 9 & 7; // = 1
      expect(nulls[byteIndex] & (1 << bitOffset)).toBe(1 << bitOffset);
    });
  });
});
