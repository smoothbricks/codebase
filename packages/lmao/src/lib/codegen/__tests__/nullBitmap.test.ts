/**
 * Unit tests for null bitmap correctness in SpanLogger code generation
 *
 * These tests verify that the generated code sets null bitmap bits correctly
 * for single writes and log entries with scoped attributes.
 *
 * Architecture changes (new design):
 * - SpanLogger handles log entries (rows 2+), NOT tag writes (row 0)
 * - Tag writing is done via ctx.tag which is a separate API (not on SpanLogger)
 * - SpanLogger constructor: (buffer, scope, createNextBuffer)
 * - SpanLogger._writeIndex starts at 1 (rows 0/1 are reserved for span-start/end)
 * - _setScope() stores scope values in buffer.scopeValues (immutable, frozen)
 * - Scope filling happens at Arrow conversion time, NOT during span execution
 * - Direct writes (tag/log) win over scope values on their respective rows
 */

import { describe, expect, it } from 'bun:test';
import { createTestTaskContext } from '../../__tests__/test-helpers.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import type { SchemaFields } from '../../schema/types.js';
import { createSpanBuffer, SpanBufferTestUtils } from '../../spanBuffer.js';
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
  describe('immutable scope semantics', () => {
    it('should store scope values in buffer.scopeValues as frozen object', () => {
      const schema = defineLogSchema({
        requestId: S.category(),
      }) as unknown as SchemaFields;
      const buffer = createSpanBuffer(schema, createTestTaskContext(schema));

      const scopeInstance = createScope(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, scopeInstance, mockCreateNextBuffer);

      // _setScope should store values in buffer.scopeValues (not fill buffer columns)
      logger._setScope({ requestId: 'req-123' });

      // Verify scope values are stored in buffer.scopeValues
      expect(buffer.scopeValues).toBeDefined();
      expect(buffer.scopeValues?.requestId).toBe('req-123');

      // Verify the object is frozen (immutable)
      expect(Object.isFrozen(buffer.scopeValues)).toBe(true);
    });

    it('should create new frozen object on each _setScope call (merge semantics)', () => {
      const schema = defineLogSchema({
        requestId: S.category(),
        userId: S.category(),
      }) as unknown as SchemaFields;
      const buffer = createSpanBuffer(schema, createTestTaskContext(schema));

      const scopeInstance = createScope(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, scopeInstance, mockCreateNextBuffer);

      // First setScope call
      logger._setScope({ requestId: 'req-123' });
      const firstScopeValues = buffer.scopeValues;
      expect(firstScopeValues?.requestId).toBe('req-123');
      expect(firstScopeValues?.userId).toBeUndefined();

      // Second setScope call - should merge with existing values
      logger._setScope({ userId: 'user-456' });
      const secondScopeValues = buffer.scopeValues;

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
      }) as unknown as SchemaFields;
      const buffer = createSpanBuffer(schema, createTestTaskContext(schema));

      const scopeInstance = createScope(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, scopeInstance, mockCreateNextBuffer);

      // Set initial values
      logger._setScope({ requestId: 'req-123', userId: 'user-456' });
      expect(buffer.scopeValues?.requestId).toBe('req-123');
      expect(buffer.scopeValues?.userId).toBe('user-456');

      // Pass null to clear requestId, undefined should be ignored
      logger._setScope({ requestId: null as unknown as string, userId: undefined });

      // requestId should be cleared (null), userId should remain (undefined ignored)
      expect(buffer.scopeValues?.requestId).toBeUndefined();
      expect(buffer.scopeValues?.userId).toBe('user-456');
    });

    it('should NOT fill buffer columns during _setScope (deferred to Arrow conversion)', () => {
      const schema = defineLogSchema({
        requestId: S.category(),
      }) as unknown as SchemaFields;
      const buffer = createSpanBuffer(schema, createTestTaskContext(schema));

      const scopeInstance = createScope(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, scopeInstance, mockCreateNextBuffer);

      // _setScope should NOT fill buffer columns
      logger._setScope({ requestId: 'req-123' });

      // Check null bitmap - should NOT have any bits set (no buffer writes)
      const nulls = SpanBufferTestUtils.getNullBitmap(buffer, 'requestId');
      expect(nulls).toBeDefined();
      if (!nulls) throw new Error('Null bitmap should be defined');
      const bitsSet = getBitsSet(nulls, 8);

      // No bits should be set - scope filling happens at Arrow conversion time
      for (let i = 0; i < 8; i++) {
        expect(bitsSet[i]).toBe(false);
      }
    });
  });

  describe('log message writes (arbitrary index)', () => {
    it('should set correct bit for log entry at index 5', () => {
      const schema = defineLogSchema({
        requestId: S.category(),
      }) as unknown as SchemaFields;
      const buffer = createSpanBuffer(schema, createTestTaskContext(schema));

      const scopeInstance = createScope(schema);
      // Set scope value directly (GeneratedScope has index signature)
      scopeInstance.requestId = 'req-123';

      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, scopeInstance, mockCreateNextBuffer);

      // Set _writeIndex to 4 so nextRow() makes it 5
      logger._writeIndex = 4;

      // Log a message - this should write scoped attributes at index 5
      logger.info('test message');

      // Check null bitmap - bit 5 should be set
      const nulls = SpanBufferTestUtils.getNullBitmap(buffer, 'requestId');
      expect(nulls).toBeDefined();
      if (!nulls) throw new Error('Null bitmap should be defined');
      expect(nulls[0] & (1 << 5)).toBe(1 << 5);

      // Check that bit 4 is NOT set (wasn't written)
      expect(nulls[0] & (1 << 4)).toBe(0);
    });

    it('should set correct bit for log entry at index 9 (crosses byte boundary)', () => {
      const schema = defineLogSchema({
        requestId: S.category(),
      }) as unknown as SchemaFields;
      // Need capacity > 9 to test writing at index 9
      const buffer = createSpanBuffer(schema, createTestTaskContext(schema), undefined, 16);

      const scopeInstance = createScope(schema);
      // Set scope value directly (GeneratedScope has index signature)
      scopeInstance.requestId = 'req-456';

      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, scopeInstance, mockCreateNextBuffer);

      // Set _writeIndex to 8 so nextRow() makes it 9
      logger._writeIndex = 8;

      // Log a message - this should write scoped attributes at index 9
      logger.info('test message');

      // Check null bitmap - bit 9 should be set (byte 1, bit 1)
      const nulls = SpanBufferTestUtils.getNullBitmap(buffer, 'requestId');
      expect(nulls).toBeDefined();
      if (!nulls) throw new Error('Null bitmap should be defined');
      const byteIndex = 9 >>> 3; // = 1
      const bitOffset = 9 & 7; // = 1
      expect(nulls[byteIndex] & (1 << bitOffset)).toBe(1 << bitOffset);
    });
  });
});
