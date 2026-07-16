/**
 * Unit tests for null bitmap correctness in SpanLogger code generation
 *
 * These tests verify that the generated code sets null bitmap bits correctly
 * for single writes and log entries with scoped attributes.
 *
 * Architecture changes (new design):
 * - SpanLogger handles log entries (rows 2+), NOT tag writes (row 0)
 * - Tag writing is done via ctx.tag which is a separate API (not on SpanLogger)
 * - SpanLogger owns only `_state`, shared with its SpanContext
 * - SpanContext owns the active buffer and write index state
 * - _setScope() stores scope values in buffer._scopeValues (immutable, frozen)
 * - Scope filling happens at Arrow conversion time, NOT during span execution
 * - Direct writes (tag/log) win over scope values on their respective rows
 */

import { describe, expect, it } from 'bun:test';
import {
  createBuffer,
  createTestSchema,
  createTestSpanContext,
  createTestTraceRoot,
} from '../../__tests__/test-helpers.js';
import { DEFAULT_METADATA } from '../../opContext/defineOp.js';
import { S } from '../../schema/builder.js';
import type { LogSchema } from '../../schema/types.js';
import { createSpanBuffer, SpanBufferTestUtils } from '../../spanBuffer.js';

import type { SpanBuffer } from '../../types.js';

/**
 * Create a test buffer from a schema
 */
function createTestBuffer<T extends LogSchema>(schema: T): SpanBuffer<T> {
  return createBuffer(schema);
}

describe('null bitmap correctness', () => {
  describe('immutable scope semantics', () => {
    it('should store scope values in buffer._scopeValues as frozen object', () => {
      const schema = createTestSchema({
        requestId: S.category(),
      });
      const buffer = createTestBuffer(schema);

      const ctx = createTestSpanContext(schema, buffer);
      const logger = ctx._spanLogger;

      // _setScope should store values in buffer._scopeValues (not fill buffer columns)
      logger._setScope({ requestId: 'req-123' });

      // Verify scope values are stored in buffer._scopeValues
      expect(buffer._scopeValues).toBeDefined();
      expect(buffer._scopeValues?.requestId).toBe('req-123');

      // Verify the object is frozen (immutable)
      expect(Object.isFrozen(buffer._scopeValues)).toBe(true);
    });

    it('should create new frozen object on each _setScope call (merge semantics)', () => {
      const schema = createTestSchema({
        requestId: S.category(),
        userId: S.category(),
      });
      const buffer = createTestBuffer(schema);

      const ctx = createTestSpanContext(schema, buffer);
      const logger = ctx._spanLogger;

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
      const schema = createTestSchema({
        requestId: S.category(),
        userId: S.category(),
      });
      const buffer = createTestBuffer(schema);

      const ctx = createTestSpanContext(schema, buffer);
      const logger = ctx._spanLogger;

      // Set initial values
      logger._setScope({ requestId: 'req-123', userId: 'user-456' });
      expect(buffer._scopeValues?.requestId).toBe('req-123');
      expect(buffer._scopeValues?.userId).toBe('user-456');

      // Pass null to clear requestId, undefined should be ignored
      logger._setScope({ requestId: null, userId: undefined });

      // requestId should be cleared (null), userId should remain (undefined ignored)
      expect(buffer._scopeValues?.requestId).toBeUndefined();
      expect(buffer._scopeValues?.userId).toBe('user-456');
    });

    it('should NOT fill buffer columns during _setScope (deferred to Arrow conversion)', () => {
      const schema = createTestSchema({
        requestId: S.category(),
      });
      const buffer = createTestBuffer(schema);

      const ctx = createTestSpanContext(schema, buffer);
      const logger = ctx._spanLogger;

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
      // Per specs/lmao/01i_span_scope_attributes.md:
      // - Scope values are stored in buffer._scopeValues as a plain object
      // - Scope filling happens at Arrow conversion time, NOT during span execution
      // - This is because scope values are "defaults" that fill null slots
      const schema = createTestSchema({
        requestId: S.category(),
      });
      const buffer = createTestBuffer(schema);

      // Set scope value via buffer._scopeValues directly for test setup
      buffer._scopeValues = Object.freeze({ requestId: 'req-123' });

      const ctx = createTestSpanContext(schema, buffer);
      const logger = ctx._spanLogger;

      // Set the shared writer state so the next append writes row 5
      ctx._buffer._writeIndex = 5;

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
      const schema = createTestSchema({
        requestId: S.category(),
      });
      // Need capacity > 9 to test writing at index 9
      const buffer = createSpanBuffer(schema, createTestTraceRoot('test-trace'), DEFAULT_METADATA, 16);

      // Set scope value via buffer._scopeValues directly for test setup
      buffer._scopeValues = Object.freeze({ requestId: 'req-456' });

      const ctx = createTestSpanContext(schema, buffer);
      const logger = ctx._spanLogger;

      // Set the shared writer state so the next append writes row 9
      ctx._buffer._writeIndex = 9;

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
