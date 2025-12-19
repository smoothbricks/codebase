/**
 * Unit tests for SpanLogger code generation
 *
 * SpanLogger extends ColumnWriter from arrow-builder and adds:
 * - info(), debug(), warn(), error() methods for log entries
 * - _setScope() for setting scoped attributes
 * - _getNextBuffer() override for SpanBuffer creation on overflow
 *
 * Note: Tag writing (row 0) is handled by TagWriter (not yet implemented).
 * SpanLogger only handles log entries (rows 2+).
 */

import { beforeEach, describe, expect, it } from 'bun:test';
import { createTestModuleContext } from '../../__tests__/test-helpers.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import {
  ENTRY_TYPE_DEBUG,
  ENTRY_TYPE_ERROR,
  ENTRY_TYPE_INFO,
  ENTRY_TYPE_TRACE,
  ENTRY_TYPE_WARN,
} from '../../schema/systemSchema.js';
import type { LogSchema } from '../../schema/types.js';
import { createNextBuffer, createSpanBuffer } from '../../spanBuffer.js';
import type { SpanBuffer } from '../../types.js';
import { type BaseSpanLogger, createSpanLoggerClass } from '../spanLoggerGenerator.js';

function createTestBuffer(schema: LogSchema): SpanBuffer {
  const module = createTestModuleContext(schema);
  return createSpanBuffer(schema, module, 'test-span');
}

describe('createSpanLoggerClass', () => {
  describe('class creation', () => {
    it('should create a SpanLogger class for empty schema', () => {
      const schema = defineLogSchema({});
      const SpanLoggerClass = createSpanLoggerClass(schema);

      expect(SpanLoggerClass).toBeDefined();
      expect(typeof SpanLoggerClass).toBe('function');
    });

    it('should cache generated classes for the same schema', () => {
      const schema = defineLogSchema({
        userId: S.category(),
      });

      const Class1 = createSpanLoggerClass(schema);
      const Class2 = createSpanLoggerClass(schema);

      expect(Class1).toBe(Class2);
    });

    it('should create different classes for different schemas', () => {
      const schema1 = defineLogSchema({ userId: S.category() });
      const schema2 = defineLogSchema({ requestId: S.category() });

      const Class1 = createSpanLoggerClass(schema1);
      const Class2 = createSpanLoggerClass(schema2);

      expect(Class1).not.toBe(Class2);
    });
  });

  describe('instance creation', () => {
    it('should create instance with buffer and createNextBuffer', () => {
      const schema = defineLogSchema({
        userId: S.category(),
      });
      const buffer = createTestBuffer(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);

      const logger = new SpanLoggerClass(buffer, createNextBuffer);

      expect(logger).toBeDefined();
      expect(logger._buffer).toBe(buffer);
      // Scope is now accessed via buffer.scopeValues
      expect(buffer.scopeValues).toBeDefined();
    });

    it('should start with _writeIndex = 1', () => {
      const schema = defineLogSchema({});
      const buffer = createTestBuffer(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);

      const logger = new SpanLoggerClass(buffer, createNextBuffer);

      expect(logger._writeIndex).toBe(1);
    });
  });

  describe('logging methods', () => {
    let schema: LogSchema;
    let buffer: SpanBuffer;
    let logger: BaseSpanLogger<LogSchema>;

    beforeEach(() => {
      schema = defineLogSchema({
        userId: S.category(),
      });
      buffer = createTestBuffer(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);
      logger = new SpanLoggerClass(buffer, createNextBuffer);
    });

    it('should increment _writeIndex on info()', () => {
      expect(logger._writeIndex).toBe(1);

      logger.info('Test message');

      // After nextRow(), _writeIndex should be 2
      expect(logger._writeIndex).toBe(2);
    });

    it('should write entry type for info()', () => {
      logger.info('Test message');
      expect(buffer._operations[2]).toBe(ENTRY_TYPE_INFO);
    });

    it('should write entry type for debug()', () => {
      logger.debug('Debug message');
      expect(buffer._operations[2]).toBe(ENTRY_TYPE_DEBUG);
    });

    it('should write entry type for warn()', () => {
      logger.warn('Warning message');
      expect(buffer._operations[2]).toBe(ENTRY_TYPE_WARN);
    });

    it('should write entry type for error()', () => {
      logger.error('Error message');
      expect(buffer._operations[2]).toBe(ENTRY_TYPE_ERROR);
    });

    it('should write entry type for trace()', () => {
      logger.trace('Trace message');
      expect(buffer._operations[2]).toBe(ENTRY_TYPE_TRACE);
    });

    it('should write timestamp for log entries', () => {
      logger.info('Test message');

      // Timestamp should be non-zero BigInt
      expect(buffer._timestamps[2]).not.toBe(0n);
      expect(typeof buffer._timestamps[2]).toBe('bigint');
    });

    it('should return this for fluent chaining', () => {
      const result = logger.info('Test message');

      // Result is typed as FluentLogEntry but at runtime it's the same object
      expect(result as unknown).toBe(logger);
    });

    it('should support chaining logging methods', () => {
      // FluentLogEntry allows chaining back to logging methods
      (logger.info('First') as unknown as typeof logger).info('Second').warn('Third');

      expect(logger._writeIndex).toBe(4);
      expect(buffer._operations[2]).toBe(ENTRY_TYPE_INFO);
      expect(buffer._operations[3]).toBe(ENTRY_TYPE_INFO);
      expect(buffer._operations[4]).toBe(ENTRY_TYPE_WARN);
    });
  });

  describe('scope management', () => {
    it('should update scope values via _setScope to buffer.scopeValues', () => {
      const schema = defineLogSchema({
        userId: S.category(),
        requestId: S.category(),
      });
      const buffer = createTestBuffer(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, createNextBuffer);

      logger._setScope({ userId: 'user123' });

      // Scope is now stored on buffer.scopeValues
      expect(buffer.scopeValues?.userId).toBe('user123');
      expect(buffer.scopeValues?.requestId).toBeUndefined();
    });

    it('should access scope via logger.scope getter', () => {
      const schema = defineLogSchema({
        userId: S.category(),
      });
      const buffer = createTestBuffer(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, createNextBuffer);

      logger._setScope({ userId: 'test123' });

      // Scope is accessed via logger.scope getter (reads from buffer.scopeValues)
      expect(logger.scope?.userId).toBe('test123');
    });
  });

  describe('fluent attribute setters', () => {
    it('should have attribute setter methods from ColumnWriter', () => {
      const schema = defineLogSchema({
        userId: S.category(),
        count: S.number(),
        enabled: S.boolean(),
      });
      const buffer = createTestBuffer(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, createNextBuffer);

      // Log an entry first to advance writeIndex
      logger.info('Test');

      // The generated class should have setter methods
      // These come from ColumnWriter extension
      expect(typeof (logger as unknown as Record<string, unknown>).userId).toBe('function');
      expect(typeof (logger as unknown as Record<string, unknown>).count).toBe('function');
      expect(typeof (logger as unknown as Record<string, unknown>).enabled).toBe('function');
    });

    it('should write attribute values at current _writeIndex', () => {
      const schema = defineLogSchema({
        userId: S.category(),
      });
      const buffer = createTestBuffer(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, createNextBuffer);

      // Log an entry and chain attribute
      logger.info('Test');
      (logger as unknown as Record<string, (v: string) => void>).userId('user123');

      // Value should be at index 2 (after info() incremented from 1 to 2)
      expect((buffer as unknown as Record<string, string[]>).userId_values[2]).toBe('user123');
    });
  });
});
