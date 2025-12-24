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

import { describe, expect, it } from 'bun:test';
import { createTestLogger } from '../../__tests__/test-helpers.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import {
  ENTRY_TYPE_DEBUG,
  ENTRY_TYPE_ERROR,
  ENTRY_TYPE_INFO,
  ENTRY_TYPE_TRACE,
  ENTRY_TYPE_WARN,
} from '../../schema/systemSchema.js';
import { createSpanLoggerClass } from '../spanLoggerGenerator.js';

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
      const schema = defineLogSchema({ userId: S.category() });
      const { buffer, logger } = createTestLogger(schema);

      expect(logger).toBeDefined();
      expect(logger._buffer).toBe(buffer);
      expect(buffer._scopeValues).toBeDefined();
    });

    it('should start with _writeIndex = 1', () => {
      const schema = defineLogSchema({});
      const { logger } = createTestLogger(schema);

      expect(logger._writeIndex).toBe(1);
    });
  });

  describe('logging methods', () => {
    it('should increment _writeIndex on info()', () => {
      const schema = defineLogSchema({ userId: S.category() });
      const { logger } = createTestLogger(schema);

      expect(logger._writeIndex).toBe(1);
      logger.info('Test message');
      expect(logger._writeIndex).toBe(2);
    });

    it('should write entry type for info()', () => {
      const schema = defineLogSchema({});
      const { buffer, logger } = createTestLogger(schema);

      logger.info('Test message');
      expect(buffer.entry_type[2]).toBe(ENTRY_TYPE_INFO);
    });

    it('should write entry type for debug()', () => {
      const schema = defineLogSchema({});
      const { buffer, logger } = createTestLogger(schema);

      logger.debug('Debug message');
      expect(buffer.entry_type[2]).toBe(ENTRY_TYPE_DEBUG);
    });

    it('should write entry type for warn()', () => {
      const schema = defineLogSchema({});
      const { buffer, logger } = createTestLogger(schema);

      logger.warn('Warning message');
      expect(buffer.entry_type[2]).toBe(ENTRY_TYPE_WARN);
    });

    it('should write entry type for error()', () => {
      const schema = defineLogSchema({});
      const { buffer, logger } = createTestLogger(schema);

      logger.error('Error message');
      expect(buffer.entry_type[2]).toBe(ENTRY_TYPE_ERROR);
    });

    it('should write entry type for trace()', () => {
      const schema = defineLogSchema({});
      const { buffer, logger } = createTestLogger(schema);

      logger.trace('Trace message');
      expect(buffer.entry_type[2]).toBe(ENTRY_TYPE_TRACE);
    });

    it('should write timestamp for log entries', () => {
      const schema = defineLogSchema({});
      const { buffer, logger } = createTestLogger(schema);

      logger.info('Test message');

      expect(buffer.timestamp[2]).not.toBe(0n);
      expect(typeof buffer.timestamp[2]).toBe('bigint');
    });

    it('should return this for fluent chaining', () => {
      const schema = defineLogSchema({});
      const { logger } = createTestLogger(schema);

      const result = logger.info('Test message');

      // FluentLogEntry is same object at runtime (types differ but object identity is same)
      expect(result === (logger as unknown)).toBe(true);
    });

    it('should support chaining logging methods', () => {
      const schema = defineLogSchema({});
      const { buffer, logger } = createTestLogger(schema);

      // FluentLogEntry includes logging methods for continued chaining
      logger.info('First').info('Second').warn('Third');

      expect(logger._writeIndex).toBe(4);
      expect(buffer.entry_type[2]).toBe(ENTRY_TYPE_INFO);
      expect(buffer.entry_type[3]).toBe(ENTRY_TYPE_INFO);
      expect(buffer.entry_type[4]).toBe(ENTRY_TYPE_WARN);
    });
  });

  describe('scope management', () => {
    it('should update scope values via _setScope to buffer._scopeValues', () => {
      const schema = defineLogSchema({
        userId: S.category(),
        requestId: S.category(),
      });
      const { buffer, logger } = createTestLogger(schema);

      logger._setScope({ userId: 'user123' });

      expect(buffer._scopeValues?.userId).toBe('user123');
      expect(buffer._scopeValues?.requestId).toBeUndefined();
    });

    it('should access scope via logger.scope getter', () => {
      const schema = defineLogSchema({ userId: S.category() });
      const { logger } = createTestLogger(schema);

      logger._setScope({ userId: 'test123' });

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
      const { logger } = createTestLogger(schema);

      // Log an entry first to advance writeIndex
      logger.info('Test');

      // Verify methods exist and are callable - types come from ColumnWriter<T>
      expect(typeof logger.userId).toBe('function');
      expect(typeof logger.count).toBe('function');
      expect(typeof logger.enabled).toBe('function');
    });

    it('should write attribute values at current _writeIndex', () => {
      const schema = defineLogSchema({ userId: S.category() });
      const { buffer, logger } = createTestLogger(schema);

      // Log an entry and chain attribute
      logger.info('Test').userId('user123');

      // Value should be at index 2 (after info() incremented from 1 to 2)
      expect(buffer.userId_values[2]).toBe('user123');
    });

    it('should support fluent chaining with attributes', () => {
      const schema = defineLogSchema({
        userId: S.category(),
        count: S.number(),
      });
      const { buffer, logger } = createTestLogger(schema);

      logger.info('Test').userId('user123').count(42);

      expect(buffer.userId_values[2]).toBe('user123');
      expect(buffer.count_values[2]).toBe(42);
    });

    it('should write multiple log entries with different attributes', () => {
      const schema = defineLogSchema({ userId: S.category() });
      const { buffer, logger } = createTestLogger(schema);

      logger.info('First').userId('user1');
      logger.info('Second').userId('user2');

      expect(buffer.userId_values[2]).toBe('user1');
      expect(buffer.userId_values[3]).toBe('user2');
    });
  });
});
