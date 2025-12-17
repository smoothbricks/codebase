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
import { createTestTaskContext } from '../../__tests__/test-helpers.js';
import { ENTRY_TYPE_DEBUG, ENTRY_TYPE_ERROR, ENTRY_TYPE_INFO, ENTRY_TYPE_TRACE, ENTRY_TYPE_WARN } from '../../lmao.js';
import { S } from '../../schema/builder.js';
import { defineTagAttributes } from '../../schema/defineTagAttributes.js';
import { getSchemaFields, type TagAttributeSchema } from '../../schema/types.js';
import { createNextBuffer, createSpanBuffer } from '../../spanBuffer.js';
import type { SpanBuffer } from '../../types.js';
import { createScope } from '../scopeGenerator.js';
import { type BaseSpanLogger, createSpanLoggerClass } from '../spanLoggerGenerator.js';

/**
 * Extract plain schema from defineTagAttributes result
 */
function extractSchema(defined: unknown): TagAttributeSchema {
  const fields = getSchemaFields(defined as TagAttributeSchema);
  const schema: TagAttributeSchema = {};
  for (const [name, field] of fields) {
    schema[name] = field;
  }
  return schema;
}

function createTestBuffer(schema: TagAttributeSchema = {}): SpanBuffer {
  const taskContext = createTestTaskContext(schema);
  return createSpanBuffer(schema, taskContext);
}

describe('createSpanLoggerClass', () => {
  describe('class creation', () => {
    it('should create a SpanLogger class for empty schema', () => {
      const schema: TagAttributeSchema = {};
      const SpanLoggerClass = createSpanLoggerClass(schema);

      expect(SpanLoggerClass).toBeDefined();
      expect(typeof SpanLoggerClass).toBe('function');
    });

    it('should cache generated classes for the same schema', () => {
      const defined = defineTagAttributes({
        userId: S.category(),
      });
      const schema = extractSchema(defined);

      const Class1 = createSpanLoggerClass(schema);
      const Class2 = createSpanLoggerClass(schema);

      expect(Class1).toBe(Class2);
    });

    it('should create different classes for different schemas', () => {
      const schema1 = extractSchema(defineTagAttributes({ userId: S.category() }));
      const schema2 = extractSchema(defineTagAttributes({ requestId: S.category() }));

      const Class1 = createSpanLoggerClass(schema1);
      const Class2 = createSpanLoggerClass(schema2);

      expect(Class1).not.toBe(Class2);
    });
  });

  describe('instance creation', () => {
    it('should create instance with buffer, scope, and createNextBuffer', () => {
      const schema = extractSchema(
        defineTagAttributes({
          userId: S.category(),
        }),
      );
      const buffer = createTestBuffer(schema);
      const scope = createScope(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);

      const logger = new SpanLoggerClass(buffer, scope, createNextBuffer);

      expect(logger).toBeDefined();
      expect(logger._buffer).toBe(buffer);
      expect(logger._getScope()).toBe(scope);
    });

    it('should start with _writeIndex = 1', () => {
      const schema = extractSchema(defineTagAttributes({}));
      const buffer = createTestBuffer(schema);
      const scope = createScope(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);

      const logger = new SpanLoggerClass(buffer, scope, createNextBuffer);

      expect(logger._writeIndex).toBe(1);
    });
  });

  describe('logging methods', () => {
    let schema: TagAttributeSchema;
    let buffer: SpanBuffer;
    let logger: BaseSpanLogger<TagAttributeSchema>;

    beforeEach(() => {
      schema = extractSchema(
        defineTagAttributes({
          userId: S.category(),
        }),
      );
      buffer = createTestBuffer(schema);
      const scope = createScope(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);
      logger = new SpanLoggerClass(buffer, scope, createNextBuffer);
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
    it('should update scope values via _setScope', () => {
      const schema = extractSchema(
        defineTagAttributes({
          userId: S.category(),
          requestId: S.category(),
        }),
      );
      const buffer = createTestBuffer(schema);
      const scope = createScope(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, scope, createNextBuffer);

      logger._setScope({ userId: 'user123' });

      expect(scope.userId).toBe('user123');
      expect(scope.requestId).toBeUndefined();
    });

    it('should return scope via _getScope()', () => {
      const schema = extractSchema(
        defineTagAttributes({
          userId: S.category(),
        }),
      );
      const buffer = createTestBuffer(schema);
      const scope = createScope(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, scope, createNextBuffer);

      scope.userId = 'test123';

      expect(logger._getScope().userId).toBe('test123');
    });
  });

  describe('fluent attribute setters', () => {
    it('should have attribute setter methods from ColumnWriter', () => {
      const schema = extractSchema(
        defineTagAttributes({
          userId: S.category(),
          count: S.number(),
          enabled: S.boolean(),
        }),
      );
      const buffer = createTestBuffer(schema);
      const scope = createScope(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, scope, createNextBuffer);

      // Log an entry first to advance writeIndex
      logger.info('Test');

      // The generated class should have setter methods
      // These come from ColumnWriter extension
      expect(typeof (logger as unknown as Record<string, unknown>).userId).toBe('function');
      expect(typeof (logger as unknown as Record<string, unknown>).count).toBe('function');
      expect(typeof (logger as unknown as Record<string, unknown>).enabled).toBe('function');
    });

    it('should write attribute values at current _writeIndex', () => {
      const schema = extractSchema(
        defineTagAttributes({
          userId: S.category(),
        }),
      );
      const buffer = createTestBuffer(schema);
      const scope = createScope(schema);
      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, scope, createNextBuffer);

      // Log an entry and chain attribute
      logger.info('Test');
      (logger as unknown as Record<string, (v: string) => void>).userId('user123');

      // Value should be at index 2 (after info() incremented from 1 to 2)
      expect((buffer as unknown as Record<string, string[]>).userId_values[2]).toBe('user123');
    });
  });
});
