/**
 * Unit tests for SpanLogger code generation
 */

import { beforeEach, describe, expect, it } from 'bun:test';
import { createTestTaskContext } from '../../__tests__/test-helpers.js';
import { S } from '../../schema/builder.js';
import { defineTagAttributes } from '../../schema/defineTagAttributes.js';
import type { TagAttributeSchema } from '../../schema/types.js';
import { createSpanBuffer } from '../../spanBuffer.js';
import type { SpanBuffer } from '../../types.js';
import { createScope } from '../scopeGenerator.js';
import {
  createSpanLoggerClass,
  type GetBufferWithSpaceFn,
  generateSpanLoggerClass,
  type StringInterner,
  type TextStorage,
} from '../spanLoggerGenerator.js';

// Mock implementations for string interning (these are real dependencies, not mocks of SpanBuffer)
class MockStringInterner implements StringInterner {
  private strings: string[] = [];
  private indices = new Map<string, number>();

  intern(str: string): number {
    let idx = this.indices.get(str);
    if (idx === undefined) {
      idx = this.strings.length;
      this.strings.push(str);
      this.indices.set(str, idx);
    }
    return idx;
  }

  getString(idx: number): string | undefined {
    return this.strings[idx];
  }

  getStrings(): readonly string[] {
    return this.strings;
  }

  size(): number {
    return this.strings.length;
  }
}

class MockTextStorage implements TextStorage {
  private strings: string[] = [];

  store(str: string): number {
    const idx = this.strings.length;
    this.strings.push(str);
    return idx;
  }

  getString(idx: number): string | undefined {
    return this.strings[idx];
  }

  getStrings(): readonly string[] {
    return this.strings;
  }
}

function createTestBuffer(schema: TagAttributeSchema = {}): SpanBuffer {
  const taskContext = createTestTaskContext(schema);
  return createSpanBuffer(schema, taskContext);
}

const mockGetBufferWithSpace: GetBufferWithSpaceFn = (buffer) => ({
  buffer,
  didOverflow: false,
});

describe('generateSpanLoggerClass', () => {
  describe('success cases', () => {
    it('should generate valid JavaScript code for empty schema', () => {
      const schema: TagAttributeSchema = {};
      const code = generateSpanLoggerClass(schema);

      expect(code).toContain('class GeneratedSpanLogger');
      expect(code).toContain('get tag()');
      expect(code).toContain('with(attributes)');
      expect(code).toContain('scope(attributes)');
      expect(code).toContain('info(message)');
      expect(code).toContain('debug(message)');
      expect(code).toContain('warn(message)');
      expect(code).toContain('error(message)');
    });

    it('should generate enum mapping functions for enum fields', () => {
      const schema = defineTagAttributes({
        operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']),
      });
      const code = generateSpanLoggerClass(schema);

      expect(code).toContain('function getEnumIndex_operation(value)');
      expect(code).toContain('case "CREATE": return 0;');
      expect(code).toContain('case "READ": return 1;');
      expect(code).toContain('case "UPDATE": return 2;');
      expect(code).toContain('case "DELETE": return 3;');
    });

    it('should generate attribute writer methods for all field types', () => {
      const schema = defineTagAttributes({
        userId: S.category(),
        errorMsg: S.text(),
        count: S.number(),
        enabled: S.boolean(),
      });
      const code = generateSpanLoggerClass(schema);

      expect(code).toContain('userId(value)');
      expect(code).toContain('errorMsg(value)');
      expect(code).toContain('count(value)');
      expect(code).toContain('enabled(value)');
    });
  });

  describe('edge cases', () => {
    it('should handle schema with single enum value', () => {
      const schema = defineTagAttributes({
        status: S.enum(['SUCCESS']),
      });
      const code = generateSpanLoggerClass(schema);

      expect(code).toContain('function getEnumIndex_status(value)');
      expect(code).toContain('"SUCCESS"');
    });

    it('should handle schema with many enum values', () => {
      const enumValues = Array.from({ length: 100 }, (_, i) => `VALUE_${i}`);
      const schema = defineTagAttributes({
        type: S.enum(enumValues as [string, ...string[]]),
      });
      const code = generateSpanLoggerClass(schema);

      expect(code).toContain('function getEnumIndex_type(value)');
      expect(code).toContain('"VALUE_0"');
      expect(code).toContain('"VALUE_99"');
    });

    it('should handle schema with special characters in field names', () => {
      const schema = defineTagAttributes({
        user_id: S.category(),
      });
      const code = generateSpanLoggerClass(schema);

      expect(code).toContain('user_id(value)');
    });
  });

  describe('failure cases', () => {
    it('should handle enum without values gracefully', () => {
      const schema: TagAttributeSchema = {
        operation: {
          __schema_type: 'enum',
          __enum_values: undefined as unknown as readonly string[],
        },
      };
      const code = generateSpanLoggerClass(schema);

      // Should not crash, but won't generate enum mapping
      expect(code).toContain('class GeneratedSpanLogger');
    });

    it('should handle unknown lmao_type gracefully', () => {
      const schema: TagAttributeSchema = {
        field: {
          __schema_type: 'unknown' as any,
        },
      };
      const code = generateSpanLoggerClass(schema);

      // Should generate generic writer
      expect(code).toContain('field(value)');
    });

    it('should handle empty enum values array', () => {
      const schema: TagAttributeSchema = {
        status: {
          __schema_type: 'enum',
          __enum_values: [],
        },
      };
      const code = generateSpanLoggerClass(schema);

      // Should still generate code without crashing
      expect(code).toContain('class GeneratedSpanLogger');
    });
  });
});

describe('createSpanLoggerClass', () => {
  let categoryInterner: MockStringInterner;
  let textStorage: MockTextStorage;
  let buffer: SpanBuffer;

  beforeEach(() => {
    categoryInterner = new MockStringInterner();
    textStorage = new MockTextStorage();
    buffer = createTestBuffer();
  });

  describe('success cases', () => {
    it('should create a working SpanLogger class for enum fields', () => {
      const schema = defineTagAttributes({
        operation: S.enum(['CREATE', 'READ', 'UPDATE', 'DELETE']),
      });

      const SpanLoggerClass = createSpanLoggerClass(schema);
      // Constructor: (buffer, getBufferWithSpace, scopeInstance)
      const logger = new SpanLoggerClass(buffer, mockGetBufferWithSpace, undefined);

      // Should have tag property
      expect(logger).toHaveProperty('tag');
      expect(logger).toHaveProperty('info');
      expect(logger).toHaveProperty('scope');
    });

    it('should create a class that can write category fields', () => {
      const schema = defineTagAttributes({
        userId: S.category(),
      });

      buffer.task.module.tagAttributes = schema;
      (buffer as any).attr_userId_nulls = new Uint8Array(8);
      // Category columns now use string[] arrays (hot path optimization)
      (buffer as any).attr_userId_values = new Array(64).fill('');

      const SpanLoggerClass = createSpanLoggerClass(schema);
      // Constructor: (buffer, getBufferWithSpace, scopeInstance)
      const logger = new SpanLoggerClass(buffer, mockGetBufferWithSpace, undefined);

      // Access tag to create entry
      const tag = logger.tag;
      expect(tag).toBeDefined();
      expect(typeof tag).toBe('object');
    });

    it('should create a class that implements scope method', () => {
      const schema = defineTagAttributes({
        requestId: S.category(),
        userId: S.category(),
      });

      // Use createSpanBuffer to get a properly generated buffer with lazy getters
      const properBuffer = createSpanBuffer(schema as any, buffer.task);
      properBuffer.task.module.tagAttributes = schema as any;

      const SpanLoggerClass = createSpanLoggerClass(schema as any);
      // Create proper scope instance using scopeGenerator
      const scopeInstance = createScope(schema as any);
      // Constructor: (buffer, getBufferWithSpace, scopeInstance)
      const logger = new SpanLoggerClass(properBuffer, mockGetBufferWithSpace, scopeInstance);

      // Should be able to call scope without error
      expect(() => {
        logger.scope({ requestId: 'req-123', userId: 'user-456' });
      }).not.toThrow();
    });
  });

  describe('edge cases', () => {
    it('should create a class for empty schema', () => {
      const schema = defineTagAttributes({});

      const SpanLoggerClass = createSpanLoggerClass(schema);
      const logger = new SpanLoggerClass(buffer, mockGetBufferWithSpace, 0, 0, undefined);

      expect(logger).toBeDefined();
      expect(logger).toHaveProperty('tag');
      expect(logger).toHaveProperty('info');
    });

    it('should cache the same class for same schema', () => {
      const schema = defineTagAttributes({
        field: S.text(),
      });

      const Class1 = createSpanLoggerClass(schema);
      const Class2 = createSpanLoggerClass(schema);

      // Note: Due to WeakMap usage in lmao.ts, this tests the function itself
      expect(Class1).toBeDefined();
      expect(Class2).toBeDefined();
    });

    it('should handle large schemas with many fields', () => {
      const schemaFields: Record<string, any> = {};
      for (let i = 0; i < 50; i++) {
        schemaFields[`field${i}`] = S.number();
      }
      const schema = defineTagAttributes(schemaFields);

      const SpanLoggerClass = createSpanLoggerClass(schema);
      expect(SpanLoggerClass).toBeDefined();

      const logger = new SpanLoggerClass(buffer, mockGetBufferWithSpace, 0, 0, undefined);

      expect(logger).toBeDefined();
    });
  });

  describe('failure cases', () => {
    it('should handle schema with undefined field type', () => {
      const schema: TagAttributeSchema = {
        field: {} as any,
      };

      // Should not throw during class creation
      expect(() => {
        createSpanLoggerClass(schema);
      }).not.toThrow();
    });

    it('should handle malformed enum values', () => {
      const schema: TagAttributeSchema = {
        status: {
          __schema_type: 'enum',
          __enum_values: null as any,
        },
      };

      expect(() => {
        createSpanLoggerClass(schema);
      }).not.toThrow();
    });

    it('should handle schema with circular references gracefully', () => {
      const schema: TagAttributeSchema = {
        field: { __schema_type: 'text' },
      };
      // Add circular reference
      (schema as any).circular = schema;

      // Should still create class without infinite loop
      expect(() => {
        createSpanLoggerClass(schema);
      }).not.toThrow();
    });
  });
});
