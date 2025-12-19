/**
 * Unit tests for library integration with prefix support
 */

import { describe, expect, it } from 'bun:test';
import { prefixSchema } from '../library.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { LogSchema, type SchemaFields } from '../schema/types.js';

describe('prefixSchema', () => {
  describe('success cases', () => {
    it('should prefix all field names in schema', () => {
      const schema = defineLogSchema({
        status: S.number(),
        method: S.enum(['GET', 'POST']),
      });

      const prefixed = prefixSchema(schema, 'http');

      expect(prefixed).toHaveProperty('http_status');
      expect(prefixed).toHaveProperty('http_method');
      expect(prefixed).not.toHaveProperty('status');
      expect(prefixed).not.toHaveProperty('method');
    });

    it('should preserve schema metadata during prefixing', () => {
      const schema = defineLogSchema({
        operation: S.enum(['CREATE', 'DELETE']),
      });

      const prefixed = prefixSchema(schema, 'db');

      expect(prefixed.db_operation).toHaveProperty('__schema_type', 'enum');
      expect(prefixed.db_operation).toHaveProperty('__enum_values');
      const enumSchema = prefixed.db_operation as { __enum_values?: readonly string[] };
      expect(enumSchema.__enum_values).toEqual(['CREATE', 'DELETE']);
    });

    it('should work with different prefix strings', () => {
      const schema = defineLogSchema({
        field: S.text(),
      });

      const prefixed1 = prefixSchema(schema, 'custom');
      const prefixed2 = prefixSchema(schema, 'another_prefix');

      expect(prefixed1).toHaveProperty('custom_field');
      expect(prefixed2).toHaveProperty('another_prefix_field');
    });
  });

  describe('edge cases', () => {
    it('should handle empty schema', () => {
      const schema = defineLogSchema({});
      const prefixed = prefixSchema(schema, 'test');

      expect(prefixed.fieldCount).toBe(0);
    });

    it('should handle schema with single field', () => {
      const schema = defineLogSchema({
        onlyField: S.category(),
      });

      const prefixed = prefixSchema(schema, 'prefix');

      expect(prefixed.fieldCount).toBe(1);
      expect(prefixed).toHaveProperty('prefix_onlyField');
    });

    it('should handle empty prefix string', () => {
      const schema = defineLogSchema({
        field: S.text(),
      });

      const prefixed = prefixSchema(schema, '');

      expect(prefixed).toHaveProperty('_field');
    });

    it('should handle schema with many fields', () => {
      const schemaFields: Record<string, ReturnType<typeof S.number>> = {};
      for (let i = 0; i < 100; i++) {
        schemaFields[`field${i}`] = S.number();
      }
      const schema = defineLogSchema(schemaFields);

      const prefixed = prefixSchema(schema, 'many');

      expect(prefixed.fieldCount).toBe(100);
      expect(prefixed).toHaveProperty('many_field0');
      expect(prefixed).toHaveProperty('many_field99');
    });
  });

  describe('failure cases', () => {
    it('should handle schema with undefined fields gracefully', () => {
      // Create a real LogSchema with an undefined value (edge case)
      const schema = new LogSchema({
        validField: S.text(),
        undefinedField: undefined as unknown as ReturnType<typeof S.text>,
      });

      const prefixed = prefixSchema(schema, 'test');

      expect(prefixed).toHaveProperty('test_validField');
      expect(prefixed).toHaveProperty('test_undefinedField');
      expect(prefixed.test_undefinedField).toBeUndefined();
    });

    it('should handle schema with null values', () => {
      // Create a real LogSchema with a null value (edge case)
      const schema = new LogSchema({
        field: null as unknown as ReturnType<typeof S.text>,
      });

      const prefixed = prefixSchema(schema, 'test');

      expect(prefixed).toHaveProperty('test_field');
      expect(prefixed.test_field).toBeNull();
    });

    it('should handle special characters in prefix', () => {
      const schema = defineLogSchema({
        field: S.text(),
      });

      const prefixed = prefixSchema(schema, 'prefix-with-dashes');

      expect(prefixed).toHaveProperty('prefix-with-dashes_field');
    });
  });
});

describe('prefix remapping', () => {
  describe('createPrefixMapping', () => {
    it('should create mapping from clean names to prefixed names', () => {
      const { createPrefixMapping } = require('../library.js');
      const schema = defineLogSchema({
        status: S.number(),
        method: S.category(),
      });

      const mapping = createPrefixMapping(schema, 'http');

      expect(mapping).toEqual({
        status: 'http_status',
        method: 'http_method',
      });
    });

    it('should handle empty schema', () => {
      const { createPrefixMapping } = require('../library.js');
      const schema = defineLogSchema({});

      const mapping = createPrefixMapping(schema, 'test');

      expect(mapping).toEqual({});
    });
  });

  describe('generateRemappedSpanLoggerClass', () => {
    it('should generate valid JavaScript code', () => {
      const { generateRemappedSpanLoggerClass, createPrefixMapping } = require('../library.js');
      const schema = defineLogSchema({
        status: S.number(),
        method: S.enum(['GET', 'POST']),
      });
      const mapping = createPrefixMapping(schema, 'http');

      const code = generateRemappedSpanLoggerClass(schema, mapping);

      // Should be valid JavaScript
      expect(() => new Function(`return ${code}`)()).not.toThrow();
    });

    it('should generate class with clean method names', () => {
      const { generateRemappedSpanLoggerClass, createPrefixMapping } = require('../library.js');
      const schema = defineLogSchema({
        status: S.number(),
        duration: S.number(),
      });
      const mapping = createPrefixMapping(schema, 'http');

      const code = generateRemappedSpanLoggerClass(schema, mapping);

      // Should contain clean method names
      expect(code).toContain('status(value)');
      expect(code).toContain('duration(value)');
      // Should reference prefixed columns
      expect(code).toContain('http_status');
      expect(code).toContain('http_duration');
    });

    it('should include enum mapping functions', () => {
      const { generateRemappedSpanLoggerClass, createPrefixMapping } = require('../library.js');
      const schema = defineLogSchema({
        method: S.enum(['GET', 'POST', 'PUT']),
      });
      const mapping = createPrefixMapping(schema, 'http');

      const code = generateRemappedSpanLoggerClass(schema, mapping);

      // Should contain enum mapping function
      expect(code).toContain('getEnumIndex_method');
      expect(code).toContain('case "GET"');
      expect(code).toContain('case "POST"');
      expect(code).toContain('case "PUT"');
    });

    it('should include prefix mapping for with() method', () => {
      const { generateRemappedSpanLoggerClass, createPrefixMapping } = require('../library.js');
      const schema = defineLogSchema({
        status: S.number(),
      });
      const mapping = createPrefixMapping(schema, 'http');

      const code = generateRemappedSpanLoggerClass(schema, mapping);

      // Should include PREFIX_MAPPING constant
      expect(code).toContain('PREFIX_MAPPING');
      expect(code).toContain('"status":"http_status"');
    });
  });

  describe('createRemappedSpanLoggerClass', () => {
    it('should create a class constructor', () => {
      const { createRemappedSpanLoggerClass, createPrefixMapping } = require('../library.js');
      const schema = defineLogSchema({
        status: S.number(),
      });
      const mapping = createPrefixMapping(schema, 'http');

      const SpanLoggerClass = createRemappedSpanLoggerClass(schema, mapping);

      expect(typeof SpanLoggerClass).toBe('function');
    });
  });
});
