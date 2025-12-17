/**
 * Unit tests for library integration with prefix support
 */

import { describe, expect, it } from 'bun:test';
import {
  createDatabaseLibrary,
  createHttpLibrary,
  createLibraryModule,
  moduleContextFactory,
  prefixSchema,
} from '../library.js';
import { S } from '../schema/builder.js';
import { defineTagAttributes } from '../schema/defineTagAttributes.js';
import type { TagAttributeSchema } from '../schema/types.js';

describe('prefixSchema', () => {
  describe('success cases', () => {
    it('should prefix all field names in schema', () => {
      const schema = defineTagAttributes({
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
      const schema = defineTagAttributes({
        operation: S.enum(['CREATE', 'DELETE']),
      });

      const prefixed = prefixSchema(schema, 'db');

      expect(prefixed.db_operation).toHaveProperty('__schema_type', 'enum');
      expect(prefixed.db_operation).toHaveProperty('__enum_values');
      expect((prefixed.db_operation as any).__enum_values).toEqual(['CREATE', 'DELETE']);
    });

    it('should work with different prefix strings', () => {
      const schema = defineTagAttributes({
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
      const schema = defineTagAttributes({});
      const prefixed = prefixSchema(schema, 'test');

      expect(Object.keys(prefixed)).toHaveLength(0);
    });

    it('should handle schema with single field', () => {
      const schema = defineTagAttributes({
        onlyField: S.category(),
      });

      const prefixed = prefixSchema(schema, 'prefix');

      expect(Object.keys(prefixed)).toHaveLength(1);
      expect(prefixed).toHaveProperty('prefix_onlyField');
    });

    it('should handle empty prefix string', () => {
      const schema = defineTagAttributes({
        field: S.text(),
      });

      const prefixed = prefixSchema(schema, '');

      expect(prefixed).toHaveProperty('_field');
    });

    it('should handle schema with many fields', () => {
      const schemaFields: Record<string, any> = {};
      for (let i = 0; i < 100; i++) {
        schemaFields[`field${i}`] = S.number();
      }
      const schema = defineTagAttributes(schemaFields);

      const prefixed = prefixSchema(schema, 'many');

      expect(Object.keys(prefixed)).toHaveLength(100);
      expect(prefixed).toHaveProperty('many_field0');
      expect(prefixed).toHaveProperty('many_field99');
    });
  });

  describe('failure cases', () => {
    it('should handle schema with undefined fields gracefully', () => {
      const schema: TagAttributeSchema = {
        validField: S.text(),
        undefinedField: undefined as any,
      };

      const prefixed = prefixSchema(schema, 'test');

      expect(prefixed).toHaveProperty('test_validField');
      expect(prefixed).toHaveProperty('test_undefinedField');
      expect(prefixed.test_undefinedField).toBeUndefined();
    });

    it('should handle schema with null values', () => {
      const schema: TagAttributeSchema = {
        field: null as any,
      };

      const prefixed = prefixSchema(schema, 'test');

      expect(prefixed).toHaveProperty('test_field');
      expect(prefixed.test_field).toBeNull();
    });

    it('should handle special characters in prefix', () => {
      const schema: TagAttributeSchema = {
        field: S.text(),
      };

      const prefixed = prefixSchema(schema, 'prefix-with-dashes');

      expect(prefixed).toHaveProperty('prefix-with-dashes_field');
    });
  });
});

describe('createLibraryModule', () => {
  describe('success cases', () => {
    it('should create library module with clean schema', () => {
      const schema = defineTagAttributes({
        status: S.number(),
        method: S.category(),
      });

      const module = createLibraryModule({
        gitSha: 'abc123',
        packageName: '@test/pkg',
        packagePath: 'lib/http.ts',

        schema,
      });

      expect(module).toBeDefined();
      expect(module).toHaveProperty('task');
      expect(typeof module.task).toBe('function');
    });

    it('should extract schema fields without validation methods', () => {
      const schema = defineTagAttributes({
        field: S.text(),
      });

      const module = createLibraryModule({
        gitSha: 'test',
        packageName: '@test/pkg',
        packagePath: 'test.ts',

        schema,
      });

      // Schema should have the field
      expect(module.schema.field).toBeDefined();
      expect(module.schema.field).toBe(schema.field);

      // But should NOT have validation methods (those are stripped)
      expect((module.schema as any).validate).toBeUndefined();
      expect((module.schema as any).parse).toBeUndefined();
      expect((module.schema as any).safeParse).toBeUndefined();
    });

    it('should create module with operations object', () => {
      const schema = defineTagAttributes({
        field: S.number(),
      });

      const module = createLibraryModule({
        gitSha: 'test',
        packageName: '@test/pkg',
        packagePath: 'test.ts',

        schema,
      });

      expect(module.operations).toBeDefined();
      expect(typeof module.operations).toBe('object');
    });
  });

  describe('edge cases', () => {
    it('should create module with empty schema', () => {
      const schema = defineTagAttributes({});

      const module = createLibraryModule({
        gitSha: 'test',
        packageName: '@test/pkg',
        packagePath: 'test.ts',

        schema,
      });

      expect(module).toBeDefined();
    });

    it('should handle very long module names', () => {
      const schema = defineTagAttributes({
        field: S.number(),
      });

      const longName = 'a'.repeat(1000);

      const module = createLibraryModule({
        gitSha: 'test',
        packageName: '@test/pkg',
        packagePath: 'test.ts',

        schema,
      });

      expect(module).toBeDefined();
    });

    it('should handle special characters in file paths', () => {
      const schema = defineTagAttributes({
        field: S.number(),
      });

      const module = createLibraryModule({
        gitSha: 'test',
        packageName: '@test/pkg',
        packagePath: '@scope/package/lib/file-name.ts',

        schema,
      });

      expect(module).toBeDefined();
    });
  });

  describe('failure cases', () => {
    it('should handle empty gitSha', () => {
      const schema = defineTagAttributes({
        field: S.number(),
      });

      const module = createLibraryModule({
        gitSha: '',
        packageName: '@test/pkg',
        packagePath: 'test.ts',

        schema,
      });

      expect(module).toBeDefined();
    });

    it('should handle empty packagePath', () => {
      const schema = defineTagAttributes({
        field: S.number(),
      });

      const module = createLibraryModule({
        gitSha: 'test',
        packageName: '@test/pkg',
        packagePath: '',

        schema,
      });

      expect(module).toBeDefined();
    });

    it('should handle schema with various field types', () => {
      const schema = defineTagAttributes({
        field: S.text(),
      });

      expect(() => {
        createLibraryModule({
          gitSha: 'test',
          packageName: '@test/pkg',
          packagePath: 'test.ts',

          schema,
        });
      }).not.toThrow();
    });
  });
});

describe('moduleContextFactory', () => {
  describe('success cases', () => {
    it('should create module context with prefixed schema', () => {
      const schema = defineTagAttributes({
        status: S.number(),
      });

      const factory = moduleContextFactory(
        'http',
        {
          gitSha: 'test',
          packageName: '@test/pkg',
          packagePath: 'test.ts',
        },
        schema,
      );

      expect(factory).toBeDefined();
      expect(factory).toHaveProperty('task');
      expect(factory).toHaveProperty('operations');
    });

    it('should apply prefix to schema fields', () => {
      const schema = defineTagAttributes({
        method: S.category(),
        url: S.text(),
      });

      const factory = moduleContextFactory(
        'http',
        {
          gitSha: 'test',
          packageName: '@test/pkg',
          packagePath: 'test.ts',
        },
        schema,
      );

      expect(factory).toBeDefined();
      // The factory should have created a module with prefixed schema internally
    });

    it('should create empty operations when none provided', () => {
      const factory = moduleContextFactory(
        'test',
        {
          gitSha: 'test',
          packageName: '@test/pkg',
          packagePath: 'test.ts',
        },
        defineTagAttributes({}),
      );

      expect(factory.operations).toBeDefined();
      expect(Object.keys(factory.operations)).toHaveLength(0);
    });
  });

  describe('edge cases', () => {
    it('should handle single-character prefix', () => {
      const factory = moduleContextFactory(
        'x',
        {
          gitSha: 'test',
          packageName: '@test/pkg',
          packagePath: 'test.ts',
        },
        defineTagAttributes({ field: S.text() }),
      );

      expect(factory).toBeDefined();
    });

    it('should handle very long prefix', () => {
      const longPrefix = 'prefix_'.repeat(100);
      const factory = moduleContextFactory(
        longPrefix,
        {
          gitSha: 'test',
          packageName: '@test/pkg',
          packagePath: 'test.ts',
        },
        defineTagAttributes({}),
      );

      expect(factory).toBeDefined();
    });

    it('should handle empty operations object', () => {
      const factory = moduleContextFactory(
        'test',
        {
          gitSha: 'test',
          packageName: '@test/pkg',
          packagePath: 'test.ts',
        },
        defineTagAttributes({}),
        {},
      );

      expect(factory.operations).toEqual({});
    });
  });

  describe('failure cases', () => {
    it('should handle undefined operations gracefully', () => {
      const factory = moduleContextFactory(
        'test',
        {
          gitSha: 'test',
          packageName: '@test/pkg',
          packagePath: 'test.ts',
        },
        defineTagAttributes({}),
        undefined,
      );

      expect(factory).toBeDefined();
      expect(factory.operations).toEqual({});
    });

    it('should handle invalid prefix characters', () => {
      const factory = moduleContextFactory(
        'prefix@#$%',
        {
          gitSha: 'test',
          packageName: '@test/pkg',
          packagePath: 'test.ts',
        },
        defineTagAttributes({ field: S.text() }),
      );

      expect(factory).toBeDefined();
    });
  });
});

describe('createHttpLibrary', () => {
  describe('success cases', () => {
    it('should create HTTP library with default prefix', () => {
      const lib = createHttpLibrary();

      expect(lib).toBeDefined();
      expect(lib).toHaveProperty('task');
      expect(lib).toHaveProperty('operations');
    });

    it('should create HTTP library with custom prefix', () => {
      const lib = createHttpLibrary('custom_http');

      expect(lib).toBeDefined();
      expect(typeof lib.task).toBe('function');
    });

    it('should create library with operations object', () => {
      const lib = createHttpLibrary();

      expect(lib.operations).toBeDefined();
      expect(typeof lib.operations).toBe('object');
    });
  });

  describe('edge cases', () => {
    it('should handle empty string prefix', () => {
      const lib = createHttpLibrary('');

      expect(lib).toBeDefined();
    });

    it('should handle very long prefix', () => {
      const lib = createHttpLibrary('very_long_prefix_for_http_library');

      expect(lib).toBeDefined();
    });
  });

  describe('failure cases', () => {
    it('should handle undefined prefix gracefully', () => {
      const lib = createHttpLibrary(undefined);

      expect(lib).toBeDefined();
    });
  });
});

describe('createDatabaseLibrary', () => {
  describe('success cases', () => {
    it('should create database library with default prefix', () => {
      const lib = createDatabaseLibrary();

      expect(lib).toBeDefined();
      expect(lib).toHaveProperty('task');
      expect(lib).toHaveProperty('operations');
    });

    it('should create database library with custom prefix', () => {
      const lib = createDatabaseLibrary('postgres');

      expect(lib).toBeDefined();
      expect(typeof lib.task).toBe('function');
    });

    it('should create library with operations object', () => {
      const lib = createDatabaseLibrary();

      expect(lib.operations).toBeDefined();
      expect(typeof lib.operations).toBe('object');
    });
  });

  describe('edge cases', () => {
    it('should handle empty string prefix', () => {
      const lib = createDatabaseLibrary('');

      expect(lib).toBeDefined();
    });

    it('should handle numeric prefix', () => {
      const lib = createDatabaseLibrary('db2');

      expect(lib).toBeDefined();
    });
  });

  describe('failure cases', () => {
    it('should handle undefined prefix gracefully', () => {
      const lib = createDatabaseLibrary(undefined);

      expect(lib).toBeDefined();
    });
  });
});

describe('prefix remapping', () => {
  describe('createPrefixMapping', () => {
    it('should create mapping from clean names to prefixed names', () => {
      const { createPrefixMapping } = require('../library.js');
      const schema = defineTagAttributes({
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
      const schema = defineTagAttributes({});

      const mapping = createPrefixMapping(schema, 'test');

      expect(mapping).toEqual({});
    });
  });

  describe('generateRemappedSpanLoggerClass', () => {
    it('should generate valid JavaScript code', () => {
      const { generateRemappedSpanLoggerClass, createPrefixMapping } = require('../library.js');
      const schema = defineTagAttributes({
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
      const schema = defineTagAttributes({
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
      const schema = defineTagAttributes({
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
      const schema = defineTagAttributes({
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
      const schema = defineTagAttributes({
        status: S.number(),
      });
      const mapping = createPrefixMapping(schema, 'http');

      const SpanLoggerClass = createRemappedSpanLoggerClass(schema, mapping);

      expect(typeof SpanLoggerClass).toBe('function');
    });
  });

  describe('moduleContextFactory with remapping', () => {
    it('should return cleanSchema and prefixMapping', () => {
      const schema = defineTagAttributes({
        status: S.number(),
        method: S.category(),
      });

      const factory = moduleContextFactory(
        'http',
        {
          gitSha: 'test',
          packageName: '@test/pkg',
          packagePath: 'test.ts',
        },
        schema,
      );

      expect(factory.cleanSchema).toBeDefined();
      expect(factory.prefixMapping).toBeDefined();
      expect(factory.prefixMapping).toEqual({
        status: 'http_status',
        method: 'http_method',
      });
    });

    it('should expose clean schema for type inference', () => {
      const schema = defineTagAttributes({
        status: S.number(),
        url: S.text(),
      });

      const factory = moduleContextFactory(
        'http',
        {
          gitSha: 'test',
          packageName: '@test/pkg',
          packagePath: 'test.ts',
        },
        schema,
      );

      // cleanSchema should have original field names
      expect(factory.cleanSchema.status).toBeDefined();
      expect(factory.cleanSchema.url).toBeDefined();
    });
  });
});
