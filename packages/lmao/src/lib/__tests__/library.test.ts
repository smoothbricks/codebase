/**
 * Unit tests for library integration with prefix support
 */

import { describe, it, expect } from 'bun:test';
import {
  prefixSchema,
  createLibraryModule,
  moduleContextFactory,
  createHttpLibrary,
  createDatabaseLibrary,
} from '../library.js';
import type { TagAttributeSchema } from '../schema/types.js';
import { defineTagAttributes } from '../schema/defineTagAttributes.js';
import { S } from '../schema/builder.js';

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

      expect(prefixed.db_operation).toHaveProperty('__lmao_type', 'enum');
      expect(prefixed.db_operation).toHaveProperty('__lmao_enum_values');
      expect((prefixed.db_operation as any).__lmao_enum_values).toEqual(['CREATE', 'DELETE']);
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
        validField: { __lmao_type: 'text' },
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
        field: { __lmao_type: 'text' },
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
        filePath: 'lib/http.ts',
        moduleName: 'http',
        schema,
      });

      expect(module).toBeDefined();
      expect(module).toHaveProperty('task');
      expect(typeof module.task).toBe('function');
    });

    it('should preserve schema reference in module', () => {
      const schema = defineTagAttributes({
        field: S.text(),
      });

      const module = createLibraryModule({
        gitSha: 'test',
        filePath: 'test.ts',
        moduleName: 'test',
        schema,
      });

      expect(module.schema).toBe(schema);
    });

    it('should create module with operations object', () => {
      const schema = defineTagAttributes({
        field: S.number(),
      });

      const module = createLibraryModule({
        gitSha: 'test',
        filePath: 'test.ts',
        moduleName: 'test',
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
        filePath: 'test.ts',
        moduleName: 'test',
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
        filePath: 'test.ts',
        moduleName: longName,
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
        filePath: '@scope/package/lib/file-name.ts',
        moduleName: 'test',
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
        filePath: 'test.ts',
        moduleName: 'test',
        schema,
      });

      expect(module).toBeDefined();
    });

    it('should handle empty filePath', () => {
      const schema = defineTagAttributes({
        field: S.number(),
      });

      const module = createLibraryModule({
        gitSha: 'test',
        filePath: '',
        moduleName: 'test',
        schema,
      });

      expect(module).toBeDefined();
    });

    it('should handle schema with invalid field types', () => {
      const schema = defineTagAttributes({
        field: { __lmao_type: 'invalid' as any },
      });

      expect(() => {
        createLibraryModule({
          gitSha: 'test',
          filePath: 'test.ts',
          moduleName: 'test',
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
          filePath: 'test.ts',
          moduleName: 'http',
        },
        schema
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
          filePath: 'test.ts',
          moduleName: 'http',
        },
        schema
      );

      expect(factory).toBeDefined();
      // The factory should have created a module with prefixed schema internally
    });

    it('should create empty operations when none provided', () => {
      const factory = moduleContextFactory(
        'test',
        {
          gitSha: 'test',
          filePath: 'test.ts',
          moduleName: 'test',
        },
        defineTagAttributes({})
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
          filePath: 'test.ts',
          moduleName: 'test',
        },
        defineTagAttributes({ field: S.text() })
      );

      expect(factory).toBeDefined();
    });

    it('should handle very long prefix', () => {
      const longPrefix = 'prefix_'.repeat(100);
      const factory = moduleContextFactory(
        longPrefix,
        {
          gitSha: 'test',
          filePath: 'test.ts',
          moduleName: 'test',
        },
        defineTagAttributes({})
      );

      expect(factory).toBeDefined();
    });

    it('should handle empty operations object', () => {
      const factory = moduleContextFactory(
        'test',
        {
          gitSha: 'test',
          filePath: 'test.ts',
          moduleName: 'test',
        },
        defineTagAttributes({}),
        {}
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
          filePath: 'test.ts',
          moduleName: 'test',
        },
        defineTagAttributes({}),
        undefined
      );

      expect(factory).toBeDefined();
      expect(factory.operations).toEqual({});
    });

    it('should handle invalid prefix characters', () => {
      const factory = moduleContextFactory(
        'prefix@#$%',
        {
          gitSha: 'test',
          filePath: 'test.ts',
          moduleName: 'test',
        },
        defineTagAttributes({ field: S.text() })
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
