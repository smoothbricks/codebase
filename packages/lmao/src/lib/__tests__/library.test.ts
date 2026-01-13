/**
 * Integration tests for Library Integration Pattern and Module Builder Pattern
 *
 * Tests specs/01e_library_integration_pattern.md and specs/01l_module_builder_pattern.md
 * - Module definition with defineModule()
 * - Prefix application for library composition
 * - Dependency wiring with .use()
 * - Clean API with prefixed column storage
 * - RemappedBufferView for Arrow conversion
 * - Collision detection and type safety
 * - Library composition scenarios
 * - End-to-end integration verification
 */

import { describe, expect, it } from 'bun:test';
import { defineOpContext } from '../defineOpContext.js';
import {
  createPrefixMapping,
  createRemappedSpanLoggerClass,
  generateRemappedBufferViewClass,
  generateRemappedSpanLoggerClass,
  type PrefixMapping,
  prefixSchema,
} from '../library.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { getMappedOpGroupInternals, getOpGroupInternals } from './test-helpers.js';

describe('Library Integration Pattern', () => {
  describe('Schema Prefixing', () => {
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

    it('should create correct prefix mapping', () => {
      const schema = defineLogSchema({
        status: S.number(),
        method: S.category(),
        url: S.text(),
      });

      const mapping = createPrefixMapping(schema, 'http');

      expect(mapping).toEqual({
        status: 'http_status',
        method: 'http_method',
        url: 'http_url',
      });
    });

    it('should handle empty schema', () => {
      const schema = defineLogSchema({});
      const mapping = createPrefixMapping(schema, 'test');

      expect(mapping).toEqual({});
    });

    it('should handle complex schema types with prefixing', () => {
      const schema = defineLogSchema({
        enumField: S.enum(['A', 'B', 'C']),
        categoryField: S.category(),
        textField: S.text(),
        numberField: S.number(),
        booleanField: S.boolean(),
      });

      const prefixed = prefixSchema(schema, 'test');

      // All fields should be prefixed
      expect(prefixed).toHaveProperty('test_enumField');
      expect(prefixed).toHaveProperty('test_categoryField');
      expect(prefixed).toHaveProperty('test_textField');
      expect(prefixed).toHaveProperty('test_numberField');
      expect(prefixed).toHaveProperty('test_booleanField');

      // Metadata should be preserved
      expect(prefixed.test_enumField).toHaveProperty('__schema_type', 'enum');
      expect(prefixed.test_categoryField).toHaveProperty('__schema_type', 'category');
      expect(prefixed.test_textField).toHaveProperty('__schema_type', 'text');
      expect(prefixed.test_numberField).toHaveProperty('__schema_type', 'number');
      expect(prefixed.test_booleanField).toHaveProperty('__schema_type', 'boolean');
    });

    it('should handle special characters in prefix', () => {
      const schema = defineLogSchema({
        field: S.text(),
      });

      const prefixed = prefixSchema(schema, 'prefix-with-dashes');

      expect(prefixed).toHaveProperty('prefix-with-dashes_field');
    });

    it('should handle schema with many fields', () => {
      const schemaFields: Record<string, ReturnType<typeof S.number>> = {};
      for (let i = 0; i < 100; i++) {
        schemaFields[`field${i}`] = S.number();
      }
      const schema = defineLogSchema(schemaFields);

      const prefixed = prefixSchema(schema, 'many');

      expect(prefixed._columnNames.length).toBe(100);
      expect(prefixed).toHaveProperty('many_field0');
      expect(prefixed).toHaveProperty('many_field99');
    });
  });

  describe('RemappedBufferView Generation', () => {
    it('should generate valid RemappedBufferView class', () => {
      const mapping: Record<string, string> = {
        http_status: 'status',
        http_method: 'method',
      };

      const ViewClass = generateRemappedBufferViewClass(mapping);

      // Should create a callable constructor
      expect(typeof ViewClass).toBe('function');

      // Should cache same mapping
      const ViewClass2 = generateRemappedBufferViewClass(mapping);
      expect(ViewClass).toBe(ViewClass2); // Same cached instance
    });

    it('should handle empty prefix mapping', () => {
      const mapping: Record<string, string> = {};
      const ViewClass = generateRemappedBufferViewClass(mapping);

      expect(typeof ViewClass).toBe('function');
    });

    it('should handle single field mapping', () => {
      const mapping: Record<string, string> = {
        http_status: 'status',
      };
      const ViewClass = generateRemappedBufferViewClass(mapping);

      expect(typeof ViewClass).toBe('function');
    });

    it('should correctly map column access from prefixed to unprefixed', () => {
      const mapping: Record<string, string> = {
        http_status: 'status',
        http_method: 'method',
      };

      const ViewClass = generateRemappedBufferViewClass(mapping);

      // Create mock column arrays to test mapping
      const mockValues = new Float64Array(1);
      const mockNulls = new Uint8Array(1);
      const mockBuffer = {
        getColumnIfAllocated: (name: string) => (name === 'status' ? mockValues : undefined),
        getNullsIfAllocated: (name: string) => (name === 'status' ? mockNulls : undefined),
        _children: [],
        _writeIndex: 0,
        timestamp: new BigInt64Array(1),
        entry_type: new Uint8Array(1),
        message_values: [],
        message_nulls: new Uint8Array(1),
        line_values: new Uint32Array(1),
        line_nulls: new Uint8Array(1),
        error_code_values: new Uint16Array(1),
        error_code_nulls: new Uint8Array(1),
        exception_stack_values: [],
        exception_stack_nulls: new Uint8Array(1),
        ff_value_values: [],
        ff_value_nulls: new Uint8Array(1),
        trace_id: 'test-trace-id',
        thread_id: 123n,
        span_id: 456,
        parent_span_id: null,
        _identity: 'test-identity',
        _logBinding: { package_name: 'test' },
      } as any;

      const view = new ViewClass(mockBuffer);

      // Should map prefixed to unprefixed
      expect(view.getColumnIfAllocated('http_status')).toBe(mockValues);
      expect(view.getNullsIfAllocated('http_status')).toBe(mockNulls);

      // Should pass through unknown names
      expect(view.getColumnIfAllocated('unknown')).toBeUndefined();
      expect(view.getNullsIfAllocated('unknown')).toBeUndefined();
    });
  });

  describe('RemappedSpanLogger Generation', () => {
    it('should generate valid JavaScript code', () => {
      const schema = defineLogSchema({
        status: S.number(),
        method: S.enum(['GET', 'POST']),
      });
      const mapping: PrefixMapping = {
        status: 'http_status',
        method: 'http_method',
      };

      const code = generateRemappedSpanLoggerClass(schema, mapping);

      // Should be valid JavaScript
      expect(() => new Function(`return ${code}`)()).not.toThrow();
    });

    it('should generate class with clean method names', () => {
      const schema = defineLogSchema({
        status: S.number(),
        duration: S.number(),
      });
      const mapping: PrefixMapping = {
        status: 'http_status',
        duration: 'http_duration',
      };

      const code = generateRemappedSpanLoggerClass(schema, mapping);

      // Should contain clean method names
      expect(code).toContain('status(value)');
      expect(code).toContain('duration(value)');
      // Should reference prefixed columns
      expect(code).toContain('http_status');
      expect(code).toContain('http_duration');
    });

    it('should include enum mapping functions', () => {
      const schema = defineLogSchema({
        method: S.enum(['GET', 'POST', 'PUT']),
      });
      const mapping: PrefixMapping = {
        method: 'http_method',
      };

      const code = generateRemappedSpanLoggerClass(schema, mapping);

      // Should contain enum mapping function
      expect(code).toContain('getEnumIndex_method');
      expect(code).toContain('case "GET"');
      expect(code).toContain('case "POST"');
      expect(code).toContain('case "PUT"');
    });

    it('should include prefix mapping for with() method', () => {
      const schema = defineLogSchema({
        status: S.number(),
      });
      const mapping: PrefixMapping = {
        status: 'http_status',
      };

      const code = generateRemappedSpanLoggerClass(schema, mapping);

      // Should include PREFIX_MAPPING constant
      expect(code).toContain('PREFIX_MAPPING');
      expect(code).toContain('"status":"http_status"');
    });

    it('should create a class constructor', () => {
      const schema = defineLogSchema({
        status: S.number(),
      });
      const mapping: PrefixMapping = {
        status: 'http_status',
      };

      const SpanLoggerClass = createRemappedSpanLoggerClass(schema, mapping);

      expect(typeof SpanLoggerClass).toBe('function');
    });

    it('should handle all field types in code generation', () => {
      const schema = defineLogSchema({
        enumField: S.enum(['A', 'B']),
        categoryField: S.category(),
        textField: S.text(),
        numberField: S.number(),
        booleanField: S.boolean(),
      });

      const mapping: PrefixMapping = {
        enumField: 'test_enumField',
        categoryField: 'test_categoryField',
        textField: 'test_textField',
        numberField: 'test_numberField',
        booleanField: 'test_booleanField',
      };

      const code = generateRemappedSpanLoggerClass(schema, mapping);

      // Should handle all field types appropriately
      expect(code).toContain('getEnumIndex_enumField'); // Enum mapping
      expect(code).toContain('categoryField(value)'); // Category storage
      expect(code).toContain('textField(value)'); // Text storage
      expect(code).toContain('numberField(value)'); // Number storage
      expect(code).toContain('booleanField(value)'); // Boolean storage
    });

    it('should cache generated classes for the same schema and mapping', () => {
      const schema = defineLogSchema({
        status: S.number(),
      });
      const mapping: PrefixMapping = {
        status: 'http_status',
      };

      const Class1 = createRemappedSpanLoggerClass(schema, mapping);
      const Class2 = createRemappedSpanLoggerClass(schema, mapping);

      // Same schema + mapping should return cached class
      expect(Class1).toBe(Class2);
    });

    it('should create different classes for different schemas', () => {
      const schema1 = defineLogSchema({ status: S.number() });
      const schema2 = defineLogSchema({ code: S.number() });
      const mapping1: PrefixMapping = { status: 'http_status' };
      const mapping2: PrefixMapping = { code: 'http_code' };

      const Class1 = createRemappedSpanLoggerClass(schema1, mapping1);
      const Class2 = createRemappedSpanLoggerClass(schema2, mapping2);

      expect(Class1).not.toBe(Class2);
    });

    it('should create different classes for same schema with different mappings', () => {
      const schema = defineLogSchema({ status: S.number() });
      const mapping1: PrefixMapping = { status: 'http_status' };
      const mapping2: PrefixMapping = { status: 'api_status' };

      const Class1 = createRemappedSpanLoggerClass(schema, mapping1);
      const Class2 = createRemappedSpanLoggerClass(schema, mapping2);

      expect(Class1).not.toBe(Class2);
    });
  });
});

describe('Module Builder Pattern Integration', () => {
  // Helper function to create test OpGroups
  const createHttpOpGroup = () => {
    const { defineOps } = defineOpContext({
      logSchema: defineLogSchema({
        status: S.number(),
        method: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
        url: S.text(),
        duration: S.number(),
      }),
    });
    return defineOps({});
  };

  const createDbOpGroup = () => {
    const { defineOps } = defineOpContext({
      logSchema: defineLogSchema({
        query: S.text(),
        duration: S.number(),
        rows: S.number(),
      }),
    });
    return defineOps({});
  };

  const createRetryOpGroup = () => {
    const { defineOps } = defineOpContext({
      logSchema: defineLogSchema({
        attempt: S.number(),
        delay: S.number(),
      }),
    });
    return defineOps({});
  };

  describe('OpGroup Definition and Schema Access', () => {
    it('should define OpGroups with schemas', () => {
      const { defineOps } = defineOpContext({
        logSchema: defineLogSchema({
          userId: S.category(),
          endpoint: S.category(),
        }),
      });

      const appOpGroup = defineOps({});

      expect(getOpGroupInternals(appOpGroup)._logSchema._columnNames).toContain('userId');
      expect(getOpGroupInternals(appOpGroup)._logSchema._columnNames).toContain('endpoint');
    });

    it('should provide access to OpGroup schema', () => {
      const httpOpGroup = createHttpOpGroup();

      // Test direct schema access
      expect(getOpGroupInternals(httpOpGroup)._logSchema._columnNames).toContain('status');
      expect(getOpGroupInternals(httpOpGroup)._logSchema._columnNames).toContain('method');
      expect(getOpGroupInternals(httpOpGroup)._logSchema._columnNames).toContain('url');
      expect(getOpGroupInternals(httpOpGroup)._logSchema._columnNames).toContain('duration');
    });

    it('should handle complex schema types', () => {
      const { defineOps } = defineOpContext({
        logSchema: defineLogSchema({
          enumField: S.enum(['A', 'B']),
          categoryField: S.category(),
          textField: S.text(),
          numberField: S.number(),
          booleanField: S.boolean(),
        }),
      });

      const complexOpGroup = defineOps({});

      expect(getOpGroupInternals(complexOpGroup)._logSchema._columnNames).toContain('enumField');
      expect(getOpGroupInternals(complexOpGroup)._logSchema._columnNames).toContain('categoryField');
      expect(getOpGroupInternals(complexOpGroup)._logSchema._columnNames).toContain('textField');
      expect(getOpGroupInternals(complexOpGroup)._logSchema._columnNames).toContain('numberField');
      expect(getOpGroupInternals(complexOpGroup)._logSchema._columnNames).toContain('booleanField');
    });

    it('should reject op names starting with underscore', () => {
      const { defineOps } = defineOpContext({
        logSchema: defineLogSchema({ field: S.category() }),
      });

      expect(() =>
        defineOps({
          _privateOp: async (ctx) => ctx.ok({}),
        }),
      ).toThrow('Op name "_privateOp" cannot start with underscore');
    });

    it('should reject reserved op names', () => {
      const { defineOps } = defineOpContext({
        logSchema: defineLogSchema({ field: S.category() }),
      });

      expect(() =>
        defineOps({
          prefix: async (ctx) => ctx.ok({}),
        }),
      ).toThrow('Op name "prefix" is reserved');

      expect(() =>
        defineOps({
          mapColumns: async (ctx) => ctx.ok({}),
        }),
      ).toThrow('Op name "mapColumns" is reserved');
    });

    it('should allow valid op names', () => {
      const { defineOps } = defineOpContext({
        logSchema: defineLogSchema({ field: S.category() }),
      });

      // These should NOT throw
      const opGroup = defineOps({
        doSomething: async (ctx) => ctx.ok({}),
        anotherOp: async (ctx) => ctx.ok({}),
      });

      expect(opGroup).toBeDefined();
    });
  });

  describe('Prefix Application', () => {
    it('should apply prefix to OpGroup schema', () => {
      const httpOpGroup = createHttpOpGroup();
      const prefixedHttpOpGroup = httpOpGroup.prefix('http');

      // Original schema should have unprefixed names
      expect(getOpGroupInternals(httpOpGroup)._logSchema._columnNames).toContain('status');
      expect(getOpGroupInternals(httpOpGroup)._logSchema._columnNames).toContain('method');

      // Verify that prefix was applied (check mapping)
      expect(getMappedOpGroupInternals(prefixedHttpOpGroup)._columnMapping.status).toBe('http_status');
      expect(getMappedOpGroupInternals(prefixedHttpOpGroup)._columnMapping.method).toBe('http_method');
    });

    it('should support prefix chaining', () => {
      const httpOpGroup = createHttpOpGroup();
      const doublePrefixed = httpOpGroup.prefix('api').prefix('v1');

      // Verify both prefixes are in the mapping
      expect(getMappedOpGroupInternals(doublePrefixed)._columnMapping.status).toBe('v1_api_status');
      expect(getMappedOpGroupInternals(doublePrefixed)._columnMapping.method).toBe('v1_api_method');
    });

    it('should handle different field types with prefixing', () => {
      const { defineOps } = defineOpContext({
        logSchema: defineLogSchema({
          enumField: S.enum(['A', 'B']),
          categoryField: S.category(),
          textField: S.text(),
          numberField: S.number(),
          booleanField: S.boolean(),
        }),
      });

      const complexOpGroup = defineOps({});
      const prefixed = complexOpGroup.prefix('complex');

      // Verify all field types are mapped with prefix
      expect(getMappedOpGroupInternals(prefixed)._columnMapping.enumField).toBe('complex_enumField');
      expect(getMappedOpGroupInternals(prefixed)._columnMapping.categoryField).toBe('complex_categoryField');
      expect(getMappedOpGroupInternals(prefixed)._columnMapping.textField).toBe('complex_textField');
      expect(getMappedOpGroupInternals(prefixed)._columnMapping.numberField).toBe('complex_numberField');
      expect(getMappedOpGroupInternals(prefixed)._columnMapping.booleanField).toBe('complex_booleanField');
    });

    it('should preserve schema metadata in prefixed OpGroups', () => {
      const httpOpGroup = createHttpOpGroup();

      // Schema should preserve field metadata
      const statusField = getOpGroupInternals(httpOpGroup)._logSchema.fields.status;
      expect(statusField).toHaveProperty('__schema_type', 'number');
    });
  });

  describe('OpGroup Mapping and Composition', () => {
    it('should map OpGroup columns', () => {
      const httpOpGroup = createHttpOpGroup();
      const retryOpGroup = createRetryOpGroup();

      // Map HTTP OpGroup with custom prefix
      const mappedHttp = httpOpGroup.prefix('http');
      const mappedRetry = retryOpGroup.prefix('http_retry');

      expect(mappedHttp).toBeDefined();
      // Verify the mappings are set
      expect(getMappedOpGroupInternals(mappedHttp)._columnMapping.status).toBe('http_status');
      expect(getMappedOpGroupInternals(mappedRetry)._columnMapping.attempt).toBe('http_retry_attempt');
      expect(getMappedOpGroupInternals(mappedRetry)._columnMapping.delay).toBe('http_retry_delay');
    });

    it('should support multiple OpGroups with different prefixes', () => {
      const httpOpGroup = createHttpOpGroup();
      const dbOpGroup = createDbOpGroup();
      const retryOpGroup = createRetryOpGroup();

      // Create different prefixed versions
      const mappedHttp = httpOpGroup.prefix('http');
      const mappedDb = dbOpGroup.prefix('db');
      const mappedRetry = retryOpGroup.prefix('shared_retry');

      expect(mappedHttp).toBeDefined();
      expect(mappedDb).toBeDefined();
      expect(mappedRetry).toBeDefined();

      // Verify each has correct mappings
      expect(getMappedOpGroupInternals(mappedHttp)._columnMapping.status).toBe('http_status');
      expect(getMappedOpGroupInternals(mappedDb)._columnMapping.query).toBe('db_query');
      expect(getMappedOpGroupInternals(mappedRetry)._columnMapping.attempt).toBe('shared_retry_attempt');
    });

    it('should handle dependency chains', () => {
      const retryOpGroup = createRetryOpGroup();
      const httpOpGroup = createHttpOpGroup();

      // Create prefixed versions
      const nestedRetry = retryOpGroup.prefix('http_retry_nested');
      const wiredHttp = httpOpGroup.prefix('http');

      expect(wiredHttp).toBeDefined();
      expect(nestedRetry).toBeDefined();
      expect(getMappedOpGroupInternals(nestedRetry)._columnMapping.attempt).toBe('http_retry_nested_attempt');
      expect(getMappedOpGroupInternals(nestedRetry)._columnMapping.delay).toBe('http_retry_nested_delay');
    });

    it('should maintain type safety in OpGroup composition', () => {
      const httpOpGroup = createHttpOpGroup();
      const retryOpGroup = createRetryOpGroup();

      // This should compile without TypeScript errors
      const mappedHttp = httpOpGroup.prefix('http');
      const mappedRetry = retryOpGroup.prefix('http_retry');

      expect(mappedHttp).toBeDefined();
      expect(mappedRetry).toBeDefined();

      // Verify that original schemas are accessible
      expect(getMappedOpGroupInternals(mappedHttp)._logSchema._columnNames).toContain('status');
      expect(getMappedOpGroupInternals(mappedRetry)._logSchema._columnNames).toContain('attempt');
    });
  });

  describe('Op Context Creation', () => {
    it('should define Op context with extra properties', () => {
      const factory = defineOpContext({
        logSchema: defineLogSchema({}),
        ctx: {
          env: null as { region: string } | null,
          requestId: '' as string,
          userId: undefined as string | undefined,
        },
      });

      expect(factory.defineOp).toBeDefined();
      expect(factory.defineOps).toBeDefined();
      // logBinding is used with new Tracer({ logBinding, sink }) pattern
      expect(factory.logBinding).toBeDefined();
    });

    it('should define Op context with required context properties', () => {
      const factory = defineOpContext({
        logSchema: defineLogSchema({}),
        ctx: {
          env: null as { region: string } | null,
          requestId: '' as string,
        },
      });

      // logBinding is used with new Tracer({ logBinding, sink }) pattern
      expect(factory.logBinding).toBeDefined();
    });

    it('should define Op context with optional context properties', () => {
      const factory = defineOpContext({
        logSchema: defineLogSchema({}),
        ctx: {
          env: null as { region: string } | null,
          requestId: '' as string,
          userId: undefined as string | undefined,
        },
      });

      // logBinding is used with new Tracer({ logBinding, sink }) pattern
      expect(factory.logBinding).toBeDefined();
    });

    it('should define Op context with context defaults', () => {
      const factory = defineOpContext({
        logSchema: defineLogSchema({}),
        ctx: {
          env: null as { region: string } | null,
          requestId: '' as string,
          timeout: 5000 as number,
        },
      });

      // logBinding is used with new Tracer({ logBinding, sink }) pattern
      expect(factory.logBinding).toBeDefined();
    });
  });
});

describe('Library Composition Scenarios', () => {
  // Helper factories for creating Op contexts
  const createHttpOpGroupFactory = () => {
    return defineOpContext({
      logSchema: defineLogSchema({
        status: S.number(),
        method: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
        url: S.text(),
        duration: S.number(),
      }),
    });
  };

  const createDbOpGroupFactory = () => {
    return defineOpContext({
      logSchema: defineLogSchema({
        query: S.text(),
        duration: S.number(),
        rows: S.number(),
      }),
    });
  };

  const createRetryOpGroupFactory = () => {
    return defineOpContext({
      logSchema: defineLogSchema({
        attempt: S.number(),
        delay: S.number(),
      }),
    });
  };

  describe('Simple Library Usage', () => {
    it('should handle single library usage with prefixing', () => {
      const httpFactory = createHttpOpGroupFactory();
      const { defineOps } = httpFactory;
      const httpOpGroup = defineOps({});
      const prefixedHttp = httpOpGroup.prefix('http');

      // Verify that the mapping has all http fields
      expect(getMappedOpGroupInternals(prefixedHttp)._columnMapping.status).toBe('http_status');
      expect(getMappedOpGroupInternals(prefixedHttp)._columnMapping.method).toBe('http_method');
      expect(getMappedOpGroupInternals(prefixedHttp)._columnMapping.url).toBe('http_url');
      expect(getMappedOpGroupInternals(prefixedHttp)._columnMapping.duration).toBe('http_duration');
    });

    it('should maintain separate namespaces with multiple OpGroups', () => {
      const httpFactory = createHttpOpGroupFactory();
      const dbFactory = createDbOpGroupFactory();

      const { defineOps: defineHttpOps } = httpFactory;
      const { defineOps: defineDbOps } = dbFactory;

      const httpOpGroup = defineHttpOps({});
      const dbOpGroup = defineDbOps({});

      const prefixedHttp = httpOpGroup.prefix('http');
      const prefixedDb = dbOpGroup.prefix('db');

      // Each library should have its own prefix
      expect(getMappedOpGroupInternals(prefixedHttp)._columnMapping.status).toBe('http_status');
      expect(getMappedOpGroupInternals(prefixedHttp)._columnMapping.method).toBe('http_method');
      expect(getMappedOpGroupInternals(prefixedDb)._columnMapping.query).toBe('db_query');
      expect(getMappedOpGroupInternals(prefixedDb)._columnMapping.duration).toBe('db_duration');
      expect(getMappedOpGroupInternals(prefixedDb)._columnMapping.rows).toBe('db_rows');
    });
  });

  describe('Nested Library Dependencies', () => {
    it('should handle chained prefix mapping', () => {
      const httpFactory = createHttpOpGroupFactory();
      const retryFactory = createRetryOpGroupFactory();

      const { defineOps: defineHttpOps } = httpFactory;
      const { defineOps: defineRetryOps } = retryFactory;

      const httpOpGroup = defineHttpOps({});
      const retryOpGroup = defineRetryOps({});

      // HTTP prefixed with 'http'
      const prefixedHttp = httpOpGroup.prefix('http');
      // Retry prefixed with 'http_retry'
      const prefixedRetry = retryOpGroup.prefix('http_retry');

      expect(getMappedOpGroupInternals(prefixedHttp)._columnMapping.status).toBe('http_status');
      expect(getMappedOpGroupInternals(prefixedHttp)._columnMapping.method).toBe('http_method');
      expect(getMappedOpGroupInternals(prefixedHttp)._columnMapping.url).toBe('http_url');
      expect(getMappedOpGroupInternals(prefixedHttp)._columnMapping.duration).toBe('http_duration');

      // Retry dependency fields should be present with prefix
      expect(getMappedOpGroupInternals(prefixedRetry)._columnMapping.attempt).toBe('http_retry_attempt');
      expect(getMappedOpGroupInternals(prefixedRetry)._columnMapping.delay).toBe('http_retry_delay');
    });

    it('should handle multiple prefix chains', () => {
      const retryFactory = createRetryOpGroupFactory();
      const httpFactory = createHttpOpGroupFactory();

      const { defineOps: defineRetryOps } = retryFactory;
      const { defineOps: defineHttpOps } = httpFactory;

      const retryOpGroup = defineRetryOps({});
      const httpOpGroup = defineHttpOps({});

      // Create different prefixed versions
      const retryForHttp = retryOpGroup.prefix('http_retry');
      const retryForAuth = retryOpGroup.prefix('http_auth_retry');
      const sharedRetry = retryOpGroup.prefix('shared_auth_retry');

      // Verify each mapping is correct
      expect(getMappedOpGroupInternals(retryForHttp)._columnMapping.attempt).toBe('http_retry_attempt');
      expect(getMappedOpGroupInternals(retryForAuth)._columnMapping.attempt).toBe('http_auth_retry_attempt');
      expect(getMappedOpGroupInternals(sharedRetry)._columnMapping.attempt).toBe('shared_auth_retry_attempt');

      // HTTP stays prefixed with 'http'
      const prefixedHttp = httpOpGroup.prefix('http');
      expect(getMappedOpGroupInternals(prefixedHttp)._columnMapping.status).toBe('http_status');
    });
  });

  describe('Complex Composition', () => {
    it('should handle multiple OpGroups with shared prefixes', () => {
      const httpFactory = createHttpOpGroupFactory();
      const dbFactory = createDbOpGroupFactory();
      const retryFactory = createRetryOpGroupFactory();
      const cacheFactory = defineOpContext({
        logSchema: defineLogSchema({
          hits: S.number(),
          misses: S.number(),
        }),
      });

      const { defineOps: defineHttpOps } = httpFactory;
      const { defineOps: defineDbOps } = dbFactory;
      const { defineOps: defineRetryOps } = retryFactory;
      const { defineOps: defineCacheOps } = cacheFactory;

      const httpOpGroup = defineHttpOps({});
      const dbOpGroup = defineDbOps({});
      const retryOpGroup = defineRetryOps({});
      const cacheOpGroup = defineCacheOps({});

      // Create shared retry instance for both HTTP and DB
      const sharedRetry = retryOpGroup.prefix('shared_retry');
      const sharedCache = cacheOpGroup.prefix('shared_cache');

      // Create prefixed versions
      const prefixedHttp = httpOpGroup.prefix('http');
      const prefixedDb = dbOpGroup.prefix('db');
      const dbCache = cacheOpGroup.prefix('db_cache');

      expect(prefixedHttp).toBeDefined();
      expect(prefixedDb).toBeDefined();
      expect(sharedRetry).toBeDefined();
      expect(sharedCache).toBeDefined();

      // Verify all mappings are correct
      expect(getMappedOpGroupInternals(prefixedHttp)._columnMapping.status).toBe('http_status');
      expect(getMappedOpGroupInternals(prefixedHttp)._columnMapping.method).toBe('http_method');
      expect(getMappedOpGroupInternals(prefixedDb)._columnMapping.query).toBe('db_query');
      expect(getMappedOpGroupInternals(prefixedDb)._columnMapping.duration).toBe('db_duration');
      expect(getMappedOpGroupInternals(prefixedDb)._columnMapping.rows).toBe('db_rows');
      expect(getMappedOpGroupInternals(sharedRetry)._columnMapping.attempt).toBe('shared_retry_attempt');
      expect(getMappedOpGroupInternals(sharedRetry)._columnMapping.delay).toBe('shared_retry_delay');
      expect(getMappedOpGroupInternals(sharedCache)._columnMapping.hits).toBe('shared_cache_hits');
      expect(getMappedOpGroupInternals(sharedCache)._columnMapping.misses).toBe('shared_cache_misses');
      expect(getMappedOpGroupInternals(dbCache)._columnMapping.hits).toBe('db_cache_hits');
      expect(getMappedOpGroupInternals(dbCache)._columnMapping.misses).toBe('db_cache_misses');
    });

    it('should handle different prefix patterns', () => {
      const httpFactory = createHttpOpGroupFactory();
      const dbFactory = createDbOpGroupFactory();
      const retryFactory = createRetryOpGroupFactory();

      const { defineOps: defineHttpOps } = httpFactory;
      const { defineOps: defineDbOps } = dbFactory;
      const { defineOps: defineRetryOps } = retryFactory;

      const httpOpGroup = defineHttpOps({});
      const dbOpGroup = defineDbOps({});
      const retryOpGroup = defineRetryOps({});

      // Pattern: Shared retry with different prefix configurations
      const fastRetry = retryOpGroup.prefix('fast_retry');
      const slowRetry = retryOpGroup.prefix('slow_retry');

      const prefixedHttp = httpOpGroup.prefix('http');
      const prefixedDb = dbOpGroup.prefix('db');

      expect(prefixedHttp).toBeDefined();
      expect(prefixedDb).toBeDefined();

      // Verify that both retry prefixes are available
      expect(getMappedOpGroupInternals(fastRetry)._columnMapping.attempt).toBe('fast_retry_attempt');
      expect(getMappedOpGroupInternals(slowRetry)._columnMapping.attempt).toBe('slow_retry_attempt');

      // HTTP and DB have their own prefixes
      expect(getMappedOpGroupInternals(prefixedHttp)._columnMapping.status).toBe('http_status');
      expect(getMappedOpGroupInternals(prefixedDb)._columnMapping.query).toBe('db_query');
    });
  });
});

describe('Edge Cases and Error Handling', () => {
  describe('Prefix Edge Cases', () => {
    it('should handle empty prefix string', () => {
      const schema = defineLogSchema({
        field: S.text(),
      });

      const prefixed = prefixSchema(schema, '');

      expect(prefixed).toHaveProperty('_field');
    });

    it('should handle null and undefined values gracefully', () => {
      // Test with undefined field (edge case)
      const schemaWithUndefined = new (require('../schema/LogSchema.js') as any).LogSchema({
        validField: S.text(),
        undefinedField: undefined,
      });

      const prefixedWithUndefined = prefixSchema(schemaWithUndefined, 'test');

      expect(prefixedWithUndefined).toHaveProperty('test_validField');
      expect(prefixedWithUndefined).toHaveProperty('test_undefinedField');
      expect((prefixedWithUndefined as any).test_undefinedField).toBeUndefined();

      // Test with null field (edge case)
      const schemaWithNull = new (require('../schema/LogSchema.js') as any).LogSchema({
        validField: S.text(),
        nullField: null,
      });

      const prefixedWithNull = prefixSchema(schemaWithNull, 'test');

      expect(prefixedWithNull).toHaveProperty('test_validField');
      expect(prefixedWithNull).toHaveProperty('test_nullField');
      expect((prefixedWithNull as any).test_nullField).toBeNull();
    });

    it('should handle schema with many fields', () => {
      const schemaFields: Record<string, ReturnType<typeof S.number>> = {};
      for (let i = 0; i < 100; i++) {
        schemaFields[`field${i}`] = S.number();
      }
      const schema = defineLogSchema(schemaFields);

      const prefixed = prefixSchema(schema, 'many');

      expect(prefixed._columnNames.length).toBe(100);
      expect(prefixed).toHaveProperty('many_field0');
      expect(prefixed).toHaveProperty('many_field99');
    });
  });

  describe('Buffer Remapping Edge Cases', () => {
    it('should handle mapping consistency', () => {
      // Test that mapping and code generation are consistent
      const schema = defineLogSchema({
        status: S.number(),
        method: S.enum(['GET', 'POST']),
      });

      const mapping = createPrefixMapping(schema, 'http');
      const code = generateRemappedSpanLoggerClass(schema, mapping);

      // Verify that the mapping referenced in code matches the generated mapping
      expect(code).toContain('"status":"http_status"');
      expect(code).toContain('"method":"http_method"');

      // Verify that all prefixed columns are properly referenced
      expect(code).toContain('http_status_values');
      expect(code).toContain('http_status_nulls');
      expect(code).toContain('http_method_values');
      expect(code).toContain('http_method_nulls');
    });
  });
});
