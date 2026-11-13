/**
 * Integration tests for Library Integration Pattern and Module Builder Pattern
 *
 * Tests specs/lmao/01e_library_integration_pattern.md and specs/lmao/01l_op_context_pattern.md
 * - Module definition with defineModule()
 * - Prefix application for library composition
 * - Dependency wiring with .use()
 * - Clean API with prefixed column storage
 * - Immutable remap descriptors for Arrow conversion
 * - Collision detection and type safety
 * - Library composition scenarios
 * - End-to-end integration verification
 */

import { describe, expect, it } from 'bun:test';
import { defineOpContext } from '../defineOpContext.js';
import { createRemapDescriptor } from '../library.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { getEnumValues, storageLaneOf } from '../schema/typeGuards.js';
import { getMappedOpGroupInternals, getOpGroupInternals } from './test-helpers.js';

describe('Library Integration Pattern', () => {
  //#region smoo/lmao!n/library-integration-remapped-view
  describe('Remap descriptor metadata', () => {
    it('preserves composed output order and schema identity in immutable cold-path metadata', () => {
      const schema = defineLogSchema({
        status: S.number(),
        method: S.enum(['GET', 'POST']),
      });

      const descriptor = createRemapDescriptor(schema, {
        v1_api_method: 'method',
        v1_api_status: 'status',
      });

      expect(descriptor.sourceNames).toEqual({
        v1_api_method: 'method',
        v1_api_status: 'status',
      });
      expect(descriptor.columns).toEqual([
        ['v1_api_method', 'method', schema.fields.method, 'method_values', 'method_nulls'],
        ['v1_api_status', 'status', schema.fields.status, 'status_values', 'status_nulls'],
      ]);
      expect(Object.isFrozen(descriptor)).toBe(true);
      expect(Object.isFrozen(descriptor.sourceNames)).toBe(true);
      expect(Object.isFrozen(descriptor.columns)).toBe(true);
      expect(descriptor.columns.every(Object.isFrozen)).toBe(true);
    });
  });
  //#endregion smoo/lmao!n/library-integration-remapped-view
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

  //#region smoo/lmao!n/op-context-defining-ops
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
  //#endregion smoo/lmao!n/op-context-defining-ops

  //#region smoo/lmao!n/library-integration-column-mapping
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
      const prefixedHttp = httpOpGroup.prefix('http');

      // The group's schema keeps its field nodes, before and after prefixing
      const statusField = getOpGroupInternals(httpOpGroup)._logSchema.fields.status;
      expect(storageLaneOf(statusField)).toBe('number');

      const prefixedFields = getMappedOpGroupInternals(prefixedHttp)._logSchema.fields;
      expect(prefixedFields.status).toBe(statusField);
      expect(getEnumValues(prefixedFields.method)).toEqual(['GET', 'POST', 'PUT', 'DELETE']);
    });
  });
  //#endregion smoo/lmao!n/library-integration-column-mapping

  //#region smoo/lmao!n/library-integration-column-mapping
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

    it('mapColumns names the library own fields after prefix, and an unnamed field keeps its column', () => {
      const dbOpGroup = createDbOpGroup();
      const wired = dbOpGroup.prefix('pg').mapColumns({ query: 'query', rows: null });

      const columns = getMappedOpGroupInternals(wired)._columnMapping;
      expect(columns.query).toBe('query');
      expect(columns.rows).toBeNull();
      expect(columns.duration).toBe('pg_duration');
    });
  });
  //#endregion smoo/lmao!n/library-integration-column-mapping

  //#region smoo/lmao!n/op-context-define-returns
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
  //#endregion smoo/lmao!n/op-context-define-returns
});

//#region smoo/lmao!n/library-integration-column-mapping
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
//#endregion smoo/lmao!n/library-integration-column-mapping
