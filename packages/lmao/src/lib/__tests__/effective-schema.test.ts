/**
 * Effective Schema Computation Tests
 *
 * Tests verify that defineOpContext properly computes the effective schema
 * by combining the app's schema with all dep contributed schemas:
 * - OpGroup deps contribute their original schema fields
 * - MappedOpGroup deps contribute their transformed fields (after prefix/mapping)
 * - Null-mapped columns are dropped
 * - Conflict detection works (same column from multiple deps throws)
 * - Schema is accessible via OpContextFactory.logSchema
 */

import { describe, expect, it } from 'bun:test';
import { defineOpContext } from '../defineOpContext.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';

describe('Effective Schema Computation', () => {
  describe('No Dependencies', () => {
    it('should return app schema when no deps', () => {
      const appSchema = defineLogSchema({
        userId: S.category(),
        endpoint: S.text(),
      });

      const context = defineOpContext({
        logSchema: appSchema,
      });

      expect(context.logSchema._columnNames).toEqual(['userId', 'endpoint']);
    });
  });

  describe('Single Dependency', () => {
    it('should combine app schema + unprefixed dep schema', () => {
      // Define library
      const httpSchema = defineLogSchema({
        status: S.number(),
        method: S.enum(['GET', 'POST'] as const),
      });

      const httpContext = defineOpContext({
        logSchema: httpSchema,
      });

      const httpOps = httpContext.defineOps({
        fetch: async (ctx) => ctx.ok({ data: 'ok' }),
      });

      // Define app using library (no prefix)
      const appSchema = defineLogSchema({
        userId: S.category(),
      });

      const appContext = defineOpContext({
        logSchema: appSchema,
        deps: {
          http: httpOps, // No prefix - contributes original fields
        },
      });

      // Should have all fields from both schemas
      expect(appContext.logSchema._columnNames).toContain('userId');
      expect(appContext.logSchema._columnNames).toContain('status');
      expect(appContext.logSchema._columnNames).toContain('method');
      expect(appContext.logSchema._columnNames.length).toBe(3);
    });

    it('should combine app schema + prefixed dep schema', () => {
      // Define library
      const httpSchema = defineLogSchema({
        status: S.number(),
        method: S.enum(['GET', 'POST'] as const),
        url: S.text(),
      });

      const httpContext = defineOpContext({
        logSchema: httpSchema,
      });

      const httpOps = httpContext.defineOps({
        fetch: async (ctx) => ctx.ok({ data: 'ok' }),
      });

      // Define app using library with prefix
      const appSchema = defineLogSchema({
        userId: S.category(),
        endpoint: S.text(),
      });

      const appContext = defineOpContext({
        logSchema: appSchema,
        deps: {
          http: httpOps.prefix('http'),
        },
      });

      // Should have app fields + prefixed library fields
      expect(appContext.logSchema._columnNames).toContain('userId');
      expect(appContext.logSchema._columnNames).toContain('endpoint');
      expect(appContext.logSchema._columnNames).toContain('http_status');
      expect(appContext.logSchema._columnNames).toContain('http_method');
      expect(appContext.logSchema._columnNames).toContain('http_url');
      expect(appContext.logSchema._columnNames.length).toBe(5);

      // Should NOT have unprefixed library fields
      expect(appContext.logSchema._columnNames).not.toContain('status');
      expect(appContext.logSchema._columnNames).not.toContain('method');
      expect(appContext.logSchema._columnNames).not.toContain('url');
    });
  });

  describe('Multiple Dependencies', () => {
    it('should combine app schema + multiple prefixed deps', () => {
      // Define HTTP library
      const httpSchema = defineLogSchema({
        status: S.number(),
        method: S.enum(['GET', 'POST'] as const),
      });

      const httpContext = defineOpContext({
        logSchema: httpSchema,
      });

      const httpOps = httpContext.defineOps({
        fetch: async (ctx) => ctx.ok({ data: 'ok' }),
      });

      // Define DB library
      const dbSchema = defineLogSchema({
        query: S.text(),
        duration: S.number(),
        rows: S.number(),
      });

      const dbContext = defineOpContext({
        logSchema: dbSchema,
      });

      const dbOps = dbContext.defineOps({
        execute: async (ctx) => ctx.ok({ rows: 0 }),
      });

      // Define app using both libraries
      const appSchema = defineLogSchema({
        userId: S.category(),
        endpoint: S.text(),
      });

      const appContext = defineOpContext({
        logSchema: appSchema,
        deps: {
          http: httpOps.prefix('http'),
          db: dbOps.prefix('db'),
        },
      });

      // Should have app fields
      expect(appContext.logSchema._columnNames).toContain('userId');
      expect(appContext.logSchema._columnNames).toContain('endpoint');

      // Should have HTTP fields with prefix
      expect(appContext.logSchema._columnNames).toContain('http_status');
      expect(appContext.logSchema._columnNames).toContain('http_method');

      // Should have DB fields with prefix
      expect(appContext.logSchema._columnNames).toContain('db_query');
      expect(appContext.logSchema._columnNames).toContain('db_duration');
      expect(appContext.logSchema._columnNames).toContain('db_rows');

      expect(appContext.logSchema._columnNames.length).toBe(7);
    });

    it('should handle mixed prefixed and unprefixed deps', () => {
      // Define retry library
      const retrySchema = defineLogSchema({
        attempt: S.number(),
        delay: S.number(),
      });

      const retryContext = defineOpContext({
        logSchema: retrySchema,
      });

      const retryOps = retryContext.defineOps({
        withRetry: async (ctx) => ctx.ok({}),
      });

      // Define HTTP library
      const httpSchema = defineLogSchema({
        status: S.number(),
      });

      const httpContext = defineOpContext({
        logSchema: httpSchema,
      });

      const httpOps = httpContext.defineOps({
        fetch: async (ctx) => ctx.ok({}),
      });

      // Define app
      const appSchema = defineLogSchema({
        userId: S.category(),
      });

      const appContext = defineOpContext({
        logSchema: appSchema,
        deps: {
          retry: retryOps, // No prefix
          http: httpOps.prefix('http'),
        },
      });

      // Should have all fields
      expect(appContext.logSchema._columnNames).toContain('userId');
      expect(appContext.logSchema._columnNames).toContain('attempt'); // unprefixed
      expect(appContext.logSchema._columnNames).toContain('delay'); // unprefixed
      expect(appContext.logSchema._columnNames).toContain('http_status'); // prefixed
      expect(appContext.logSchema._columnNames.length).toBe(4);
    });
  });

  describe('Column Mapping', () => {
    it('should drop null-mapped columns', () => {
      // Define library with debug column
      const libSchema = defineLogSchema({
        publicData: S.text(),
        debugFlag: S.number(),
      });

      const libContext = defineOpContext({
        logSchema: libSchema,
      });

      const libOps = libContext.defineOps({
        doWork: async (ctx) => ctx.ok({}),
      });

      // Define app that drops debug column
      const appSchema = defineLogSchema({
        userId: S.category(),
      });

      const appContext = defineOpContext({
        logSchema: appSchema,
        deps: {
          lib: libOps.mapColumns({ publicData: 'lib_data', debugFlag: null }),
        },
      });

      // Should have userId + lib_data, but NOT debugFlag
      expect(appContext.logSchema._columnNames).toContain('userId');
      expect(appContext.logSchema._columnNames).toContain('lib_data');
      expect(appContext.logSchema._columnNames).not.toContain('debugFlag');
      expect(appContext.logSchema._columnNames).not.toContain('publicData'); // renamed to lib_data
      expect(appContext.logSchema._columnNames.length).toBe(2);
    });
  });

  describe('Conflict Detection', () => {
    it('should throw when deps contribute conflicting column names', () => {
      // Define two libraries with same column name
      const lib1Schema = defineLogSchema({
        status: S.number(),
      });

      const lib1Context = defineOpContext({
        logSchema: lib1Schema,
      });

      const lib1Ops = lib1Context.defineOps({
        op1: async (ctx) => ctx.ok({}),
      });

      const lib2Schema = defineLogSchema({
        status: S.text(), // Same name, different type
      });

      const lib2Context = defineOpContext({
        logSchema: lib2Schema,
      });

      const lib2Ops = lib2Context.defineOps({
        op2: async (ctx) => ctx.ok({}),
      });

      // Define app with both (should throw due to conflict)
      const appSchema = defineLogSchema({
        userId: S.category(),
      });

      expect(() => {
        defineOpContext({
          logSchema: appSchema,
          deps: {
            lib1: lib1Ops,
            lib2: lib2Ops, // Conflict: both contribute 'status'
          },
        });
      }).toThrow(/conflict/i);
    });

    it('should NOT conflict when using different prefixes', () => {
      // Define two libraries with same column name
      const lib1Schema = defineLogSchema({
        status: S.number(),
      });

      const lib1Context = defineOpContext({
        logSchema: lib1Schema,
      });

      const lib1Ops = lib1Context.defineOps({
        op1: async (ctx) => ctx.ok({}),
      });

      const lib2Schema = defineLogSchema({
        status: S.text(), // Same name, different type
      });

      const lib2Context = defineOpContext({
        logSchema: lib2Schema,
      });

      const lib2Ops = lib2Context.defineOps({
        op2: async (ctx) => ctx.ok({}),
      });

      // Define app with prefixes (no conflict)
      const appSchema = defineLogSchema({
        userId: S.category(),
      });

      const appContext = defineOpContext({
        logSchema: appSchema,
        deps: {
          lib1: lib1Ops.prefix('lib1'),
          lib2: lib2Ops.prefix('lib2'),
        },
      });

      // Should have both with different names
      expect(appContext.logSchema._columnNames).toContain('userId');
      expect(appContext.logSchema._columnNames).toContain('lib1_status');
      expect(appContext.logSchema._columnNames).toContain('lib2_status');
      expect(appContext.logSchema._columnNames.length).toBe(3);
    });
  });

  //#region smoo/lmao!n/library-integration-shared-columns
  describe('Shared Columns Across Libraries', () => {
    const library = <T extends Parameters<typeof defineLogSchema>[0]>(fields: T) =>
      defineOpContext({ logSchema: defineLogSchema(fields) }).defineOps({ run: async (ctx) => ctx.ok({}) });

    it('two_libraries_mapping_a_field_to_one_column_share_it', () => {
      const pg = library({ query: S.text(), rows: S.number() });
      const mysql = library({ query: S.text(), rows: S.number() });
      const pgWired = pg.mapColumns({ query: 'query', rows: 'pg_rows' });
      const mysqlWired = mysql.mapColumns({ query: 'query', rows: 'mysql_rows' });

      const app = defineOpContext({
        logSchema: defineLogSchema({ user_id: S.category() }),
        deps: { pg: pgWired, mysql: mysqlWired },
      });

      expect(app.logSchema._columnNames).toEqual(['user_id', 'query', 'pg_rows', 'mysql_rows']);
      // Each library's rows reach the one output column from its own source column.
      expect(pgWired.run.callsitePlan.remapDescriptor?.sourceNames).toEqual({ query: 'query', pg_rows: 'rows' });
      expect(mysqlWired.run.callsitePlan.remapDescriptor?.sourceNames).toEqual({ query: 'query', mysql_rows: 'rows' });
    });

    it('a_library_field_mapped_onto_the_applications_own_column_shares_it', () => {
      const pg = library({ query: S.text() });
      const app = defineOpContext({
        logSchema: defineLogSchema({ query: S.text() }),
        deps: { pg: pg.mapColumns({ query: 'query' }) },
      });
      expect(app.logSchema._columnNames).toEqual(['query']);
    });

    it('a_shared_column_of_two_types_is_refused_naming_both_sides', () => {
      const http = library({ status: S.number() });
      const process = library({ state: S.text() });
      expect(() =>
        defineOpContext({
          logSchema: defineLogSchema({ user_id: S.category() }),
          deps: { http: http.mapColumns({ status: 'status' }), process: process.mapColumns({ state: 'status' }) },
        }),
      ).toThrow(
        "Schema conflict: column 'status' would hold two types: field 'status' of dep 'http' is f64(), field 'state' of dep 'process' is text().",
      );
    });

    it('a_name_reached_without_mapColumns_is_refused_even_at_equal_types', () => {
      const pg = library({ query: S.text() });
      const mysql = library({ query: S.text() });
      expect(() =>
        defineOpContext({
          logSchema: defineLogSchema({ user_id: S.category() }),
          deps: { pg, mysql: mysql.mapColumns({ query: 'query' }) },
        }),
      ).toThrow(/column 'query' is reached by field 'query' of dep 'pg' and field 'query' of dep 'mysql'/);
    });

    it('two_fields_of_one_library_cannot_be_one_span_column', () => {
      const lib = library({ first: S.text(), second: S.text() });
      expect(() => lib.mapColumns({ first: 'both', second: 'both' })).toThrow(
        /fields 'first' and 'second' of library 'log' are both mapped to column 'both'/,
      );
    });

    it('a_field_mapColumns_does_not_name_keeps_its_column_in_the_descriptor', () => {
      const lib = library({ kept: S.number(), renamed: S.number(), dropped: S.number() });
      const wired = lib.mapColumns({ renamed: 'lib_renamed', dropped: null });
      expect(wired.run.callsitePlan.remapDescriptor?.sourceNames).toEqual({ kept: 'kept', lib_renamed: 'renamed' });
    });
  });
  //#endregion smoo/lmao!n/library-integration-shared-columns

  //#region smoo/lmao!n/schema-fields
  describe('Span buffer lanes', () => {
    it('a_field_of_a_kind_span_buffers_cannot_store_is_refused_by_name', () => {
      expect(() => defineOpContext({ logSchema: defineLogSchema({ retries: S.u16() }) })).toThrow(
        "Field 'retries' is u16(): span buffers store enum, category, text, f64, boolean, u64 and binary-column kinds",
      );
    });

    it('an_optional_field_is_stored_in_its_kinds_lane', () => {
      const context = defineOpContext({ logSchema: defineLogSchema({ retries: S.optional(S.number()) }) });
      expect(context.logSchema.fields.retries.kind).toBe('f64');
    });
  });
  //#endregion smoo/lmao!n/schema-fields

  describe('LogBinding Schema', () => {
    it('should expose effective schema via factory.logSchema', () => {
      // Define library
      const httpSchema = defineLogSchema({
        status: S.number(),
      });

      const httpContext = defineOpContext({
        logSchema: httpSchema,
      });

      const httpOps = httpContext.defineOps({
        fetch: async (ctx) => ctx.ok({}),
      });

      // Define app
      const appSchema = defineLogSchema({
        userId: S.category(),
      });

      const appContext = defineOpContext({
        logSchema: appSchema,
        deps: {
          http: httpOps.prefix('http'),
        },
      });

      // Factory's logSchema should be the effective schema
      expect(appContext.logSchema._columnNames).toContain('userId');
      expect(appContext.logSchema._columnNames).toContain('http_status');
      expect(appContext.logSchema._columnNames.length).toBe(2);
    });
  });

  describe('Reserved Property Validation', () => {
    it('should throw when using reserved SpanContext property names in ctx', () => {
      const schema = defineLogSchema({ userId: S.category() });

      // Test each reserved property
      const reservedProps = [
        'buffer',
        'tag',
        'log',
        'scope',
        'setScope',
        'ok',
        'err',
        'blocked',
        'spanName',
        'span',
        'ff',
        'deps',
      ];

      for (const prop of reservedProps) {
        const invalidCtx: Record<string, unknown> = { [prop]: 'bad' };
        expect(() => {
          defineOpContext({
            logSchema: schema,
            ctx: invalidCtx,
          });
        }).toThrow(`Cannot use '${prop}' in ctx - it is a reserved SpanContext property`);
      }
    });

    it('should throw when using underscore-prefixed property names in ctx', () => {
      const schema = defineLogSchema({ userId: S.category() });

      // Use props NOT in the explicit RESERVED_CONTEXT_PROPS list
      // to test the underscore prefix check specifically
      const underscoreProps = ['_internal', '_private', '_meta', '_custom', '_foo'];

      for (const prop of underscoreProps) {
        const invalidCtx: Record<string, unknown> = { [prop]: 'bad' };
        expect(() => {
          defineOpContext({
            logSchema: schema,
            ctx: invalidCtx,
          });
        }).toThrow(`Cannot use '${prop}' in ctx - properties starting with '_' are reserved for internal use`);
      }
    });

    it('should allow valid property names in ctx', () => {
      const schema = defineLogSchema({ userId: S.category() });

      // These should NOT throw
      expect(() => {
        defineOpContext({
          logSchema: schema,
          ctx: {
            env: null as unknown,
            config: { retry: 3 },
            requestId: null as string | null,
            userId: undefined as string | undefined,
            debug: false,
          },
        });
      }).not.toThrow();
    });
  });
});
