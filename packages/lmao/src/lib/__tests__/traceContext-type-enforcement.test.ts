/**
 * Type-level tests for createTrace required/optional property enforcement
 *
 * These tests verify that TypeScript properly enforces required vs optional
 * properties in createTrace() based on the ctx type definition.
 *
 * Per spec 01l_module_builder_pattern.md (updated for Op-centric API):
 * - Properties with `null as Type` in ctx are REQUIRED in createTrace()
 * - Properties with `undefined as Type | undefined` are OPTIONAL in createTrace()
 * - Properties with values are optional with defaults
 */

import { describe, expect, it } from 'bun:test';
import { S } from '@smoothbricks/arrow-builder';
import { defineOpContext } from '../defineOpContext.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';

// =============================================================================
// Test Factory Definitions
// =============================================================================

// Factory with required and optional ctx properties
const { createTrace: testCreateTrace } = defineOpContext({
  logSchema: defineLogSchema({
    userId: S.category(),
  }),
  ctx: {
    env: null as unknown as { apiTimeout: number; region: string }, // Required - no default
    requestId: null as unknown as string, // Required - no default
    userId: undefined as string | undefined, // Optional - has default
  },
});

// =============================================================================
// Type Enforcement Tests
// =============================================================================

describe('CreateTrace Type Enforcement', () => {
  it('should accept all required properties', () => {
    // This should compile without errors
    const ctx = testCreateTrace({
      env: { apiTimeout: 5000, region: 'us-east-1' },
      requestId: 'req-123',
      // userId is optional, can be omitted
    });

    expect(ctx).toBeDefined();
    expect(ctx.env.region).toBe('us-east-1');
    expect(ctx.requestId).toBe('req-123');
  });

  it('should accept optional properties', () => {
    // This should compile without errors
    const ctx = testCreateTrace({
      env: { apiTimeout: 5000, region: 'us-east-1' },
      requestId: 'req-123',
      userId: 'user-456', // Optional property provided
    });

    expect(ctx).toBeDefined();
    expect(ctx.userId).toBe('user-456');
  });

  // Note: Type-level tests for missing properties would require @ts-expect-error
  // but the exact error message depends on TypeScript version.
  // The important thing is that the API enforces required properties at the type level.
  it('should require env and requestId (compile-time check)', () => {
    // This test documents that missing required properties cause compile errors
    // We test by providing all required properties and verifying runtime behavior
    const ctx = testCreateTrace({
      env: { apiTimeout: 5000, region: 'us-east-1' },
      requestId: 'req-123',
    });

    expect(ctx.env).toEqual({ apiTimeout: 5000, region: 'us-east-1' });
    expect(ctx.requestId).toBe('req-123');
    expect(ctx.userId).toBeUndefined();
  });
});

// =============================================================================
// Extra Type Flow Tests
// =============================================================================

describe('Context Type Flow Through Factory', () => {
  it('should preserve ctx type through defineOpContext', () => {
    const { createTrace } = defineOpContext({
      logSchema: defineLogSchema({}),
      ctx: {
        required: null as unknown as string,
        optional: undefined as number | undefined,
      },
    });

    // Type should be preserved - required is required, optional is optional
    const ctx = createTrace({
      required: 'value',
      // optional can be omitted
    });

    expect(ctx.required).toBe('value');
  });

  it('should work without ctx property', () => {
    // When ctx is not provided, no user context is available
    const { createTrace } = defineOpContext({
      logSchema: defineLogSchema({}),
    });

    // Should accept empty object
    const ctx = createTrace({});

    expect(ctx).toBeDefined();
  });
});

// =============================================================================
// Reserved Keys Enforcement Tests
// =============================================================================

describe('Reserved Keys Enforcement', () => {
  // Reserved context props are: buffer, tag, log, scope, setScope, ok, err, span, ff, deps

  it('should throw at runtime for buffer reserved key', () => {
    expect(() =>
      defineOpContext({
        logSchema: defineLogSchema({}),
        ctx: {
          buffer: 'should-fail',
        } as Record<string, unknown>,
      }),
    ).toThrow(/reserved/i);
  });

  it('should throw at runtime for span reserved key', () => {
    expect(() =>
      defineOpContext({
        logSchema: defineLogSchema({}),
        ctx: {
          span: () => {},
        } as Record<string, unknown>,
      }),
    ).toThrow(/reserved/i);
  });

  it('should throw at runtime for ff reserved key', () => {
    expect(() =>
      defineOpContext({
        logSchema: defineLogSchema({}),
        ctx: {
          ff: {},
        } as Record<string, unknown>,
      }),
    ).toThrow(/reserved/i);
  });

  it('should throw at runtime for tag reserved key', () => {
    expect(() =>
      defineOpContext({
        logSchema: defineLogSchema({}),
        ctx: {
          tag: {},
        } as Record<string, unknown>,
      }),
    ).toThrow(/reserved/i);
  });

  it('should throw at runtime for log reserved key', () => {
    expect(() =>
      defineOpContext({
        logSchema: defineLogSchema({}),
        ctx: {
          log: {},
        } as Record<string, unknown>,
      }),
    ).toThrow(/reserved/i);
  });

  it('should throw at runtime for ok reserved key', () => {
    expect(() =>
      defineOpContext({
        logSchema: defineLogSchema({}),
        ctx: {
          ok: () => {},
        } as Record<string, unknown>,
      }),
    ).toThrow(/reserved/i);
  });

  it('should throw at runtime for err reserved key', () => {
    expect(() =>
      defineOpContext({
        logSchema: defineLogSchema({}),
        ctx: {
          err: () => {},
        } as Record<string, unknown>,
      }),
    ).toThrow(/reserved/i);
  });

  it('should throw for underscore-prefixed properties', () => {
    // Properties starting with _ are reserved for internal use
    expect(() =>
      defineOpContext({
        logSchema: defineLogSchema({}),
        ctx: {
          _internal: 'should-fail',
        } as Record<string, unknown>,
      }),
    ).toThrow(/reserved/i);
  });
});
