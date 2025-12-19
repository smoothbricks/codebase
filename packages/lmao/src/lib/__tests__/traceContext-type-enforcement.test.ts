/**
 * Type-level tests for TraceContext required/optional property enforcement
 *
 * These tests verify that TypeScript properly enforces required vs optional
 * properties in traceContext() based on the Extra type definition.
 *
 * Per spec 01l_module_builder_pattern.md:
 * - Properties without `?` in Extra type are REQUIRED in traceContext()
 * - Properties with `?` in Extra type are OPTIONAL in traceContext()
 * - The `null!` convention in defaults is for runtime/V8 optimization, not type enforcement
 */

import { describe, expect, it } from 'bun:test';
import { S } from '@smoothbricks/arrow-builder';
import { defineModule } from '../defineModule.js';

// =============================================================================
// Test Module Definitions
// =============================================================================

// Module with required and optional Extra properties
const testModule = defineModule({
  metadata: { packageName: '@test/module', packagePath: 'src/test.ts' },
  logSchema: {
    userId: S.category(),
  },
})
  .ctx<{
    env: { apiTimeout: number; region: string };
    requestId: string;
    userId?: string;
  }>({
    env: null!, // Required - no default
    requestId: null!, // Required - no default
    userId: undefined, // Optional - has default
  })
  .make();

// =============================================================================
// Type Enforcement Tests
// =============================================================================

describe('TraceContext Type Enforcement', () => {
  it('should accept all required properties', () => {
    // This should compile without errors
    const ctx = testModule.traceContext({
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
    const ctx = testModule.traceContext({
      env: { apiTimeout: 5000, region: 'us-east-1' },
      requestId: 'req-123',
      userId: 'user-456', // Optional property provided
    });

    expect(ctx).toBeDefined();
    expect(ctx.userId).toBe('user-456');
  });

  // Type-level test: missing required property should cause TypeScript error
  // @ts-expect-error Property 'requestId' is missing in type '{ env: { apiTimeout: number; region: string; } }' but required in type '{ env: { apiTimeout: number; region: string; }; requestId: string; userId?: string | undefined; }'
  const _missingRequired = testModule.traceContext({
    env: { apiTimeout: 5000, region: 'us-east-1' },
    // requestId is missing - should cause TypeScript error
  });

  // Type-level test: missing another required property should cause TypeScript error
  // @ts-expect-error Property 'env' is missing in type '{ requestId: string; }' but required in type '{ env: { apiTimeout: number; region: string; }; requestId: string; userId?: string | undefined; }'
  const _missingEnv = testModule.traceContext({
    requestId: 'req-123',
    // env is missing - should cause TypeScript error
  });

  // Type-level test: all required properties present, optional omitted - should compile
  const _allRequired = testModule.traceContext({
    env: { apiTimeout: 5000, region: 'us-east-1' },
    requestId: 'req-123',
    // userId omitted - should be fine (it's optional)
  });

  void _missingRequired;
  void _missingEnv;
  void _allRequired;
});

// =============================================================================
// Extra Type Flow Tests
// =============================================================================

describe('Extra Type Flow Through Builder Chain', () => {
  it('should preserve Extra type through .ctx().make() chain', () => {
    const module = defineModule({
      metadata: { packageName: '@test/flow', packagePath: 'src/flow.ts' },
      logSchema: {},
    })
      .ctx<{
        required: string;
        optional?: number;
      }>({
        required: null!,
        optional: undefined,
      })
      .make();

    // Type should be preserved - required is required, optional is optional
    const ctx = module.traceContext({
      required: 'value',
      // optional can be omitted
    });

    expect(ctx.required).toBe('value');
  });

  it('should preserve Extra type when .make() is called without .ctx()', () => {
    // When .ctx() is not called, Extra defaults to Record<string, unknown>
    const module = defineModule({
      metadata: { packageName: '@test/no-ctx', packagePath: 'src/no-ctx.ts' },
      logSchema: {},
    });

    // Should accept any properties (all optional since Extra = Record<string, unknown>)
    const ctx = module.traceContext({
      anyProperty: 'anyValue',
    });

    expect(ctx).toBeDefined();
  });
});

// =============================================================================
// Reserved Keys Enforcement Tests
// =============================================================================

describe('Reserved Keys Enforcement', () => {
  it('should make reserved keys unusable via never type', () => {
    // ValidateExtra maps reserved keys to `never`, making them unusable at call sites
    // The error occurs when trying to PROVIDE a value, not when defining the type

    // These build but produce `never` for the reserved key, causing errors when used:
    const badTraceIdModule = defineModule({
      metadata: { packageName: '@test/bad', packagePath: 'test.ts' },
      logSchema: {},
    }).ctx<{ traceId: string }>({
      // @ts-expect-error Type 'null' is not assignable to type 'never'
      traceId: null!,
    });

    const badFfModule = defineModule({
      metadata: { packageName: '@test/bad', packagePath: 'test.ts' },
      logSchema: {},
    }).ctx<{ ff: string }>({
      // @ts-expect-error Type 'null' is not assignable to type 'never'
      ff: null!,
    });

    const badSpanModule = defineModule({
      metadata: { packageName: '@test/bad', packagePath: 'test.ts' },
      logSchema: {},
    }).ctx<{ span: () => void }>({
      // @ts-expect-error Type 'null' is not assignable to type 'never'
      span: null!,
    });

    void badTraceIdModule;
    void badFfModule;
    void badSpanModule;
  });
});
