/**
 * Op Metadata Tests
 *
 * Tests that OpMetadata is correctly set when using defineOp and defineOps.
 * Verifies that:
 * - Op name is correctly captured from defineOp('name', fn)
 * - Metadata flows through to the Op instance
 * - Stack extraction provides file/line info when transformer not installed
 * - Transformer-injected metadata takes precedence
 */

import { describe, expect, it } from 'bun:test';
import { defineOpContext } from '../defineOpContext.js';
import { Op } from '../op.js';
import { createOpMetadata } from '../opContext/defineOp.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { TestTracer } from '../tracers/TestTracer.js';
import { createTestTracerOptions } from './test-helpers.js';

// Test schema
const testSchema = defineLogSchema({
  userId: S.category(),
});

describe('Op Metadata', () => {
  describe('defineOp name parameter', () => {
    it('should use the name parameter as Op metadata name', () => {
      const ctx = defineOpContext({ logSchema: testSchema });
      const { defineOp } = ctx;

      const myOp = defineOp('my-operation', (ctx) => ctx.ok('done'));

      // Op should have the name from defineOp
      expect(myOp.metadata.name).toBe('my-operation');
    });

    it('should use different names for different ops', () => {
      const ctx = defineOpContext({ logSchema: testSchema });
      const { defineOp } = ctx;

      const fetchOp = defineOp('fetch-user', (ctx) => ctx.ok('user'));
      const saveOp = defineOp('save-user', (ctx) => ctx.ok('saved'));

      expect(fetchOp.metadata.name).toBe('fetch-user');
      expect(saveOp.metadata.name).toBe('save-user');
    });

    it('should have some metadata even without transformer', () => {
      const ctx = defineOpContext({ logSchema: testSchema });
      const { defineOp } = ctx;

      const myOp = defineOp('test-op', (ctx) => ctx.ok('done'));

      // Metadata should exist (either from stack extraction or defaults)
      expect(myOp.metadata.package_file).toBeDefined();
      expect(typeof myOp.metadata.line).toBe('number');
      // Most importantly, the name should be correct
      expect(myOp.metadata.name).toBe('test-op');
    });
  });

  describe('defineOps with inline functions', () => {
    it('should use the key name as Op metadata name', () => {
      const ctx = defineOpContext({ logSchema: testSchema });
      const { defineOps } = ctx;

      const ops = defineOps({
        fetchUser: (ctx) => ctx.ok('user'),
        saveUser: (ctx) => ctx.ok('saved'),
      });

      expect(ops.fetchUser.metadata.name).toBe('fetchUser');
      expect(ops.saveUser.metadata.name).toBe('saveUser');
    });

    it('should preserve name when using pre-defined Op', () => {
      const ctx = defineOpContext({ logSchema: testSchema });
      const { defineOp, defineOps } = ctx;

      // Pre-define an op with explicit name
      const customOp = defineOp('custom-name', (ctx) => ctx.ok('done'));

      const ops = defineOps({
        differentKey: customOp,
      });

      // Should keep the original Op name, not the key name
      expect(ops.differentKey.metadata.name).toBe('custom-name');
    });
  });

  describe('transformer-injected metadata', () => {
    it('should use explicit metadata when provided', () => {
      const ctx = defineOpContext({ logSchema: testSchema });
      const { defineOp } = ctx;

      const injectedMetadata = createOpMetadata('transformer-name', '@my/package', 'src/ops.ts', 'abc123', 42);

      const myOp = defineOp('runtime-name', (ctx) => ctx.ok('done'), injectedMetadata);

      // Transformer-injected name should take precedence
      expect(myOp.metadata.name).toBe('transformer-name');
      expect(myOp.metadata.package_name).toBe('@my/package');
      expect(myOp.metadata.package_file).toBe('src/ops.ts');
      expect(myOp.metadata.git_sha).toBe('abc123');
      expect(myOp.metadata.line).toBe(42);
    });

    it('should use runtime name if transformer metadata has no name', () => {
      const ctx = defineOpContext({ logSchema: testSchema });
      const { defineOp } = ctx;

      // Partial metadata without name - simulating transformer that doesn't inject name
      const partialMetadata: Partial<ReturnType<typeof createOpMetadata>> = {
        package_name: '@my/package',
        package_file: 'src/ops.ts',
        git_sha: 'abc123',
        line: 42,
      };

      const myOp = defineOp('runtime-name', (ctx) => ctx.ok('done'), partialMetadata);

      // Runtime name should be used since metadata.name wasn't provided
      expect(myOp.metadata.name).toBe('runtime-name');
      expect(myOp.metadata.package_name).toBe('@my/package');
    });
  });

  describe('metadata flows to SpanBuffer', () => {
    it('should have metadata accessible on buffer._opMetadata during trace', async () => {
      const ctx = defineOpContext({ logSchema: testSchema });
      const { defineOp } = ctx;

      let capturedMetadata: { name: string } | undefined;
      const myOp = defineOp('traced-op', (ctx) => {
        capturedMetadata = ctx.buffer._opMetadata as { name: string };
        return ctx.ok('done');
      });

      const tracer = new TestTracer(ctx, createTestTracerOptions());
      await tracer.trace('test-trace', myOp);

      expect(capturedMetadata).toBeDefined();
      expect(capturedMetadata?.name).toBe('traced-op');
    });

    it('should distinguish callsite vs op metadata', async () => {
      const ctx = defineOpContext({ logSchema: testSchema });
      const { defineOp } = ctx;

      let innerCallsiteMetadata: { name: string } | undefined;
      let innerOpMetadata: { name: string } | undefined;

      const innerOp = defineOp('inner-op', (ctx) => {
        innerCallsiteMetadata = ctx.buffer._callsiteMetadata as { name: string };
        innerOpMetadata = ctx.buffer._opMetadata as { name: string };
        return ctx.ok('inner-done');
      });

      const outerOp = defineOp('outer-op', async (ctx) => {
        await ctx.span('call-inner', innerOp);
        return ctx.ok('outer-done');
      });

      const tracer = new TestTracer(ctx, createTestTracerOptions());
      await tracer.trace('test-trace', outerOp);

      // Inner op should have:
      // - _callsiteMetadata from outer-op (where span() was called)
      // - _opMetadata from inner-op (the op being executed)
      expect(innerOpMetadata?.name).toBe('inner-op');
      expect(innerCallsiteMetadata?.name).toBe('outer-op');
    });
  });

  describe('Op instance type', () => {
    it('should create Op instances', () => {
      const ctx = defineOpContext({ logSchema: testSchema });
      const { defineOp } = ctx;

      const myOp = defineOp('test-op', (ctx) => ctx.ok('done'));

      expect(myOp).toBeInstanceOf(Op);
    });

    it('should have required Op properties', () => {
      const ctx = defineOpContext({ logSchema: testSchema });
      const { defineOp } = ctx;

      const myOp = defineOp('test-op', (ctx) => ctx.ok('done'));

      expect(myOp.metadata).toBeDefined();
      expect(myOp.fn).toBeInstanceOf(Function);
      expect(myOp.SpanBufferClass).toBeDefined();
    });
  });
});
