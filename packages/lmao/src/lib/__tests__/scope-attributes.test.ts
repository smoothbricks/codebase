/**
 * Tests for span scope attributes
 *
 * Per specs/lmao/01i_span_scope_attributes.md:
 * - Scope attributes set at span level propagate to all child entries
 * - Child spans inherit parent's scoped attributes
 * - Tasks inherit scoped attributes from calling context
 * - Pre-filling optimization ensures zero runtime overhead
 */

import { describe, expect, test } from 'bun:test';
import { convertSpanTreeToArrowTable } from '../convertToArrow.js';
import { defineOpContext } from '../defineOpContext.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { TestTracer } from '../tracers/TestTracer.js';
import { createTestTracerOptions } from './test-helpers.js';

function getColumnValue(
  table: ReturnType<typeof convertSpanTreeToArrowTable>,
  columnName: string,
  rowIndex: number,
): unknown {
  const column = table.getChild(columnName);
  if (!column) throw new Error(`column not found: ${columnName}`);
  return column.get(rowIndex);
}

describe('Span Scope Attributes', () => {
  describe('Basic Scope Setting', () => {
    test('should set scope attributes on span', async () => {
      const schema = defineLogSchema({
        userId: S.category(),
        requestId: S.category(),
        orderId: S.category(),
      });

      const ctx = defineOpContext({
        logSchema: schema,
      });
      const { defineOp } = ctx;

      const testOp = defineOp('test-op', async (ctx) => {
        // Set scope attributes
        ctx.setScope({
          userId: 'user123',
          requestId: 'req456',
        });

        // Write a tag entry
        ctx.tag.orderId('order789');

        return ctx.ok('done');
      });

      const { trace } = new TestTracer(ctx, { ...createTestTracerOptions() });
      const result = await trace('test-span', testOp);
      expect(result.success).toBe(true);
    });

    test('should include scoped attributes in subsequent log entries', async () => {
      const schema = defineLogSchema({
        userId: S.category(),
        requestId: S.category(),
        step: S.category(),
      });

      const ctx = defineOpContext({
        logSchema: schema,
      });
      const { defineOp } = ctx;

      const testOp = defineOp('test-op', async (ctx) => {
        // Set scope once
        ctx.setScope({
          userId: 'user123',
          requestId: 'req456',
        });

        // All subsequent operations should include scoped attributes
        ctx.log.info('Step 1');
        ctx.tag.step('step2');
        ctx.log.info('Step 3');

        return ctx.ok('done');
      });

      const { trace } = new TestTracer(ctx, { ...createTestTracerOptions() });
      await trace('test-span', testOp);
      // If this completes without error, scope inheritance is working
    });
  });

  describe('Child Span Inheritance', () => {
    test('should inherit scoped attributes in child spans', async () => {
      const schema = defineLogSchema({
        userId: S.category(),
        requestId: S.category(),
        operation: S.category(),
        step: S.category(),
      });

      const ctx = defineOpContext({
        logSchema: schema,
      });
      const { defineOp } = ctx;

      const testOp = defineOp('test-op', async (ctx) => {
        // Set scope at parent level
        ctx.setScope({
          userId: 'user123',
          requestId: 'req456',
          operation: 'process_order',
        });

        ctx.log.info('Parent span started');

        // Create child span using inline closure
        const childResult = await ctx.span('child-task', async (childCtx) => {
          // Child should have access to parent's scoped attributes
          // Add additional scope in child
          childCtx.setScope({
            step: 'validation',
          });

          childCtx.log.info('Child span with inherited scope');

          return childCtx.ok({ validated: true });
        });

        expect(childResult.success).toBe(true);
        if (childResult.success) {
          expect(childResult.value.validated).toBe(true);
        }
        return ctx.ok('done');
      });

      const { trace } = new TestTracer(ctx, { ...createTestTracerOptions() });
      const result = await trace('test-span', testOp);
      expect(result.success).toBe(true);
    });

    test('should support deep nesting of scoped attributes', async () => {
      const schema = defineLogSchema({
        userId: S.category(),
        level1: S.category(),
        level2: S.category(),
        level3: S.category(),
      });

      const ctx = defineOpContext({
        logSchema: schema,
      });
      const { defineOp } = ctx;

      const testOp = defineOp('test-op', async (ctx) => {
        ctx.setScope({ userId: 'user123', level1: 'L1' });

        await ctx.span('level-2', async (ctx2) => {
          ctx2.setScope({ level2: 'L2' });

          await ctx2.span('level-3', async (ctx3) => {
            ctx3.setScope({ level3: 'L3' });

            // This span should have all scoped attributes: userId, level1, level2, level3
            ctx3.log.info('Deep nested span');

            return ctx3.ok('level3-done');
          });

          return ctx2.ok('level2-done');
        });

        return ctx.ok('done');
      });

      const { trace } = new TestTracer(ctx, { ...createTestTracerOptions() });
      const result = await trace('test-span', testOp);
      expect(result.success).toBe(true);
    });
  });

  describe('Task Wrapper Inheritance', () => {
    test('should inherit scoped attributes across span boundaries', async () => {
      const schema = defineLogSchema({
        userId: S.category(),
        requestId: S.category(),
        taskName: S.category(),
      });

      const ctx = defineOpContext({
        logSchema: schema,
      });
      const { defineOp } = ctx;

      const parentOp = defineOp('parent-op', async (ctx) => {
        ctx.setScope({
          userId: 'user123',
          requestId: 'req456',
          taskName: 'parent',
        });

        // Call child op via span with inline closure
        const childResult = await ctx.span('child-span', async (childCtx) => {
          // This should inherit userId and requestId from parent
          childCtx.setScope({
            taskName: 'child',
          });

          childCtx.log.info('Child op with inherited scope');

          return childCtx.ok('child-done');
        });

        return childResult;
      });

      const { trace } = new TestTracer(ctx, { ...createTestTracerOptions() });
      const result = await trace('parent-span', parentOp);
      expect(result.success).toBe(true);
    });
  });

  describe('Middleware Pattern', () => {
    test('should support middleware-style scope setup', async () => {
      const schema = defineLogSchema({
        requestId: S.category(),
        userId: S.category(),
        endpoint: S.category(),
        method: S.category(),
        userAgent: S.text(),
        ip: S.category(),
      });

      const ctx = defineOpContext({
        logSchema: schema,
      });
      const { defineOp } = ctx;

      const middlewareOp = defineOp('middleware', async (ctx) => {
        // Middleware sets up request-level scope
        ctx.setScope({
          requestId: 'req123',
          userId: 'user456',
          endpoint: '/api/users',
          method: 'POST',
          userAgent: 'Mozilla/5.0',
          ip: '192.168.1.1',
        });

        // Business logic should inherit all middleware scope
        const businessResult = await ctx.span('business-span', async (bizCtx) => {
          // All middleware scope should be inherited
          bizCtx.log.info('Processing business logic');

          // Business can add its own scope
          bizCtx.setScope({
            endpoint: '/api/users/create', // Override endpoint
          });

          bizCtx.log.info('User creation started');

          return bizCtx.ok('user-created');
        });

        return ctx.ok(businessResult);
      });

      const { trace } = new TestTracer(ctx, { ...createTestTracerOptions() });
      const result = await trace('middleware-span', middlewareOp);
      expect(result.success).toBe(true);
    });
  });

  describe('Scope with Different Types', () => {
    test('should handle different string types (enum, category, text)', async () => {
      const schema = defineLogSchema({
        status: S.enum(['pending', 'active', 'completed']),
        userId: S.category(),
        errorMessage: S.text(),
        count: S.number(),
        isValid: S.boolean(),
      });

      const ctx = defineOpContext({
        logSchema: schema,
      });
      const { defineOp } = ctx;

      const testOp = defineOp('test-op', async (ctx) => {
        ctx.setScope({
          status: 'active',
          userId: 'user123',
          errorMessage: 'No errors',
          count: 42,
          isValid: true,
        });

        ctx.log.info('All types scoped');

        return ctx.ok('done');
      });

      const { trace } = new TestTracer(ctx, { ...createTestTracerOptions() });
      const result = await trace('test-span', testOp);
      expect(result.success).toBe(true);
    });
  });

  describe('Scope Updates', () => {
    test('should allow updating scoped attributes', async () => {
      const schema = defineLogSchema({
        phase: S.category(),
        status: S.category(),
        progress: S.number(),
      });

      const ctx = defineOpContext({
        logSchema: schema,
      });
      const { defineOp } = ctx;

      const testOp = defineOp('test-op', async (ctx) => {
        // Initial scope
        ctx.setScope({
          phase: 'initialization',
          status: 'starting',
          progress: 0,
        });

        ctx.log.info('Phase 1');

        // Update scope
        ctx.setScope({
          phase: 'processing',
          status: 'in_progress',
          progress: 50,
        });

        ctx.log.info('Phase 2');

        // Update again
        ctx.setScope({
          phase: 'finalization',
          status: 'completing',
          progress: 100,
        });

        ctx.log.info('Phase 3');

        return ctx.ok('done');
      });

      const { trace } = new TestTracer(ctx, { ...createTestTracerOptions() });
      const result = await trace('test-span', testOp);
      expect(result.success).toBe(true);
    });
  });

  describe('Row-1 Direct Writes Beat Scope', () => {
    // Per specs/lmao/01i_span_scope_attributes.md ("Direct writes win"): scope fills
    // every row as a default, but tag wins on row 0, ok/err wins on row 1, and a
    // log entry's own fluent write wins on its row. This is the row-1 leg —
    // ctx.ok(v).status(...) / ctx.err(v).status(...) — reproducing the spec's exact
    // example table (01i lines 306-334), which no test exercised before this setter
    // existed (every prior test here uses bare ctx.ok('done')).
    test('ctx.ok(v).status(...) overrides scope on row 1 only, log entries keep scope default', async () => {
      const schema = defineLogSchema({
        status: S.category(),
        orderId: S.category(),
      });

      const ctx = defineOpContext({ logSchema: schema });
      const { defineOp } = ctx;

      const testOp = defineOp('test-op', (ctx) => {
        ctx.setScope({ status: 'processing', orderId: 'ord-1' });
        ctx.tag.status('started'); // row 0: tag wins over scope

        ctx.log.info('Step 1'); // row 2: no direct write, keeps scope value
        ctx.log.info('Step 2').status('validating'); // row 3: direct write wins
        ctx.log.info('Step 3'); // row 4: no direct write, keeps scope value

        // ctx.ok()'s declared return type stays Ok<S,T> (not the wider OkResult<S,T>)
        // because OkResult's per-field setters make T invariant, which breaks
        // assignability to OpFn's Result<S,E> — see spanContextTypes.ts's WHY on
        // SpanContext.ok. The setter exists on the returned object at runtime
        // regardless; invoke it via Reflect like the internals this suite already
        // probes (e.g. capability-span-context.test.ts's `_state`/`_writer` checks).
        const okResult = ctx.ok({ done: true });
        const statusSetter = Reflect.get(okResult, 'status');
        if (typeof statusSetter !== 'function') throw new TypeError('ok result missing status setter');
        Reflect.apply(statusSetter, okResult, ['completed']); // row 1: ok() wins over scope
        return okResult;
      });

      const { trace, rootBuffers } = new TestTracer(ctx, { ...createTestTracerOptions() });
      const result = await trace('test-span', testOp);
      expect(result.success).toBe(true);

      const table = convertSpanTreeToArrowTable(rootBuffers[0]);
      expect(getColumnValue(table, 'status', 0)).toBe('started'); // tag wins row 0
      expect(getColumnValue(table, 'status', 1)).toBe('completed'); // ok() wins row 1
      expect(getColumnValue(table, 'status', 2)).toBe('processing'); // scope default
      expect(getColumnValue(table, 'status', 3)).toBe('validating'); // direct write wins
      expect(getColumnValue(table, 'status', 4)).toBe('processing'); // scope default

      // orderId was only ever set via scope — it should default onto every row,
      // including the row-1 span-ok that status() wrote to directly.
      for (let row = 0; row <= 4; row++) {
        expect(getColumnValue(table, 'orderId', row)).toBe('ord-1');
      }
    });
  });
});
