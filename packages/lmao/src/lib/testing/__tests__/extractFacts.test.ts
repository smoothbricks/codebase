/**
 * Tests for extractFacts - the bridge between buffers and facts.
 */

import { describe, expect, it } from 'bun:test';
import { createTestTracerOptions } from '../../__tests__/test-helpers.js';
import { defineOpContext } from '../../defineOpContext.js';
import { defineCodeError } from '../../result.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import { TestTracer } from '../../tracers/TestTracer.js';
import { extractFacts } from '../extractFacts.js';
import { logInfo, logWarn, spanErr, spanOk, spanStarted, tagFact } from '../facts.js';

// Define a test error for ctx.err tests
const NOT_FOUND = defineCodeError('NOT_FOUND')<{ detail: string }>();

describe('extractFacts', () => {
  // Create a test schema
  const testSchema = defineLogSchema({
    userId: S.category(),
    orderId: S.category(),
    amount: S.number(),
  });

  const opContext = defineOpContext({ logSchema: testSchema });

  it('extracts span lifecycle facts from a simple trace', async () => {
    const tracer = new TestTracer(opContext, { ...createTestTracerOptions() });

    await tracer.trace('my-operation', async (ctx) => {
      return ctx.ok('done');
    });

    expect(tracer.rootBuffers).toHaveLength(1);
    const facts = extractFacts(tracer.rootBuffers[0]);

    // Should have started and ok facts
    expect(facts.has(spanStarted('my-operation'))).toBe(true);
    expect(facts.has(spanOk('my-operation'))).toBe(true);
  });

  it('extracts log facts', async () => {
    const tracer = new TestTracer(opContext, { ...createTestTracerOptions() });

    await tracer.trace('with-logs', async (ctx) => {
      ctx.log.info('Starting process');
      ctx.log.warn('Something to note');
      return ctx.ok('done');
    });

    const facts = extractFacts(tracer.rootBuffers[0]);

    expect(facts.has(logInfo('Starting process'))).toBe(true);
    expect(facts.has(logWarn('Something to note'))).toBe(true);
  });

  it('extracts tag facts from row 0', async () => {
    const tracer = new TestTracer(opContext, { ...createTestTracerOptions() });

    await tracer.trace('with-tags', async (ctx) => {
      ctx.tag.userId('user-123').orderId('order-456');
      return ctx.ok('done');
    });

    const facts = extractFacts(tracer.rootBuffers[0]);

    expect(facts.has(tagFact('userId', 'user-123'))).toBe(true);
    expect(facts.has(tagFact('orderId', 'order-456'))).toBe(true);
  });

  it('extracts facts from nested spans in order', async () => {
    const tracer = new TestTracer(opContext, { ...createTestTracerOptions() });

    await tracer.trace('parent', async (ctx) => {
      await ctx.span('child-1', async (childCtx) => {
        return childCtx.ok('child-1 done');
      });

      await ctx.span('child-2', async (childCtx) => {
        return childCtx.ok('child-2 done');
      });

      return ctx.ok('parent done');
    });

    const facts = extractFacts(tracer.rootBuffers[0]);

    // Check order: parent starts, child-1 complete, child-2 complete, parent completes
    expect(
      facts.hasInOrder([
        spanStarted('parent'),
        spanStarted('child-1'),
        spanOk('child-1'),
        spanStarted('child-2'),
        spanOk('child-2'),
        spanOk('parent'),
      ]),
    ).toBe(true);
  });

  it('extracts error facts from span:err', async () => {
    const tracer = new TestTracer(opContext, { ...createTestTracerOptions() });

    await tracer.trace('failing-op', async (ctx) => {
      return ctx.err(NOT_FOUND({ detail: 'Resource missing' }));
    });

    const facts = extractFacts(tracer.rootBuffers[0]);

    expect(facts.has(spanStarted('failing-op'))).toBe(true);
    expect(facts.has(spanErr('failing-op', 'NOT_FOUND'))).toBe(true);
    expect(facts.has(spanOk('failing-op'))).toBe(false);
  });

  it('filters facts by namespace', async () => {
    const tracer = new TestTracer(opContext, { ...createTestTracerOptions() });

    await tracer.trace('mixed', async (ctx) => {
      ctx.log.info('A log message');
      ctx.tag.userId('user-1');
      return ctx.ok('done');
    });

    const facts = extractFacts(tracer.rootBuffers[0]);

    // Filter to just span facts
    const spanFacts = facts.byNamespace('span');
    expect(spanFacts.length).toBe(2); // started + ok

    // Filter to just log facts
    const logFacts = facts.byNamespace('log');
    expect(logFacts.length).toBe(1);

    // Filter to just tag facts
    const tagFacts = facts.byNamespace('tag');
    expect(tagFacts.length).toBe(1);
  });

  it('supports pattern matching', async () => {
    const tracer = new TestTracer(opContext, { ...createTestTracerOptions() });

    await tracer.trace('fetch-user', async (ctx) => {
      await ctx.span('fetch-profile', async (c) => c.ok(null));
      await ctx.span('fetch-orders', async (c) => c.ok(null));
      return ctx.ok('done');
    });

    const facts = extractFacts(tracer.rootBuffers[0]);

    // Match any fetch-* span that started
    expect(facts.hasMatch('span:fetch-*: started')).toBe(true);

    // Match any span that completed ok
    expect(facts.hasMatch('span:*: ok')).toBe(true);

    // Should match multiple
    const fetchStarts = facts.match('span:fetch-*: started');
    expect(fetchStarts.length).toBe(3); // fetch-user, fetch-profile, fetch-orders
  });

  it('handles deeply nested spans', async () => {
    const tracer = new TestTracer(opContext, { ...createTestTracerOptions() });

    await tracer.trace('level-0', async (ctx) => {
      await ctx.span('level-1', async (c1) => {
        await c1.span('level-2', async (c2) => {
          await c2.span('level-3', async (c3) => {
            return c3.ok('deepest');
          });
          return c2.ok('l2');
        });
        return c1.ok('l1');
      });
      return ctx.ok('l0');
    });

    const facts = extractFacts(tracer.rootBuffers[0]);

    // All levels should appear
    expect(facts.has(spanStarted('level-0'))).toBe(true);
    expect(facts.has(spanStarted('level-1'))).toBe(true);
    expect(facts.has(spanStarted('level-2'))).toBe(true);
    expect(facts.has(spanStarted('level-3'))).toBe(true);

    // Inner spans complete before outer
    expect(facts.hasInOrder([spanOk('level-3'), spanOk('level-2'), spanOk('level-1'), spanOk('level-0')])).toBe(true);
  });
});

describe('trace-testing patterns', () => {
  const testSchema = defineLogSchema({
    sku: S.category(),
    quantity: S.number(),
    reservationId: S.category(),
  });

  const opContext = defineOpContext({ logSchema: testSchema });

  it('example: verify operation order without implementation details', async () => {
    const tracer = new TestTracer(opContext, { ...createTestTracerOptions() });

    // Simulate an order processing workflow
    await tracer.trace('process-order', async (ctx) => {
      ctx.tag.sku('SKU-A').quantity(5);

      // Validate
      await ctx.span('validate', async (c) => {
        c.log.info('Validating order');
        return c.ok({ valid: true });
      });

      // Reserve inventory
      await ctx.span('reserve-inventory', async (c) => {
        c.tag.reservationId('res-123');
        return c.ok({ reserved: true });
      });

      // Charge payment
      await ctx.span('charge-payment', async (c) => {
        c.log.info('Charging $49.99');
        return c.ok({ charged: true });
      });

      return ctx.ok('Order processed');
    });

    const facts = extractFacts(tracer.rootBuffers[0]);

    // Test WHAT happened, not HOW

    // 1. Order was processed successfully
    expect(facts.has(spanOk('process-order'))).toBe(true);

    // 2. Validation happened before inventory reservation
    expect(facts.hasInOrder([spanOk('validate'), spanStarted('reserve-inventory')])).toBe(true);

    // 3. Inventory was reserved before payment was charged
    expect(facts.hasInOrder([spanOk('reserve-inventory'), spanStarted('charge-payment')])).toBe(true);

    // 4. Expected tags were set
    expect(facts.has(tagFact('sku', 'SKU-A'))).toBe(true);
    expect(facts.has(tagFact('quantity', 5))).toBe(true);
    expect(facts.has(tagFact('reservationId', 'res-123'))).toBe(true);

    // 5. No errors occurred
    expect(facts.hasMatch('span:*: err(*)')).toBe(false);
    expect(facts.hasMatch('log:error: *')).toBe(false);
  });
});
