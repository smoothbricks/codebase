/**
 * Tests for TypeScript type narrowing with FluentResult
 * Ensures that result.success properly narrows types
 */

import { describe, expect, it } from 'bun:test';
import type { TagWriter } from '../codegen/fixedPositionWriterGenerator.js';
import { defineOpContext } from '../defineOpContext.js';
import { defineCodeError } from '../result.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import type { InferSchema } from '../schema/types.js';
import type { FluentLogEntry } from '../spanContext.js';
import { TestTracer } from '../tracers/TestTracer.js';
import { createTestTracerOptions } from './test-helpers.js';

// Error code factories for tests
const TEST_ERROR = defineCodeError('TEST_ERROR')<{ field: string; reason: string }>();
const VALIDATION_ERROR = defineCodeError('VALIDATION_ERROR')<{ message: string }>();

const testSchema = defineLogSchema({
  userId: S.category(),
  customField: S.category(),
});

const inlineEnumSchema = defineLogSchema({
  outcome: S.enum(['failure', 'success']),
  category: S.category(),
});

type InlineEnumOutcome = InferSchema<typeof inlineEnumSchema>['outcome'];

function assertInlineEnumInference(
  outcome: InlineEnumOutcome,
  category: string,
  tag: TagWriter<typeof inlineEnumSchema>,
  log: FluentLogEntry<typeof inlineEnumSchema>,
): void {
  const exactOutcome: 'failure' | 'success' = outcome;

  tag.outcome('failure').outcome('success');
  log.outcome('failure').outcome('success');
  tag.category(category);
  log.category(category);

  // @ts-expect-error - enum setters must reject values outside the inferred literal union
  tag.outcome('pending');
  // @ts-expect-error - enum setters must reject values outside the inferred literal union
  log.outcome('pending');

  void exactOutcome;
}

void assertInlineEnumInference;

// Create op context factory
const opContext = defineOpContext({
  logSchema: testSchema,
  ctx: {
    requestId: undefined as string | undefined,
  },
});

const { defineOp } = opContext;

// Create a properly typed tracer - new API passes opContext directly
function createTestTracer() {
  return new TestTracer(opContext, { ...createTestTracerOptions() });
}

describe('Type Narrowing with FluentResult', () => {
  it('should properly narrow success result type', async () => {
    const { trace } = createTestTracer();

    const testOp = defineOp('testOp', async (ctx) => {
      const result = ctx.ok({ id: 123, name: 'test' });

      // Type narrowing should work with FluentResult
      if (result.success) {
        // result.value should be accessible and typed correctly
        const value = result.value;
        expect(value.id).toBe(123);
        expect(value.name).toBe('test');
        return result;
      }

      return ctx.ok(null);
    });

    const output = await trace('testOp', testOp);
    expect(output.success).toBe(true);
    if (output.success) {
      expect(output.value).toEqual({ id: 123, name: 'test' });
    }
  });

  it('should properly narrow error result type', async () => {
    const { trace } = createTestTracer();

    const testOp = defineOp('testOp', async (ctx) => {
      const result = ctx.err(TEST_ERROR({ field: 'email', reason: 'invalid' }));

      // Type narrowing should work with FluentResult
      if (!result.success) {
        // result.error should be accessible and typed correctly
        const error = result.error;
        expect(error.code).toBe('TEST_ERROR');
        expect(error.field).toBe('email');
        expect(error.reason).toBe('invalid');
        return ctx.ok(error.code);
      }

      return ctx.ok(null);
    });

    const output = await trace('testOp', testOp);
    expect(output.success).toBe(true);
    if (output.success) {
      expect(output.value).toBe('TEST_ERROR');
    }
  });

  it('should support chaining before type check', async () => {
    const { trace } = createTestTracer();

    const testOp = defineOp('testOp', async (ctx) => {
      const result = ctx.ok({ id: 456 }).with({ userId: 'user1' }).message('Success');

      // Type narrowing should still work after chaining
      if (result.success) {
        expect(result.value.id).toBe(456);
        return ctx.ok(result.value.id);
      }

      return ctx.ok(0);
    });

    const output = await trace('testOp', testOp);
    expect(output.success).toBe(true);
    if (output.success) {
      expect(output.value).toBe(456);
    }
  });

  it('should handle error result with chaining', async () => {
    const { trace } = createTestTracer();

    const testOp = defineOp('testOp', async (ctx) => {
      const result = ctx
        .err(VALIDATION_ERROR({ message: 'Invalid input' }))
        .with({ userId: 'user1' })
        .message('Validation failed');

      // Type narrowing should work after chaining
      if (!result.success) {
        expect(result.error.code).toBe('VALIDATION_ERROR');
        expect(result.error.message).toBe('Invalid input');
        return ctx.ok(result.error.code);
      }

      return ctx.ok('OK');
    });

    const output = await trace('testOp', testOp);
    expect(output.success).toBe(true);
    if (output.success) {
      expect(output.value).toBe('VALIDATION_ERROR');
    }
  });
  it('preserves schema-specific writer and result inference without casts', async () => {
    const { trace } = createTestTracer();

    const testOp = defineOp('writerInference', (ctx) => {
      const tag = ctx.tag.userId('tag-user').customField('tag-custom');
      tag.userId('tag-user-updated');

      const log = ctx.log.info('inferred log').userId('log-user').customField('log-custom');
      log.warn('inferred warning');

      const ok = ctx.ok({ id: 789 }).with({ userId: 'result-user' }).message('Result succeeded');
      if (!ok.success) throw new Error('expected inferred Ok');
      const id: number = ok.value.id;

      const err = ctx
        .err(TEST_ERROR({ field: 'writer', reason: 'expected' }))
        .with({ customField: 'result-error' })
        .message('Result failed');
      if (err.success) throw new Error('expected inferred Err');
      const code: 'TEST_ERROR' = err.error.code;
      const field: string = err.error.field;

      return ctx.ok({ id, code, field });
    });

    const output = await trace('writerInference', testOp);
    expect(output.success).toBe(true);
    if (!output.success) throw new Error('expected writer inference operation to succeed');
    expect(output.value).toEqual({ id: 789, code: 'TEST_ERROR', field: 'writer' });
  });
});
