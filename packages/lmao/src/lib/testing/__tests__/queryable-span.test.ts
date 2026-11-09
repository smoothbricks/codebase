/**
 * Tests for QueryableSpan: navigating a span tree and reading one span's facts.
 */

import { describe, expect, it } from 'bun:test';
import { createTestTracerOptions } from '../../__tests__/test-helpers.js';
import { defineOpContext } from '../../defineOpContext.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import { TestTracer } from '../../tracers/TestTracer.js';
import { spanOk, tagFact } from '../facts.js';
import { querySpan } from '../queryable-span.js';

describe('QueryableSpan.ownFacts', () => {
  const opContext = defineOpContext({ logSchema: defineLogSchema({ result: S.category() }) });

  it("states a span's own tags and outcome, never a child's", async () => {
    const tracer = new TestTracer(opContext, createTestTracerOptions());
    await tracer.trace('run', async (ctx) => {
      await ctx.span('op', async (op) => {
        await op.span('inner', (inner) => {
          inner.tag.result('completed');
          return inner.ok(undefined);
        });
        op.tag.result('blocked');
        return op.ok(undefined);
      });
      return ctx.ok(undefined);
    });

    const root = tracer.rootBuffers[0];
    if (!root) throw new Error('the traced run left no root buffer');
    const op = querySpan(root).find('op');
    if (!op) throw new Error(`no op span among ${querySpan(root).names().join(', ')}`);

    expect(op.ownFacts().has(tagFact('result', 'blocked'))).toBe(true);
    expect(op.ownFacts().has(tagFact('result', 'completed'))).toBe(false);
    expect(op.ownFacts().has(spanOk('op'))).toBe(true);
    expect(op.ownFacts().has(spanOk('inner'))).toBe(false);
    expect(op.facts().has(tagFact('result', 'completed'))).toBe(true);
  });
});
