/**
 * ctx.blocked() porcelain (specs/lmao/01r_transient_errors_and_retry.md "ctx.blocked()").
 *
 * The span's own row-0 name is the Blocked service, the caller's BlockedConfig reaches the
 * error untouched, and the span records the row-0 tags, the row-1 result tags and the
 * tagged error's error_code.
 */

import { describe, expect, it } from 'bun:test';
import { defineOpContext } from '../defineOpContext.js';
import { Blocked, type BlockedConfig } from '../errors/Blocked.js';
import { RetriesExhausted } from '../errors/RetriesExhausted.js';
import type { OpContextOf, SpanContext } from '../opContext/types.js';
import { resolveEntryType, resolveMessage } from '../resolveMessage.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { ENTRY_TYPE_SPAN_ERR } from '../schema/systemSchema.js';
import { extractSpanIdentity, type SpanIdentity } from '../traceId.js';
import { TestTracer } from '../tracers/TestTracer.js';
import type { SpanBuffer } from '../types.js';
import { createTestTracerOptions } from './test-helpers.js';

const schema = defineLogSchema({ http_status: S.number(), retry_after_sec: S.number() });
const context = defineOpContext({ logSchema: schema });
type Ctx = SpanContext<OpContextOf<typeof context>>;
type Buffer = SpanBuffer<(typeof context)['logBinding']['logSchema']>;

function captured(buffer: Buffer | undefined): Buffer {
  if (!buffer) throw new Error('span buffer was not captured');
  return buffer;
}

// Compile-only: the porcelain keeps ErrResult's schema-typed row-1 setters and Blocked payload.
function assertBlockedTypes(ctx: Ctx): void {
  const chained = ctx.blocked({ maxAttempts: 2 }).retry_after_sec(60).http_status(503);
  const error: Blocked = chained.error;
  const name: string = ctx.spanName;
  // @ts-expect-error retry_after_sec is a number column.
  ctx.blocked().retry_after_sec('60');
  // @ts-expect-error the schema declares no such column.
  ctx.blocked().not_a_column(1);
  // @ts-expect-error the service is the span's own name; a name argument is not a BlockedConfig.
  ctx.blocked('other-service');
  // @ts-expect-error spanName is the span's row-0 name, never assigned.
  ctx.spanName = 'other-service';
  void error;
  void name;
}
void assertBlockedTypes;

describe('ctx.blocked()', () => {
  it('blocks on the span name with the caller config and records the tags on the span', async () => {
    const nextRetry = (attempt: number) => attempt * 1000;
    const config: BlockedConfig = { maxAttempts: 4, nextRetry };
    let rootBuffer: Buffer | undefined;
    let serviceBuffer: Buffer | undefined;
    let serviceBlocked: Blocked | undefined;
    let serviceOrigin: SpanIdentity | undefined;
    const tracer = new TestTracer(context, createTestTracerOptions());

    const result = await tracer.trace('poll', async (ctx) => {
      rootBuffer = ctx.buffer;
      const call = await ctx.span('telegram:bot1', (spanCtx) => {
        serviceBuffer = spanCtx.buffer;
        spanCtx.tag.http_status(503);
        return spanCtx.blocked(config).retry_after_sec(60);
      });
      if (!call.success) {
        serviceBlocked = call.error;
        serviceOrigin = call.error.origin;
        return ctx.err(call.error);
      }
      return ctx.ok(call.value);
    });

    if (result.success) throw new Error('a blocked span must not succeed');
    expect(result.error).toBeInstanceOf(Blocked);
    expect(result.error.reason).toEqual({ type: 'service', name: 'telegram:bot1' });
    expect(result.error.blockedConfig).toBe(config);
    expect(result.error.blockedConfig?.nextRetry).toBe(nextRetry);
    expect(result.error.message).toBe(Blocked.service('telegram:bot1', config).message);
    const { origin, ...view } = result.error.toJSON();
    expect(view).toEqual(Blocked.service('telegram:bot1', config).toJSON());

    const service = captured(serviceBuffer);
    expect(resolveMessage(service, 0)).toBe('telegram:bot1');
    expect(service.http_status_values[0]).toBe(503);
    expect(resolveEntryType(service, 1)).toBe(ENTRY_TYPE_SPAN_ERR);
    expect(service.retry_after_sec_values[1]).toBe(60);
    expect(service.error_code_values[1]).toBe('Blocked');

    // The explicit ctx.err(blocked) path that propagates it records the same code.
    const root = captured(rootBuffer);
    expect(resolveEntryType(root, 1)).toBe(ENTRY_TYPE_SPAN_ERR);
    expect(root.error_code_values[1]).toBe('Blocked');

    // The service span bound the origin; the root's rebind kept the same value.
    if (!serviceBlocked) throw new Error('the service span must block');
    expect(result.error).toBe(serviceBlocked);
    expect(result.error.origin).toBe(serviceOrigin);
    expect(result.error.origin).toEqual(extractSpanIdentity(service));
    expect(result.error.origin?.span_id).not.toBe(root.span_id);
    expect(origin).toEqual({
      trace_id: service.trace_id,
      span_id: service.span_id,
      thread_id: service.thread_id.toString(),
    });
    expect(JSON.parse(JSON.stringify(result.error)).origin).toEqual(origin);
  });

  it('equals ctx.err(Blocked.service(ctx.spanName, config)) row for row', async () => {
    const config: BlockedConfig = { maxAttempts: 3 };
    const buffers: Buffer[] = [];
    const tracer = new TestTracer(context, createTestTracerOptions());

    const results = await tracer.trace('both', async (ctx) => {
      const porcelain = await ctx.span('payment-api', (spanCtx) => {
        buffers.push(spanCtx.buffer);
        return spanCtx.blocked(config).retry_after_sec(5);
      });
      const explicit = await ctx.span('payment-api', (spanCtx) => {
        buffers.push(spanCtx.buffer);
        return spanCtx.err(Blocked.service(spanCtx.spanName, config)).retry_after_sec(5);
      });
      return ctx.ok([porcelain, explicit]);
    });

    if (!results.success) throw new Error('root must succeed');
    const [porcelain, explicit] = results.value;
    if (porcelain.success || explicit.success) throw new Error('both spans must block');
    expect(porcelain.error.reason).toEqual(explicit.error.reason);
    expect(porcelain.error.blockedConfig).toBe(explicit.error.blockedConfig);
    const [blockedSpan, errSpan] = buffers;
    if (!blockedSpan || !errSpan) throw new Error('both span buffers must be captured');
    for (const span of [blockedSpan, errSpan]) {
      expect(resolveMessage(span, 0)).toBe('payment-api');
      expect(resolveEntryType(span, 1)).toBe(ENTRY_TYPE_SPAN_ERR);
      expect(span.error_code_values[1]).toBe('Blocked');
      expect(span.retry_after_sec_values[1]).toBe(5);
    }
    expect(porcelain.error.origin).toEqual(extractSpanIdentity(blockedSpan));
    expect(explicit.error.origin).toEqual(extractSpanIdentity(errSpan));
  });

  it('starts unbound and keeps the first binding', async () => {
    const blocked = Blocked.service('payment-api');
    expect(blocked.origin).toBeUndefined();
    expect(blocked.toJSON().origin).toBeUndefined();
    const tracer = new TestTracer(context, createTestTracerOptions());

    await tracer.trace('bind', async (ctx) => {
      const first = extractSpanIdentity(ctx.buffer);
      expect(blocked.bindOrigin(first)).toBe(blocked);
      expect(blocked.bindOrigin({ ...first, span_id: first.span_id + 1 }).origin).toBe(first);
      // A span-bound ctx.err finds it bound and keeps the identity value.
      expect(ctx.err(blocked).error.origin).toBe(first);
      return ctx.ok(null);
    });
  });

  it('binds the context when destructured', async () => {
    const names: string[] = [];
    const tracer = new TestTracer(context, createTestTracerOptions());

    const result = await tracer.trace('root', async (ctx) => {
      const call = await ctx.span('ledger', ({ blocked, spanName }) => {
        names.push(spanName);
        return blocked();
      });
      if (!call.success) return ctx.err(call.error);
      return ctx.ok(call.value);
    });

    expect(names).toEqual(['ledger']);
    if (result.success) throw new Error('a blocked span must not succeed');
    expect(result.error.reason).toEqual({ type: 'service', name: 'ledger' });
    expect(result.error.blockedConfig).toBeUndefined();
  });

  it('records any tagged error by its tag', async () => {
    let buffer: Buffer | undefined;
    const tracer = new TestTracer(context, createTestTracerOptions());

    await tracer.trace('exhausted', async (ctx) => {
      buffer = ctx.buffer;
      return ctx.err(new RetriesExhausted({ type: 'service', name: 'payment-api' }, 5, 5));
    });

    expect(captured(buffer).error_code_values[1]).toBe('RetriesExhausted');
  });
});
