/**
 * End-to-end integration test for binary/unknown/object columns through the full Tracer API.
 *
 * Tests the complete lifecycle: defineLogSchema -> Tracer -> defineOp -> tag payload -> flush -> Arrow -> decode.
 */

import { describe, expect, it } from 'bun:test';
// Must import test-helpers first to initialize timestamp implementation
import './test-helpers.js';
import { decode } from '@msgpack/msgpack';
import { convertSpanTreeToArrowTable } from '../convertToArrow.js';
import { defineOpContext } from '../defineOpContext.js';
import { S } from '../schema/builder.js';
import { defineLogSchema } from '../schema/defineLogSchema.js';
import { TestTracer } from '../tracers/TestTracer.js';
import type { AnySpanBuffer } from '../types.js';
import { requireColumn } from './arrow-test-helpers.js';
import { createTestTracerOptions } from './test-helpers.js';

function requireCapturedBuffer(buffer: AnySpanBuffer | undefined): AnySpanBuffer {
  if (!buffer) {
    throw new Error('Expected trace to capture a root buffer');
  }
  return buffer;
}

function isResponsePayload(value: unknown): value is { items: number[]; total: number } {
  return (
    typeof value === 'object' &&
    value !== null &&
    'items' in value &&
    'total' in value &&
    Array.isArray(value.items) &&
    value.items.every((item) => typeof item === 'number') &&
    typeof value.total === 'number'
  );
}

describe('Binary columns E2E through Tracer API', () => {
  // Define schema with binary columns alongside traditional types
  const apiSchema = defineLogSchema({
    userId: S.category(),
    httpMethod: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
    statusCode: S.number(),
    // New binary column types
    requestBody: S.unknown(),
    responsePayload: S.object<{ items: number[]; total: number }>(),
  });

  const opContext = defineOpContext({
    logSchema: apiSchema,
    ctx: {
      userId: undefined as string | undefined,
    },
  });

  const { defineOp } = opContext;

  it('roundtrips S.unknown() payload through trace -> flush -> Arrow -> decode', async () => {
    let capturedBuffer: AnySpanBuffer | undefined;

    const testOp = defineOp('getUser', async (ctx) => {
      capturedBuffer = ctx.buffer;

      // Tag with mixed types including binary payload
      ctx.tag
        .userId('user-42')
        .httpMethod('GET')
        .statusCode(200)
        .requestBody({
          params: { id: 42 },
          filters: ['active', 'verified'],
          meta: null,
        });

      ctx.log.info('fetching user');

      return ctx.ok({ found: true });
    });

    const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });
    await trace('test', testOp);

    const rootBuffer = requireCapturedBuffer(capturedBuffer);

    // Convert to Arrow table via the tree path
    const table = convertSpanTreeToArrowTable(rootBuffer);
    expect(table.numRows).toBeGreaterThan(0);

    // Find the requestBody column
    const requestBodyCol = requireColumn(table, 'requestBody');
    expect(requestBodyCol).toBeDefined();

    // Find a row that has a non-null requestBody
    let foundPayload = false;
    for (let i = 0; i < requestBodyCol.length; i++) {
      const bytes = requestBodyCol.at(i);
      if (bytes instanceof Uint8Array) {
        const decoded = decode(bytes);
        expect(decoded).toEqual({
          params: { id: 42 },
          filters: ['active', 'verified'],
          meta: null,
        });
        foundPayload = true;
        break;
      }
    }
    expect(foundPayload).toBe(true);
  });

  it('roundtrips S.object<T>() through trace -> flush -> Arrow -> decode', async () => {
    let capturedBuffer: AnySpanBuffer | undefined;

    const testOp = defineOp('listItems', async (ctx) => {
      capturedBuffer = ctx.buffer;

      const responseData = { items: [1, 2, 3, 4, 5], total: 5 };

      ctx.tag.userId('user-99').httpMethod('POST').statusCode(200).responsePayload(responseData);

      return ctx.ok(responseData);
    });

    const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });
    await trace('test', testOp);

    const rootBuffer = requireCapturedBuffer(capturedBuffer);

    const table = convertSpanTreeToArrowTable(rootBuffer);
    const responseCol = requireColumn(table, 'responsePayload');
    expect(responseCol).toBeDefined();

    // Find a non-null response row
    let foundResponse = false;
    for (let i = 0; i < responseCol.length; i++) {
      const bytes = responseCol.at(i);
      if (bytes instanceof Uint8Array) {
        const decoded = decode(bytes);
        if (!isResponsePayload(decoded)) {
          throw new Error('Decoded response payload did not match the expected shape');
        }
        expect(decoded.items).toEqual([1, 2, 3, 4, 5]);
        expect(decoded.total).toBe(5);
        foundResponse = true;
        break;
      }
    }
    expect(foundResponse).toBe(true);
  });

  it('handles log entries with binary payloads via ctx.log', async () => {
    let capturedBuffer: AnySpanBuffer | undefined;

    const testOp = defineOp('processRequest', async (ctx) => {
      capturedBuffer = ctx.buffer;

      ctx.tag
        .userId('user-1')
        .httpMethod('POST')
        .statusCode(201)
        .requestBody({
          action: 'create',
          data: { name: 'test', value: 42 },
        });

      // Log entries with binary attributes
      ctx.log.info('processing request').requestBody({ step: 'validation', ok: true });
      ctx.log.info('completed').requestBody({ step: 'persist', ok: true });

      return ctx.ok(undefined);
    });

    const { trace } = new TestTracer(opContext, { ...createTestTracerOptions() });
    await trace('test', testOp);

    const rootBuffer = requireCapturedBuffer(capturedBuffer);

    const table = convertSpanTreeToArrowTable(rootBuffer);
    const requestBodyCol = requireColumn(table, 'requestBody');
    expect(requestBodyCol).toBeDefined();

    // Count non-null binary entries (tag row + 2 log rows = at least 3)
    let nonNullCount = 0;
    const decodedPayloads: unknown[] = [];
    for (let i = 0; i < requestBodyCol.length; i++) {
      const bytes = requestBodyCol.at(i);
      if (bytes instanceof Uint8Array) {
        nonNullCount++;
        decodedPayloads.push(decode(bytes));
      }
    }
    expect(nonNullCount).toBeGreaterThanOrEqual(3);

    // Verify we got the tag payload and at least one log payload
    expect(decodedPayloads).toContainEqual({
      action: 'create',
      data: { name: 'test', value: 42 },
    });
    expect(decodedPayloads).toContainEqual({ step: 'validation', ok: true });
    expect(decodedPayloads).toContainEqual({ step: 'persist', ok: true });
  });
});
