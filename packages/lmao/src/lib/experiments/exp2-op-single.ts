/**
 * Experiment 2: Single op() with name as first argument
 *
 * Pattern:
 *   const GET = op('GET', async ({ span, log, tag }, url: string) => { ... })
 *
 * This is simpler but requires separate declarations.
 * Self-reference needs external variable or closure.
 */

/* eslint-disable @typescript-eslint/no-explicit-any */

import type { Module, OpContext, RequestOpts } from './ops-types.js';
import { httpModule, Op } from './ops-types.js';

// =============================================================================
// op() implementation - name as first arg
// =============================================================================

export function createOp(module: Module) {
  return function op<Args extends any[], Result>(
    name: string,
    fn: (ctx: OpContext, ...args: Args) => Promise<Result>,
  ): Op<Args, Result> {
    return new Op(name, fn, module);
  };
}

// Get the op factory for httpModule
const op = createOp(httpModule);

// =============================================================================
// Test usage - separate declarations
// =============================================================================

// Each op is defined separately with explicit name
const request = op('request', async ({ tag }, url: string, opts: RequestOpts) => {
  tag.method(opts.method).url(url);
  const res = await fetch(url);
  tag.status(200);
  return res;
});

const GET = op('GET', async ({ span, log }, url: string) => {
  log.info(`GET ${url}`);
  // Reference `request` directly (it's in scope)
  return span('request', request, url, { method: 'GET' as const });
});

const POST = op('POST', async ({ span, log }, url: string, body: unknown) => {
  log.info(`POST ${url}`);
  return span('request', request, url, { method: 'POST' as const, body });
});

// =============================================================================
// Type verification - correct usage (should compile)
// =============================================================================

export async function testCorrectUsage(ctx: OpContext) {
  await ctx.span('a', GET, 'https://example.com');
  await ctx.span('b', POST, 'https://example.com', { data: 1 });
  await ctx.span('c', request, 'https://example.com', { method: 'GET' });
}

// =============================================================================
// Type verification - wrong usage (should error)
// =============================================================================

export async function testWrongUsage(_ctx: OpContext) {
  // Test 1: GET doesn't take extra argument
  // @ts-expect-error GET only takes url:string
  await _ctx.span('a', GET, 'https://example.com', { body: 1 });

  // Test 2: request requires opts (second arg)
  // @ts-expect-error request requires (url, opts)
  await _ctx.span('b', request, 'https://example.com');

  // Test 3: wrong type for url (number instead of string)
  // @ts-expect-error url must be string
  await _ctx.span('c', GET, 123);
}

// Type investigation
type GETArgs = typeof GET extends Op<infer A, unknown> ? A : never;
type RequestArgsCheck = typeof request extends Op<infer A, unknown> ? A : never;

// These compile because inference works correctly
const checkGET: GETArgs = ['test']; // Should be [string]
const checkRequest: RequestArgsCheck = ['test', { method: 'GET' }]; // Should be [string, RequestOpts]
void checkGET;
void checkRequest;

export { GET, POST, request, op };
