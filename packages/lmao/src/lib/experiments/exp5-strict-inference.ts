/**
 * Experiment 5: Trying to get BOTH this binding AND strict args inference
 *
 * Hypothesis: The constraint `T extends Record<string, (ctx, ...args: any[]) => Promise<any>>`
 * is widening the type. Let's try different approaches.
 *
 * ============================================================================
 * CONCLUSION: ALL APPROACHES WORK!
 * ============================================================================
 *
 * Key insight: The problem was NOT the constraint - it was missing `ctx: OpContext`
 * annotation in the function definitions. TypeScript needs the ctx type to be explicit
 * for proper inference of the remaining args.
 *
 * Working approaches:
 * 1. ops1<T> - No constraint, just generic T
 * 2. ops2<const T> - With const modifier (TS 5.0+)
 * 4. ops4 with defineOps - Separate type inference step
 *
 * All require: `async GET(ctx: OpContext, url: string)` - explicit ctx type!
 *
 * Inferred types verified:
 * - Test1GETArgs = [string] ✅ (not any[])
 * - Test2GETArgs = [string] ✅ (not any[])
 * - Test4GETArgs = [string] ✅ (not any[])
 *
 * The @ts-expect-error tests prove TypeScript catches wrong arg types.
 *
 * RECOMMENDATION: Use ops1 (no constraint) - simplest, works great.
 * The only requirement is explicit `ctx: OpContext` annotation.
 */

import type { Module, OpContext, RequestOpts } from './ops-types.js';
import { httpModule, Op } from './ops-types.js';

// =============================================================================
// Approach 1: No constraint at all
// =============================================================================

type OpsResult<T> = {
  [K in keyof T]: T[K] extends (ctx: OpContext, ...args: infer Args) => Promise<infer R> ? Op<Args, R> : never;
};

function ops1<T>(module: Module, definitions: T & ThisType<OpsResult<T>>): OpsResult<T> {
  const result = {} as OpsResult<T>;

  for (const [name, fn] of Object.entries(definitions)) {
    (result as Record<string, unknown>)[name] = new Op(
      name,
      fn as (ctx: OpContext, ...args: unknown[]) => Promise<unknown>,
      module,
    );
  }

  for (const op of Object.values(result)) {
    const opInstance = op as Op<unknown[], unknown>;
    const originalFn = opInstance.fn;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (opInstance as any).fn = originalFn.bind(result);
  }

  return result;
}

const test1 = ops1(httpModule, {
  async GET(ctx: OpContext, url: string) {
    ctx.log.info(`GET ${url}`);
    return ctx.span('request', this.request, url, { method: 'GET' as const });
  },
  async request(ctx: OpContext, url: string, opts: RequestOpts) {
    ctx.tag.method(opts.method).url(url);
    return fetch(url);
  },
});

// Check inference
type Test1GETArgs = typeof test1.GET extends Op<infer A, unknown> ? A : never;
// Hover over this to see what A is

// =============================================================================
// Approach 2: Use `const` type parameter (TS 5.0+)
// =============================================================================

function ops2<const T>(module: Module, definitions: T & ThisType<OpsResult<T>>): OpsResult<T> {
  const result = {} as OpsResult<T>;

  for (const [name, fn] of Object.entries(definitions)) {
    (result as Record<string, unknown>)[name] = new Op(
      name,
      fn as (ctx: OpContext, ...args: unknown[]) => Promise<unknown>,
      module,
    );
  }

  for (const op of Object.values(result)) {
    const opInstance = op as Op<unknown[], unknown>;
    const originalFn = opInstance.fn;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (opInstance as any).fn = originalFn.bind(result);
  }

  return result;
}

const test2 = ops2(httpModule, {
  async GET(ctx: OpContext, url: string) {
    ctx.log.info(`GET ${url}`);
    return ctx.span('request', this.request, url, { method: 'GET' as const });
  },
  async request(ctx: OpContext, url: string, opts: RequestOpts) {
    ctx.tag.method(opts.method).url(url);
    return fetch(url);
  },
});

// Check inference
type Test2GETArgs = typeof test2.GET extends Op<infer A, unknown> ? A : never;

// =============================================================================
// Approach 3: Use `satisfies` at call site
// =============================================================================

// Approach 3 removed - `never[]` constraint is too strict

// =============================================================================
// Approach 4: Separate inference from constraint
// =============================================================================

// First, infer the exact type
function defineOps<T>(definitions: T): T {
  return definitions;
}

// Then, use it with ops
function ops4<T>(module: Module, definitions: T & ThisType<OpsResult<T>>): OpsResult<T> {
  const result = {} as OpsResult<T>;

  for (const [name, fn] of Object.entries(definitions)) {
    (result as Record<string, unknown>)[name] = new Op(
      name,
      fn as (ctx: OpContext, ...args: unknown[]) => Promise<unknown>,
      module,
    );
  }

  for (const op of Object.values(result)) {
    const opInstance = op as Op<unknown[], unknown>;
    const originalFn = opInstance.fn;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (opInstance as any).fn = originalFn.bind(result);
  }

  return result;
}

const defs4 = defineOps({
  async GET(ctx: OpContext, url: string) {
    ctx.log.info(`GET ${url}`);
    return fetch(url);
  },
  async request(ctx: OpContext, url: string, opts: RequestOpts) {
    ctx.tag.method(opts.method).url(url);
    return fetch(url);
  },
});

const test4 = ops4(httpModule, defs4);

// Check inference
type Test4GETArgs = typeof test4.GET extends Op<infer A, unknown> ? A : never;

// =============================================================================
// Type verification tests
// =============================================================================

// Test which approach gives us [string] instead of any[] or unknown[]
const _check1: Test1GETArgs = ['test'];
void _check1;
const _check2: Test2GETArgs = ['test'];
void _check2;
const _check4: Test4GETArgs = ['test'];
void _check4;

// If inference works, these should ERROR:
export async function testStrictInference(ctx: OpContext) {
  // Test 1 - no constraint
  // @ts-expect-error if inference works, this should error (number not string)
  await ctx.span('a', test1.GET, 123);

  // Test 2 - const type parameter
  // @ts-expect-error if inference works, this should error
  await ctx.span('b', test2.GET, 123);

  // Test 4 - separate inference
  // @ts-expect-error if inference works, this should error
  await ctx.span('c', test4.GET, 123);

  // Valid calls should work:
  await ctx.span('d', test1.GET, 'https://example.com'); // ✅
  await ctx.span('e', test1.request, 'https://example.com', { method: 'GET' }); // ✅
}

// Test that `this` binding works inside ops - this.request should have correct type
const testThisBinding = ops1(httpModule, {
  async GET(ctx: OpContext, url: string) {
    // `this.request` should be Op<[string, RequestOpts], Response>
    // @ts-expect-error wrong args to this.request - missing RequestOpts
    return ctx.span('request', this.request, url);
  },
  async request(ctx: OpContext, url: string, opts: RequestOpts) {
    ctx.tag.method(opts.method).url(url);
    return fetch(url);
  },
});
void testThisBinding;

export { test1, test2, test4, ops1, ops2, ops4 };
