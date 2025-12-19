/**
 * Experiment 1: ops() with method shorthand and `this` binding
 *
 * CONCLUSION: This approach has a fundamental TypeScript limitation.
 *
 * The `this` binding via `ThisType<T>` WORKS - you can reference
 * `this.request` and it's typed correctly.
 *
 * BUT: The function constraint required for the generic causes
 * argument inference to collapse to `any[]` or `unknown[]`.
 *
 * You can have EITHER:
 * - Strict arg inference (single op() with explicit generics)
 * - Batch definition with `this` binding (loses arg strictness)
 *
 * RECOMMENDATION: Use single op() definitions (see exp2-op-single.ts)
 */

/* eslint-disable @typescript-eslint/no-explicit-any */

import type { Module, OpContext, RequestOpts } from './ops-types.js';
import { httpModule, Op } from './ops-types.js';

// =============================================================================
// ops() - batch definition with this binding
// Args inference is lost due to the `any` in the constraint
// =============================================================================

type OpsResult<T> = {
  [K in keyof T]: T[K] extends (ctx: OpContext, ...args: infer Args) => Promise<infer R> ? Op<Args, R> : never;
};

export function ops<T extends Record<string, (ctx: OpContext, ...args: any[]) => Promise<any>>>(
  module: Module,
  definitions: T & ThisType<OpsResult<T>>,
): OpsResult<T> {
  const result = {} as OpsResult<T>;

  for (const [name, fn] of Object.entries(definitions)) {
    (result as any)[name] = new Op(name, fn, module);
  }

  for (const op of Object.values(result)) {
    const opInstance = op as Op<unknown[], unknown>;
    const originalFn = opInstance.fn;
    (opInstance as any).fn = originalFn.bind(result);
  }

  return result;
}

// =============================================================================
// Usage - `this` works but args are any[]
// =============================================================================

const httpOps = ops(httpModule, {
  async GET(ctx, url: string) {
    ctx.log.info(`GET ${url}`);
    // ✅ `this.request` IS typed correctly
    return ctx.span('request', this.request, url, { method: 'GET' as const });
  },

  async POST(ctx, url: string, body: unknown) {
    return ctx.span('request', this.request, url, { method: 'POST' as const, body });
  },

  async request(ctx, url: string, opts: RequestOpts) {
    ctx.tag.method(opts.method).url(url);
    return fetch(url);
  },
});

// =============================================================================
// The problem: Args inference is lost
// =============================================================================

// GETArgs is `any[]` instead of `[string]` - inference lost
type GETArgs = typeof httpOps.GET extends Op<infer A, unknown> ? A : never;
// Use it to avoid unused error
const _proveArgsAreAny: GETArgs = [1, 2, 3]; // This compiles because GETArgs is any[]
void _proveArgsAreAny;

// This should error but doesn't because args are any[]
export async function _brokenTypeCheck(ctx: OpContext) {
  // ❌ These should error but DON'T because args are any[]
  await ctx.span('a', httpOps.GET, 123); // number instead of string - NO ERROR
  await ctx.span('b', httpOps.GET, 'url', 'extra'); // extra arg - NO ERROR
}

export { httpOps };
