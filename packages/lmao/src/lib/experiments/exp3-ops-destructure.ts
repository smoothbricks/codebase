/**
 * Experiment 3: ops() destructured from module
 *
 * Pattern:
 *   const { op } = httpModule;  // or const { ops } = httpModule;
 *   const GET = op('GET', async ({ span }, url) => { ... });
 *
 * Or batch:
 *   const { GET, POST } = httpModule.ops({
 *     GET(ctx, url) { this.request(...) },
 *     request(ctx, url, opts) { ... }
 *   });
 *
 * This combines the best of both: module binding + clean syntax
 */

/* eslint-disable @typescript-eslint/no-explicit-any */

import type { Module, OpContext, RequestOpts } from './ops-types.js';
import { Op } from './ops-types.js';

// =============================================================================
// Enhanced Module with ops() method
// =============================================================================

type OpsResult<T> = {
  [K in keyof T]: T[K] extends (ctx: OpContext, ...args: infer Args) => Promise<infer R> ? Op<Args, R> : never;
};

class ModuleBuilder {
  constructor(readonly module: Module) {}

  // Single op definition
  op<Args extends any[], Result>(
    name: string,
    fn: (ctx: OpContext, ...args: Args) => Promise<Result>,
  ): Op<Args, Result> {
    return new Op(name, fn, this.module);
  }

  // Batch ops definition with `this` binding
  ops<T extends Record<string, (ctx: OpContext, ...args: any[]) => Promise<any>>>(
    definitions: T & ThisType<OpsResult<T>>,
  ): OpsResult<T> {
    const result = {} as OpsResult<T>;

    for (const [name, fn] of Object.entries(definitions)) {
      (result as any)[name] = new Op(name, fn, this.module);
    }

    // Bind `this` to result object
    for (const op of Object.values(result)) {
      const opInstance = op as Op<unknown[], unknown>;
      const originalFn = opInstance.fn;
      (opInstance as any).fn = originalFn.bind(result);
    }

    return result;
  }
}

function defineModule(config: { name: string }): ModuleBuilder {
  return new ModuleBuilder({ name: config.name });
}

// =============================================================================
// Test usage - single op() style
// =============================================================================

const httpLib = defineModule({ name: '@mycompany/http' });

// Destructure op from module
const { op } = httpLib;

const request = op('request', async ({ tag }, url: string, opts: RequestOpts) => {
  tag.method(opts.method).url(url);
  const res = await fetch(url);
  tag.status(200);
  return res;
});

const GET = op('GET', async ({ span, log }, url: string) => {
  log.info(`GET ${url}`);
  return span('request', request, url, { method: 'GET' as const });
});

// =============================================================================
// Test usage - batch ops() style
// =============================================================================

const httpLib2 = defineModule({ name: '@mycompany/http2' });

// Batch definition with this binding
const httpOps = httpLib2.ops({
  async GET(ctx, url: string) {
    ctx.log.info(`GET ${url}`);
    return ctx.span('request', this.request, url, { method: 'GET' as const });
  },

  async POST(ctx, url: string, body: unknown) {
    ctx.log.info(`POST ${url}`);
    return ctx.span('request', this.request, url, { method: 'POST' as const, body });
  },

  async request(ctx, url: string, opts: RequestOpts) {
    ctx.tag.method(opts.method).url(url);
    const res = await fetch(url);
    ctx.tag.status(200);
    return res;
  },
});

// =============================================================================
// Type verification
// =============================================================================

export async function testSingleOp(ctx: OpContext) {
  // Single op style
  await ctx.span('a', GET, 'https://example.com');
  await ctx.span('b', request, 'https://example.com', { method: 'GET' });
}

export async function testBatchOps(ctx: OpContext) {
  // Batch ops style
  await ctx.span('a', httpOps.GET, 'https://example.com');
  await ctx.span('b', httpOps.POST, 'https://example.com', { data: 1 });
  await ctx.span('c', httpOps.request, 'https://example.com', { method: 'GET' });
}

export { GET, request, httpOps, httpLib, httpLib2, defineModule, ModuleBuilder };
