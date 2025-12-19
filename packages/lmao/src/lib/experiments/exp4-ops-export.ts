/**
 * Experiment 4: Re-export ops without repeating names
 *
 * Goal: Define ops with names inferred from keys, and export them easily
 */

/* eslint-disable @typescript-eslint/no-explicit-any */

import type { Module, OpContext, RequestOpts } from './ops-types.js';
import { httpModule, Op } from './ops-types.js';

// =============================================================================
// ops() - returns object with named ops
// =============================================================================

type OpsResult<T> = {
  [K in keyof T]: T[K] extends (ctx: OpContext, ...args: infer Args) => Promise<infer R> ? Op<Args, R> : never;
};

function ops<T extends Record<string, (ctx: OpContext, ...args: any[]) => Promise<any>>>(
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
// Option A: Define and destructure in one go, then re-export the object
// =============================================================================

// Define all ops - names come from object keys
const httpOps = ops(httpModule, {
  async GET(ctx, url: string) {
    ctx.log.info(`GET ${url}`);
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

// Destructure for internal use
const { GET, POST, request } = httpOps;

// Export the whole object - consumers can destructure
export { httpOps };

// Or export individual ops (but this repeats names)
export { GET, POST, request };

// =============================================================================
// Option B: Export directly from ops() - no intermediate variable
// =============================================================================

export const authOps = ops(httpModule, {
  async validateToken(ctx, _token: string) {
    ctx.log.info('Validating token');
    return { valid: true, userId: 'user-123' };
  },

  async refreshToken(ctx, _refreshToken: string) {
    ctx.log.info('Refreshing token');
    return { accessToken: 'new-token' };
  },
});

// Consumer does: import { authOps } from './auth'; const { validateToken } = authOps;

// =============================================================================
// Option C: Spread export (doesn't work in ES modules)
// =============================================================================

// This would be ideal but ES modules don't support dynamic exports:
// export { ...httpOps }; // ❌ Syntax error

// =============================================================================
// Option D: Default export the ops object
// =============================================================================

// export default httpOps;
// Consumer: import http from './http'; http.GET(...)

// =============================================================================
// Option E: Module as namespace-like object
// =============================================================================

// Define the module with ops attached
const http = {
  module: httpModule,
  ...ops(httpModule, {
    async GET(_ctx, url: string) {
      return fetch(url);
    },
    async POST(_ctx, url: string, body: unknown) {
      return fetch(url, { method: 'POST', body: JSON.stringify(body) });
    },
  }),
};

export { http };
// Consumer: import { http } from './http'; await span('get', http.GET, url);

// =============================================================================
// Summary of options
// =============================================================================

/**
 * Option A: export { httpOps } + export { GET, POST, request }
 *   - Pros: Named exports for tree-shaking, consumers can import either way
 *   - Cons: Repeats names in export statement
 *
 * Option B: export const httpOps = ops(...)
 *   - Pros: Single export, no name repetition
 *   - Cons: Consumers must destructure: const { GET } = httpOps
 *
 * Option D: export default httpOps
 *   - Pros: Clean import: import http from './http'
 *   - Cons: Default exports have issues with tree-shaking
 *
 * Option E: Namespace object with spread
 *   - Pros: Clean: http.GET, http.POST
 *   - Cons: Extra object wrapper
 *
 * RECOMMENDATION: Option B (export const httpOps = ops(...))
 *   - No name repetition
 *   - Consumers do: const { GET, POST } = httpOps
 *   - Or: await span('get', httpOps.GET, url)
 */
