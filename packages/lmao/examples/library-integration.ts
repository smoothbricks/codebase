#!/usr/bin/env bun
/**
 * Example: Composing library traces with `deps`
 *
 * Demonstrates the library-integration pattern:
 * - Each library defines its OWN clean schema (`status`, `key`, ...)
 * - Libraries are wired as dependencies with `.prefix('name')`, so their columns are
 *   namespaced in the host trace (`cache_key`, `http_status`, ...) with no collisions
 * - A library's ops are invoked, fully typed and cast-free, via `ctx.deps.<name>.<op>`
 * - `defineOps({...})` turns a record of ops into an OpGroup that can be `.prefix()`-ed
 *
 * Run it:
 *   bun run examples/library-integration.ts
 */

import { createTraceRoot, defineLogSchema, defineOpContext, JsBufferStrategy, S, StdioTracer } from '../src/node.js';

// ── Cache library ────────────────────────────────────────────────────────────
const cacheSchema = defineLogSchema({
  operation: S.enum(['GET', 'SET', 'DELETE']),
  key: S.category(),
  hit: S.boolean(),
});
const cacheContext = defineOpContext({ logSchema: cacheSchema });
const cacheOps = cacheContext.defineOps({
  // Library code writes to its own UNPREFIXED column names.
  lookup: async (ctx) => {
    ctx.tag.operation('GET').key('session:user-456').hit(true);
    return ctx.ok({ cached: true });
  },
});

// ── HTTP library — depends on the cache library ──────────────────────────────
const httpSchema = defineLogSchema({
  status: S.number(),
  method: S.enum(['GET', 'POST', 'PUT', 'DELETE']),
  url: S.text(),
});
const httpContext = defineOpContext({
  logSchema: httpSchema,
  deps: { cache: cacheOps.prefix('cache') },
});
const httpOps = httpContext.defineOps({
  request: async (ctx) => {
    ctx.tag.method('GET').url('/users/456').status(200);
    // Invoke the wired cache dependency — typed, no casts.
    await ctx.span('cache-lookup', ctx.deps.cache.lookup);
    return ctx.ok({ ok: true });
  },
});

// ── Application — composes both libraries ────────────────────────────────────
const appSchema = defineLogSchema({
  route: S.text(),
  userId: S.category(),
});
const appContext = defineOpContext({
  logSchema: appSchema,
  deps: {
    http: httpOps.prefix('http'),
    cache: cacheOps.prefix('cache'),
  },
});
const { defineOp } = appContext;

const handleRequest = defineOp('handle-request', async (ctx) => {
  ctx.tag.route('/api/users/456').userId('user-456');
  // The app calls the HTTP library, which in turn calls the cache library.
  await ctx.span('http-request', ctx.deps.http.request);
  return ctx.ok({ handled: true });
});

const { trace } = new StdioTracer(appContext, {
  bufferStrategy: new JsBufferStrategy(),
  createTraceRoot,
});

async function main(): Promise<void> {
  const result = await trace('handle-request', handleRequest);
  console.log(result.success ? '✅ handled' : '❌ failed');
  console.log('\nColumns are namespaced by prefix: cache_* and http_* nest cleanly into the app trace.');
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
