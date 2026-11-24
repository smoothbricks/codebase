import * as bunTest from 'bun:test';
import { afterAll, mock } from 'bun:test';
import { defineOpContext } from '../../../defineOpContext.js';
import { defineLogSchema } from '../../../schema/defineLogSchema.js';
import { createBunTestMock, initTraceTestRun, makeBunTestSuiteTracer } from '../../bun-harness.js';

/**
 * Fixture preload for `bun-harness-worker-runs.test.ts`.
 *
 * Installs the Bun trace harness against `LMAO_FIXTURE_SINK` using the entry named by `LMAO_FIXTURE_ENTRY`
 * (`instance` or `global`), then registers a later preload-level `afterAll` hook so the parent test can verify
 * teardown never throws and never skips later preload hooks.
 */
const entry = process.env.LMAO_FIXTURE_ENTRY;
const dbPath = process.env.LMAO_FIXTURE_SINK;
if (dbPath === undefined) throw new Error('LMAO_FIXTURE_SINK names the trace sink');

const binding = defineOpContext({ logSchema: defineLogSchema({}) });
const sqlite = { dbPath };

if (entry === 'instance') {
  const suite = makeBunTestSuiteTracer(binding, { sqlite });
  suite.setupBunTestSuiteTracing();
} else if (entry === 'global') {
  initTraceTestRun(binding, { sqlite });
  mock.module('bun:test', () => createBunTestMock(bunTest));
} else {
  throw new Error('LMAO_FIXTURE_ENTRY is instance or global');
}

console.log('FIXTURE preload');

afterAll(() => {
  console.log('FIXTURE later preload afterAll');
});
