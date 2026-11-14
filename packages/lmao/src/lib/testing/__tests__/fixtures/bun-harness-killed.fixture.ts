import * as bunTest from 'bun:test';
import { defineOpContext } from '../../../defineOpContext.js';
import { defineLogSchema } from '../../../schema/defineLogSchema.js';
import { createBunTestMock, initTraceTestRun } from '../../bun-harness.js';

/**
 * A traced Bun run that is killed inside its second test: the process ends by SIGKILL while that test's body runs, so
 * no teardown and no trace end ever happens. What the sink holds afterwards is what the harness wrote while running.
 */
const dbPath = process.env.LMAO_FIXTURE_SINK;
if (dbPath === undefined) throw new Error('LMAO_FIXTURE_SINK names the trace sink');
initTraceTestRun(defineOpContext({ logSchema: defineLogSchema({}) }), { sqlite: { dbPath } });
// The traced `it` a preload would install for `bun:test` importers.
const { it } = createBunTestMock(bunTest);

it('finishes before the kill', () => {});

it('is running when the process is killed', () => {
  process.kill(process.pid, 'SIGKILL');
});
