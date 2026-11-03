import { Database } from 'bun:sqlite';
import { it } from 'bun:test';
import { defineOpContext } from '../../../defineOpContext.js';
import { defineLogSchema } from '../../../schema/defineLogSchema.js';
import { SQLiteTracer } from '../../../tracers/SQLiteTracer.js';
import { initTraceTestRun, makeTestTracer } from '../../bun-harness.js';

/**
 * A traced Bun run whose sink loses its table while the run is open, so the root `test-run` trace cannot be written
 * when its span ends. `LMAO_FIXTURE_ENTRY` picks the harness entry that owns the teardown: the instance tracer the
 * preload installs, or the global `initTraceTestRun`.
 */
const entry = process.env.LMAO_FIXTURE_ENTRY;
const dbPath = process.env.LMAO_FIXTURE_SINK;
if (dbPath === undefined) throw new Error('LMAO_FIXTURE_SINK names the trace sink');
const binding = defineOpContext({ logSchema: defineLogSchema({}) });
const sqlite = { dbPath };

// The marker is how the parent process sees the harness close the tracer.
const close = SQLiteTracer.prototype.close;
SQLiteTracer.prototype.close = function (this: SQLiteTracer) {
  console.log('FIXTURE tracer closed');
  return close.call(this);
};

if (entry === 'instance') makeTestTracer(binding, { sqlite }).setup();
else if (entry === 'global') initTraceTestRun(binding, { sqlite });
else throw new Error('LMAO_FIXTURE_ENTRY is instance or global');

it('drops the sink table under the open run', () => {
  const sink = new Database(dbPath);
  sink.exec('DROP TABLE spans');
  sink.close();
});
