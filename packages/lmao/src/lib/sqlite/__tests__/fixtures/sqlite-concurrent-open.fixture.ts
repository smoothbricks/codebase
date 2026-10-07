import { createTestTracerOptions } from '../../../__tests__/test-helpers.js';
import { defineOpContext } from '../../../defineOpContext.js';
import { defineLogSchema } from '../../../schema/defineLogSchema.js';
import { SQLiteTracer } from '../../../tracers/SQLiteTracer.js';
import { createNodeSQLiteDatabase } from '../../sqlite-node.js';

function waitForMessage(expected: string): Promise<void> {
  const { promise, resolve } = Promise.withResolvers<void>();
  function onMessage(message: unknown): void {
    if (message === expected) {
      process.off('message', onMessage);
      resolve();
    }
  }
  process.on('message', onMessage);
  return promise;
}

const [dbPath, worker] = process.argv.slice(2);
if (!dbPath || !worker || !process.send) throw new Error('expected database path, worker identity and IPC');
const binding = defineOpContext({ logSchema: defineLogSchema({}) });
const open = waitForMessage('open');
process.send('ready');
await open;

const db = createNodeSQLiteDatabase(dbPath);
const tracer = new SQLiteTracer(binding, { ...createTestTracerOptions(), db });
try {
  const commit = waitForMessage('commit');
  process.send('initialized');
  await commit;
  await tracer.trace(`worker-${worker}`, (ctx) => {
    ctx.log.info(`committed-worker-${worker}`);
    return ctx.ok(undefined);
  });
} finally {
  await tracer.close();
  process.disconnect?.();
}
