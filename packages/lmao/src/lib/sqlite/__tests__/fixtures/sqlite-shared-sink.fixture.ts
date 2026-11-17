import { Database } from 'bun:sqlite';
import { readSync, writeSync } from 'node:fs';
import { createTestTracerOptions } from '../../../__tests__/test-helpers.js';
import { defineOpContext } from '../../../defineOpContext.js';
import { defineLogSchema } from '../../../schema/defineLogSchema.js';
import { SQLiteTracer } from '../../../tracers/SQLiteTracer.js';
import type { SyncSQLiteDatabase } from '../../sqlite-db.js';
import { openWalDatabase } from '../../sqlite-wal.js';

/**
 * One of the processes that write one trace sink together, as the test processes of a package do.
 *
 * `holder` writes a trace and stops inside the commit of its write transaction until its stdin closes: a sibling whose
 * transaction is open for as long as the test says. `writer` opens the sink, shortens SQLite's lock wait on its own
 * connection to the given milliseconds, and when its stdin closes runs a trace the way a test run does (live rows for
 * the root and its direct children, then the whole trace).
 *
 * Each phase is announced as a line on stdout, written synchronously because the next statement blocks the thread.
 */
const [dbPath, role, lockWaitMs] = process.argv.slice(2);
if (!dbPath || (role !== 'holder' && role !== 'writer') || !lockWaitMs) {
  throw new Error('expected a sink path, holder or writer, and the lock wait in milliseconds');
}

function announce(phase: string): void {
  writeSync(1, `${phase}\n`);
}

let holdCommit = false;
const db = openWalDatabase(dbPath, (path): SyncSQLiteDatabase => {
  const database = new Database(path);
  return {
    exec(sql) {
      if (holdCommit && sql === 'COMMIT') {
        holdCommit = false;
        announce('holding');
        // Returns 0 at end of file: the test closed this process's stdin.
        readSync(0, new Uint8Array(1));
      }
      database.exec(sql);
    },
    prepare: (sql) => database.prepare(sql),
    close: () => database.close(),
  };
});
const binding = defineOpContext({ logSchema: defineLogSchema({}) });
const tracer = new SQLiteTracer(binding, { ...createTestTracerOptions(), db, liveChildRows: role === 'writer' });
try {
  if (role === 'holder') {
    holdCommit = true;
    await tracer.trace('holder', (ctx) => ctx.ok(undefined));
  } else {
    db.exec(`PRAGMA busy_timeout = ${Number(lockWaitMs)}`);
    announce('ready');
    await Bun.stdin.text();
    announce('writing');
    await tracer.trace('writer', async (ctx) => {
      await ctx.span('test', (test) => test.ok(undefined));
      return ctx.ok(undefined);
    });
  }
} finally {
  await tracer.close();
}
