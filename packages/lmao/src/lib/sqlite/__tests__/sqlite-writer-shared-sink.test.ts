import { Database } from 'bun:sqlite';
import { describe, expect, it } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { isRecord } from '@smoothbricks/validation';
import { createTestTracerOptions } from '../../__tests__/test-helpers.js';
import { defineOpContext } from '../../defineOpContext.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import { SQLiteTracer } from '../../tracers/SQLiteTracer.js';
import { shareSinkWriteLock } from '../sqlite-write-lock.js';

const fixture = fileURLToPath(new URL('./fixtures/sqlite-shared-sink.fixture.ts', import.meta.url));
const WRITERS = 8;
/** SQLite's lock wait on each writer's connection; the holder's transaction stays open for several of them. */
const LOCK_WAIT_MS = 250;

type Phase = 'ready' | 'writing' | 'holding';

function start(dbPath: string, role: 'holder' | 'writer') {
  const child = Bun.spawn({
    cmd: [process.execPath, fixture, dbPath, role, String(LOCK_WAIT_MS)],
    stdin: 'pipe',
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const phases: Record<Phase, ReturnType<typeof Promise.withResolvers<void>>> = {
    ready: Promise.withResolvers(),
    writing: Promise.withResolvers(),
    holding: Promise.withResolvers(),
  };
  async function readPhases(): Promise<string> {
    const decoder = new TextDecoder();
    let stdout = '';
    for await (const chunk of child.stdout) {
      stdout += decoder.decode(chunk, { stream: true });
      for (const line of stdout.split('\n')) {
        if (line === 'ready' || line === 'writing' || line === 'holding') phases[line].resolve();
      }
    }
    return stdout;
  }
  const result = Promise.all([child.exited, readPhases(), new Response(child.stderr).text()]).then(
    ([exitCode, stdout, stderr]) => ({ exitCode, output: stdout + stderr }),
  );
  return {
    child,
    result,
    reached(phase: Phase): Promise<void> {
      return Promise.race([
        phases[phase].promise,
        result.then(({ exitCode, output }) => {
          throw new Error(`${role} exited ${exitCode} before ${phase}:\n${output}`);
        }),
      ]);
    },
  };
}

describe('SQLiteTraceWriter processes sharing one sink', () => {
  it('flushes a trace at a write lock in turns of at most 4,096 rows', async () => {
    const db = new Database(':memory:');
    const countRows = () => {
      const table = db.query("SELECT COUNT(*) AS tables FROM sqlite_schema WHERE name = 'spans'").get();
      if (!isRecord(table) || table.tables !== 1) return 0;
      const row = db.query('SELECT COUNT(*) AS rows FROM spans').get();
      if (!isRecord(row) || typeof row.rows !== 'number') throw new Error('COUNT(*) returned no number');
      return row.rows;
    };
    /** Rows each hold of the lock committed, in order. */
    const turns: number[] = [];
    let closed = 0;
    shareSinkWriteLock(db, () => ({
      hold(write) {
        const before = countRows();
        const result = write();
        turns.push(countRows() - before);
        return result;
      },
      close() {
        closed += 1;
      },
    }));
    const tracer = new SQLiteTracer(defineOpContext({ logSchema: defineLogSchema({}) }), {
      ...createTestTracerOptions(),
      db,
    });
    await tracer.trace('run', (ctx) => {
      for (let entry = 0; entry < 10_000; entry++) ctx.log.info('entry');
      return ctx.ok(undefined);
    });
    // The first hold creates the table; start row, completion row and 10,000 entries follow.
    expect(turns).toEqual([0, 4096, 4096, 1810]);
    expect(countRows()).toBe(10_002);
    await tracer.close();
    expect(closed).toBe(1);
  });
  it('writers wait out a sibling whose write transaction outlasts their SQLite lock wait', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'lmao-shared-sink-'));
    const dbPath = join(directory, 'traces.db');
    const writers = Array.from({ length: WRITERS }, () => start(dbPath, 'writer'));
    let holder: ReturnType<typeof start> | undefined;
    try {
      // Every writer has opened the sink and created its table before the holder's transaction opens.
      await Promise.all(writers.map((writer) => writer.reached('ready')));
      holder = start(dbPath, 'holder');
      await holder.reached('holding');
      for (const writer of writers) writer.child.stdin.end();
      await Promise.all(writers.map((writer) => writer.reached('writing')));
      // The subject is a wait longer than the writers' SQLite lock wait, so the holder's transaction stays open for a
      // multiple of it. A writer that gives up ends the hold at once: its exit status is the failure.
      await Promise.race([Bun.sleep(4 * LOCK_WAIT_MS), ...writers.map((writer) => writer.result)]);
      holder.child.stdin.end();
      for (const { exitCode, output } of await Promise.all([holder, ...writers].map((worker) => worker.result))) {
        expect(exitCode, output).toBe(0);
      }
      const db = new Database(dbPath);
      try {
        expect(db.query('PRAGMA integrity_check').get()).toEqual({ integrity_check: 'ok' });
        expect(
          db
            .query(
              'SELECT message, COUNT(DISTINCT trace_id) AS traces FROM spans WHERE parent_span_id = 0 AND row_index = 0 GROUP BY message ORDER BY message',
            )
            .all(),
        ).toEqual([
          { message: 'holder', traces: 1 },
          { message: 'writer', traces: WRITERS },
        ]);
      } finally {
        db.close();
      }
    } finally {
      for (const worker of [holder, ...writers]) {
        if (worker && worker.child.exitCode === null) worker.child.kill();
      }
      await Promise.all([holder, ...writers].map((worker) => worker?.result));
      await rm(directory, { recursive: true, force: true });
    }
  });
});
