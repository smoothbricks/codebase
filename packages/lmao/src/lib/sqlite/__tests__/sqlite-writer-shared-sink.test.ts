import { Database } from 'bun:sqlite';
import { describe, expect, it } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

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
  const phases: Record<Phase, PromiseWithResolvers<void>> = {
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
