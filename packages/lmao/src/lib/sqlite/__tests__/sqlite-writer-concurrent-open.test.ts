import { Database } from 'bun:sqlite';
import { describe, expect, it } from 'bun:test';
import { mkdtemp, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { SPANS_TABLE_INIT_SQL } from '../sqlite-common.js';

const fixture = fileURLToPath(new URL('./fixtures/sqlite-concurrent-open.fixture.ts', import.meta.url));
const WORKERS = 32;

function startWorker(dbPath: string, worker: number) {
  const ready = Promise.withResolvers<void>();
  const initialized = Promise.withResolvers<void>();
  const child = Bun.spawn({
    cmd: [process.execPath, fixture, dbPath, String(worker)],
    stdout: 'pipe',
    stderr: 'pipe',
    ipc(message) {
      if (message === 'ready') ready.resolve();
      if (message === 'initialized') initialized.resolve();
    },
  });
  const result = Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()]).then(
    ([exitCode, stdout, stderr]) => ({ exitCode, output: stdout + stderr }),
  );

  function waitFor(signal: Promise<void>, phase: string): Promise<void> {
    return Promise.race([
      signal,
      result.then(({ exitCode, output }) => {
        throw new Error(`worker ${worker} exited ${exitCode} before ${phase}:\n${output}`);
      }),
    ]);
  }
  return {
    child,
    result,
    ready: () => waitFor(ready.promise, 'ready'),
    initialized: () => waitFor(initialized.promise, 'initialized'),
  };
}

async function runParallelOpen(dbPath: string, releaseReader?: () => void) {
  const workers = Array.from({ length: WORKERS }, (_, worker) => startWorker(dbPath, worker));
  try {
    await Promise.all(workers.map((worker) => worker.ready()));
    for (const worker of workers) worker.child.send('open');
    await Promise.all(workers.map((worker) => worker.initialized()));
    releaseReader?.();
    for (const worker of workers) worker.child.send('commit');
    const results = await Promise.all(workers.map((worker) => worker.result));
    for (const result of results) expect(result.exitCode, result.output).toBe(0);
  } finally {
    releaseReader?.();
    for (const worker of workers) {
      if (worker.child.exitCode === null) worker.child.kill();
    }
    await Promise.all(workers.map((worker) => worker.result));
  }
}

function assertCommittedWorkers(dbPath: string, mode: string): void {
  const db = new Database(dbPath, { readonly: true });
  try {
    expect(db.query('PRAGMA journal_mode').get()).toEqual({ journal_mode: mode });
    expect(db.query('PRAGMA integrity_check').get()).toEqual({ integrity_check: 'ok' });
    expect(db.query('SELECT COUNT(DISTINCT trace_id) AS count FROM spans').get()).toEqual({ count: WORKERS });
    expect(
      db.query("SELECT COUNT(DISTINCT message) AS count FROM spans WHERE message LIKE 'committed-worker-%'").get(),
    ).toEqual({ count: WORKERS });
  } finally {
    db.close();
  }
}

describe('SQLiteTraceWriter concurrent process open', () => {
  it('opens an existing recoverable sink without upgrading a held reader lock', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'lmao-open-reader-'));
    const dbPath = join(directory, 'traces.db');
    const reader = new Database(dbPath);
    let holdingReader = false;
    function releaseReader(): void {
      if (holdingReader) {
        reader.exec('ROLLBACK');
        holdingReader = false;
      }
    }
    try {
      reader.exec(SPANS_TABLE_INIT_SQL);
      reader.exec("CREATE TABLE preserved (value TEXT); INSERT INTO preserved VALUES ('before-open')");
      reader.exec('BEGIN');
      reader.query('SELECT COUNT(*) FROM spans').get();
      holdingReader = true;
      // A DELETE->WAL header upgrade cannot coexist with this SHARED lock.
      // Workers must finish setup before it is released; only their writes wait.
      await runParallelOpen(dbPath, releaseReader);
      expect(reader.query('PRAGMA journal_mode').get()).toEqual({ journal_mode: 'delete' });
      expect(reader.query('SELECT value FROM preserved').all()).toEqual([{ value: 'before-open' }]);
      assertCommittedWorkers(dbPath, 'delete');
    } finally {
      releaseReader();
      reader.close();
      await rm(directory, { recursive: true, force: true });
    }
  });

  it('publishes one persistent WAL sink before concurrent workers initialize it', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'lmao-open-fresh-'));
    const dbPath = join(directory, 'traces.db');
    try {
      await runParallelOpen(dbPath);
      assertCommittedWorkers(dbPath, 'wal');
      // Every worker has closed: persistence lives in the database header, not
      // a surviving connection or a private publication file.
      expect((await readdir(directory)).filter((name) => name.startsWith('.'))).toEqual([]);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
});
