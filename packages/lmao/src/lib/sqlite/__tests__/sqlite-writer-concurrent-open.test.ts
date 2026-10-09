import { Database } from 'bun:sqlite';
import { describe, expect, it } from 'bun:test';
import { existsSync, statSync } from 'node:fs';
import { mkdtemp, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { isRecord } from '@smoothbricks/validation';
import { SPANS_TABLE_INIT_SQL } from '../sqlite-common.js';
import { createNodeSQLiteDatabase, openWalDatabase } from '../sqlite-node.js';
import { SQLiteTraceWriter } from '../sqlite-writer.js';

const fixture = fileURLToPath(new URL('./fixtures/sqlite-concurrent-open.fixture.ts', import.meta.url));
const publicationFixture = fileURLToPath(new URL('./fixtures/sqlite-publication.fixture.mjs', import.meta.url));
const WORKERS = 32;

function startWorker(dbPath: string, worker: number, source = fixture, program = process.execPath) {
  const ready = Promise.withResolvers<void>();
  const publishing = Promise.withResolvers<void>();
  const initialized = Promise.withResolvers<void>();
  const snapshot = Promise.withResolvers<{ readonly ino: string; readonly mode: string }>();
  const child = Bun.spawn({
    cmd: [program, source, dbPath, String(worker)],
    stdout: 'pipe',
    stderr: 'pipe',
    serialization: 'json',
    ipc(message) {
      if (message === 'ready') ready.resolve();
      if (message === 'publishing') publishing.resolve();
      if (message === 'initialized') initialized.resolve();
      if (isRecord(message) && typeof message.ino === 'string' && typeof message.mode === 'string') {
        snapshot.resolve({ ino: message.ino, mode: message.mode });
      }
    },
  });
  const result = Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()]).then(
    ([exitCode, stdout, stderr]) => ({ exitCode, output: stdout + stderr }),
  );

  function waitFor<T>(signal: Promise<T>, phase: string): Promise<T> {
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
    publishing: () => waitFor(publishing.promise, 'publishing'),
    initialized: () => waitFor(initialized.promise, 'initialized'),
    snapshot: () => waitFor(snapshot.promise, 'publication snapshot'),
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
  it.each(['', ':memory:'])('opens fileless path %j without publishing a draft', (dbPath) => {
    const opened: string[] = [];
    const db = openWalDatabase(dbPath, (path) => {
      opened.push(path);
      return createNodeSQLiteDatabase(path);
    });
    try {
      new SQLiteTraceWriter(db);
      expect(opened).toEqual([dbPath]);
      expect(db.prepare('PRAGMA database_list').all()).toEqual([{ seq: 0, name: 'main', file: '' }]);
      expect(db.prepare("SELECT name FROM sqlite_schema WHERE name = 'spans'").all()).toEqual([{ name: 'spans' }]);
    } finally {
      db.close();
    }
  });

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

  it('recovers a killed publisher and gives mixed Bun/Node openers one persistent WAL inode', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'lmao-open-fresh-'));
    const dbPath = join(directory, 'traces.db');
    const node = Bun.which('node');
    if (node === null) throw new Error('mixed SQLite publication regression requires Node on PATH');
    const holder = startWorker(dbPath, -1, publicationFixture);
    const workers = Array.from({ length: WORKERS }, (_, worker) =>
      startWorker(dbPath, worker, publicationFixture, worker % 2 === 0 ? process.execPath : node),
    );
    try {
      await writeFile(join(directory, '.traces.db.draft'), 'killed publisher draft');
      await writeFile(join(directory, '.traces.db.draft-wal'), 'killed publisher WAL');
      await writeFile(join(directory, '.traces.db.draft-shm'), 'killed publisher SHM');
      await writeFile(join(directory, '.traces.db.draft-journal'), 'killed publisher journal');
      await holder.ready();
      holder.child.send('open');
      await holder.initialized();
      const gateInode = statSync(join(directory, '.traces.db.publish'), { bigint: true }).ino;
      await Promise.all(workers.map((worker) => worker.ready()));
      for (const worker of workers) worker.child.send('open');
      await Promise.all(workers.map((worker) => worker.publishing()));
      expect(existsSync(dbPath)).toBe(false);
      holder.child.kill('SIGKILL');
      expect((await holder.result).exitCode).not.toBe(0);
      await Promise.all(workers.map((worker) => worker.initialized()));
      const snapshots = await Promise.all(workers.map((worker) => worker.snapshot()));
      expect(new Set(snapshots.map(({ ino }) => ino)).size).toBe(1);
      expect(snapshots.every(({ mode }) => mode === 'wal')).toBe(true);
      for (const worker of workers) worker.child.send('commit');
      for (const result of await Promise.all(workers.map((worker) => worker.result))) {
        expect(result.exitCode, result.output).toBe(0);
      }
      // Node's last close removes WAL/SHM; a normal sink opener may recreate these coordination files.
      const db = new Database(dbPath);
      try {
        expect(db.query('PRAGMA integrity_check').get()).toEqual({ integrity_check: 'ok' });
        expect(db.query('SELECT count(*) AS count FROM publication_test').get()).toEqual({ count: WORKERS });
      } finally {
        db.close();
      }
      expect((await readdir(directory)).filter((name) => name.startsWith('.')).sort()).toEqual(['.traces.db.publish']);
      expect(statSync(join(directory, '.traces.db.publish'), { bigint: true }).ino).toBe(gateInode);
    } finally {
      for (const worker of [holder, ...workers]) {
        if (worker.child.exitCode === null) worker.child.kill('SIGKILL');
      }
      await Promise.all([holder, ...workers].map((worker) => worker.result));
      await rm(directory, { recursive: true, force: true });
    }
  });
});
