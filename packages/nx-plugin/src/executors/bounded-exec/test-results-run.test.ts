import { expect, it } from 'bun:test';
import { once } from 'node:events';
import { existsSync } from 'node:fs';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { createServer, type Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { whenFound } from '../../__tests__/path-change.js';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from '../../bounded-test-policy.js';
import { createProcessTreeKiller, runBoundedExec } from './executor.js';
import { isClosedTestResult, TEST_RESULTS_FILE } from './test-results.js';
import { readRecord } from './verdict.js';

const streamArchive = join(import.meta.dir, '../../../.cache/test-stream/archive.tar.zst');
const streamFixture = join(import.meta.dir, '../../../test-fixtures/stream-witness');
it.each(['bun', 'nextest'] as const)(
  'publishes a closed failure while later %s tests are held, before JUnit and verdict',
  async (runner) => {
    const root = await mkdtemp(join(tmpdir(), 'bounded-test-stream-'));
    const directory = join(root, 'artifacts');
    const sockets = new Set<Socket>();
    const held = Promise.withResolvers<void>();
    const readiness = new AbortController();
    const server = createServer((socket) => {
      sockets.add(socket);
      socket.once('close', () => sockets.delete(socket));
      socket.on('error', held.reject);
      held.resolve();
    });
    let running: ReturnType<typeof runBoundedExec> | null = null;
    const inherited = ['MBX_SCHEDULER', 'MBX_SCHED_DISABLE', 'BOUNDED_EXEC_CPU_POOL', 'BOUNDED_EXEC_CPU_TOKENS'].map(
      (key) => [key, process.env[key]] as const,
    );
    for (const [key] of inherited) delete process.env[key];
    try {
      if (runner === 'bun') {
        await writeFile(
          join(root, 'stream.test.ts'),
          String.raw`
import { test } from 'bun:test';
import { connect } from 'node:net';
test('early_failure', () => { throw new Error('closed failure evidence é'); });
test('later_test', () => new Promise<void>((resolve, reject) => {
  const [host, port] = process.env.STREAM_GATE!.split(':');
  const gate = connect({host, port: Number(port)}, () => {});
  gate.on('data', () => { gate.end(); resolve(); });
  gate.on('error', reject);
}));
`,
        );
      }
      server.listen(0, '127.0.0.1');
      await once(server, 'listening');
      const address = server.address();
      if (address === null || typeof address === 'string') throw new Error('fixture needs a TCP address');
      let settled = false;
      running = runBoundedExec(
        {
          command:
            runner === 'bun'
              ? 'bun test --timeout=0 ./stream.test.ts'
              : `cargo nextest run --archive-file '${streamArchive}' --workspace-remap '${streamFixture}' --no-fail-fast --user-config-file none`,
          // Exercise the symbol/ANSI output used by Nx too; readiness must not depend on its spelling.
          env: { STREAM_GATE: `127.0.0.1:${address.port}`, FORCE_COLOR: '1' },
          idleTimeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
        },
        {
          root,
          record: { task: { id: 'fixture:test', hash: null }, directory },
        },
        createProcessTreeKiller(),
        null,
      );
      void running.then(
        () => {
          settled = true;
          held.resolve();
          readiness.abort(new Error('runner ended before the live failure was observed'));
        },
        (error) => {
          held.reject(error);
          readiness.abort(error);
        },
      );
      await held.promise;
      // The held test started after the writer opened its artifacts. Watch the file, not console output.
      const failure = await whenFound(
        join(directory, TEST_RESULTS_FILE),
        async () => (await readClosedResults(directory)).find((record) => record.status === 'fail') ?? null,
        readiness.signal,
      );
      expect(settled).toBe(false);
      expect(existsSync(join(directory, 'report.xml'))).toBe(false);
      expect(await readRecord(directory)).toBeNull();
      expect(failure.id).toEndWith('::early_failure');
      expect(failure.source).toBe(runner === 'bun' ? 'progress' : 'native');
      expect(failure.sourceRef).toBeNull();
      expect(failure.contradictsCursor).toBeUndefined();
      expect(failure.cursor).toBe((await readFile(join(directory, TEST_RESULTS_FILE))).length);
      if (failure.causeRef === null) throw new Error('missing early failure evidence');
      const causes = await readFile(join(directory, failure.causeRef.path));
      expect(causes.subarray(failure.causeRef.startByte, failure.causeRef.endByte).toString('utf8')).toContain(
        'closed failure evidence é',
      );
      for (const socket of sockets) socket.end(Buffer.from([1]));
      const result = await running;
      expect(result.success).toBe(false);
      expect((await readRecord(directory))?.verdict.outcome).toBe('failed');
      expect(await readFile(join(directory, 'report.xml'), 'utf8')).toContain('early_failure');
      const records = await readClosedResults(directory);
      expect(records).toHaveLength(2);
      expect(records.find((record) => record.status === 'fail')).toEqual(failure);
      expect(records.every((record) => record.runId === failure.runId)).toBe(true);
      expect(records.at(-1)?.cursor).toBe((await readFile(join(directory, TEST_RESULTS_FILE))).length);
      expect(records.find((record) => record.id.endsWith('::later_test'))).toMatchObject({
        status: 'pass',
        source: runner === 'bun' ? 'junit' : 'native',
      });
    } finally {
      try {
        for (const socket of sockets) socket.end(Buffer.from([1]));
        await running;
      } finally {
        for (const socket of sockets) socket.destroy();
        try {
          if (server.listening)
            await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
        } finally {
          try {
            await rm(root, { recursive: true, force: true });
          } finally {
            for (const [key, value] of inherited) {
              if (value === undefined) delete process.env[key];
              else process.env[key] = value;
            }
          }
        }
      }
    }
  },
);

/** Read complete protocol records only, using the public schema rather than runner-console spellings. */
async function readClosedResults(directory: string) {
  const lines = (await readFile(join(directory, TEST_RESULTS_FILE), 'utf8')).split('\n');
  lines.pop(); // Reread an incomplete final line on the next file event, as the live protocol requires.
  return lines.map((line) => {
    const record: unknown = JSON.parse(line);
    if (!isClosedTestResult(record)) throw new Error('invalid closed result');
    return record;
  });
}
