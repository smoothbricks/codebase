import { expect, it } from 'bun:test';
import { once } from 'node:events';
import { existsSync } from 'node:fs';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { createServer, type Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from '../../bounded-test-policy.js';
import { createProcessTreeKiller, runBoundedExec } from './executor.js';
import { isClosedTestResult, TEST_FAILURES_FILE, TEST_RESULTS_FILE } from './test-results.js';
import { readRecord } from './verdict.js';

it.each(['bun', 'nextest'] as const)(
  'publishes a closed failure while later %s tests are held, before JUnit and verdict',
  async (runner) => {
    const root = await mkdtemp(join(tmpdir(), 'bounded-test-stream-'));
    const directory = join(root, 'artifacts');
    const sockets = new Set<Socket>();
    const held = Promise.withResolvers<void>();
    const failed = Promise.withResolvers<void>();
    const server = createServer((socket) => {
      sockets.add(socket);
      socket.once('close', () => sockets.delete(socket));
      socket.on('error', held.reject);
      held.resolve();
    });
    const stderr = process.stderr.write;
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
      } else {
        await mkdir(join(root, 'src'));
        await writeFile(
          join(root, 'Cargo.toml'),
          '[package]\nname = "stream-witness"\nversion = "0.0.0"\nedition = "2021"\n[workspace]\n',
        );
        await writeFile(
          join(root, 'src/lib.rs'),
          String.raw`
#[test]
fn early_failure() { panic!("closed failure evidence é"); }
#[test]
fn later_test() {
    use std::io::Read;
    let mut gate = std::net::TcpStream::connect(std::env::var("STREAM_GATE").unwrap()).unwrap();
    let mut release = [0];
    gate.read_exact(&mut release).unwrap();
}
`,
        );
      }
      server.listen(0, '127.0.0.1');
      await once(server, 'listening');
      const address = server.address();
      if (address === null || typeof address === 'string') throw new Error('fixture needs a TCP address');
      let output = '';
      let settled = false;
      process.stderr.write = (chunk) => {
        output += chunk.toString();
        if (output.includes('(fail) early_failure')) failed.resolve();
        return stderr.call(process.stderr, chunk);
      };
      running = runBoundedExec(
        {
          command:
            runner === 'bun' ? 'bun test --timeout=0 ./stream.test.ts' : 'cargo nextest run --offline --no-fail-fast',
          env: { STREAM_GATE: `127.0.0.1:${address.port}`, CARGO_TARGET_DIR: join(root, 'target') },
          idleTimeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
        },
        {
          root,
          record: { task: { id: 'fixture:test', hash: null }, directory },
          stdout: (text) => {
            output += text;
            if (output.includes('"event":"failed"') || output.includes('"event": "failed"')) failed.resolve();
          },
        },
        createProcessTreeKiller(),
        null,
      );
      void running.then(() => {
        settled = true;
        failed.resolve();
        held.resolve();
      }, failed.reject);
      await Promise.all([held.promise, failed.promise]);
      expect(settled).toBe(false);
      expect(existsSync(join(directory, 'report.xml'))).toBe(false);
      expect(await readRecord(directory)).toBeNull();
      expect(existsSync(join(directory, 'test-results.jsonl'))).toBe(true);
      const lines = await readFile(join(directory, 'test-results.jsonl'), 'utf8');
      expect(lines).toContain('"status":"fail"');
      expect(lines).toContain('::early_failure');
      for (const socket of sockets) socket.end(Buffer.from([1]));
      const result = await running;
      expect(result.success).toBe(false);
      expect((await readRecord(directory))?.verdict.outcome).toBe('failed');
      expect(await readFile(join(directory, 'report.xml'), 'utf8')).toContain('early_failure');
      const records = (await readFile(join(directory, TEST_RESULTS_FILE), 'utf8'))
        .trimEnd()
        .split('\n')
        .map((line) => {
          const record: unknown = JSON.parse(line);
          if (!isClosedTestResult(record)) throw new Error('invalid closed result');
          return record;
        });
      expect(records).toHaveLength(2);
      const failure = records.find((record) => record.status === 'fail');
      expect(failure?.source).toBe(runner === 'bun' ? 'progress' : 'native');
      expect(records.some((record) => record.status === 'pass' && record.id.endsWith('::later_test'))).toBe(true);
      if (failure?.causeRef === null || failure?.causeRef === undefined)
        throw new Error('missing early failure evidence');
      const causes = await readFile(join(directory, TEST_FAILURES_FILE));
      expect(causes.subarray(failure.causeRef.startByte, failure.causeRef.endByte).toString('utf8')).toContain(
        'closed failure evidence é',
      );
    } finally {
      process.stderr.write = stderr;
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
