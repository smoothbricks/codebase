import { afterEach, describe, expect, it, jest } from 'bun:test';
import { chmod, mkdir, rm, writeFile } from 'node:fs/promises';
import { createServer, type Socket } from 'node:net';
import { join } from 'node:path';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from '../../bounded-test-policy.js';
import { ownedSocketDirectory } from '../../testing.js';
import { createProcessTreeKiller, runBoundedExec } from './executor.js';

const roots: string[] = [];

/** Real Go build/run events; only the executor's clock is controlled, never compiler speed. */
describe('Go test progress host proofs', () => {
  afterEach(async () => {
    jest.useRealTimers();
    await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
  });

  it('excludes held initial linking and inter-package linking from test idle time', async () => {
    const root = await goFixture();
    for (const name of ['first', 'second']) {
      await mkdir(join(root, name));
      await writeFile(
        join(root, name, 'ready_test.go'),
        `package ${name}\nimport "testing"\nfunc TestReady(t *testing.T) {}\n`,
      );
    }
    const endpoint = join(root, 'runner.sock');
    const wrapper = join(root, 'tool.cjs');
    await writeFile(
      wrapper,
      [
        '#!/usr/bin/env node',
        "const { spawnSync } = require('node:child_process');",
        "const { connect } = require('node:net');",
        "const { basename } = require('node:path');",
        'const [tool, ...args] = process.argv.slice(2);',
        'function run() {',
        "  const result = spawnSync(tool, args, { stdio: 'inherit' });",
        '  if (result.error) throw result.error;',
        '  if (result.signal) process.kill(process.pid, result.signal);',
        '  else process.exit(result.status ?? 1);',
        '}',
        "if (basename(tool) !== 'link' || args.some(arg => arg.startsWith('-V'))) run();",
        'else {',
        `  const socket = connect(${JSON.stringify(endpoint)}, () => socket.write('link-ready\\n'));`,
        "  socket.once('error', error => { throw error; });",
        "  let answer = '';",
        "  socket.on('data', data => { answer += data.toString(); });",
        "  socket.once('end', () => {",
        "    if (answer !== 'go\\n') throw new Error('unexpected link release');",
        '    socket.end(); run();',
        '  });',
        '}',
        '',
      ].join('\n'),
    );
    await chmod(wrapper, 0o700);
    const sockets = new Set<Socket>();
    const exchanges: Promise<unknown>[] = [];
    const firstFinished = Promise.withResolvers<boolean>();
    let links = 0;
    const server = createServer((socket) => {
      sockets.add(socket);
      socket.once('close', () => sockets.delete(socket));
      exchanges.push(
        new Promise<void>((resolve, reject) => {
          socket.once('error', reject);
          let request = '';
          const received = (data: Buffer) => {
            request += data.toString();
            if (!request.includes('\n')) return;
            socket.off('data', received);
            void (async () => {
              expect(request).toBe('link-ready\n');
              links += 1;
              if (links === 2) expect(await firstFinished.promise).toBe(true);
              // The linker is held at a real IPC barrier while virtual idle time crosses the
              // unchanged production bound twice. No test binary exists for this package yet.
              jest.advanceTimersByTime(2 * BOUNDED_TEST_IDLE_TIMEOUT_MS);
              socket.end('go\n');
              resolve();
            })().catch(reject);
          };
          socket.on('data', received);
        }).catch((error: unknown) => error),
      );
    });
    await new Promise<void>((resolve) => server.listen(endpoint, resolve));
    const stdout = process.stdout.write;
    const stderr = process.stderr.write;
    let output = '';
    process.stdout.write = (chunk) => {
      output += chunk.toString();
      if (output.includes('"Action":"pass","Package":"example/first","Elapsed":')) firstFinished.resolve(true);
      return true;
    };
    process.stderr.write = () => true;
    jest.useFakeTimers();
    try {
      const command = ['first', 'second']
        .map((name) => `go test -p=1 -count=1 -toolexec=${JSON.stringify(wrapper)} ./${name}`)
        .join(' && ');
      const result = await runBoundedExec(
        { command, idleTimeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS, killAfterMs: 0 },
        { root },
        createProcessTreeKiller(),
        null,
      );
      firstFinished.resolve(false);
      const exchangeErrors = (await Promise.all(exchanges)).filter((error) => error !== undefined);
      expect(result.success, result.terminalOutput).toBe(true);
      expect(exchangeErrors).toEqual([]);
      expect(links).toBe(2);
      expect(result.terminalOutput).not.toContain('Command made no progress');
    } finally {
      firstFinished.resolve(false);
      jest.useRealTimers();
      process.stdout.write = stdout;
      process.stderr.write = stderr;
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
    }
  });

  it('ends a real hanging Go test when its unchanged active-test budget expires', async () => {
    const root = await goFixture();
    await writeFile(
      join(root, 'hang_test.go'),
      'package example\nimport ("runtime"; "testing")\nfunc TestHang(t *testing.T) { for { runtime.Gosched() } }\n',
    );
    const stdout = process.stdout.write;
    const stderr = process.stderr.write;
    let output = '';
    let advanced = false;
    process.stdout.write = (chunk) => {
      output += chunk.toString();
      if (!advanced && output.includes('"Action":"run","Package":"example","Test":"TestHang"')) {
        advanced = true;
        queueMicrotask(() => jest.advanceTimersByTime(BOUNDED_TEST_IDLE_TIMEOUT_MS));
      }
      return true;
    };
    process.stderr.write = () => true;
    jest.useFakeTimers();
    try {
      const result = await runBoundedExec(
        { command: 'go test -p=1 -count=1 .', idleTimeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS, killAfterMs: 0 },
        { root },
        createProcessTreeKiller(),
        null,
      );
      expect(advanced).toBe(true);
      expect(result.success).toBe(false);
      expect(result.terminalOutput).toContain(`no test finished for ${BOUNDED_TEST_IDLE_TIMEOUT_MS}ms`);
      expect(result.terminalOutput).toContain('still running: example TestHang');
    } finally {
      jest.useRealTimers();
      process.stdout.write = stdout;
      process.stderr.write = stderr;
    }
  });
});

async function goFixture(): Promise<string> {
  const root = await ownedSocketDirectory('go-progress-', 'runner.sock');
  roots.push(root);
  await writeFile(join(root, 'go.mod'), 'module example\n\ngo 1.24\n');
  return root;
}
