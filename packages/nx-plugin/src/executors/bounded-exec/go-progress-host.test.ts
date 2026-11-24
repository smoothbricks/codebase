import { afterAll, afterEach, beforeAll, describe, expect, it, jest } from 'bun:test';
import { existsSync } from 'node:fs';
import { cp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
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

  const wrapper = join(import.meta.dir, '__tests__', 'go-link-barrier-toolexec.cjs');
  const stdlibCache = join(import.meta.dir, '../../..', '.cache', 'go-progress-stdlib-cache');

  async function copyStdlibCache(destination: string): Promise<void> {
    if (!existsSync(stdlibCache)) {
      throw new Error(
        `missing Go stdlib cache at ${stdlibCache}; run \`nx run nx-plugin:go-progress-stdlib-cache\` first`,
      );
    }
    await cp(stdlibCache, destination, { recursive: true });
  }

  describe('held linking phases', () => {
    let root = '';
    let endpoint = '';
    let goCache = '';
    let failedPhase: { readonly name: string; readonly error: unknown } | null = null;

    async function runPhase(name: string, body: () => Promise<void>): Promise<void> {
      if (failedPhase !== null) {
        throw new Error(`skipped ${name}: depends on earlier phase "${failedPhase.name}", which failed`);
      }
      try {
        await body();
      } catch (error) {
        failedPhase ??= { name, error };
        throw error;
      }
    }

    beforeAll(async () => {
      try {
        root = await ownedSocketDirectory('go-progress-', 'runner.sock');
        await writeFile(join(root, 'go.mod'), 'module example\n\ngo 1.24\n');
        for (const name of ['first', 'second']) {
          await mkdir(join(root, name));
          await writeFile(
            join(root, name, 'ready_test.go'),
            `package ${name}\nimport "testing"\nfunc TestReady(t *testing.T) {}\n`,
          );
        }
        endpoint = join(root, 'runner.sock');
        goCache = join(root, 'gocache');
        await copyStdlibCache(goCache);
      } catch (error) {
        failedPhase ??= { name: 'beforeAll fixture setup', error };
        throw error;
      }
    });

    afterAll(async () => {
      if (root !== '') {
        await rm(root, { recursive: true, force: true });
      }
    });

    it('excludes held initial linking from test idle time', async () => {
      await runPhase('held initial linking', async () => {
        const sockets = new Set<Socket>();
        const exchanges: Promise<unknown>[] = [];
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
        await rm(endpoint, { force: true });
        await new Promise<void>((resolve) => server.listen(endpoint, resolve));
        const toolLog = join(root, 'initial-tools.log');
        const stdout = process.stdout.write;
        const stderr = process.stderr.write;
        process.stdout.write = () => true;
        process.stderr.write = () => true;
        jest.useFakeTimers();
        try {
          const command = `go test -p=1 -count=1 -toolexec=${JSON.stringify(`node ${wrapper}`)} ./first`;
          const result = await runBoundedExec(
            {
              command,
              env: {
                GOCACHE: goCache,
                GOWORK: 'off',
                GOTOOLCHAIN: 'local',
                GOPROXY: 'off',
                GO_PROGRESS_BARRIER_SOCKET: endpoint,
                GO_PROGRESS_TOOL_LOG: toolLog,
              },
              idleTimeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
              killAfterMs: 0,
            },
            { root },
            createProcessTreeKiller(),
            null,
          );
          const exchangeErrors = (await Promise.all(exchanges)).filter((error) => error !== undefined);
          expect(result.success, result.terminalOutput).toBe(true);
          expect(exchangeErrors).toEqual([]);
          expect(links).toBe(1);
          expect(result.terminalOutput).not.toContain('Command made no progress');
          const compiled = (await readFile(toolLog, 'utf8'))
            .trim()
            .split('\n')
            .filter((line) => line.startsWith('compile\t'))
            .map((line) => line.slice('compile\t'.length));
          expect(
            compiled.sort(),
            'initial linking must compile only the fixture package, never the Go standard library inside the test',
          ).toEqual(['example/first', 'main']);
        } finally {
          jest.useRealTimers();
          process.stdout.write = stdout;
          process.stderr.write = stderr;
          for (const socket of sockets) socket.destroy();
          await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
        }
      });
    });

    it('excludes held inter-package linking from test idle time', async () => {
      await runPhase('held inter-package linking', async () => {
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
                  if (links === 2) {
                    expect(await firstFinished.promise).toBe(true);
                    // The linker is held at a real IPC barrier while virtual idle time crosses the
                    // unchanged production bound twice. No test binary exists for this package yet.
                    jest.advanceTimersByTime(2 * BOUNDED_TEST_IDLE_TIMEOUT_MS);
                  }
                  socket.end('go\n');
                  resolve();
                })().catch(reject);
              };
              socket.on('data', received);
            }).catch((error: unknown) => error),
          );
        });
        await rm(endpoint, { force: true });
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
            .map((name) => `go test -p=1 -count=1 -toolexec=${JSON.stringify(`node ${wrapper}`)} ./${name}`)
            .join(' && ');
          const result = await runBoundedExec(
            {
              command,
              env: {
                GOCACHE: goCache,
                GOWORK: 'off',
                GOTOOLCHAIN: 'local',
                GOPROXY: 'off',
                GO_PROGRESS_BARRIER_SOCKET: endpoint,
              },
              idleTimeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
              killAfterMs: 0,
            },
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
    });
  });

  it('ends a real hanging Go test when its unchanged active-test budget expires', async () => {
    const root = await goFixture();
    const goCache = join(root, 'gocache');
    await copyStdlibCache(goCache);
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
        {
          command: 'go test -p=1 -count=1 .',
          env: {
            GOCACHE: goCache,
            GOWORK: 'off',
            GOTOOLCHAIN: 'local',
            GOPROXY: 'off',
          },
          idleTimeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
          killAfterMs: 0,
        },
        { root },
        createProcessTreeKiller(),
        null,
      );
      expect(advanced).toBe(true);
      expect(result.success).toBe(false);
      expect(result.terminalOutput).toContain(
        `no test finished or reported progress for ${BOUNDED_TEST_IDLE_TIMEOUT_MS}ms`,
      );
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
