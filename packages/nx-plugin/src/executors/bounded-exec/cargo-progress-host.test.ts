import { describe, expect, it, jest } from 'bun:test';
import { mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { createServer, type Server, type Socket } from 'node:net';
import { join } from 'node:path';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from '../../bounded-test-policy.js';
import { ownedSocketDirectory } from '../../testing.js';
import { createProcessTreeKiller, runBoundedExec } from './executor.js';
import { VERDICT_FILE } from './verdict.js';

const BUILD_BARRIER = String.raw`use std::env;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;

fn main() {
    let mut socket = UnixStream::connect(env::var("CARGO_PHASE_SOCKET").expect("build barrier path"))
        .expect("connect build barrier");
    writeln!(socket, "{}", env::var("CARGO_PKG_NAME").expect("package name"))
        .expect("announce held build");
    let mut release = [0; 3];
    socket.read_exact(&mut release).expect("read build release");
    assert_eq!(&release, b"go\n");
    println!("cargo:rerun-if-changed=build.rs");
}
`;

/** Real Cargo/rustc/test processes; IPC, not compiler speed, determines every clock step. */
describe('Cargo test progress host proofs', () => {
  it('excludes both held cold builds in a direct cargo test chain from test idle time', async () => {
    const root = await ownedSocketDirectory('cargo-progress-', 'runner.sock');
    const sockets = new Set<Socket>();
    const exchanges: Promise<void>[] = [];
    const exchangeErrors: unknown[] = [];
    const firstFinished = Promise.withResolvers<boolean>();
    const builds: string[] = [];
    const observed: string[] = [];
    const stdout = process.stdout.write;
    const stderr = process.stderr.write;
    let output = '';
    let firstObserved = false;
    const server = createServer((socket) => {
      sockets.add(socket);
      socket.once('close', () => sockets.delete(socket));
      const { promise, resolve, reject } = Promise.withResolvers<void>();
      socket.once('error', reject);
      socket.once('close', () => reject(new Error('Cargo closed its held build before release')));
      let request = '';
      const received = (data: Buffer) => {
        request += data.toString();
        if (!request.includes('\n')) return;
        socket.off('data', received);
        void (async () => {
          expect(request).toBe(builds.length === 0 ? 'first\n' : 'second\n');
          const name = request.trimEnd();
          builds.push(name);
          if (name === 'second') {
            // && requires the first Cargo/hook to exit before this build can connect. Also
            // observe its libtest terminal here before advancing the clock for the next build.
            expect(await firstFinished.promise).toBe(true);
          }
          observed.push(`${name} build`);
          // build.rs cannot finish until this exchange releases it, so neither cold build
          // can accidentally finish before the unchanged production idle bound is crossed.
          jest.advanceTimersByTime(2 * BOUNDED_TEST_IDLE_TIMEOUT_MS);
          socket.end('go\n');
          resolve();
        })().catch(reject);
      };
      socket.on('data', received);
      exchanges.push(
        promise.catch((error: unknown) => {
          exchangeErrors.push(error);
          socket.destroy();
        }),
      );
    });
    try {
      for (const name of ['first', 'second']) {
        await writeCargoPackage(root, name, `#[test]\nfn ${name}_terminal() { assert_eq!(2 + 2, 4); }\n`);
        await writeFile(join(root, name, 'build.rs'), BUILD_BARRIER);
      }
      await listen(server, join(root, 'runner.sock'));
      process.stdout.write = (chunk) => {
        output += chunk.toString();
        if (
          !firstObserved &&
          output.includes('test first_terminal ... ok\n') &&
          output.includes('test result: ok. 1 passed;')
        ) {
          firstObserved = true;
          observed.push('first terminal');
          firstFinished.resolve(true);
        }
        return true;
      };
      process.stderr.write = () => true;
      jest.useFakeTimers();
      const result = await runBoundedExec(
        {
          command:
            'cargo test --manifest-path first/Cargo.toml --lib && cargo test --manifest-path second/Cargo.toml --lib',
          env: cargoEnvironment(root),
          idleTimeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
          killAfterMs: 0,
        },
        { root },
        createProcessTreeKiller(),
        null,
        null,
      );
      // The unfixed executor dies at the first gate: no terminal or second gate will arrive.
      // Resolve absent-event waits before joining the exchanges instead of hanging the baseline.
      firstFinished.resolve(false);
      await Promise.all(exchanges);
      expect(result.success, result.terminalOutput).toBe(true);
      expect(exchangeErrors).toEqual([]);
      expect(builds).toEqual(['first', 'second']);
      expect(observed).toEqual(['first build', 'first terminal', 'second build']);
      expect(output).toContain('test second_terminal ... ok\n');
      expect(output.match(/test result: ok\. 1 passed;/g)).toHaveLength(2);
      expect(result.terminalOutput).not.toContain('Command made no progress');
    } finally {
      firstFinished.resolve(false);
      jest.useRealTimers();
      process.stdout.write = stdout;
      process.stderr.write = stderr;
      try {
        await closeFixture(server, sockets, root);
      } finally {
        await Promise.all(exchanges);
      }
    }
  });

  it('expires real custom-harness teardown despite a passing summary and nested Cargo JSON', async () => {
    expect(BOUNDED_TEST_IDLE_TIMEOUT_MS).toBe(180_000);
    const root = await ownedSocketDirectory('cargo-progress-', 'runner.sock');
    const sockets = new Set<Socket>();
    const socketErrors: unknown[] = [];
    const stdout = process.stdout.write;
    const stderr = process.stderr.write;
    const treeKiller = createProcessTreeKiller();
    const kills: NodeJS.Signals[] = [];
    let output = '';
    let diagnostics = '';
    let advanced = false;
    let released = false;
    const releaseTeardown = () => {
      if (released) return;
      released = true;
      for (const socket of sockets) socket.end('cleanup\n');
    };
    const server = createServer((socket) => {
      sockets.add(socket);
      socket.once('close', () => sockets.delete(socket));
      socket.once('error', (error) => socketErrors.push(error));
      if (released) socket.end('cleanup\n');
    });
    try {
      await writeCargoPackage(
        root,
        'active',
        'pub fn completed_body() { assert_eq!(2 + 2, 4); }\n',
        '\n[[test]]\nname = "teardown"\npath = "tests/teardown.rs"\nharness = false\n',
      );
      await mkdir(join(root, 'active', 'tests'));
      await writeFile(
        join(root, 'active', 'tests', 'teardown.rs'),
        String.raw`use std::env;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;

struct Teardown {
    gate: UnixStream,
}

impl Drop for Teardown {
    fn drop(&mut self) {
        println!("test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;");
        println!("{}", r#"{"reason":"build-finished","success":true}"#);
        println!("cargo-teardown-ready");
        std::io::stdout().flush().expect("flush teardown readiness");
        // The body passed, but the real harness process is still alive in teardown. Only the
        // executor's process-group kill should finish an accepted run of this proof.
        let mut release = [0; 8];
        self.gate.read_exact(&mut release).expect("read failed-proof cleanup");
        assert_eq!(&release, b"cleanup\n");
    }
}

fn main() {
    let _teardown = Teardown {
        gate: UnixStream::connect(env::var("CARGO_PHASE_SOCKET").expect("teardown barrier path"))
            .expect("connect teardown barrier"),
    };
    active::completed_body();
    println!("test completed_body ... ok");
}
`,
      );
      await listen(server, join(root, 'runner.sock'));
      process.stdout.write = (chunk) => {
        output += chunk.toString();
        if (!advanced && output.includes('cargo-teardown-ready\n')) {
          advanced = true;
          queueMicrotask(() => {
            jest.advanceTimersByTime(BOUNDED_TEST_IDLE_TIMEOUT_MS);
            // If the misleading output disabled the watchdog, let this failed proof return
            // rather than leaving a harness parked forever behind fake timers.
            if (!diagnostics.includes('Command made no progress')) releaseTeardown();
          });
        }
        return true;
      };
      process.stderr.write = (chunk) => {
        diagnostics += chunk.toString();
        return true;
      };
      jest.useFakeTimers();
      const result = await runBoundedExec(
        {
          command: 'cargo test --manifest-path active/Cargo.toml --test teardown',
          env: cargoEnvironment(root),
          idleTimeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
          killAfterMs: 0,
        },
        { root, record: { task: { id: 'cargo:teardown', hash: null }, directory: join(root, 'record') } },
        {
          kill(pid, signal) {
            kills.push(signal);
            return treeKiller.kill(pid, signal);
          },
        },
        null,
        null,
      );
      expect(advanced, result.terminalOutput).toBe(true);
      expect(output).toContain('test completed_body ... ok\n');
      expect(output).toContain('test result: ok. 1 passed;');
      expect(output).toContain('{"reason":"build-finished","success":true}\n');
      expect(released, 'the production watchdog must terminate teardown, not fixture cleanup').toBe(false);
      expect(result.success).toBe(false);
      expect(result.terminalOutput).toContain(`no test finished for ${BOUNDED_TEST_IDLE_TIMEOUT_MS}ms`);
      expect(kills).toContain('SIGTERM');
      const record: unknown = JSON.parse(await readFile(join(root, 'record', VERDICT_FILE), 'utf8'));
      expect(record).toHaveProperty('verdict.outcome', 'wedged');
      expect(record).toHaveProperty('verdict.idleMs', BOUNDED_TEST_IDLE_TIMEOUT_MS);
      expect(socketErrors).toEqual([]);
    } finally {
      jest.useRealTimers();
      process.stdout.write = stdout;
      process.stderr.write = stderr;
      await closeFixture(server, sockets, root);
    }
  });

  it('preserves Cargo compile-error diagnostics and exit status 101', async () => {
    const root = await ownedSocketDirectory('cargo-progress-', 'runner.sock');
    const stdout = process.stdout.write;
    const stderr = process.stderr.write;
    try {
      await writeCargoPackage(root, 'broken', 'compile_error!("cargo-phase-compile-error");\n');
      process.stdout.write = () => true;
      process.stderr.write = () => true;
      jest.useFakeTimers();
      const result = await runBoundedExec(
        {
          command: 'cargo test --manifest-path broken/Cargo.toml --lib',
          env: cargoEnvironment(root),
          idleTimeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS,
          killAfterMs: 0,
        },
        { root },
        createProcessTreeKiller(),
        null,
        null,
      );
      expect(result.success).toBe(false);
      expect(result.terminalOutput).toContain('cargo-phase-compile-error');
      expect(result.terminalOutput).toContain('Command exited with status 101:');
      expect(result.terminalOutput).not.toContain('Command made no progress');
    } finally {
      jest.useRealTimers();
      process.stdout.write = stdout;
      process.stderr.write = stderr;
      await rm(root, { recursive: true, force: true });
    }
  });
});

function cargoEnvironment(root: string): Record<string, string> {
  return {
    CARGO_BUILD_JOBS: '2',
    RUST_TEST_THREADS: '2',
    CARGO_TERM_COLOR: 'never',
    CARGO_NET_OFFLINE: 'true',
    CARGO_TARGET_DIR: join(root, 'target'),
    CARGO_PHASE_SOCKET: join(root, 'runner.sock'),
  };
}

async function writeCargoPackage(root: string, name: string, source: string, targets = ''): Promise<void> {
  const directory = join(root, name);
  await mkdir(join(directory, 'src'), { recursive: true });
  await writeFile(
    join(directory, 'Cargo.toml'),
    `[package]\nname = "${name}"\nversion = "0.0.0"\nedition = "2021"\n\n[workspace]\n${targets}`,
  );
  await writeFile(join(directory, 'src', 'lib.rs'), source);
}

function listen(server: Server, endpoint: string): Promise<void> {
  const { promise, resolve, reject } = Promise.withResolvers<void>();
  server.once('error', reject);
  server.listen(endpoint, () => {
    server.off('error', reject);
    resolve();
  });
  return promise;
}

async function closeFixture(server: Server, sockets: Set<Socket>, root: string): Promise<void> {
  for (const socket of sockets) socket.destroy();
  try {
    if (server.listening) {
      const { promise, resolve, reject } = Promise.withResolvers<void>();
      server.close((error) => (error ? reject(error) : resolve()));
      await promise;
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}
