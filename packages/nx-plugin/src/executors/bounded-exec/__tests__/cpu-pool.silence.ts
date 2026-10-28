/**
 * A CPU pool's SILENT verdict in real time (specs/cowshed/05_gateway.md, "Deadlines"): a pool that is still working is
 * never failed for being slow, and one that falls silent fails its waiter, naming itself. This file is not
 * `*.test.ts`, so the shards skip it: each case waits out the 15 s silence bound or more on the host's clock, and the
 * `test-pool-silence` target names it.
 */
import { expect, it } from 'bun:test';
import { type ChildProcess, spawn } from 'node:child_process';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { createServer, type Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { BOUNDED_TEST_IDLE_TIMEOUT_MS } from '../../../bounded-test-policy.js';
import { ownedSocketDirectory } from '../../../testing.js';
import { readInheritedCpuPool, takeParentCpuTokens } from '../cpu-pool.js';
import { createProcessTreeKiller, runBoundedExec } from '../executor.js';
import { HEARTBEAT_MS, HEARTBEAT_SILENCE_MS } from '../lease-wait.js';
import { closeRunCpuPool, serveRunCpuPool } from '../run-cpu-pool.js';
import { VERDICT_FILE } from '../verdict.js';

/** The runner loads this package's source outside its bunfig, so it takes the same transform preload as this test. */
const VALIDATION_PRELOAD = fileURLToPath(import.meta.resolve('@smoothbricks/validation/bun/preload'));
const LEASE_CLIENT = fileURLToPath(new URL('../gateway-lease.ts', import.meta.url));
const LEASE_WAIT = fileURLToPath(new URL('../lease-wait.ts', import.meta.url));

/**
 * A runner that asks the pool at `socket` for one token with the CPU lease client `takeParentCpuTokens` holds, and its
 * silence rule: it prints `queued` when the pool queues it, then `granted` or the reason it has no grant.
 */
function runner(socket: string): string {
  return [
    `const { holdLease } = await import(${JSON.stringify(LEASE_CLIENT)});`,
    `const { CpuLeaseWait } = await import(${JSON.stringify(LEASE_WAIT)});`,
    `const lease = await holdLease(${JSON.stringify(socket)}, { op: 'cpu-pool', want: 1 }, 'the run CPU pool', new CpuLeaseWait(), () => console.log('queued'));`,
    "console.log(lease.granted ? 'granted' : lease.reason);",
    'if (lease.granted) lease.release();',
  ].join('\n');
}

/**
 * The Nx daemon serves every run's pool when installed plugins load in it, and it hashes a run's tasks in one
 * synchronous native call. In a gate on 2026-10-28 one ran for 14.2 s, and sixteen queued runners failed SILENT
 * together while their pool was alive and granted again three seconds later. Here the test's own thread serves the run
 * and runs nothing for the silence bound plus a heartbeat, while a runner in another process waits in the pool's queue
 * under the real lease client.
 */
it("keeps a run pool's queue alive while the thread that serves the run is blocked past the silence bound", async () => {
  const context = { id: 'blocked-host' };
  const env: NodeJS.ProcessEnv = {};
  let child: ChildProcess | undefined;
  let release: (() => void) | undefined;
  try {
    await serveRunCpuPool(context, env, 1);
    const pool = readInheritedCpuPool(env);
    if (pool === null) throw new Error('the run published no CPU pool');
    const holder = await takeParentCpuTokens(pool, 1);
    if (!holder.granted) throw new Error(holder.reason);
    release = holder.release;
    const waiting = spawn(process.execPath, ['--preload', VALIDATION_PRELOAD, '--eval', runner(pool.socket)], {
      stdio: ['ignore', 'pipe', 'inherit'],
    });
    child = waiting;
    const lines = createInterface({ input: waiting.stdout })[Symbol.asyncIterator]();
    expect((await lines.next()).value).toBe('queued');
    // The daemon's hashPlans, in shape: this thread runs nothing past the runner's silence bound.
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, HEARTBEAT_SILENCE_MS + HEARTBEAT_MS);
    holder.release();
    expect((await lines.next()).value).toBe('granted');
  } finally {
    child?.kill('SIGKILL');
    release?.();
    await closeRunCpuPool(context);
  }
});

it("refuses a run whose parent pool falls silent, and records the wait as a typed row naming the pool's pid", async () => {
  const directory = await ownedSocketDirectory('cpu-silent-', 'p.sock');
  const socket = join(directory, 'p.sock');
  const workspace = await mkdtemp(join(tmpdir(), 'smoothbricks-bounded-exec-'));
  const peers = new Set<Socket>();
  // A pool that queues the request and then writes nothing, as one whose thread stopped turning does.
  const silent = createServer((peer) => {
    peers.add(peer);
    peer.once('data', () => peer.write(`{"ok":true,"lease":"queued","server":${process.pid}}\n`));
  });
  const { promise: listening, resolve: ready, reject } = Promise.withResolvers<void>();
  silent.once('error', reject);
  silent.listen(socket, ready);
  try {
    await listening;
    const result = await runBoundedExec(
      { command: 'touch ran.marker', timeoutMs: BOUNDED_TEST_IDLE_TIMEOUT_MS },
      { root: workspace, record: { task: { id: 'silent', hash: null }, directory: join(workspace, 'silent') } },
      createProcessTreeKiller(),
      null,
      { socket, tokens: 1 },
    );
    expect(result.success).toBe(false);
    expect(result.terminalOutput).toContain(
      `cowshed: cpu-pool parent pool refused: SILENT: cpu-pool: the lease endpoint ${socket} (pid ${process.pid}) exceeded`,
    );
    const record: unknown = JSON.parse(await readFile(join(workspace, 'silent', VERDICT_FILE), 'utf8'));
    expect(record).toHaveProperty('cpuWait', {
      code: 'SILENT',
      boundMs: HEARTBEAT_SILENCE_MS,
      elapsedMs: expect.any(Number),
      last: 'queued',
      sequence: 0,
      server: process.pid,
      socket,
    });
  } finally {
    for (const peer of peers) peer.destroy();
    await new Promise<void>((resolve) => silent.close(() => resolve()));
    await rm(directory, { recursive: true, force: true });
    await rm(workspace, { recursive: true, force: true });
  }
});
