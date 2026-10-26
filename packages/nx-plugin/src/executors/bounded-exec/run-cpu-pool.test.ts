import { describe, expect, it, jest } from 'bun:test';
import { rm } from 'node:fs/promises';
import { connect, createServer, type Socket } from 'node:net';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { ownedSocketDirectory } from '../../testing.js';
import { CPU_POOL_ENV, readInheritedCpuPool, takeParentCpuTokens } from './cpu-pool.js';
import { holdLease } from './gateway-lease.js';
import { CpuLeaseWait } from './lease-wait.js';
import { closeRunCpuPool, serveRunCpuPool } from './run-cpu-pool.js';
import { CPU_TOKENS_ENV } from './runner-sizing.js';

describe("an Nx run's own CPU pool", () => {
  it('serves its tasks one pool of one token per core, so a second runner waits for the first', async () => {
    const env: NodeJS.ProcessEnv = {};
    await serveRunCpuPool({ id: 'run-477' }, env, 4);
    const pool = readInheritedCpuPool(env);
    if (pool === null) {
      throw new Error(`the run published no pool: ${JSON.stringify(env)}`);
    }
    expect(pool.tokens).toBe(4);
    // A Cargo that Nx's own `nx:run-commands` starts reads the same count through the run's jobserver fifo.
    expect(env.CARGO_MAKEFLAGS).toMatch(/^-j --jobserver-auth=fifo:\/\S+$/);
    expect(env.MAKEFLAGS).toBe(env.CARGO_MAKEFLAGS);
    // Nightly 477's shape: two mutation sweeps, each asking for the whole host. The first holds the run's cores;
    // the second queues for them instead of starting a second host's worth of workers beside it.
    const first = await takeParentCpuTokens(pool, 31);
    expect(first.granted && first.tokens).toBe(4);
    const { promise: queued, resolve: queue } = Promise.withResolvers<void>();
    const second = holdLease(pool.socket, { op: 'cpu-pool', want: 31 }, 'the run CPU pool', new CpuLeaseWait(), queue);
    let secondSettled = false;
    void second.then(() => {
      secondSettled = true;
    });
    await queued;
    expect(secondSettled).toBe(false);
    if (first.granted) first.release();
    const granted = await second;
    expect(granted.granted && granted.answer.tokens).toBe(4);
    if (granted.granted) granted.release();

    await closeRunCpuPool({ id: 'run-477' });
    // The run's end closes its pool: nothing is left to grant from it.
    const late = await takeParentCpuTokens(pool, 1);
    expect(late.granted === false && late.cause).toBe('absent');
  });

  it('leaves a run that inherits a pool to that pool', async () => {
    const env: NodeJS.ProcessEnv = { [CPU_POOL_ENV]: '/parent/pool.sock', [CPU_TOKENS_ENV]: '2' };
    await serveRunCpuPool({ id: 'nested' }, env, 4);
    expect(env).toEqual({ [CPU_POOL_ENV]: '/parent/pool.sock', [CPU_TOKENS_ENV]: '2' });
  });

  it("takes a cowshed job's smaller inherited count as its size, and never more than the cores", async () => {
    for (const [inherited, cores, size] of [
      ['3', 8, 3],
      ['16', 8, 8],
    ] as const) {
      const env: NodeJS.ProcessEnv = { [CPU_TOKENS_ENV]: inherited };
      await serveRunCpuPool({ id: `sized-${inherited}` }, env, cores);
      expect(readInheritedCpuPool(env)?.tokens).toBe(size);
      await closeRunCpuPool({ id: `sized-${inherited}` });
    }
  });
});

type WaitEvent = { at: number; kind: 'sent' | 'queued' } | { at: number; kind: 'heartbeat'; sequence: number };

it('decides CPU admission from control completion and advancing actor progress, never queue age', () => {
  const cases = [
    { name: 'connect still in control budget', events: [], at: 4_999, code: null },
    { name: 'connect or send never completed', events: [], at: 5_000, code: 'TIMEOUT' },
    {
      name: 'valid queued reply after two seconds',
      events: [
        { at: 0, kind: 'sent' },
        { at: 2_001, kind: 'queued' },
      ],
      at: 2_001,
      code: null,
    },
    { name: 'silent before queued', events: [{ at: 0, kind: 'sent' }], at: 15_000, code: 'SILENT' },
    {
      name: 'queued restarts silence',
      events: [
        { at: 0, kind: 'sent' },
        { at: 2_001, kind: 'queued' },
      ],
      at: 17_000,
      code: null,
    },
    {
      name: 'silent queued actor',
      events: [
        { at: 0, kind: 'sent' },
        { at: 2_001, kind: 'queued' },
      ],
      at: 17_001,
      code: 'SILENT',
    },
    {
      name: 'live queue exceeds old total bounds',
      events: [
        { at: 0, kind: 'sent' },
        { at: 2_001, kind: 'queued' },
        { at: 7_000, kind: 'heartbeat', sequence: 1 },
        { at: 12_000, kind: 'heartbeat', sequence: 2 },
        { at: 17_000, kind: 'heartbeat', sequence: 3 },
      ],
      at: 31_999,
      code: null,
    },
    {
      name: 'duplicate heartbeat does not renew silence',
      events: [
        { at: 0, kind: 'sent' },
        { at: 5_000, kind: 'heartbeat', sequence: 1 },
        { at: 19_999, kind: 'heartbeat', sequence: 1 },
      ],
      at: 20_000,
      code: 'SILENT',
    },
  ] satisfies { name: string; events: WaitEvent[]; at: number; code: 'TIMEOUT' | 'SILENT' | null }[];
  for (const check of cases) {
    let now = 0;
    const wait = new CpuLeaseWait(() => now);
    for (const event of check.events) {
      now = event.at;
      switch (event.kind) {
        case 'sent':
          wait.sent();
          break;
        case 'queued':
          wait.queued();
          break;
        case 'heartbeat':
          wait.heartbeat(event.sequence);
          break;
      }
    }
    now = check.at;
    expect(wait.expired()?.code ?? null, check.name).toBe(check.code);
  }
});

it('accepts a CPU queue acknowledgement after the disk compatibility probe expires', async () => {
  const directory = await ownedSocketDirectory('cpu-admission-', 'p.sock');
  const socket = join(directory, 'p.sock');
  const { promise: requested, resolve: sawRequest } = Promise.withResolvers<Socket>();
  const peers = new Set<Socket>();
  const server = createServer({ allowHalfOpen: true }, (peer) => {
    peers.add(peer);
    peer.once('data', () => sawRequest(peer));
  });
  const { promise: listening, resolve: ready, reject } = Promise.withResolvers<void>();
  server.once('error', reject);
  server.listen(socket, ready);
  await listening;
  jest.useFakeTimers();
  try {
    const pending = takeParentCpuTokens({ socket, tokens: 1 }, 1);
    const peer = await requested;
    jest.advanceTimersByTime(2_001);
    peer.write('{"ok":true,"progress":1}\n{"ok":true,"lease":"queued"}\n{"ok":true,"lease":"granted","tokens":1}\n');
    const lease = await pending;
    if (lease.granted) {
      lease.release();
      await lease.closed;
    }
    expect(lease).toMatchObject({ granted: true });
  } finally {
    jest.useRealTimers();
    for (const peer of peers) peer.destroy();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(directory, { recursive: true });
  }
});

it('heartbeats a real CPU queue on its actor until a holder returns the token', async () => {
  jest.useFakeTimers();
  const context = { id: 'heartbeat-wire' };
  const env: NodeJS.ProcessEnv = {};
  let child: Socket | undefined;
  let release: (() => void) | undefined;
  try {
    await serveRunCpuPool(context, env, 1);
    const pool = readInheritedCpuPool(env);
    if (pool === null) throw new Error('the run published no CPU pool');
    const holder = await takeParentCpuTokens(pool, 1);
    if (!holder.granted) throw new Error(holder.reason);
    release = holder.release;
    child = connect(pool.socket);
    const lines = createInterface({ input: child })[Symbol.asyncIterator]();
    child.once('connect', () => child?.write('{"op":"cpu-pool","want":1}\n'));
    expect((await lines.next()).value).toBe('{"ok":true,"lease":"queued"}');
    const progress = lines.next();
    jest.advanceTimersByTime(5_000);
    expect((await progress).value).toBe('{"ok":true,"progress":1}');
    holder.release();
    expect((await lines.next()).value).toBe('{"ok":true,"lease":"granted","tokens":1}');
  } finally {
    child?.destroy();
    release?.();
    await closeRunCpuPool(context);
    jest.useRealTimers();
  }
});
