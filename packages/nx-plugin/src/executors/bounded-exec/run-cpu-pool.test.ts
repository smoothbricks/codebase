import { describe, expect, it, jest } from 'bun:test';
import { rm } from 'node:fs/promises';
import { createServer, type Socket } from 'node:net';
import { join } from 'node:path';
import { ownedSocketDirectory } from '../../testing.js';
import { takeParentCpuTokens } from './cpu-pool.js';
import { holdLease } from './gateway-lease.js';
import { type RunMemoryError, RunMemoryUnreadable } from './host-memory.js';
import { CONTROL_MS, CpuLeaseWait, HEARTBEAT_MS, HEARTBEAT_SILENCE_MS } from './lease-wait.js';
import { CPU_POOL_ENV, readInheritedCpuPool } from './pool-environment.js';
import { closeRunCpuPool, runPoolTokens, serveRunCpuPool } from './run-cpu-pool.js';
import { CPU_TOKENS_ENV } from './runner-sizing.js';

describe("an Nx run's own CPU pool", () => {
  it('holds one token per core, or a smaller inherited count, and is none beside an inherited pool or mbx', () => {
    const cases = [
      { name: 'no inherited count', env: {}, cores: 4, tokens: 4 },
      { name: "a cowshed job's smaller count", env: { [CPU_TOKENS_ENV]: '3' }, cores: 8, tokens: 3 },
      { name: 'never more than the cores', env: { [CPU_TOKENS_ENV]: '16' }, cores: 8, tokens: 8 },
      {
        name: "a pool the run inherits, an enclosing executor's",
        env: { [CPU_POOL_ENV]: '/parent/pool.sock', [CPU_TOKENS_ENV]: '2' },
        cores: 4,
        tokens: null,
      },
      { name: "mbx's one pool", env: { MBX_SCHEDULER: '1', MBX_CACHE_DIR: '/bc/mbx' }, cores: 4, tokens: null },
    ] satisfies { name: string; env: NodeJS.ProcessEnv; cores: number; tokens: number | null }[];
    for (const check of cases) {
      expect(runPoolTokens(check.env, check.cores), check.name).toBe(check.tokens);
    }
  });

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
    const first = await takeParentCpuTokens(pool, 31, null);
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
    const late = await takeParentCpuTokens(pool, 1, null);
    expect(late.granted === false && late.cause).toBe('absent');
  });

  it('leaves a run that inherits a pool to that pool', async () => {
    const env: NodeJS.ProcessEnv = { [CPU_POOL_ENV]: '/parent/pool.sock', [CPU_TOKENS_ENV]: '2' };
    await serveRunCpuPool({ id: 'nested' }, env, 4);
    expect(env).toEqual({ [CPU_POOL_ENV]: '/parent/pool.sock', [CPU_TOKENS_ENV]: '2' });
  });

  it("holds a leaf whose charge does not fit beside the holders' until a holder returns its memory", async () => {
    const env: NodeJS.ProcessEnv = {};
    // A run of 4 cores and 10 bytes: the 8-byte holder leaves room for no 4-byte leaf, however many cores are free.
    await serveRunCpuPool({ id: 'run-memory' }, env, 4, () =>
      Promise.resolve({ ok: true, memory: { bytes: 10, source: 'a test run' } }),
    );
    const pool = readInheritedCpuPool(env);
    if (pool === null) {
      throw new Error(`the run published no pool: ${JSON.stringify(env)}`);
    }
    const holder = await takeParentCpuTokens(pool, 1, 8);
    expect(holder.granted && holder.tokens).toBe(1);
    const { promise: queued, resolve: queue } = Promise.withResolvers<void>();
    const leaf = holdLease(
      pool.socket,
      { op: 'cpu-pool', want: 1, memory: 4 },
      'the run CPU pool',
      new CpuLeaseWait(),
      queue,
    );
    let leafSettled = false;
    void leaf.then(() => {
      leafSettled = true;
    });
    await queued;
    // Three cores are free for it; the run's memory is not.
    expect(leafSettled).toBe(false);
    if (holder.granted) holder.release();
    const granted = await leaf;
    expect(granted.granted && granted.answer.tokens).toBe(1);
    if (granted.granted) granted.release();
    await closeRunCpuPool({ id: 'run-memory' });
  });

  it("cuts a grant to the workers the run's memory holds, and names the bytes it charged", async () => {
    const env: NodeJS.ProcessEnv = {};
    // 4 cores and 10 bytes: a leaf measured at 4 bytes on one worker, asking for all four, is lent two.
    await serveRunCpuPool({ id: 'run-scaled' }, env, 4, () =>
      Promise.resolve({ ok: true, memory: { bytes: 10, source: 'a test run' } }),
    );
    const pool = readInheritedCpuPool(env);
    if (pool === null) {
      throw new Error(`the run published no pool: ${JSON.stringify(env)}`);
    }
    const first = await takeParentCpuTokens(pool, 4, 4, { parallelism: 1 });
    expect(first.granted && [first.tokens, first.charged]).toEqual([2, 8]);
    const { promise: queued, resolve: queue } = Promise.withResolvers<void>();
    const second = holdLease(
      pool.socket,
      { op: 'cpu-pool', want: 4, memory: 4, parallelism: 1 },
      'the run CPU pool',
      new CpuLeaseWait(),
      queue,
    );
    let settled = false;
    void second.then(() => {
      settled = true;
    });
    await queued;
    // Two cores are free; the run's two bytes are not enough for one more worker of four.
    expect(settled).toBe(false);
    if (first.granted) first.release();
    const granted = await second;
    expect(granted.granted && granted.answer.tokens).toBe(2);
    if (granted.granted) granted.release();
    await closeRunCpuPool({ id: 'run-scaled' });
  });
  it('runs a leaf the kernel killed alone: ahead of the leaves that wait, once nothing holds a grant, and nothing beside it', async () => {
    const env: NodeJS.ProcessEnv = {};
    await serveRunCpuPool({ id: 'run-alone' }, env, 4, () =>
      Promise.resolve({ ok: true, memory: { bytes: 10, source: 'a test run' } }),
    );
    const pool = readInheritedCpuPool(env);
    if (pool === null) {
      throw new Error(`the run published no pool: ${JSON.stringify(env)}`);
    }
    const holder = await takeParentCpuTokens(pool, 4, null);
    expect(holder.granted && holder.tokens).toBe(4);
    const asked = (request: Record<string, unknown>) => {
      const { promise: queued, resolve } = Promise.withResolvers<void>();
      const lease = holdLease(pool.socket, request, 'the run CPU pool', new CpuLeaseWait(), resolve);
      return { queued, lease };
    };
    // A leaf that has not run waits for the holder; the killed leaf, asked after it, goes ahead of it.
    const waiting = asked({ op: 'cpu-pool', want: 1 });
    await waiting.queued;
    const again = asked({ op: 'cpu-pool', want: 4, alone: true });
    await again.queued;
    if (holder.granted) holder.release();
    const granted = await again.lease;
    expect(granted.granted && granted.answer.tokens).toBe(4);
    // Nothing is granted beside it: the leaf that was ahead of it in time is still queued after the pool's turn.
    let waitingSettled = false;
    void waiting.lease.then(() => {
      waitingSettled = true;
    });
    await new Promise((turn) => setImmediate(turn));
    expect(waitingSettled).toBe(false);
    if (granted.granted) granted.release();
    const last = await waiting.lease;
    expect(last.granted && last.answer.tokens).toBe(1);
    if (last.granted) last.release();
    await closeRunCpuPool({ id: 'run-alone' });
  });
  it('does not start a run whose memory cannot be read: it is not shown to have room', async () => {
    const env: NodeJS.ProcessEnv = {};
    const error = {
      operation: 'read-run-memory',
      code: 'unreadable',
      subject: '/sys/fs/cgroup/memory.max',
      cause: 'EACCES: permission denied',
    } satisfies RunMemoryError;
    await expect(
      serveRunCpuPool({ id: 'unread' }, env, 4, () => Promise.resolve({ ok: false, error })),
    ).rejects.toThrow(new RunMemoryUnreadable(error).message);
    expect(env).toEqual({});
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
          wait.queued(null);
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

it('judges only a silence the client was running to see: its own late wake gives the pool one heartbeat more', () => {
  // 05_gateway.md "Deadlines": a client whose own event loop did not run past the silence bound plus a
  // heartbeat (a loaded host) has not read what the pool sent meanwhile, so it re-arms one heartbeat.
  let now = 0;
  const wait = new CpuLeaseWait(() => now);
  wait.sent();
  now = 5_000;
  wait.heartbeat(1);
  // The client's timer, due at 20 s, fires at 32 s.
  now = 32_000;
  wait.look();
  expect(wait.expired()).toBeNull();
  expect(wait.remainingMs()).toBe(HEARTBEAT_MS);
  // A pool still silent one heartbeat after the client runs again is SILENT.
  now = 37_000;
  wait.look();
  expect(wait.expired()?.code).toBe('SILENT');
  // A client that looks in time judges the pool at the bound.
  now = 0;
  const timely = new CpuLeaseWait(() => now);
  timely.sent();
  for (const at of [5_000, 10_000, 15_000]) {
    now = at;
    timely.look();
  }
  expect(timely.expired()?.code).toBe('SILENT');
});

it("answers a failure naming the pool's pid and the last heartbeat sequence the client read", () => {
  let now = 0;
  const unsent = new CpuLeaseWait(() => now);
  now = CONTROL_MS;
  expect(unsent.expired()).toEqual({
    code: 'TIMEOUT',
    boundMs: CONTROL_MS,
    elapsedMs: CONTROL_MS,
    last: 'connecting or sending request',
    sequence: 0,
    server: null,
  });
  now = 0;
  const queued = new CpuLeaseWait(() => now);
  queued.sent();
  now = 1_000;
  queued.queued(4242);
  now = 6_000;
  queued.heartbeat(1);
  now = 6_000 + HEARTBEAT_SILENCE_MS;
  expect(queued.expired()).toEqual({
    code: 'SILENT',
    boundMs: HEARTBEAT_SILENCE_MS,
    elapsedMs: 6_000 + HEARTBEAT_SILENCE_MS,
    last: 'heartbeat 1',
    sequence: 1,
    server: 4242,
  });
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
    const pending = takeParentCpuTokens({ socket, tokens: 1 }, 1, null);
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

it("keeps a silent pool's wait as a typed row naming its socket and the pid its acknowledgement gave", async () => {
  const directory = await ownedSocketDirectory('cpu-silent-', 'p.sock');
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
    let now = 0;
    const { promise: queued, resolve: sawQueued } = Promise.withResolvers<void>();
    const wait = new CpuLeaseWait(() => now);
    const pending = holdLease(socket, { op: 'cpu-pool', want: 1 }, 'the parent CPU pool', wait, sawQueued);
    const peer = await requested;
    peer.write('{"ok":true,"lease":"queued","server":4242}\n');
    await queued;
    // The pool writes nothing more: its silence bound passes on the client's clock and timer alike.
    now = HEARTBEAT_SILENCE_MS;
    jest.advanceTimersByTime(HEARTBEAT_SILENCE_MS);
    const lease = await pending;
    expect(lease).toMatchObject({
      granted: false,
      code: 'SILENT',
      wait: {
        code: 'SILENT',
        boundMs: HEARTBEAT_SILENCE_MS,
        elapsedMs: HEARTBEAT_SILENCE_MS,
        last: 'queued',
        sequence: 0,
        server: 4242,
        socket,
      },
    });
    expect(lease.granted ? '' : lease.reason).toContain(`the lease endpoint ${socket} (pid 4242) exceeded`);
  } finally {
    jest.useRealTimers();
    for (const peer of peers) peer.destroy();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(directory, { recursive: true });
  }
});
