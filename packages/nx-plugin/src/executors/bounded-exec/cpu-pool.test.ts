import { afterEach, describe, expect, it } from 'bun:test';
import {
  chmodSync,
  closeSync,
  constants,
  existsSync,
  mkdirSync,
  openSync,
  readdirSync,
  readSync,
  rmSync,
  statSync,
  writeSync,
} from 'node:fs';
import { writeFile } from 'node:fs/promises';
import { connect, type Socket } from 'node:net';
import { dirname, join } from 'node:path';
import typia from 'typia';
import { ownedSocketDirectory, socketNamespace } from '../../testing.js';
import {
  CPU_POOL_ENV,
  type CpuPool,
  createCpuPool,
  type InheritedCpuPool,
  readInheritedCpuPool,
  type TokenGrant,
  takeParentCpuTokens,
} from './cpu-pool.js';
import type { CpuLeaseProgress } from './lease-wait.js';
import { CPU_TOKENS_ENV } from './runner-sizing.js';

/** The pool's replies as the wire carries them, one line each. */
type PoolReply =
  | CpuLeaseProgress
  | { ok: true; lease: 'queued' }
  | { ok: true; lease: 'granted'; tokens: number }
  | { ok: false; code: 'invalid-request'; error: string };

const parseReply = typia.json.createAssertParse<PoolReply>();

type Granted = Extract<TokenGrant, { granted: true }>;

/** A raw child connection: its replies come one line at a time, and `closed` resolves when it ends. */
interface Child {
  readonly socket: Socket;
  readonly closed: Promise<void>;
  next(): Promise<PoolReply>;
}

interface Opened {
  readonly pool: CpuPool;
  readonly parent: InheritedCpuPool;
}

const NEVER: Promise<void> = new Promise<void>(() => {});
const QUEUED: PoolReply = { ok: true, lease: 'queued' };
const pools: CpuPool[] = [];
const sockets: Socket[] = [];
const directories: string[] = [];
// Descriptors a test opened on a pool's jobserver fifo, as a make-protocol client does.
const descriptors: number[] = [];

afterEach(async () => {
  for (const socket of sockets.splice(0)) {
    socket.destroy();
  }
  for (const fd of descriptors.splice(0)) {
    closeSync(fd);
  }
  for (const pool of pools.splice(0)) {
    // Every pool a test leaves open is closed here, and its outcome is asserted, not dropped.
    expect(await pool.close()).toEqual({ ok: true });
  }
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

/** Make a pool of `tokens` and read its endpoint back through the same path a child uses. */
async function openPool(tokens: number, closed: Promise<void> = NEVER): Promise<Opened> {
  const made = await createCpuPool(tokens, closed);
  if (!made.ok) {
    throw new Error(made.error);
  }
  pools.push(made.pool);
  const parent = readInheritedCpuPool(made.pool.environment);
  if (parent === null) {
    throw new Error('the pool environment names no endpoint');
  }
  return { pool: made.pool, parent };
}

/** A granted lease for a child that the pool must admit: a refusal here is a test failure. */
async function takeOrThrow(parent: InheritedCpuPool, want: number): Promise<Granted> {
  const grant = await takeParentCpuTokens(parent, want);
  if (!grant.granted) {
    throw new Error(`the pool refused ${want} tokens: ${grant.reason}`);
  }
  return grant;
}

/**
 * The count a child is granted for `want`. The grant is released and its close awaited before
 * returning, so the next ask starts from the pool as this one left it.
 */
async function grantedFor(parent: InheritedCpuPool, want: number): Promise<number> {
  const grant = await takeParentCpuTokens(parent, want);
  if (!grant.granted) {
    throw new Error(`the pool refused ${want} tokens: ${grant.reason}`);
  }
  grant.release();
  await grant.closed;
  return grant.tokens;
}

/** The pool directories in the socket namespace right now; a closed pool leaves none behind. */
const cpuPoolDirectories = (): string[] =>
  readdirSync(socketNamespace().directory)
    .filter((name) => name.startsWith('cpu-pool-'))
    .sort();

/**
 * A raw child: connects, writes `request` as its one line, and reads the replies. The child's
 * socket errors are recorded and surfaced by `next()`, never dropped.
 */
function childOf(endpoint: string, request: string): Child {
  const socket = connect(endpoint);
  sockets.push(socket);
  socket.setEncoding('utf8');
  const lines: string[] = [];
  let buffered = '';
  let ended = false;
  let failure: Error | undefined;
  let wake: (() => void) | undefined;
  const rouse = (): void => {
    const resume = wake;
    wake = undefined;
    resume?.();
  };
  socket.on('connect', () => {
    socket.write(`${request}\n`);
  });
  socket.on('data', (chunk: string) => {
    buffered += chunk;
    for (let newline = buffered.indexOf('\n'); newline !== -1; newline = buffered.indexOf('\n')) {
      lines.push(buffered.slice(0, newline));
      buffered = buffered.slice(newline + 1);
    }
    rouse();
  });
  socket.on('error', (error: Error) => {
    failure = error;
    rouse();
  });
  const closed = new Promise<void>((resolve) => {
    socket.once('close', () => {
      ended = true;
      rouse();
      resolve();
    });
  });
  return {
    socket,
    closed,
    async next(): Promise<PoolReply> {
      for (;;) {
        const line = lines.shift();
        if (line !== undefined) {
          const reply = parseReply(line);
          if ('progress' in reply) continue;
          return reply;
        }
        if (ended) {
          throw (
            failure ?? new Error(`the pool closed before replying to ${request.slice(0, 80)} (${request.length} bytes)`)
          );
        }
        await new Promise<void>((resolve) => {
          wake = resolve;
        });
      }
    },
  };
}

/** Read a child process's stdout until `marker` has been printed. */
async function untilLine(stream: ReadableStream<Uint8Array>, marker: string): Promise<void> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let text = '';
  while (!text.includes(marker)) {
    const next = await reader.read();
    if (next.done) {
      throw new Error(`the child exited before printing ${marker}: ${text}`);
    }
    text += decoder.decode(next.value, { stream: true });
  }
  reader.releaseLock();
}

/** The fifo a pool's environment names for Cargo and make, which name the same one. */
function jobserverFifo(pool: CpuPool): string {
  const flags = pool.environment.CARGO_MAKEFLAGS ?? '';
  expect(pool.environment.MAKEFLAGS).toBe(flags);
  const named = /^-j --jobserver-auth=fifo:(\/\S+)$/.exec(flags)?.[1];
  if (named === undefined) {
    throw new Error(`the pool names no jobserver fifo: ${JSON.stringify(pool.environment)}`);
  }
  return named;
}

/**
 * A make-protocol client of `fifo`, opened read-write as Cargo opens it, and non-blocking here so that `take` reads
 * what the fifo holds now and an empty fifo reads 0.
 */
function jobserverClient(fifo: string): { take(most: number): number; give(count: number): void } {
  const fd = openSync(fifo, constants.O_RDWR | constants.O_NONBLOCK);
  descriptors.push(fd);
  return {
    take(most) {
      try {
        return readSync(fd, Buffer.alloc(most), 0, most, null);
      } catch (error) {
        if (error instanceof Error && 'code' in error && error.code === 'EAGAIN') return 0;
        throw error;
      }
    },
    give(count) {
      expect(writeSync(fd, Buffer.alloc(count, '+'))).toBe(count);
    },
  };
}

/** Wait for `count` token bytes in `fifo` and take them, as a blocked Cargo does; resolves to the count it took. */
async function awaitTokens(fifo: string, count: number): Promise<number> {
  const fd = openSync(fifo, constants.O_RDONLY | constants.O_NONBLOCK);
  const reader = Bun.file(fd).stream().getReader();
  let taken = 0;
  try {
    while (taken < count) {
      const chunk = await reader.read();
      if (chunk.done) break;
      taken += chunk.value.byteLength;
    }
  } finally {
    await reader.cancel();
    closeSync(fd);
  }
  return taken;
}

describe('the parent-held CPU pool', () => {
  it('admits queued children first-in, first-out, each at most its want', async () => {
    const { parent } = await openPool(4);
    const a = childOf(parent.socket, JSON.stringify({ op: 'cpu-pool', want: 3 }));
    expect(await a.next()).toEqual(QUEUED);
    expect(await a.next()).toEqual({ ok: true, lease: 'granted', tokens: 3 });
    const b = childOf(parent.socket, JSON.stringify({ op: 'cpu-pool', want: 2 }));
    expect(await b.next()).toEqual(QUEUED);
    expect(await b.next()).toEqual({ ok: true, lease: 'granted', tokens: 1 });
    const c = childOf(parent.socket, JSON.stringify({ op: 'cpu-pool', want: 2 }));
    expect(await c.next()).toEqual(QUEUED);
    const d = childOf(parent.socket, JSON.stringify({ op: 'cpu-pool', want: 3 }));
    expect(await d.next()).toEqual(QUEUED);
    a.socket.destroy();
    expect(await c.next()).toEqual({ ok: true, lease: 'granted', tokens: 2 });
    expect(await d.next()).toEqual({ ok: true, lease: 'granted', tokens: 1 });
  });

  it('never grants more than the pool holds, and admits queued siblings in arrival order', async () => {
    const { parent } = await openPool(4);
    const request = JSON.stringify({ op: 'cpu-pool', want: 2 });
    const one = childOf(parent.socket, request);
    expect(await one.next()).toEqual(QUEUED);
    expect(await one.next()).toEqual({ ok: true, lease: 'granted', tokens: 2 });
    const two = childOf(parent.socket, request);
    expect(await two.next()).toEqual(QUEUED);
    expect(await two.next()).toEqual({ ok: true, lease: 'granted', tokens: 2 });
    // Two siblings of two hold all four tokens: each later sibling queues, and holds nothing.
    const third = childOf(parent.socket, request);
    expect(await third.next()).toEqual(QUEUED);
    const fourth = childOf(parent.socket, request);
    expect(await fourth.next()).toEqual(QUEUED);
    const fifth = childOf(parent.socket, request);
    expect(await fifth.next()).toEqual(QUEUED);
    const sixth = childOf(parent.socket, request);
    expect(await sixth.next()).toEqual(QUEUED);
    one.socket.destroy();
    expect(await third.next()).toEqual({ ok: true, lease: 'granted', tokens: 2 });
    two.socket.destroy();
    expect(await fourth.next()).toEqual({ ok: true, lease: 'granted', tokens: 2 });
    third.socket.destroy();
    expect(await fifth.next()).toEqual({ ok: true, lease: 'granted', tokens: 2 });
    fourth.socket.destroy();
    expect(await sixth.next()).toEqual({ ok: true, lease: 'granted', tokens: 2 });
  });

  it('drops a queued child that closes, and still admits the next child', async () => {
    const { parent } = await openPool(4);
    const holder = childOf(parent.socket, JSON.stringify({ op: 'cpu-pool', want: 4 }));
    expect(await holder.next()).toEqual(QUEUED);
    expect(await holder.next()).toEqual({ ok: true, lease: 'granted', tokens: 4 });
    const queued = childOf(parent.socket, JSON.stringify({ op: 'cpu-pool', want: 2 }));
    expect(await queued.next()).toEqual(QUEUED);
    queued.socket.destroy();
    await queued.closed;
    // The server may process the two closes in either order: a dropped waiter is never granted, so
    // the next child gets its tokens in both orders.
    holder.socket.destroy();
    const next = childOf(parent.socket, JSON.stringify({ op: 'cpu-pool', want: 2 }));
    expect(await next.next()).toEqual(QUEUED);
    expect(await next.next()).toEqual({ ok: true, lease: 'granted', tokens: 2 });
  });

  it("returns a SIGKILLed holder's tokens to its queued sibling", async () => {
    const { parent } = await openPool(4);
    // A plain Node-API child: it needs no typia transform, and SIGKILL closes its socket as a crash does.
    // Its connected socket keeps the child alive, so it needs no timer.
    const holder = Bun.spawn(
      [
        process.execPath,
        '-e',
        `
        const { connect } = require('node:net');
        const socket = connect(${JSON.stringify(parent.socket)});
        socket.on('connect', () => socket.write('{"op":"cpu-pool","want":2}\\n'));
        socket.on('data', (chunk) => process.stdout.write(chunk));
        `,
      ],
      { stdout: 'pipe', stderr: 'inherit' },
    );
    try {
      await untilLine(holder.stdout, '"granted"');
      const filler = childOf(parent.socket, JSON.stringify({ op: 'cpu-pool', want: 2 }));
      expect(await filler.next()).toEqual(QUEUED);
      expect(await filler.next()).toEqual({ ok: true, lease: 'granted', tokens: 2 });
      const sibling = childOf(parent.socket, JSON.stringify({ op: 'cpu-pool', want: 2 }));
      expect(await sibling.next()).toEqual(QUEUED);
      holder.kill('SIGKILL');
      await holder.exited;
      expect(await sibling.next()).toEqual({ ok: true, lease: 'granted', tokens: 2 });
    } finally {
      holder.kill('SIGKILL');
      await holder.exited;
    }
  });

  it('treats a dead or non-socket endpoint as absent, and a malformed endpoint as no pool', async () => {
    const { pool, parent } = await openPool(2);
    expect(await pool.close()).toEqual({ ok: true });
    expect(await takeParentCpuTokens(parent, 1)).toMatchObject({ granted: false, cause: 'absent' });

    const stale = await ownedSocketDirectory('cpu-stale-', 'p.sock');
    directories.push(stale);
    await writeFile(join(stale, 'p.sock'), 'not a socket');
    expect(await takeParentCpuTokens({ socket: join(stale, 'p.sock'), tokens: 2 }, 1)).toMatchObject({
      granted: false,
      cause: 'absent',
    });

    expect(readInheritedCpuPool({ [CPU_POOL_ENV]: 'a\0b', [CPU_TOKENS_ENV]: '2' })).toBeNull();
    expect(await takeParentCpuTokens({ socket: 'a\0b', tokens: 2 }, 1)).toMatchObject({
      granted: false,
      cause: 'other',
    });
    expect(readInheritedCpuPool({ [CPU_POOL_ENV]: '', [CPU_TOKENS_ENV]: '2' })).toBeNull();
    expect(readInheritedCpuPool({ [CPU_POOL_ENV]: '/tmp/p.sock' })).toBeNull();
    expect(readInheritedCpuPool({ [CPU_TOKENS_ENV]: '4' })).toBeNull();
  });

  it('names its endpoint, count and jobserver fifo in private owned paths, and removes them on close', async () => {
    const made = await createCpuPool(3, NEVER);
    if (!made.ok) {
      throw new Error(made.error);
    }
    pools.push(made.pool);
    const inherited = readInheritedCpuPool(made.pool.environment);
    if (inherited === null) {
      throw new Error('the pool environment names no endpoint');
    }
    expect(inherited.tokens).toBe(3);
    expect(made.pool.environment[CPU_TOKENS_ENV]).toBe('3');
    expect(statSync(dirname(inherited.socket)).mode & 0o777).toBe(0o700);
    expect(statSync(inherited.socket).mode & 0o777).toBe(0o600);
    // The jobserver fifo lies beside the socket, as private, and holds the pool's tokens but the implicit first.
    const fifo = jobserverFifo(made.pool);
    expect(dirname(fifo)).toBe(dirname(inherited.socket));
    expect(statSync(fifo).isFIFO()).toBe(true);
    expect(statSync(fifo).mode & 0o777).toBe(0o600);
    const cargo = jobserverClient(fifo);
    expect(cargo.take(3)).toBe(2);
    cargo.give(2);
    expect(await made.pool.close()).toEqual({ ok: true });
    expect(await made.pool.close()).toEqual({ ok: true });
    expect(existsSync(fifo)).toBe(false);
    expect(existsSync(dirname(inherited.socket))).toBe(false);
  });

  it('refuses each malformed request, and keeps admitting valid ones', async () => {
    const { parent } = await openPool(2);
    const malformed = [
      'not json',
      '{"op":"other","want":1}',
      '{"op":"cpu-pool","want":0}',
      '{"op":"cpu-pool","want":1.5}',
      // A grant never grows, so `grow` is a field like any other this pool does not read.
      '{"op":"cpu-pool","want":1,"grow":true}',
      'x'.repeat(5000),
      'x'.repeat(1024 * 1024),
    ];
    for (const line of malformed) {
      const refused = childOf(parent.socket, line);
      expect(await refused.next()).toMatchObject({ ok: false, code: 'invalid-request' });
    }
    expect((await takeOrThrow(parent, 2)).tokens).toBe(2);
  });

  it('destroys a granted child that sends a second request, returning its tokens', async () => {
    const { parent } = await openPool(2);
    const first = childOf(parent.socket, JSON.stringify({ op: 'cpu-pool', want: 2 }));
    expect(await first.next()).toEqual(QUEUED);
    expect(await first.next()).toEqual({ ok: true, lease: 'granted', tokens: 2 });
    first.socket.write(`${JSON.stringify({ op: 'cpu-pool', want: 1 })}\n`);
    await first.closed;
    expect((await takeOrThrow(parent, 2)).tokens).toBe(2);
  });

  it('makes release idempotent, closes the connection, and grants again', async () => {
    const { parent } = await openPool(2);
    const grant = await takeOrThrow(parent, 2);
    grant.release();
    grant.release();
    await grant.closed;
    expect((await takeOrThrow(parent, 2)).tokens).toBe(2);
  });

  it('closes a lent pool and its grandchildren when the upstream grant closes', async () => {
    const upstream = await openPool(4);
    const lent = await takeOrThrow(upstream.parent, 2);
    const sub = await openPool(lent.tokens, lent.closed);
    const grandchild = await takeParentCpuTokens(sub.parent, 2);
    if (!grandchild.granted) {
      throw new Error(`the sub-pool refused its grandchild: ${grandchild.reason}`);
    }
    // The grandchild asks its whole want and the sub-pool grants all it was lent.
    expect(grandchild.tokens).toBe(2);

    expect(await upstream.pool.close()).toEqual({ ok: true });
    await lent.closed;
    await grandchild.closed;
    expect(await sub.pool.close()).toEqual({ ok: true });
    expect(existsSync(dirname(sub.parent.socket))).toBe(false);
    expect(await takeParentCpuTokens(sub.parent, 1)).toMatchObject({ granted: false, cause: 'absent' });
    expect(await takeParentCpuTokens(upstream.parent, 1)).toMatchObject({ granted: false, cause: 'absent' });
  });

  it('asks for its own want whatever count it inherited, and the grant follows that ask', async () => {
    const cases = [
      { want: 1, inherited: 8, tokens: 1 },
      { want: 8, inherited: 2, tokens: 8 },
      { want: 8, inherited: 8, tokens: 8 },
      { want: 3, inherited: 1, tokens: 3 },
    ];
    for (const check of cases) {
      // A fresh full pool isolates each ask from delivery of the previous client's close event.
      const { parent } = await openPool(8);
      expect(await grantedFor({ socket: parent.socket, tokens: check.inherited }, check.want)).toBe(check.tokens);
    }
  });

  it('never grants more than the live pool holds, even when the inherited count claims more', async () => {
    const { parent } = await openPool(2);
    // The child asks 8, but the live pool holds 2 and grants 2.
    expect(await grantedFor({ socket: parent.socket, tokens: 8 }, 8)).toBe(2);
  });

  it('reports a teardown that cannot remove its directory, and reports the same outcome on every close', async () => {
    const made = await createCpuPool(2, NEVER);
    if (!made.ok) {
      throw new Error(made.error);
    }
    const inherited = readInheritedCpuPool(made.pool.environment);
    if (inherited === null) {
      throw new Error('the pool environment names no endpoint');
    }
    const directory = dirname(inherited.socket);
    directories.push(directory);
    // A nonempty subdirectory no one may read makes recursive removal fail on real files, not a stub.
    const stuck = join(directory, 'stuck');
    mkdirSync(stuck);
    await writeFile(join(stuck, 'held'), 'owned cleanup witness');
    chmodSync(stuck, 0o000);
    try {
      const first = await made.pool.close();
      expect(first).toMatchObject({ ok: false, error: expect.stringContaining('its directory was not removed') });
      expect(await made.pool.close()).toEqual(first);
    } finally {
      if (existsSync(stuck)) chmodSync(stuck, 0o700);
    }
  });

  it('makes no pool when its grant closes during setup, and refuses a non-positive count', async () => {
    const before = cpuPoolDirectories();
    const made = await createCpuPool(2, Promise.resolve());
    expect(made.ok).toBe(false);
    expect(cpuPoolDirectories()).toEqual(before);
    await expect(createCpuPool(0, NEVER)).rejects.toBeInstanceOf(RangeError);
  });

  it("lends a 1-token pool's implicit slot to a socket child, since its fifo holds no byte", async () => {
    const { pool, parent } = await openPool(1);
    expect(jobserverClient(jobserverFifo(pool)).take(1)).toBe(0);
    // The holder is not using its own token while it waits on this child: lending it is what avoids the deadlock.
    const first = childOf(parent.socket, JSON.stringify({ op: 'cpu-pool', want: 2 }));
    expect(await first.next()).toEqual(QUEUED);
    expect(await first.next()).toEqual({ ok: true, lease: 'granted', tokens: 1 });
    const second = childOf(parent.socket, JSON.stringify({ op: 'cpu-pool', want: 1 }));
    expect(await second.next()).toEqual(QUEUED);
    first.socket.destroy();
    expect(await second.next()).toEqual({ ok: true, lease: 'granted', tokens: 1 });
  });

  it('grants no byte a fifo reader holds, and wakes its queued head when the reader writes it back', async () => {
    const { pool, parent } = await openPool(2);
    const cargo = jobserverClient(jobserverFifo(pool));
    // Cargo takes the fifo's one byte: only the implicit slot is left to lend.
    expect(cargo.take(1)).toBe(1);
    const first = childOf(parent.socket, JSON.stringify({ op: 'cpu-pool', want: 2 }));
    expect(await first.next()).toEqual(QUEUED);
    expect(await first.next()).toEqual({ ok: true, lease: 'granted', tokens: 1 });
    const second = childOf(parent.socket, JSON.stringify({ op: 'cpu-pool', want: 1 }));
    expect(await second.next()).toEqual(QUEUED);
    const third = childOf(parent.socket, JSON.stringify({ op: 'cpu-pool', want: 1 }));
    expect(await third.next()).toEqual(QUEUED);
    // No socket child releases anything: only the fifo turning readable can wake the head, and it takes the byte.
    cargo.give(1);
    expect(await second.next()).toEqual({ ok: true, lease: 'granted', tokens: 1 });
    // The implicit slot goes back with the first child, to the next in line.
    first.socket.destroy();
    expect(await third.next()).toEqual({ ok: true, lease: 'granted', tokens: 1 });
  });

  it("writes a released child's bytes back to the fifo, where Cargo and the next child both find them", async () => {
    const { pool, parent } = await openPool(3);
    const fifo = jobserverFifo(pool);
    const cargo = jobserverClient(fifo);
    const holder = await takeOrThrow(parent, 3);
    expect(holder.tokens).toBe(3);
    // The grant took both bytes and the implicit slot: the fifo is empty while it holds them.
    expect(cargo.take(2)).toBe(0);
    holder.release();
    await holder.closed;
    // The pool hears the close after the child does; a waiting reader takes the two bytes it writes back.
    expect(await awaitTokens(fifo, 2)).toBe(2);
    cargo.give(2);
    expect(await grantedFor(parent, 3)).toBe(3);
  });
});
