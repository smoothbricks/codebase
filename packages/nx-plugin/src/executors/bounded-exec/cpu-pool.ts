import { execFile } from 'node:child_process';
import { closeSync, constants, openSync, readSync, writeSync } from 'node:fs';
import { chmod, rm } from 'node:fs/promises';
import { createServer, type Server, Socket } from 'node:net';
import { join } from 'node:path';
import { promisify } from 'node:util';
import typia from 'typia';
import { ownedSocketDirectory } from '../../testing.js';
import { CPU_TOKENS_ENV, parseInheritedTokens, type TokenGrant } from './cpu-tokens.js';
import { holdLease } from './gateway-lease.js';

/**
 * A parent's CPU pool: the tokens a granted process lends its own children, held live rather than
 * inherited as a bare count (specs/cowshed/05_gateway.md, "Host CPU budget", "Nested runners use a
 * parent-held sub-ledger").
 *
 * The pool is a private Unix socket in a directory the process owns: the directory 0700, the socket
 * 0600. A child asks it for a count with one request line, exactly as it asks the gateway
 * (`holdLease`). The pool answers `queued` at once and `granted` when the child reaches the head of
 * its FIFO queue. A grant lasts while its connection does, so a child that dies returns its tokens
 * with its socket. A pool grants only from its own count, and a pool whose upstream grant has gone
 * grants nothing.
 *
 * The same count is a GNU make jobserver fifo beside the socket, mode 0600, which Cargo, its build
 * scripts and every nested `cargo` read through `CARGO_MAKEFLAGS`/`MAKEFLAGS` (specs/cowshed/05_gateway.md,
 * "The jobserver fifo"). A pool of `g` holds `g − 1` token bytes there; the holder's own command holds the
 * implicit first token. A socket grant takes fifo bytes first and that implicit slot last, which the holder
 * is not using while it waits on the child it lends it to; release writes the bytes back and frees the slot.
 * A Cargo reading the fifo and a nested runner holding a socket grant so share one count.
 */

/** The environment name a child reads its parent's pool endpoint from; its count is `CPU_TOKENS_ENV`. */
export const CPU_POOL_ENV = 'BOUNDED_EXEC_CPU_POOL';

/** An inherited pool descriptor; only a grant from its endpoint proves a live allocation. */
export interface InheritedCpuPool {
  readonly socket: string;
  /** The count the parent names beside its endpoint: 0 for a resident's relay pool, which holds no token. */
  readonly tokens: number;
}

/** A running pool: the environment a child inherits to reach it, and how to close it. */
export interface CpuPool {
  /** The endpoint, the count the pool actually holds, and its make jobserver fifo. */
  readonly environment: Record<string, string>;
  /**
   * Refuses new grants, ends every child connection, drains and closes the jobserver fifo, and removes the pool's
   * directory with the fifo in it. The outcome is cached: every call, and an upstream-loss close, returns the first
   * close's outcome. A failed cleanup is an `ok: false` value, never a rejection.
   */
  close(): Promise<CloseOutcome>;
}

type CloseOutcome = { ok: true } | { ok: false; error: string };

/**
 * The run's count of descendant work queued for host CPU tokens (specs/cowshed/05_gateway.md, "The bounds start at
 * the grant"). A queued job is not an idle one: its owner stops the run's idle clock while any wait is held. Every
 * pool of one run, its sub-pools included, reports to the same one.
 */
export interface HostWaits {
  begin(): void;
  end(): void;
}

/** The one line a child sends: a count of whole tokens, at least one. */
interface CpuPoolRequest {
  op: 'cpu-pool';
  want: number & typia.tags.Type<'uint32'> & typia.tags.Minimum<1>;
  /** Ask the owning pool to serve this allocation as its own child pool, with a jobserver fifo of its own. */
  children?: boolean;
}

/**
 * A descendant's word that work it started waits in a host CPU queue (a cowshed job the gateway holds `waiting`). It
 * is held like a grant: the wait lasts while its connection does, so a descendant that dies ends its wait.
 */
interface HostWaitRequest {
  op: 'host-wait';
}

/** The pool's answers, in `holdLease`'s framing. */
type CpuPoolReply =
  | { ok: true; lease: 'queued' }
  | { ok: true; lease: 'granted'; tokens: number; pool?: string }
  | { ok: true; wait: 'held' }
  | { ok: false; code: 'invalid-request'; error: string };

/** Exactly a request's fields, as the gateway's own pools read them: any other field is an invalid request. */
const isRequest = typia.createEquals<CpuPoolRequest | HostWaitRequest>();
const stringifyReply = typia.json.createStringify<CpuPoolReply>();

/** A request over this many bytes is refused as `invalid-request` (specs/cowshed/05_gateway.md, "Wire"). */
const MAX_REQUEST_BYTES = 4096;
/** The refusal for a request too long to be one line, or a chunk that cannot be one. */
const TOO_LONG = `a request is one line of at most ${MAX_REQUEST_BYTES} bytes`;
/** The one empty buffer every connection starts from, so a request never copies a buffer it does not need. */
const EMPTY: Buffer = Buffer.alloc(0);
const POOL_DIRECTORY = 'cpu-pool-';
const POOL_SOCKET = 'p.sock';
/** The jobserver fifo's name beside the socket; the directory's random name is what makes the path unguessable. */
const POOL_FIFO = 'j';
/** The byte make's protocol writes for a token. Any byte read is one token; the pool writes this one back. */
const TOKEN = 0x2b;
const CLOSED_WHILE_MADE = 'the grant that lends this CPU pool closed while the pool was being made';
const run = promisify(execFile);

/**
 * A child's phase. Tokens are owed back only while `holding`: the bytes it took from the jobserver fifo, and whether it
 * holds the pool's implicit slot. A queued child holds nothing; a `waiting` child holds a host wait on the run's idle
 * clock.
 */
type Phase =
  | { readonly kind: 'reading' }
  | { readonly kind: 'queued' }
  | { readonly kind: 'holding'; readonly bytes: number; readonly slot: boolean }
  | { readonly kind: 'waiting' }
  | { readonly kind: 'done' };

/** A child connection and what the pool owes it. Its grant, or its place in line, ends with the connection. */
interface Child {
  readonly socket: Socket;
  phase: Phase;
}

/** A queued child and the count it asked for. */
interface Waiter {
  readonly child: Child;
  readonly want: number;
  readonly children: boolean;
}

/** One framed reply line. The newline is the framing; every reply goes through here. */
const frame = (reply: CpuPoolReply): string => `${stringifyReply(reply)}\n`;

/**
 * The inherited descriptor, or null when malformed or absent. A missing or bare count, or an empty
 * or NUL-bearing endpoint, proves no allocation. A resident's relay pool names a count of 0 beside its
 * endpoint: it holds no token for its lifetime and asks the host for each child that asks it, so zero is
 * valid only with the endpoint, and every other count is a positive whole number. A valid descriptor still
 * needs a live endpoint grant (specs/cowshed/05_gateway.md, "Host CPU budget", "A resident holds no token").
 */
export function readInheritedCpuPool(environment: Readonly<NodeJS.ProcessEnv>): InheritedCpuPool | null {
  const socket = environment[CPU_POOL_ENV];
  const count = environment[CPU_TOKENS_ENV];
  // Zero is valid only as the literal 0 a relay pool names; any other count must be a positive safe integer.
  const tokens = count === '0' ? 0 : parseInheritedTokens(count);
  if (socket === undefined || socket.length === 0 || socket.includes('\0') || tokens === null) {
    return null;
  }
  return { socket, tokens };
}

/**
 * Ask the immediate parent pool for `want` tokens. Neither the inherited count nor the host's core count bounds the
 * ask: a lent pool grants at most what it holds, and a gate's or a resident's relay pool asks the host for each
 * child. A granted lease is the child's local allocation: its connection is the grant, and closing it returns only
 * that allocation. An absent endpoint allows normal host acquisition; a connected endpoint's refusal
 * or protocol failure must fail admission rather than taking a second grant above its parent.
 */
export async function takeParentCpuTokens(parent: InheritedCpuPool, want: number): Promise<TokenGrant> {
  if (!Number.isSafeInteger(want) || want < 1) {
    throw new RangeError(`a child asks its parent pool for a positive count, not ${want} (pool ${parent.tokens})`);
  }
  if (parent.socket.length === 0 || parent.socket.includes('\0')) {
    return {
      granted: false,
      cause: 'other',
      reason: `the parent CPU pool's endpoint ${JSON.stringify(parent.socket)} is not a socket path`,
    };
  }
  const lease = await holdLease(parent.socket, { op: 'cpu-pool', want }, 'the parent CPU pool', {
    ackMs: 2_000,
    afterCloseMs: 5_000,
    grantMs: null,
  });
  if (!lease.granted) {
    return lease;
  }
  const tokens = lease.answer.tokens;
  if (typeof tokens !== 'number' || !Number.isInteger(tokens) || tokens < 1 || tokens > want) {
    lease.release();
    return {
      granted: false,
      cause: 'other',
      reason: `the parent CPU pool granted ${JSON.stringify(tokens)} tokens for ${want}`,
    };
  }
  return { granted: true, tokens, release: lease.release, closed: lease.closed, inherited: true };
}

/** A thrown value's message. */
const describe = (error: unknown): string => (error instanceof Error ? error.message : String(error));

/** Write `count` token bytes to the fifo's descriptor; a short write throws. The fifo holds a pipe buffer of them. */
function writeTokens(fd: number, count: number): void {
  if (count === 0) {
    return;
  }
  const written = writeSync(fd, Buffer.alloc(count, TOKEN));
  if (written !== count) {
    throw new Error(`it took ${written} of ${count} token bytes`);
  }
}

/** Up to `most` token bytes read from the fifo's non-blocking descriptor: 0 when it is empty. */
function readTokens(fd: number, into: Buffer, most: number): number {
  try {
    return readSync(fd, into, 0, Math.min(most, into.length), null);
  } catch (error) {
    const code = error instanceof Error && 'code' in error ? error.code : undefined;
    if (code === 'EAGAIN' || code === 'EWOULDBLOCK') {
      return 0;
    }
    throw error;
  }
}

/**
 * Make the jobserver fifo at `path`, mode 0600, holding `bytes` tokens. Node has no mkfifo, so the system's `mkfifo`
 * makes it. The pool keeps it open read-write and non-blocking: the fifo always has a writer, so its bytes outlive
 * every client that opens and closes it, and the pool's own reads and writes never block.
 */
async function makeJobserverFifo(
  path: string,
  bytes: number,
): Promise<{ ok: true; fd: number } | { ok: false; error: string }> {
  try {
    await run('mkfifo', ['-m', '600', path]);
  } catch (error) {
    return { ok: false, error: `the CPU pool's jobserver fifo could not be made: ${describe(error)}` };
  }
  let fd: number;
  try {
    fd = openSync(path, constants.O_RDWR | constants.O_NONBLOCK);
  } catch (error) {
    return { ok: false, error: `the CPU pool's jobserver fifo could not be opened: ${describe(error)}` };
  }
  try {
    writeTokens(fd, bytes);
  } catch (error) {
    const failure = `the CPU pool's jobserver fifo did not take its ${bytes} tokens: ${describe(error)}`;
    try {
      closeSync(fd);
    } catch (closing) {
      return { ok: false, error: `${failure}; its descriptor did not close: ${describe(closing)}` };
    }
    return { ok: false, error: failure };
  }
  return { ok: true, fd };
}

/** The one Bun API the readability wait uses: a descriptor read as Bun's own poll-driven byte stream. */
interface BunFileStreams {
  file(fd: number): { stream(): ReadableStream<Uint8Array> };
}

function isBunFileStreams(runtime: unknown): runtime is BunFileStreams {
  return typeof runtime === 'object' && runtime !== null && 'file' in runtime && typeof runtime.file === 'function';
}

const bunRuntime: unknown = Reflect.get(globalThis, 'Bun');
/** Bun's file streams when this process runs on Bun, else null (Node). */
const bunFiles: BunFileStreams | null = isBunFileStreams(bunRuntime) ? bunRuntime : null;

/** A wait for the fifo to become readable, until `stop`. */
interface FifoWatch {
  stop(): void;
}

/**
 * Wait, event-driven, for token bytes in the fifo at `path`: no polling loop and no blocking read on the threadpool.
 * A fresh non-blocking read descriptor is watched by the runtime's event loop. Every byte it reads is taken from the
 * fifo and reaches `took` as tokens in hand, also after `stop`; `stop` leaves unread bytes in the fifo.
 *
 * Node watches the descriptor as a libuv pipe stream (`net.Socket({ fd })`). Bun's `node:net` never reads a fifo, and
 * its `tty` and `fs` read streams leave a blocking read behind that takes the next token after they are destroyed
 * (measured on Bun 1.4.3), so on Bun the wait reads through `Bun.file(fd).stream()`, which polls the descriptor and
 * leaves the fifo untouched when cancelled.
 */
function watchFifo(
  path: string,
  took: (bytes: number) => void,
  failed: (error: string) => void,
): { ok: true; watch: FifoWatch } | { ok: false; error: string } {
  let fd: number;
  try {
    fd = openSync(path, constants.O_RDONLY | constants.O_NONBLOCK);
  } catch (error) {
    return { ok: false, error: describe(error) };
  }
  if (bunFiles !== null) {
    const reader = bunFiles.file(fd).stream().getReader();
    let stopped = false;
    const next = (): void => {
      void reader.read().then(
        (chunk) => {
          if (chunk.done) {
            return;
          }
          took(chunk.value.byteLength);
          if (!stopped) next();
        },
        (error: unknown) => {
          if (!stopped) failed(describe(error));
        },
      );
    };
    next();
    const release = (): void => {
      try {
        closeSync(fd);
      } catch (error) {
        failed(`its watch descriptor did not close: ${describe(error)}`);
      }
    };
    return {
      ok: true,
      watch: {
        stop: () => {
          stopped = true;
          void reader.cancel().then(release, release);
        },
      },
    };
  }
  let reader: Socket;
  try {
    reader = new Socket({ fd, readable: true, writable: false });
  } catch (error) {
    closeSync(fd);
    return { ok: false, error: describe(error) };
  }
  reader.on('data', (chunk: Buffer) => took(chunk.length));
  reader.on('error', (error) => failed(error.message));
  // Destroying the stream closes its descriptor and stops its reads at once.
  return { ok: true, watch: { stop: () => reader.destroy() } };
}

/**
 * Make a pool of `tokens` that lives while `closed` does: when `closed` resolves the pool refuses
 * grants, ends its child connections, drains its jobserver fifo and removes its directory. A child's
 * `host-wait` is reported to `waits` for as long as its connection holds it; a sub-pool reports to the
 * same `waits`. Setup failures are values; a non-positive or unsafe count is a programmer bug and throws.
 */
export async function createCpuPool(
  tokens: number,
  closed: Promise<void>,
  waits: HostWaits,
): Promise<{ ok: true; pool: CpuPool } | { ok: false; error: string }> {
  if (!Number.isSafeInteger(tokens) || tokens < 1) {
    throw new RangeError(`a CPU pool holds a positive safe integer of tokens, not ${tokens}`);
  }
  let directory: string;
  try {
    directory = await ownedSocketDirectory(POOL_DIRECTORY, POOL_SOCKET);
  } catch (error) {
    return { ok: false, error: `the CPU pool's directory could not be made: ${describe(error)}` };
  }
  const socketPath = join(directory, POOL_SOCKET);
  const fifoPath = join(directory, POOL_FIFO);
  const fifo = await makeJobserverFifo(fifoPath, tokens - 1);
  if (!fifo.ok) {
    try {
      await rm(directory, { recursive: true, force: true });
    } catch (error) {
      return { ok: false, error: `${fifo.error}; its directory was not removed: ${describe(error)}` };
    }
    return { ok: false, error: fifo.error };
  }
  const jobserver = fifo.fd;
  // The pool reads at most what it holds at once: the fifo carries no more of its tokens than that.
  const scratch = Buffer.allocUnsafe(tokens);
  // The holder's implicit first token, while a socket child holds it.
  let slotLent = false;
  // The readability wait while the queue's head finds no token, else undefined.
  let watch: FifoWatch | undefined;
  let closing = false;
  let listening = false;
  let shut: Promise<CloseOutcome> | undefined;
  // Every failure the pool records, including one its listener reports while it runs. The outcome reads them.
  const failures: string[] = [];
  const queue: Waiter[] = [];
  // Allocated only for children that need the owner to serve their local sub-ledger.
  let subpools: Set<CpuPool> | undefined;
  let creating: Promise<void>[] | undefined;
  const children = new Set<Socket>();
  let resolveStarted!: (failure: Error | null) => void;
  const started = new Promise<Error | null>((done) => {
    resolveStarted = done;
  });

  // A fault in the pool's own fifo, said and recorded; the pool then closes, as on a listener failure.
  const fail = (failure: string): void => {
    failures.push(failure);
    process.stderr.write(`bounded-exec: the CPU pool at ${socketPath} failed: ${failure}\n`);
    closeInBackground();
  };

  const returnTokens = (count: number): void => {
    try {
      writeTokens(jobserver, count);
    } catch (error) {
      fail(`its jobserver fifo did not take back ${count} tokens: ${describe(error)}`);
    }
  };

  const stopWatching = (): void => {
    watch?.stop();
    watch = undefined;
  };

  // The head found no token: wake when a holder outside the pool, such as Cargo, writes one back.
  const watchForTokens = (): void => {
    if (watch !== undefined) {
      return;
    }
    const watching = watchFifo(fifoPath, drain, (error) =>
      fail(`its jobserver fifo failed while a child waited for a token: ${error}`),
    );
    if (watching.ok) {
      watch = watching.watch;
    } else {
      fail(`its jobserver fifo could not be watched for a token: ${watching.error}`);
    }
  };

  // FIFO: the head takes `min(want, available)` as one grant once any token is available, where available is the
  // fifo bytes it can read without blocking plus the implicit slot while no child holds it. `arrived` is bytes the
  // readability wait already took. A waiting child holds nothing, so no child waits while it holds tokens another
  // child needs. Whatever bytes are in hand when no head can use them go straight back to the fifo.
  function drain(arrived: number): void {
    let inHand = arrived;
    while (!closing) {
      const waiter = queue[0];
      if (waiter === undefined) {
        break;
      }
      if (!waiter.child.socket.writable) {
        // A child that half-closed or failed while queued. Its `close` finds it gone from the queue.
        queue.shift();
        continue;
      }
      if (inHand < waiter.want) {
        try {
          inHand += readTokens(jobserver, scratch, waiter.want - inHand);
        } catch (error) {
          fail(`its jobserver fifo could not be read: ${describe(error)}`);
          return;
        }
      }
      const granted = Math.min(waiter.want, inHand + (slotLent ? 0 : 1));
      if (granted === 0) {
        watchForTokens();
        return;
      }
      queue.shift();
      // Fifo bytes first, the implicit slot last.
      const bytes = Math.min(granted, inHand);
      inHand -= bytes;
      const slot = granted > bytes;
      if (slot) slotLent = true;
      waiter.child.phase = { kind: 'holding', bytes, slot };
      if (!waiter.children) {
        waiter.child.socket.write(frame({ ok: true, lease: 'granted', tokens: granted }));
        continue;
      }
      const ended = new Promise<void>((done) => {
        if (waiter.child.socket.destroyed) done();
        else waiter.child.socket.once('close', done);
      });
      const creation = createCpuPool(granted, ended, waits).then(async (made) => {
        if (!made.ok) {
          failures.push(made.error);
          // Keep holding until close credits exactly this allocation once.
          waiter.child.socket.end(frame({ ok: false, code: 'invalid-request', error: made.error }));
          return;
        }
        subpools ??= new Set();
        subpools.add(made.pool);
        if (waiter.child.phase.kind !== 'holding' || closing) {
          const outcome = await made.pool.close();
          if (!outcome.ok) failures.push(outcome.error);
          return;
        }
        waiter.child.socket.write(
          frame({ ok: true, lease: 'granted', tokens: granted, pool: made.pool.environment[CPU_POOL_ENV] }),
        );
      });
      creating ??= [];
      creating.push(creation);
    }
    stopWatching();
    if (!closing) returnTokens(inHand);
  }

  const refuse = (child: Child, error: string): void => {
    child.phase = { kind: 'done' };
    child.socket.end(frame({ ok: false, code: 'invalid-request', error }));
  };

  const serve = (socket: Socket): void => {
    if (closing) {
      socket.destroy();
      return;
    }
    const child: Child = { socket, phase: { kind: 'reading' } };
    children.add(socket);
    // The child's own process reports a socket error. Accounting comes from `close`, the one event that
    // reports every way a socket ends.
    socket.on('error', () => socket.destroy());
    socket.on('close', () => {
      children.delete(socket);
      if (child.phase.kind === 'queued') {
        const at = queue.findIndex((waiter) => waiter.child === child);
        if (at !== -1) {
          queue.splice(at, 1);
        }
      } else if (child.phase.kind === 'holding') {
        // A closing pool drains its fifo whole; until then the bytes go back for the next holder.
        if (child.phase.slot) slotLent = false;
        if (!closing) returnTokens(child.phase.bytes);
      } else if (child.phase.kind === 'waiting') {
        waits.end();
      }
      child.phase = { kind: 'done' };
      drain(0);
    });
    let buffered: Buffer = EMPTY;
    // Refusing releases what the connection buffered: a refused request holds no storage.
    const rejectRequest = (error: string): void => {
      buffered = EMPTY;
      refuse(child, error);
    };
    socket.on('data', (chunk: Buffer) => {
      // A refusal already ended the writable side. Discard remaining request bytes without resetting
      // the socket: an RST here can erase the typed refusal before the peer reads it.
      if (child.phase.kind === 'done') return;
      if (child.phase.kind !== 'reading') {
        // A child that writes past its one request has broken the protocol. Its grant or place in line
        // ends with the connection, and `close` returns whatever it held.
        socket.destroy();
        return;
      }
      // Bound the total before copying: one request is at most MAX_REQUEST_BYTES and its newline, so a
      // larger total is refused without copying any of it, however much one chunk carries.
      if (buffered.length + chunk.length > MAX_REQUEST_BYTES + 1) {
        rejectRequest(TOO_LONG);
        return;
      }
      // The first chunk is the buffer itself: a request that arrives whole is never copied.
      buffered = buffered.length === 0 ? chunk : Buffer.concat([buffered, chunk]);
      const newline = buffered.indexOf(0x0a);
      const lineBytes = newline === -1 ? buffered.length : newline;
      if (lineBytes > MAX_REQUEST_BYTES) {
        rejectRequest(TOO_LONG);
        return;
      }
      if (newline === -1) {
        return;
      }
      if (newline !== buffered.length - 1) {
        rejectRequest('a connection makes one request; this one sent more');
        return;
      }
      const line = buffered.subarray(0, newline).toString('utf8');
      buffered = EMPTY;
      let parsed: unknown;
      try {
        parsed = JSON.parse(line);
      } catch (error) {
        if (!(error instanceof SyntaxError)) throw error;
        parsed = undefined;
      }
      if (!isRequest(parsed)) {
        refuse(
          child,
          'a request is {"op":"cpu-pool","want":<a whole number of tokens, at least 1>} or {"op":"host-wait"}',
        );
        return;
      }
      const request = parsed;
      if (request.op === 'host-wait') {
        // Counted from here until the connection closes, which `close` reports however it ends.
        child.phase = { kind: 'waiting' };
        waits.begin();
        socket.write(frame({ ok: true, wait: 'held' }));
        return;
      }
      child.phase = { kind: 'queued' };
      queue.push({ child, want: request.want, children: request.children === true });
      socket.write(frame({ ok: true, lease: 'queued' }));
      drain(0);
    });
  };

  const server: Server = createServer(serve);

  // Memoized: the first call does the teardown, every later call awaits its outcome. The outcome
  // collects every cleanup failure as text, so the teardown itself never rejects.
  const shutdown = (): Promise<CloseOutcome> => {
    shut ??= (async (): Promise<CloseOutcome> => {
      closing = true;
      stopWatching();
      await started;
      for (const socket of children) {
        socket.destroy();
      }
      if (listening) {
        // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: every accepted socket was destroyed just above, so the server closes without waiting on a peer
        await new Promise<void>((done) => {
          server.close((error) => {
            if (error) {
              failures.push(`its socket did not close: ${error.message}`);
            }
            done();
          });
        });
      }
      if (creating !== undefined) await Promise.all(creating);
      if (subpools !== undefined) {
        for (const pool of subpools) {
          const outcome = await pool.close();
          if (!outcome.ok) failures.push(outcome.error);
        }
      }
      // The fifo's bytes are drained before it goes: a Cargo that outlives the pool keeps its descriptor and the tokens
      // it holds, and takes no more.
      try {
        let drained = readTokens(jobserver, scratch, scratch.length);
        while (drained > 0) drained = readTokens(jobserver, scratch, scratch.length);
      } catch (error) {
        failures.push(`its jobserver fifo was not drained: ${describe(error)}`);
      }
      try {
        closeSync(jobserver);
      } catch (error) {
        failures.push(`its jobserver fifo did not close: ${describe(error)}`);
      }
      try {
        await rm(directory, { recursive: true, force: true });
      } catch (error) {
        failures.push(`its directory was not removed: ${describe(error)}`);
      }
      return failures.length === 0
        ? { ok: true }
        : { ok: false, error: `the CPU pool at ${socketPath} did not close cleanly: ${failures.join('; ')}` };
    })();
    return shut;
  };

  // Upstream loss and a server failure close the pool with no caller to return to. A failed close is
  // written to stderr and stays cached for the owner's own `close()`; the callback never rejects.
  const closeInBackground = (): void => {
    void shutdown().then((outcome) => {
      if (!outcome.ok) {
        process.stderr.write(`bounded-exec: ${outcome.error}\n`);
      }
    });
  };

  void closed.then(closeInBackground);

  server.on('error', (error) => {
    if (!listening) {
      resolveStarted(error);
      return;
    }
    // The failure joins the pool's outcome, so the owner's close reports it as an ordinary failure.
    failures.push(`it failed while listening: ${error.message}`);
    process.stderr.write(`bounded-exec: the CPU pool at ${socketPath} failed: ${error.message}\n`);
    closeInBackground();
  });

  server.listen(socketPath, () => {
    listening = true;
    resolveStarted(null);
  });

  const abort = async (reason: string): Promise<{ ok: false; error: string }> => {
    const outcome = await shutdown();
    return { ok: false, error: outcome.ok ? reason : `${reason}; ${outcome.error}` };
  };

  const failure = await started;
  if (failure !== null) {
    return abort(`the CPU pool could not listen at ${socketPath}: ${failure.message}`);
  }
  if (closing) {
    return abort(CLOSED_WHILE_MADE);
  }
  try {
    // Until this runs the socket carries the umask's mode, which is safe: the 0700 directory above it
    // denies every other user the path.
    await chmod(socketPath, 0o600);
  } catch (error) {
    return abort(`the CPU pool's socket could not be made private: ${describe(error)}`);
  }
  if (closing) {
    return abort(CLOSED_WHILE_MADE);
  }
  // Cargo and make read the fifo from here; `-j` with a jobserver names no count of its own.
  const jobserverFlags = `-j --jobserver-auth=fifo:${fifoPath}`;
  return {
    ok: true,
    pool: {
      environment: {
        [CPU_POOL_ENV]: socketPath,
        [CPU_TOKENS_ENV]: String(tokens),
        CARGO_MAKEFLAGS: jobserverFlags,
        MAKEFLAGS: jobserverFlags,
      },
      close: shutdown,
    },
  };
}
