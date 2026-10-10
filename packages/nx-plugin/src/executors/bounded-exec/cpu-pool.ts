import { chmod, rm } from 'node:fs/promises';
import { createServer, type Server, type Socket } from 'node:net';
import { join } from 'node:path';
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
 */

/** The environment name a child reads its parent's pool endpoint from; its count is `CPU_TOKENS_ENV`. */
export const CPU_POOL_ENV = 'BOUNDED_EXEC_CPU_POOL';

/** An inherited pool descriptor; only a grant from its endpoint proves a live allocation. */
export interface InheritedCpuPool {
  readonly socket: string;
  readonly tokens: number;
}

/** A running pool: the environment a child inherits to reach it, and how to close it. */
export interface CpuPool {
  /** The endpoint and the count the pool actually holds. */
  readonly environment: Record<string, string>;
  /**
   * Refuses new grants, ends every child connection and removes the pool's directory. The outcome is
   * cached: every call, and an upstream-loss close, returns the first close's outcome. A failed
   * cleanup is an `ok: false` value, never a rejection.
   */
  close(): Promise<CloseOutcome>;
}

type CloseOutcome = { ok: true } | { ok: false; error: string };

/** The one line a child sends: a count of whole tokens, at least one. */
interface CpuPoolRequest {
  op: 'cpu-pool';
  want: number & typia.tags.Type<'uint32'> & typia.tags.Minimum<1>;
}

/** The pool's answers, in `holdLease`'s framing. */
type CpuPoolReply =
  | { ok: true; lease: 'queued' }
  | { ok: true; lease: 'granted'; tokens: number }
  | { ok: false; code: 'invalid-request'; error: string };

const parseRequest = typia.json.createIsParse<CpuPoolRequest>();
const stringifyReply = typia.json.createStringify<CpuPoolReply>();

/** A request over this many bytes is refused as `invalid-request` (specs/cowshed/05_gateway.md, "Wire"). */
const MAX_REQUEST_BYTES = 4096;
/** The refusal for a request too long to be one line, or a chunk that cannot be one. */
const TOO_LONG = `a request is one line of at most ${MAX_REQUEST_BYTES} bytes`;
/** The one empty buffer every connection starts from, so a request never copies a buffer it does not need. */
const EMPTY: Buffer = Buffer.alloc(0);
const POOL_DIRECTORY = 'cpu-pool-';
const POOL_SOCKET = 'p.sock';
const CLOSED_WHILE_MADE = 'the grant that lends this CPU pool closed while the pool was being made';

/** A child's phase. Tokens are owed back only while `holding`; a queued child holds nothing. */
type Phase =
  | { readonly kind: 'reading' }
  | { readonly kind: 'queued' }
  | { readonly kind: 'holding'; readonly tokens: number }
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
}

/** One framed reply line. The newline is the framing; every reply goes through here. */
const frame = (reply: CpuPoolReply): string => `${stringifyReply(reply)}\n`;

/**
 * The inherited descriptor, or null when malformed or absent. A missing or bare count, or an empty
 * or NUL-bearing endpoint, proves no allocation. A valid descriptor still needs a live endpoint grant.
 */
export function readInheritedCpuPool(environment: Readonly<NodeJS.ProcessEnv>): InheritedCpuPool | null {
  const socket = environment[CPU_POOL_ENV];
  const tokens = parseInheritedTokens(environment[CPU_TOKENS_ENV]);
  if (socket === undefined || socket.length === 0 || socket.includes('\0') || tokens === null) {
    return null;
  }
  return { socket, tokens };
}

/**
 * Ask the immediate parent pool for `min(want, parent.tokens, ceil(cores / 2))` tokens. A granted
 * lease is the child's local allocation: its connection is the grant, and closing it returns only
 * that allocation. An absent endpoint allows normal host acquisition; a connected endpoint's refusal
 * or protocol failure must fail admission rather than taking a second grant above its parent.
 */
export async function takeParentCpuTokens(parent: InheritedCpuPool, want: number, cores: number): Promise<TokenGrant> {
  const ask = Math.min(want, parent.tokens, Math.ceil(cores / 2));
  if (!Number.isSafeInteger(ask) || ask < 1) {
    throw new RangeError(
      `a child asks its parent pool for a positive count, not ${ask} (want ${want}, pool ${parent.tokens}, cores ${cores})`,
    );
  }
  if (parent.socket.length === 0 || parent.socket.includes('\0')) {
    return {
      granted: false,
      cause: 'other',
      reason: `the parent CPU pool's endpoint ${JSON.stringify(parent.socket)} is not a socket path`,
    };
  }
  const lease = await holdLease(parent.socket, { op: 'cpu-pool', want: ask }, 'the parent CPU pool', {
    ackMs: 2_000,
    afterCloseMs: 5_000,
    grantMs: null,
  });
  if (!lease.granted) {
    return lease;
  }
  const tokens = lease.answer.tokens;
  if (typeof tokens !== 'number' || !Number.isInteger(tokens) || tokens < 1 || tokens > ask) {
    lease.release();
    return {
      granted: false,
      cause: 'other',
      reason: `the parent CPU pool granted ${JSON.stringify(tokens)} tokens for ${ask}`,
    };
  }
  return { granted: true, tokens, release: lease.release, closed: lease.closed, inherited: true };
}

/**
 * Make a pool of `tokens` that lives while `closed` does: when `closed` resolves the pool refuses
 * grants, ends its child connections and removes its directory. Setup failures are values; a
 * non-positive or unsafe count is a programmer bug and throws.
 */
export async function createCpuPool(
  tokens: number,
  closed: Promise<void>,
): Promise<{ ok: true; pool: CpuPool } | { ok: false; error: string }> {
  if (!Number.isSafeInteger(tokens) || tokens < 1) {
    throw new RangeError(`a CPU pool holds a positive safe integer of tokens, not ${tokens}`);
  }
  let directory: string;
  try {
    directory = await ownedSocketDirectory(POOL_DIRECTORY, POOL_SOCKET);
  } catch (error) {
    return {
      ok: false,
      error: `the CPU pool's directory could not be made: ${error instanceof Error ? error.message : String(error)}`,
    };
  }
  const socketPath = join(directory, POOL_SOCKET);
  let free = tokens;
  let closing = false;
  let listening = false;
  let shut: Promise<CloseOutcome> | undefined;
  // Every failure the pool records, including one its listener reports while it runs. The outcome reads them.
  const failures: string[] = [];
  const queue: Waiter[] = [];
  const children = new Set<Socket>();
  let resolveStarted!: (failure: Error | null) => void;
  const started = new Promise<Error | null>((done) => {
    resolveStarted = done;
  });

  // FIFO: the head takes `min(want, free)` as one grant once any token is free. A waiting child holds
  // nothing, so no child waits while it holds tokens another child needs.
  const drain = (): void => {
    while (!closing && free > 0) {
      const waiter = queue.shift();
      if (waiter === undefined) {
        return;
      }
      if (!waiter.child.socket.writable) {
        // A child that half-closed or failed while queued. Its `close` finds it gone from the queue.
        continue;
      }
      const granted = Math.min(waiter.want, free);
      free -= granted;
      waiter.child.phase = { kind: 'holding', tokens: granted };
      waiter.child.socket.write(frame({ ok: true, lease: 'granted', tokens: granted }));
    }
  };

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
        free += child.phase.tokens;
      }
      child.phase = { kind: 'done' };
      drain();
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
      let request: ReturnType<typeof parseRequest>;
      try {
        request = parseRequest(line);
      } catch (error) {
        // isParse returns null for a wrong shape, but JSON syntax errors still throw.
        if (!(error instanceof SyntaxError)) throw error;
        request = null;
      }
      if (request === null) {
        refuse(child, 'a request is {"op":"cpu-pool","want":<a whole number of tokens, at least 1>}');
        return;
      }
      child.phase = { kind: 'queued' };
      queue.push({ child, want: request.want });
      socket.write(frame({ ok: true, lease: 'queued' }));
      drain();
    });
  };

  const server: Server = createServer(serve);

  // Memoized: the first call does the teardown, every later call awaits its outcome. The outcome
  // collects every cleanup failure as text, so the teardown itself never rejects.
  const shutdown = (): Promise<CloseOutcome> => {
    shut ??= (async (): Promise<CloseOutcome> => {
      closing = true;
      await started;
      for (const socket of children) {
        socket.destroy();
      }
      if (listening) {
        await new Promise<void>((done) => {
          server.close((error) => {
            if (error) {
              failures.push(`its socket did not close: ${error.message}`);
            }
            done();
          });
        });
      }
      try {
        await rm(directory, { recursive: true, force: true });
      } catch (error) {
        failures.push(`its directory was not removed: ${error instanceof Error ? error.message : String(error)}`);
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
    return abort(
      `the CPU pool's socket could not be made private: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  if (closing) {
    return abort(CLOSED_WHILE_MADE);
  }
  return {
    ok: true,
    pool: {
      environment: { [CPU_POOL_ENV]: socketPath, [CPU_TOKENS_ENV]: String(tokens) },
      close: shutdown,
    },
  };
}
