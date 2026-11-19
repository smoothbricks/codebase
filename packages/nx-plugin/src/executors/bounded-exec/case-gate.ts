import { rm } from 'node:fs/promises';
import { createServer, type Server, type Socket } from 'node:net';
import { join } from 'node:path';
import typia from 'typia';
import { ownedSocketDirectory } from '../../testing.js';

/**
 * A reserved nextest leaf's case gate (specs/cowshed/05_gateway.md, "A case starts on a core"). The leaf's runner
 * starts every thread of its ask, and each case takes a core before it runs: one of the leaf's grant when one is free,
 * else one reserved for that case alone, which the leaf's executor asks its run's endpoint for and gives back when the
 * case ends. A grant never grows, so this is how the cores other work frees after the leaf's grant reach it.
 *
 * The gate is a Unix stream socket the executor serves for one run and names in the command's environment as
 * {@link TEST_GATE_ENV}. mbx's test shim, through which nextest starts every case, connects before the case starts,
 * writes one line ({@link CaseLine}), starts the case when the line {@link GO} answers, and keeps the connection until
 * the case has exited: the connection's end is the case's end.
 */
export const TEST_GATE_ENV = 'MBX_TEST_GATE';
/**
 * Set to `1` in a run's environment once its endpoint answered that the installed mbx's shim gates its cases
 * (endpoint-life.ts, `gatesTests`): without it a leaf's runner is sized to its grant.
 */
export const TEST_GATE_SERVED_ENV = 'BOUNDED_EXEC_MBX_TEST_GATE';
/** The line that starts a case. */
export const GO = 'go';

/** The line a case's shim writes: nextest's binary id and test name, which together are the case's result id. */
interface CaseLine {
  binary: string;
  test: string;
}
const parseCaseLine = typia.json.createValidateParse<CaseLine>();

/** A case's id as its result names it (progress.ts): the binary id, `::`, the test. */
export function caseId(binary: string, test: string): string {
  return `${binary}::${test}`;
}

/** What the gate does outside itself. */
export interface CaseGateEffects {
  /** Start the case: answer its connection. */
  readonly start: (id: string) => void;
  /** Ask the pool for one core in the case's name. */
  readonly reserve: (id: string) => void;
  /** Give the case's core back to the pool, or withdraw its request while it waits. */
  readonly release: (id: string) => void;
}

/** What a leaf's cases did at its gate. */
export interface CaseGateSummary {
  /** Cases that ran on the leaf's grant, and on a core of their own. */
  readonly own: number;
  readonly lent: number;
  /** How long the cases on a core of their own held it, summed. */
  readonly lentCoreMs: number;
  /** How long the cases waited at the gate, summed. */
  readonly waitedCaseMs: number;
}

type CaseState =
  | { readonly kind: 'waiting'; readonly since: number }
  | { readonly kind: 'own' }
  | { readonly kind: 'lent'; readonly since: number };

/**
 * The gate's decisions, as a state machine over three events: a case arrived, the pool granted a case's request, a
 * case ended. `own` is the leaf's granted cores; `wall` a case's last recorded wall, by which a freed core of the
 * grant picks the longest waiting case, so the cores the pool gives go to the shorter ones and come back soonest.
 */
export class CaseGate {
  readonly #cases = new Map<string, CaseState>();
  readonly #waited = new Map<string, number>();
  #ownRunning = 0;
  #own = 0;
  #lent = 0;
  #lentCoreMs = 0;
  #waitedCaseMs = 0;

  constructor(
    private readonly own: number,
    private readonly wall: (id: string) => number,
    private readonly effects: CaseGateEffects,
    private readonly now: () => number = Date.now,
  ) {}

  /** A case's shim connected. False for a case already at the gate, which its second connection may not join. */
  arrived(id: string): boolean {
    if (this.#cases.has(id)) return false;
    if (this.#ownRunning < this.own) {
      this.#startOwn(id, 0);
      return true;
    }
    this.#cases.set(id, { kind: 'waiting', since: this.now() });
    this.effects.reserve(id);
    return true;
  }

  /** The pool granted the case's request. One whose case no longer waits gives the core straight back. */
  granted(id: string): void {
    const state = this.#cases.get(id);
    if (state?.kind !== 'waiting') {
      this.effects.release(id);
      return;
    }
    const at = this.now();
    this.#startedAfter(id, at - state.since);
    this.#cases.set(id, { kind: 'lent', since: at });
    this.#lent += 1;
    this.effects.start(id);
  }

  /** A case's connection ended: its core goes back where it came from, a waiting case's request is withdrawn. */
  ended(id: string): void {
    const state = this.#cases.get(id);
    if (state === undefined) return;
    this.#cases.delete(id);
    switch (state.kind) {
      case 'waiting':
        this.effects.release(id);
        return;
      case 'lent':
        this.#lentCoreMs += this.now() - state.since;
        this.effects.release(id);
        return;
      case 'own': {
        this.#ownRunning -= 1;
        let longest: { id: string; since: number } | null = null;
        for (const [waiting, other] of this.#cases) {
          if (other.kind !== 'waiting') continue;
          if (longest === null || this.wall(waiting) > this.wall(longest.id)) {
            longest = { id: waiting, since: other.since };
          }
        }
        if (longest === null) return;
        this.effects.release(longest.id);
        this.#startOwn(longest.id, this.now() - longest.since);
        return;
      }
    }
  }

  /** How long the case waited at the gate before it started, in ms; zero for one that never did. */
  waitedMs(id: string): number {
    return this.#waited.get(id) ?? 0;
  }

  summary(): CaseGateSummary {
    return { own: this.#own, lent: this.#lent, lentCoreMs: this.#lentCoreMs, waitedCaseMs: this.#waitedCaseMs };
  }

  #startOwn(id: string, waitedMs: number): void {
    this.#startedAfter(id, waitedMs);
    this.#cases.set(id, { kind: 'own' });
    this.#ownRunning += 1;
    this.#own += 1;
    this.effects.start(id);
  }

  #startedAfter(id: string, waitedMs: number): void {
    this.#waited.set(id, waitedMs);
    this.#waitedCaseMs += waitedMs;
  }
}

/** One core asked of a pool for one case: whether the pool granted it, and how it is given back or withdrawn. */
export interface CaseCore {
  /** Settles once the pool grants the core (true), or refuses, loses or is withdrawn the request (false). */
  readonly granted: Promise<boolean>;
  /** Gives the core back, or withdraws the request while it waits. Later calls do nothing. */
  release(): void;
}

/** A leaf's gate decisions and the cores its waiting cases asked their pool for. */
export interface GatedCases {
  readonly cases: CaseGate;
  /** Gives back every core a case still holds and withdraws every request that waits. */
  releaseAll(): void;
}

/**
 * The gate of a leaf that holds `own` cores, over whichever pool `ask` asks: mbx's, through the run's reservation
 * endpoint, or the run's own token pool (cpu-pool.ts, `lendParentToken`). `start` answers a case's connection.
 */
export function gateCases(
  own: number,
  wall: (id: string) => number,
  start: (id: string) => void,
  ask: (id: string) => CaseCore,
): GatedCases {
  const requests = new Map<string, CaseCore>();
  const cases: CaseGate = new CaseGate(own, wall, {
    start,
    reserve: (id) => {
      const held = ask(id);
      requests.set(id, held);
      void held.granted.then((granted) => {
        // Released since: the case started on a core of the grant, or ended while it waited.
        if (requests.get(id) !== held) return;
        if (granted) cases.granted(id);
        else requests.delete(id);
      });
    },
    release: (id) => {
      requests.get(id)?.release();
      requests.delete(id);
    },
  });
  return {
    cases,
    releaseAll: () => {
      for (const held of requests.values()) held.release();
      requests.clear();
    },
  };
}

/**
 * A case's wall as its result records it: nextest times a case from the shim's start, so its own duration includes the
 * wait at the gate, which is taken off. Never below zero.
 */
export function runWallMs(nextestMs: number, waitedMs: number): number {
  return Math.max(0, nextestMs - waitedMs);
}

const GATE_DIRECTORY = 'bounded-gate-';
const GATE_SOCKET = 'g.sock';
/** No case line is longer: a binary id and a test name. */
const LINE_BYTES = 16 * 1024;

export interface CaseGateServer {
  /** The socket's path, as the command's environment names it. */
  readonly socket: string;
  /** Answer the case's connection: it starts. */
  start(id: string): void;
  /** Ends every connection and removes the socket's directory; the error that left something behind, or null. */
  close(): Promise<string | null>;
}

export interface CaseGateHandlers {
  /** A case's shim connected and named it. False refuses the connection. */
  readonly arrived: (id: string) => boolean;
  /** A named case's connection ended. */
  readonly ended: (id: string) => void;
  /** A client wrote something that is no case line, or named a case already at the gate; its connection is ended. */
  readonly invalid: (detail: string) => void;
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** Serve one run's case gate; a gate that cannot be served is the run's refusal. */
export async function serveCaseGate(
  handlers: CaseGateHandlers,
): Promise<{ ok: true; gate: CaseGateServer } | { ok: false; error: string }> {
  let directory: string;
  try {
    directory = await ownedSocketDirectory(GATE_DIRECTORY, GATE_SOCKET);
  } catch (error) {
    return { ok: false, error: `its directory could not be made: ${describe(error)}` };
  }
  const socketPath = join(directory, GATE_SOCKET);
  const clients = new Set<Socket>();
  const named = new Map<string, Socket>();
  const serve = (client: Socket): void => {
    clients.add(client);
    // A case that is killed loses its connection with its process: that end is the case's end, read below.
    client.on('error', () => {});
    client.setEncoding('utf8');
    let partial = '';
    let id: string | null = null;
    const refuse = (detail: string): void => {
      client.destroy();
      handlers.invalid(detail);
    };
    client.on('data', (text: string) => {
      if (id !== null) {
        refuse(`case ${id} wrote a second line at its gate`);
        return;
      }
      partial += text;
      const end = partial.indexOf('\n');
      if (end === -1) {
        if (partial.length > LINE_BYTES) refuse(`a case line is at most ${LINE_BYTES} bytes`);
        return;
      }
      let parsed: ReturnType<typeof parseCaseLine> | null;
      try {
        parsed = parseCaseLine(partial.slice(0, end));
      } catch {
        parsed = null;
      }
      if (parsed === null || !parsed.success || partial.length > end + 1) {
        refuse(`a case line names a binary and a test, one line; got ${JSON.stringify(partial.slice(0, 200))}`);
        return;
      }
      const arriving = caseId(parsed.data.binary, parsed.data.test);
      named.set(arriving, client);
      if (!handlers.arrived(arriving)) {
        named.delete(arriving);
        refuse(`case ${arriving} is already at its gate`);
        return;
      }
      id = arriving;
    });
    client.on('close', () => {
      clients.delete(client);
      if (id === null) return;
      if (named.get(id) === client) named.delete(id);
      handlers.ended(id);
    });
  };
  const server: Server = createServer(serve);
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: listen is this process's own bind of a path in its fresh directory; the kernel answers it with the listening callback or an error, and no peer takes part
  const listened = await new Promise<Error | null>((settle) => {
    server.once('error', settle);
    server.listen(socketPath, () => {
      server.off('error', settle);
      settle(null);
    });
  });
  const close = async (): Promise<string | null> => {
    const failures: string[] = [];
    for (const client of clients) client.destroy();
    if (listened === null) {
      // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: every accepted connection was destroyed just above, so the server closes without waiting on a peer
      const closed = await new Promise<Error | undefined>((settle) => server.close(settle));
      if (closed !== undefined) failures.push(`its socket did not close: ${closed.message}`);
    }
    try {
      await rm(directory, { recursive: true, force: true });
    } catch (error) {
      failures.push(`its directory was not removed: ${describe(error)}`);
    }
    return failures.length === 0 ? null : `the case gate at ${socketPath}: ${failures.join('; ')}`;
  };
  if (listened !== null) {
    const left = await close();
    return {
      ok: false,
      error: `its socket ${socketPath} could not be bound: ${listened.message}${left === null ? '' : `; ${left}`}`,
    };
  }
  return {
    ok: true,
    gate: {
      socket: socketPath,
      start: (id) => {
        named.get(id)?.write(`${GO}\n`);
      },
      close,
    },
  };
}
