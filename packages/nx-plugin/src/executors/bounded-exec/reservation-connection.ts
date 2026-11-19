import typia, { type tags } from 'typia';
import type { MbxPriority } from './mbx-pool.js';

/**
 * One process's connection to its run's mbx reservation endpoint (mbx-reservations.ts; specs/cowshed/05_gateway.md,
 * "Parallelism inside a job"), as a state machine over what the process asks of it and what its socket says. Pure: no
 * I/O and no clock. The shell maps its callers' requests and the socket's events into `ConnectionEvent`s and executes
 * the `ConnectionEffect`s each step answers, in their order; it decides nothing.
 *
 * Every request is answered once: granted, refused by the pool, withdrawn by its own release, or failed by the
 * connection's end, which also loses every grant still held. A grant lasts until its release or that end, and the
 * endpoint is told of every release it can still hear. Nothing is written once the connection has ended.
 */
export interface Connection {
  readonly endpoint: string;
  /** What the socket said after its last whole line. */
  readonly unread: string;
  /** Each request neither released nor refused: waiting for its answer, or holding its grant. */
  readonly entries: ReadonlyMap<number, 'waiting' | 'held'>;
  /** Why the connection ended, which answers every later request; null while it serves. */
  readonly closed: ReservationRefusal | null;
}

/** Why a reservation holds no cores: the endpoint asked, and the connect error's code when it could not be reached. */
export interface ReservationRefusal {
  readonly endpoint: string;
  /** The code of the error that ended the connection (`ENOENT`: the endpoint's socket is gone), else null. */
  readonly errno: string | null;
  readonly message: string;
}

/** What the shell observes: its callers' requests, each under an id of its own, and its socket's events. */
export type ConnectionEvent =
  | {
      readonly type: 'reserve';
      readonly id: number;
      readonly cpus: number;
      readonly priority: MbxPriority;
      /** The fewest cores the reservation starts on before mbx's aging bound; one is no minimum. */
      readonly min: number;
      /** The class mbx predicts the reservation by. */
      readonly name?: string | undefined;
    }
  | { readonly type: 'release'; readonly id: number }
  | { readonly type: 'data'; readonly text: string }
  | { readonly type: 'error'; readonly message: string; readonly code: string | null }
  | { readonly type: 'close' };

/** A request's answer: its cores, or why it has none. */
export type Answer =
  | { readonly ok: true; readonly cpus: number }
  | { readonly ok: false; readonly refusal: ReservationRefusal };

export type ConnectionEffect =
  /** Keep the process alive for the connection: a request waits or holds. */
  | { readonly do: 'ref' }
  /** Nothing waits or holds: the connection no longer keeps the process alive. */
  | { readonly do: 'unref' }
  | { readonly do: 'write'; readonly line: string }
  | { readonly do: 'answer'; readonly id: number; readonly answer: Answer }
  /** A grant its request was answered with is lost, and why. */
  | { readonly do: 'lose'; readonly id: number; readonly why: string }
  /** Forget the connection, so the next request opens another, and destroy its socket. */
  | { readonly do: 'hang-up' };

export type ConnectionStep = readonly [Connection, readonly ConnectionEffect[]];

type Uint = number & tags.Type<'uint32'>;

/**
 * The lines a client writes (mbx's `docs/scheduling.md`, "Serve reservations to many commands"). `min` is the fewest
 * of its cores a reservation starts on before it has waited mbx's aging bound; absent, it starts on one.
 */
type ReservationRequest =
  | {
      type: 'reserve';
      id: Uint;
      cpus: Uint & tags.Minimum<1>;
      priority: MbxPriority;
      min?: Uint & tags.Minimum<2>;
      /** The class mbx keeps the reservation's run times under and predicts it by. */
      name?: string;
    }
  | { type: 'release'; id: Uint };

/** The lines the endpoint answers; an added field is mbx's to add. */
type ReservationReply =
  | { type: 'granted'; id: Uint; cpus: Uint & tags.Minimum<1> }
  | { type: 'refused'; id: Uint; error: string }
  | { type: 'error'; error: string };

const stringifyRequest = typia.json.createStringify<ReservationRequest>();
const parseReply = typia.json.createValidateParse<ReservationReply>();

/** A connection just opened to `endpoint`. */
export function opened(endpoint: string): Connection {
  return { endpoint, unread: '', entries: new Map(), closed: null };
}

/** The next state of `connection` at `event`, and what the shell does about it. */
export function connectionStep(connection: Connection, event: ConnectionEvent): ConnectionStep {
  if (connection.closed !== null) {
    // An ended connection answers a request with its end; nothing else of it is left to decide.
    return event.type === 'reserve'
      ? [connection, [{ do: 'answer', id: event.id, answer: { ok: false, refusal: connection.closed } }]]
      : [connection, []];
  }
  switch (event.type) {
    case 'reserve': {
      // A minimum of one is no minimum, and mbx refuses one above the ask.
      const gathers = Math.min(event.min, event.cpus);
      const line = stringifyRequest({
        type: 'reserve',
        id: event.id,
        cpus: event.cpus,
        priority: event.priority,
        ...(gathers > 1 ? { min: gathers } : {}),
        ...(event.name === undefined ? {} : { name: event.name }),
      });
      return [
        { ...connection, entries: new Map(connection.entries).set(event.id, 'waiting') },
        [{ do: 'ref' }, { do: 'write', line: `${line}\n` }],
      ];
    }
    case 'release': {
      const entry = connection.entries.get(event.id);
      // Released, refused or never asked: nothing of it is left to give back.
      if (entry === undefined) return [connection, []];
      const entries = without(connection.entries, event.id);
      return [
        { ...connection, entries },
        [
          ...(entry === 'waiting' ? [refused(connection, event.id, 'released while it waited')] : []),
          { do: 'write', line: `${stringifyRequest({ type: 'release', id: event.id })}\n` },
          ...unreferenced(entries),
        ],
      ];
    }
    case 'data':
      return read({ ...connection, unread: connection.unread + event.text }, []);
    case 'error':
      return close(connection, `mbx reservation endpoint ${connection.endpoint}: ${event.message}`, event.code, []);
    case 'close':
      return close(connection, `mbx reservation endpoint ${connection.endpoint} closed the connection`, null, []);
  }
}

function without(entries: ReadonlyMap<number, 'waiting' | 'held'>, id: number): Map<number, 'waiting' | 'held'> {
  const rest = new Map(entries);
  rest.delete(id);
  return rest;
}

function unreferenced(entries: ReadonlyMap<number, 'waiting' | 'held'>): ConnectionEffect[] {
  return entries.size === 0 ? [{ do: 'unref' }] : [];
}

function refused(connection: Connection, id: number, message: string): ConnectionEffect {
  return { do: 'answer', id, answer: { ok: false, refusal: { endpoint: connection.endpoint, errno: null, message } } };
}

/** `line` as a reply, or null when it is none: typia's parse validates what `JSON.parse` returns and throws what it throws. */
function replyOf(line: string): ReservationReply | null {
  try {
    const parsed = parseReply(line);
    return parsed.success ? parsed.data : null;
  } catch {
    return null;
  }
}
/** Each whole line of `connection.unread` in turn, until none is left or one ends the connection. */
function read(connection: Connection, done: readonly ConnectionEffect[]): ConnectionStep {
  const end = connection.unread.indexOf('\n');
  if (end === -1) return [connection, done];
  const line = connection.unread.slice(0, end);
  const rest: Connection = { ...connection, unread: connection.unread.slice(end + 1) };
  const reply = replyOf(line);
  if (reply === null) {
    const why = `mbx reservation endpoint ${connection.endpoint} answered ${JSON.stringify(line)}, which is no reply`;
    return close(rest, why, null, done);
  }
  const [next, effects] = answer(rest, reply);
  return next.closed === null ? read(next, [...done, ...effects]) : [next, [...done, ...effects]];
}

function answer(connection: Connection, reply: ReservationReply): ConnectionStep {
  const { endpoint } = connection;
  if (reply.type === 'error') {
    return close(connection, `mbx reservation endpoint ${endpoint} closed this connection: ${reply.error}`, null, []);
  }
  const entry = connection.entries.get(reply.id);
  // Released before its answer arrived: the endpoint drops what it won.
  if (entry === undefined) return [connection, []];
  if (entry === 'held') {
    return close(connection, `mbx reservation endpoint ${endpoint} answered reservation ${reply.id} twice`, null, []);
  }
  if (reply.type === 'granted') {
    return [
      { ...connection, entries: new Map(connection.entries).set(reply.id, 'held') },
      [{ do: 'answer', id: reply.id, answer: { ok: true, cpus: reply.cpus } }],
    ];
  }
  const entries = without(connection.entries, reply.id);
  return [{ ...connection, entries }, [refused(connection, reply.id, reply.error), ...unreferenced(entries)]];
}

/** End the connection: what waits is refused by its end, what is held is lost to it. */
function close(
  connection: Connection,
  message: string,
  errno: string | null,
  done: readonly ConnectionEffect[],
): ConnectionStep {
  const refusal: ReservationRefusal = { endpoint: connection.endpoint, errno, message };
  const settled = Array.from(
    connection.entries,
    ([id, entry]): ConnectionEffect =>
      entry === 'waiting' ? { do: 'answer', id, answer: { ok: false, refusal } } : { do: 'lose', id, why: message },
  );
  return [{ ...connection, unread: '', entries: new Map(), closed: refusal }, [...done, ...settled, { do: 'hang-up' }]];
}
