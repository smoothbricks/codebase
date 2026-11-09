import { type ChildProcessByStdio, spawn } from 'node:child_process';
import { rm } from 'node:fs/promises';
import { createConnection, type Socket } from 'node:net';
import { join } from 'node:path';
import type { Readable, Writable } from 'node:stream';
import { logger } from 'nx/src/devkit-exports.js';
import type { PostTasksExecutionContext, PreTasksExecutionContext } from 'nx/src/project-graph/plugins/public-api.js';
import typia, { type tags } from 'typia';
import { ownedSocketDirectory } from '../../testing.js';
import { CONTROL_MS } from './lease-wait.js';
import { mbxPool } from './mbx-pool.js';

/**
 * mbx's reservation endpoint of one Nx run (specs/cowshed/05_gateway.md, "Parallelism inside a job"). A reserved leaf
 * once ran under its own `mbx reserve`, two mbx processes per task whose dynamic loading alone kept the host's vnode
 * table recycling. With mbx's pool, the run's `preTasksExecution` starts `mbx reservations <socket>` once and publishes
 * the socket in `RESERVATIONS_ENV`, which every task inherits; each task's executor asks it for its leaf's cores over
 * one connection per process (`reserveCores`), and the run's `postTasksExecution` closes the endpoint's standard input,
 * which ends it. Admission is `mbx reserve`'s: the same pool, priority and order. A grant lasts until its leaf ends, or
 * until its connection closes, so an executor that dies gives back what it held.
 */
export const RESERVATIONS_ENV = 'MBX_RESERVATIONS_SOCKET';

/**
 * Why the run serves its tasks no endpoint, published beside {@link RESERVATIONS_ENV} when its endpoint did not start.
 * Every task inherits it, so each reserved leaf's refusal names the cause: a gate's failures-only output shows the
 * leaves' refusals, never the run's own warning.
 */
export const RESERVATIONS_UNSERVED_ENV = 'BOUNDED_EXEC_RESERVATIONS_UNSERVED';

/** What a task's environment says of its run's reservation endpoint. */
export type RunEndpoint =
  | { readonly kind: 'served'; readonly socket: string }
  | { readonly kind: 'unserved'; readonly reason: string }
  | { readonly kind: 'unpublished' };

/** The endpoint `env` names, else why its run serves none, else that its run published neither. */
export function runEndpoint(env: Readonly<NodeJS.ProcessEnv>): RunEndpoint {
  const socket = env[RESERVATIONS_ENV] ?? '';
  if (socket !== '') return { kind: 'served', socket };
  const reason = env[RESERVATIONS_UNSERVED_ENV] ?? '';
  return reason === '' ? { kind: 'unpublished' } : { kind: 'unserved', reason };
}

/** Why a task's run serves it no endpoint, as its reserved leaf's refusal says it. */
export function describeUnserved(endpoint: Exclude<RunEndpoint, { kind: 'served' }>): string {
  return endpoint.kind === 'unserved'
    ? `the run's endpoint did not start (${endpoint.reason})`
    : `the run published neither an endpoint nor why it has none (${RESERVATIONS_UNSERVED_ENV} is unset too), so its preTasksExecution never served this run or its environment never reached this task`;
}

const ENDPOINT_DIRECTORY = 'mbx-r-';
const ENDPOINT_SOCKET = 'r.sock';
/** What is kept of the endpoint's stderr to say why it ended: its last bytes. */
const SAID_BYTES = 4096;

/** A run's endpoint: the `mbx reservations` process, the directory its socket is in, and how it ended once it has. */
interface Endpoint {
  readonly server: ChildProcessByStdio<Writable, Readable, Readable>;
  readonly directory: string;
  readonly ended: Promise<string>;
}

type Started = { ok: true; endpoint: Endpoint; socket: string } | { ok: false; error: string };

/** The endpoints this process serves, by Nx run id, until that run's `postTasksExecution`. */
const endpoints = new Map<string, Endpoint>();

/**
 * Serve `context`'s run an mbx reservation endpoint and publish it in `env`, which every task inherits, when the
 * environment names mbx's pool (`mbxPool`). An endpoint that does not start is said, and each reserved leaf of the run
 * then refuses to run, naming `RESERVATIONS_ENV`; a leaf in no pool, or one mbx admits itself, runs as before.
 */
export async function serveRunReservations(
  context: Pick<PreTasksExecutionContext, 'id'>,
  env: NodeJS.ProcessEnv = process.env,
): Promise<void> {
  if (mbxPool(env) === null) {
    return;
  }
  const started = await startEndpoint(env);
  if (!started.ok) {
    logger.warn(
      `bounded-exec: this run's mbx reservation endpoint did not start (${started.error}); its reserved leaves refuse to run`,
    );
    env[RESERVATIONS_UNSERVED_ENV] = started.error;
    return;
  }
  endpoints.set(context.id, started.endpoint);
  env[RESERVATIONS_ENV] = started.socket;
  delete env[RESERVATIONS_UNSERVED_ENV];
}

/** Close the endpoint `context`'s run was served, if it was served one: its standard input's end ends it. */
export async function closeRunReservations(context: Pick<PostTasksExecutionContext, 'id'>): Promise<void> {
  const endpoint = endpoints.get(context.id);
  if (endpoint === undefined) {
    return;
  }
  endpoints.delete(context.id);
  endpoint.server.stdin.end();
  const ended = await endedWithin(endpoint, CONTROL_MS);
  await rm(endpoint.directory, { recursive: true, force: true });
  if (ended !== 'mbx reservations ended with status 0') {
    logger.warn(`bounded-exec: this run's mbx reservation endpoint did not close cleanly: ${ended}`);
  }
}

/** Start `mbx reservations` by its PATH name in an owned socket directory, and wait until it says it listens. */
async function startEndpoint(env: NodeJS.ProcessEnv): Promise<Started> {
  const directory = await ownedSocketDirectory(ENDPOINT_DIRECTORY, ENDPOINT_SOCKET);
  const socket = join(directory, ENDPOINT_SOCKET);
  const server = spawn('mbx', ['reservations', socket], { env, stdio: ['pipe', 'pipe', 'pipe'] });
  let said = '';
  server.stderr.setEncoding('utf8');
  server.stderr.on('data', (text: string) => {
    said = (said + text).slice(-SAID_BYTES);
  });
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a child that runs ends with its run's postTasksExecution, which ends its standard input and bounds its exit (`endedWithin`); one that cannot run says so at once
  const ended = new Promise<string>((resolve) => {
    server.once('error', (error) => resolve(`mbx reservations could not run: ${error.message}`));
    server.once('close', (code, signal) => {
      const last = said.trim().split('\n').at(-1) ?? '';
      resolve(
        `mbx reservations ended ${signal === null ? `with status ${code}` : `on ${signal}`}${last === '' ? '' : `: ${last}`}`,
      );
    });
  });
  const endpoint: Endpoint = { server, directory, ended };
  const listening = await saysListening(server.stdout, ended);
  if (listening !== null) {
    server.kill('SIGKILL');
    await endedWithin(endpoint, CONTROL_MS);
    await rm(directory, { recursive: true, force: true });
    return { ok: false, error: listening };
  }
  return { ok: true, endpoint, socket };
}

/** Null once the endpoint's first line is `listening`; otherwise why not, within the control-plane bound. */
function saysListening(stdout: Readable, ended: Promise<string>): Promise<string | null> {
  return new Promise<string | null>((resolve) => {
    let text = '';
    const timer = setTimeout(
      () => resolve(`mbx reservations did not say it was listening within ${CONTROL_MS}ms`),
      CONTROL_MS,
    );
    stdout.setEncoding('utf8');
    stdout.on('data', (chunk: string) => {
      text += chunk;
      const end = text.indexOf('\n');
      if (end === -1) return;
      clearTimeout(timer);
      const line = text.slice(0, end);
      resolve(line === 'listening' ? null : `mbx reservations said ${JSON.stringify(line)}, not that it was listening`);
    });
    void ended.then((why) => {
      clearTimeout(timer);
      resolve(why);
    });
  });
}

/** How the endpoint ended, killing it once `ms` pass first. */
async function endedWithin(endpoint: Endpoint, ms: number): Promise<string> {
  let timer: NodeJS.Timeout | undefined;
  const late = new Promise<null>((resolve) => (timer = setTimeout(() => resolve(null), ms)));
  const ended = await Promise.race([endpoint.ended, late]);
  clearTimeout(timer);
  if (ended !== null) {
    return ended;
  }
  endpoint.server.kill('SIGKILL');
  return `${await endpoint.ended}, killed after it outlived its standard input by ${ms}ms`;
}

/** The pool's answer to one reservation: the cores it holds and a promise of why it was lost, or why there are none. */
export type Granted =
  | { readonly ok: true; readonly cpus: number; readonly lost: Promise<string> }
  | { readonly ok: false; readonly error: string };

/** One request for cores on a run's endpoint. */
export interface Reservation {
  /** Settles once the pool grants or refuses it, or its connection ends first. */
  readonly granted: Promise<Granted>;
  /** Gives the grant back, or withdraws the request while it waits. Later calls do nothing. */
  release(): void;
}

type Uint = number & tags.Type<'uint32'>;

/** The lines a client writes (mbx's `docs/scheduling.md`, "Serve reservations to many commands"). */
type ReservationRequest =
  | { type: 'reserve'; id: Uint; cpus: Uint & tags.Minimum<1>; priority: 'normal' | 'low' }
  | { type: 'release'; id: Uint };

/** The lines the endpoint answers; an added field is mbx's to add. */
type ReservationReply =
  | { type: 'granted'; id: Uint; cpus: Uint & tags.Minimum<1> }
  | { type: 'refused'; id: Uint; error: string }
  | { type: 'error'; error: string };

const stringifyRequest = typia.json.createStringify<ReservationRequest>();
const parseReply = typia.json.createValidateParse<ReservationReply>();

/** A request's place on its connection: waiting for its answer, or held until released or lost. */
type Entry =
  | { readonly kind: 'waiting'; readonly answer: (granted: Granted) => void }
  | { readonly kind: 'held'; readonly lose: (why: string) => void };

/**
 * One connection to an endpoint, shared by every reservation this process makes there. It keeps the process alive only
 * while one of them waits or holds.
 */
class Connection {
  readonly #socket: Socket;
  readonly #entries = new Map<number, Entry>();
  #nextId = 1;
  #unread = '';
  #closed: string | null = null;

  constructor(
    private readonly endpoint: string,
    private readonly forget: () => void,
  ) {
    // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a Unix-socket connect completes or fails at once, and a reservation's answer waits as long as the pool's admission holds it, as `mbx reserve`'s did; the connection's end answers every request still waiting
    this.#socket = createConnection(endpoint);
    this.#socket.setEncoding('utf8');
    this.#socket.unref();
    this.#socket.on('data', (text: string) => this.#read(text));
    this.#socket.on('error', (error) => this.#close(`mbx reservation endpoint ${this.endpoint}: ${error.message}`));
    this.#socket.on('close', () => this.#close(`mbx reservation endpoint ${this.endpoint} closed the connection`));
  }

  reserve(cpus: number, priority: 'normal' | 'low'): Reservation {
    if (this.#closed !== null) {
      return { granted: Promise.resolve({ ok: false, error: this.#closed }), release() {} };
    }
    const id = this.#nextId++;
    let answer!: (granted: Granted) => void;
    const granted = new Promise<Granted>((resolve) => (answer = resolve));
    this.#entries.set(id, { kind: 'waiting', answer });
    this.#socket.ref();
    this.#socket.write(`${stringifyRequest({ type: 'reserve', id, cpus, priority })}\n`);
    let released = false;
    return {
      granted,
      release: () => {
        if (released) return;
        released = true;
        const entry = this.#entries.get(id);
        if (entry === undefined) return;
        this.#entries.delete(id);
        if (entry.kind === 'waiting') entry.answer({ ok: false, error: 'released while it waited' });
        this.#socket.write(`${stringifyRequest({ type: 'release', id })}\n`);
        if (this.#entries.size === 0) this.#socket.unref();
      },
    };
  }

  #read(text: string): void {
    this.#unread += text;
    for (let end = this.#unread.indexOf('\n'); end !== -1; end = this.#unread.indexOf('\n')) {
      const line = this.#unread.slice(0, end);
      this.#unread = this.#unread.slice(end + 1);
      const parsed = parseReply(line);
      if (!parsed.success) {
        this.#close(`mbx reservation endpoint ${this.endpoint} answered ${JSON.stringify(line)}, which is no reply`);
        return;
      }
      this.#answer(parsed.data);
      if (this.#closed !== null) return;
    }
  }

  #answer(reply: ReservationReply): void {
    if (reply.type === 'error') {
      this.#close(`mbx reservation endpoint ${this.endpoint} closed this connection: ${reply.error}`);
      return;
    }
    const entry = this.#entries.get(reply.id);
    // Released before its answer arrived: the endpoint drops what it won.
    if (entry === undefined) return;
    if (entry.kind === 'held') {
      this.#close(`mbx reservation endpoint ${this.endpoint} answered reservation ${reply.id} twice`);
      return;
    }
    if (reply.type === 'granted') {
      let lose!: (why: string) => void;
      const lost = new Promise<string>((resolve) => (lose = resolve));
      this.#entries.set(reply.id, { kind: 'held', lose });
      entry.answer({ ok: true, cpus: reply.cpus, lost });
      return;
    }
    this.#entries.delete(reply.id);
    entry.answer({ ok: false, error: reply.error });
    if (this.#entries.size === 0) this.#socket.unref();
  }

  #close(why: string): void {
    if (this.#closed !== null) return;
    this.#closed = why;
    this.forget();
    for (const entry of this.#entries.values()) {
      if (entry.kind === 'waiting') entry.answer({ ok: false, error: why });
      else entry.lose(why);
    }
    this.#entries.clear();
    this.#socket.destroy();
  }
}

/** This process's connection to each endpoint it has asked. */
const connections = new Map<string, Connection>();

/**
 * Ask the endpoint at `socket` for at most `cpus` cores at `priority`, over this process's one connection to it,
 * opened by its first request and kept while another one may come.
 */
export function reserveCores(socket: string, cpus: number, priority: 'normal' | 'low'): Reservation {
  let connection = connections.get(socket);
  if (connection === undefined) {
    const opened: Connection = new Connection(socket, () => {
      if (connections.get(socket) === opened) connections.delete(socket);
    });
    connections.set(socket, opened);
    connection = opened;
  }
  return connection.reserve(cpus, priority);
}
