import { spawn } from 'node:child_process';
import { rm } from 'node:fs/promises';
import { createConnection, type Socket } from 'node:net';
import { join } from 'node:path';
import { logger } from 'nx/src/devkit-exports.js';
import type { PostTasksExecutionContext, PreTasksExecutionContext } from 'nx/src/project-graph/plugins/public-api.js';
import typia, { type tags } from 'typia';
import { ownedSocketDirectory } from '../../testing.js';
import { TEST_GATE_SERVED_ENV } from './case-gate.js';
import { type Ended, type EndpointEffect, type EndpointEvent, endpointStep, spawned } from './endpoint-life.js';
import { type MbxPriority, mbxHold, mbxPool } from './mbx-pool.js';

/**
 * mbx's reservation endpoint of one Nx run (specs/cowshed/05_gateway.md, "Parallelism inside a job"). A reserved leaf
 * once ran under its own `mbx reserve`, two mbx processes per task whose dynamic loading alone kept the host's vnode
 * table recycling. With mbx's pool, the run's `preTasksExecution` starts `mbx reservations <socket>` once and publishes
 * the socket in `RESERVATIONS_ENV`, which every task inherits; each task's executor asks it for its leaf's cores over
 * one connection per process (`reserveCores`), and the run's `postTasksExecution` closes the endpoint's standard input,
 * which ends it. Nx runs both hooks in the process that runs the tasks, never in a daemon (patches/README.md, "A run's
 * task hooks run in its own process"), so the endpoint is a child of the run and ends with it, whatever becomes of the
 * run's daemon. Admission is `mbx reserve`'s: the same pool, priority and order. A grant lasts until its leaf ends, or
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

/** A run's endpoint once it serves. */
interface Endpoint {
  /** The run's end, which ends the endpoint: how it ended, once its socket's directory is removed. */
  end(): Promise<Ended>;
}

type Started =
  | { readonly ok: true; readonly endpoint: Endpoint; readonly socket: string; readonly gates: boolean }
  | { readonly ok: false; readonly error: string };

/** The endpoints this process serves, by Nx run id, until that run's `postTasksExecution`. */
const endpoints = new Map<string, Endpoint>();

/**
 * Serve `context`'s run an mbx reservation endpoint and publish it in `env`, which every task inherits, when the
 * environment names mbx's pool (`mbxPool`). An endpoint that does not start is said, and each reserved leaf of the run
 * then refuses to run, naming `RESERVATIONS_ENV`; a leaf in no pool, or one mbx admits itself, runs as before. A run
 * under a hold on mbx's pool is served no endpoint, and learns whether its mbx gates test cases (`learnHeldGate`).
 */
export async function serveRunReservations(
  context: Pick<PreTasksExecutionContext, 'id'>,
  env: NodeJS.ProcessEnv = process.env,
): Promise<void> {
  if (mbxPool(env) === null) {
    if (mbxHold(env)) await learnHeldGate(env);
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
  // A leaf's runner starts every thread of its ask only where the installed mbx's shim takes each case through its gate.
  if (started.gates) env[TEST_GATE_SERVED_ENV] = '1';
  else delete env[TEST_GATE_SERVED_ENV];
}

/**
 * A run under a hold on mbx's pool reserves nothing and serves its own token pool (run-cpu-pool.ts), and its nextest
 * leaves still start every case through mbx's shim. Whether that shim gates is the endpoint's own answer, so the run
 * starts the endpoint once, outside the hold's `MBX_SCHED_DISABLE` and `MBX_SCHED_DIR`, which it refuses to serve
 * under, asks, and ends it; no request is made of it (specs/cowshed/05_gateway.md, "A case starts on a core", Inside a
 * hold). An mbx that does not start it, or does not answer, gates nothing: the run's leaves are sized to their grants.
 */
async function learnHeldGate(env: NodeJS.ProcessEnv): Promise<void> {
  const unheld = { ...env };
  delete unheld.MBX_SCHED_DISABLE;
  delete unheld.MBX_SCHED_DIR;
  const started = await startEndpoint(unheld);
  if (!started.ok) {
    logger.warn(
      `bounded-exec: the installed mbx did not say whether its test shim gates cases (${started.error}); this run's nextest leaves are sized to their grants`,
    );
    delete env[TEST_GATE_SERVED_ENV];
    return;
  }
  await started.endpoint.end();
  if (started.gates) env[TEST_GATE_SERVED_ENV] = '1';
  else delete env[TEST_GATE_SERVED_ENV];
}

/** Close the endpoint `context`'s run was served, if it was served one: its standard input's end ends it. */
export async function closeRunReservations(context: Pick<PostTasksExecutionContext, 'id'>): Promise<void> {
  const endpoint = endpoints.get(context.id);
  if (endpoint === undefined) {
    return;
  }
  endpoints.delete(context.id);
  const ended = await endpoint.end();
  if (!ended.clean) {
    logger.warn(`bounded-exec: this run's mbx reservation endpoint did not close cleanly: ${ended.how}`);
  }
}

/**
 * Start `mbx reservations` by its PATH name in an owned socket directory, and answer once its life answers its start
 * (endpoint-life.ts). This is that machine's shell: it feeds the child's and the probe's events to `endpointStep` and
 * executes the effects each step answers, and decides nothing itself.
 */
async function startEndpoint(env: NodeJS.ProcessEnv): Promise<Started> {
  const directory = await ownedSocketDirectory(ENDPOINT_DIRECTORY, ENDPOINT_SOCKET);
  const socket = join(directory, ENDPOINT_SOCKET);
  const server = spawn('mbx', ['reservations', socket], { env, stdio: ['pipe', 'pipe', 'pipe'] });
  const remove = (): Promise<void> => rm(directory, { recursive: true, force: true });
  let life = spawned(server.pid);
  let probe: Socket | undefined;
  let timer: NodeJS.Timeout | undefined;
  // `Promise.withResolvers` would read better but needs lib es2024; this package inherits lib es2022.
  let answer!: (started: Started) => void;
  let conclude!: (ended: Ended) => void;
  let fail!: (error: unknown) => void;
  let failEnd!: (error: unknown) => void;
  const started = new Promise<Started>((resolve, reject) => {
    answer = resolve;
    fail = reject;
  });
  const ended = new Promise<Ended>((resolve, reject) => {
    conclude = resolve;
    failEnd = reject;
  });
  const endpoint: Endpoint = {
    end() {
      feed({ type: 'run-end' });
      return ended;
    },
  };
  const execute = (effect: EndpointEffect): void => {
    switch (effect.do) {
      case 'probe': {
        // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a Unix-socket connect completes or fails at once, and the probe ends at its answer, its connection's end or the child's end (endpoint-life.ts), which the endpoint's death brings: it is this run's child
        const connection = createConnection(socket);
        probe = connection;
        connection.setEncoding('utf8');
        connection.on('connect', () => connection.write('{"type":"capabilities"}\n'));
        connection.on('data', (text: string) => feed({ type: 'probe', text }));
        connection.on('error', () => feed({ type: 'probe-end' }));
        connection.on('close', () => feed({ type: 'probe-end' }));
        return;
      }
      case 'hang-up':
        probe?.destroy();
        return;
      case 'arm':
        timer = setTimeout(() => feed({ type: 'expired' }), effect.ms);
        return;
      case 'disarm':
        clearTimeout(timer);
        return;
      case 'kill':
        server.kill('SIGKILL');
        return;
      case 'close-stdin':
        server.stdin.end();
        return;
      case 'serve':
        answer({ ok: true, endpoint, socket, gates: effect.gates });
        return;
      case 'refuse':
        void remove().then(() => answer({ ok: false, error: effect.why }), fail);
        return;
      case 'conclude':
        void remove().then(() => conclude(effect.ended), failEnd);
        return;
    }
  };
  const feed = (event: EndpointEvent): void => {
    const [next, effects] = endpointStep(life, event);
    life = next;
    for (const effect of effects) execute(effect);
  };
  server.stdout.setEncoding('utf8');
  server.stdout.on('data', (text: string) => feed({ type: 'stdout', text }));
  server.stdout.on('end', () => feed({ type: 'stdout-end' }));
  server.stderr.setEncoding('utf8');
  server.stderr.on('data', (text: string) => feed({ type: 'stderr', text }));
  server.once('error', (error) => feed({ type: 'error', message: error.message }));
  server.once('close', (code, signal) => feed({ type: 'close', code, signal }));
  return started;
}

/** The pool's answer to one reservation: the cores it holds and a promise of why it was lost, or why there are none. */
export type Granted =
  | { readonly ok: true; readonly cpus: number; readonly lost: Promise<string> }
  | { readonly ok: false; readonly refusal: ReservationRefusal };

/** Why a reservation holds no cores: the endpoint asked, and the connect error's code when it could not be reached. */
export interface ReservationRefusal {
  readonly endpoint: string;
  /** The code of the error that ended the connection (`ENOENT`: the endpoint's socket is gone), else null. */
  readonly errno: string | null;
  readonly message: string;
}

/** One request for cores on a run's endpoint. */
export interface Reservation {
  /** Settles once the pool grants or refuses it, or its connection ends first. */
  readonly granted: Promise<Granted>;
  /** Gives the grant back, or withdraws the request while it waits. Later calls do nothing. */
  release(): void;
}

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
  #closed: ReservationRefusal | null = null;

  constructor(
    private readonly endpoint: string,
    private readonly forget: () => void,
  ) {
    // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a Unix-socket connect completes or fails at once, and a reservation's answer waits as long as the pool's admission holds it, as `mbx reserve`'s did; the connection's end answers every request still waiting
    this.#socket = createConnection(endpoint);
    this.#socket.setEncoding('utf8');
    this.#socket.unref();
    this.#socket.on('data', (text: string) => this.#read(text));
    this.#socket.on('error', (error) =>
      this.#close(
        `mbx reservation endpoint ${this.endpoint}: ${error.message}`,
        'code' in error && typeof error.code === 'string' ? error.code : null,
      ),
    );
    this.#socket.on('close', () => this.#close(`mbx reservation endpoint ${this.endpoint} closed the connection`));
  }

  reserve(cpus: number, priority: MbxPriority, min: number, name?: string): Reservation {
    if (this.#closed !== null) {
      return { granted: Promise.resolve({ ok: false, refusal: this.#closed }), release() {} };
    }
    const id = this.#nextId++;
    let answer!: (granted: Granted) => void;
    const granted = new Promise<Granted>((resolve) => (answer = resolve));
    this.#entries.set(id, { kind: 'waiting', answer });
    this.#socket.ref();
    // A minimum of one is no minimum, and mbx refuses one above the ask.
    const gathers = Math.min(min, cpus);
    this.#socket.write(
      `${stringifyRequest({ type: 'reserve', id, cpus, priority, ...(gathers > 1 ? { min: gathers } : {}), ...(name === undefined ? {} : { name }) })}\n`,
    );
    let released = false;
    return {
      granted,
      release: () => {
        if (released) return;
        released = true;
        const entry = this.#entries.get(id);
        if (entry === undefined) return;
        this.#entries.delete(id);
        if (entry.kind === 'waiting')
          entry.answer({ ok: false, refusal: this.#refusal('released while it waited', null) });
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
    entry.answer({ ok: false, refusal: this.#refusal(reply.error, null) });
    if (this.#entries.size === 0) this.#socket.unref();
  }

  #refusal(message: string, errno: string | null): ReservationRefusal {
    return { endpoint: this.endpoint, errno, message };
  }

  #close(why: string, errno: string | null = null): void {
    if (this.#closed !== null) return;
    const refusal = this.#refusal(why, errno);
    this.#closed = refusal;
    this.forget();
    for (const entry of this.#entries.values()) {
      if (entry.kind === 'waiting') entry.answer({ ok: false, refusal });
      else entry.lose(refusal.message);
    }
    this.#entries.clear();
    this.#socket.destroy();
  }
}

/** This process's connection to each endpoint it has asked. */
const connections = new Map<string, Connection>();

/**
 * Ask the endpoint at `socket` for at most `cpus` cores at `priority`, starting on no fewer than `min` of them until
 * mbx's aging bound, over this process's one connection to it, opened by its first request and kept while another one
 * may come. `name` is the class mbx predicts the reservation by; absent, mbx's own for an unnamed one.
 */
export function reserveCores(
  socket: string,
  cpus: number,
  priority: MbxPriority,
  min: number,
  name?: string,
): Reservation {
  let connection = connections.get(socket);
  if (connection === undefined) {
    const opened: Connection = new Connection(socket, () => {
      if (connections.get(socket) === opened) connections.delete(socket);
    });
    connections.set(socket, opened);
    connection = opened;
  }
  return connection.reserve(cpus, priority, min, name);
}
