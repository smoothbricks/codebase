import { spawn } from 'node:child_process';
import { rm } from 'node:fs/promises';
import { createConnection, type Socket } from 'node:net';
import { join } from 'node:path';
import { logger } from 'nx/src/devkit-exports.js';
import type { PostTasksExecutionContext, PreTasksExecutionContext } from 'nx/src/project-graph/plugins/public-api.js';
import { ownedSocketDirectory } from '../../testing.js';
import { TEST_GATE_SERVED_ENV } from './case-gate.js';
import { type Ended, type EndpointEffect, type EndpointEvent, endpointStep, spawned } from './endpoint-life.js';
import { type MbxPriority, mbxHold, mbxPool } from './mbx-pool.js';
import {
  type Connection,
  type ConnectionEffect,
  type ConnectionEvent,
  connectionStep,
  opened,
  type ReservationRefusal,
} from './reservation-connection.js';

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

      case 'unref-child': {
        server.unref();
        // Node exposes its pipes as sockets; Bun's readable pipes also unref but are not sockets.
        type Pipe = (typeof server.stdin | typeof server.stdout) & { unref?: () => void };
        const pipes: readonly Pipe[] = [server.stdin, server.stdout, server.stderr];
        for (const pipe of pipes) pipe.unref?.();
        return;
      }
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

/** One request for cores on a run's endpoint. */
export interface Reservation {
  /** Settles once the pool grants or refuses it, or its connection ends first. */
  readonly granted: Promise<Granted>;
  /** Gives the grant back, or withdraws the request while it waits. Later calls do nothing. */
  release(): void;
}

/**
 * The socket of one connection to an endpoint, shared by every reservation this process makes there. It is the shell
 * of that connection's machine (reservation-connection.ts): it feeds its callers' requests and its socket's events to
 * `connectionStep` and executes the effects each step answers, keeping each request's answer and each grant's loss to
 * settle; it decides nothing itself.
 */
class EndpointSocket {
  readonly #socket: Socket;
  readonly #answers = new Map<number, (granted: Granted) => void>();
  readonly #losses = new Map<number, (why: string) => void>();
  #connection: Connection;
  #nextId = 1;

  constructor(
    endpoint: string,
    private readonly forget: () => void,
  ) {
    this.#connection = opened(endpoint);
    // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a Unix-socket connect completes or fails at once, and a reservation's answer waits as long as the pool's admission holds it, as `mbx reserve`'s did; the connection's end answers every request still waiting
    this.#socket = createConnection(endpoint);
    this.#socket.setEncoding('utf8');
    this.#socket.unref();
    this.#socket.on('data', (text: string) => this.#feed({ type: 'data', text }));
    this.#socket.on('error', (error) =>
      this.#feed({
        type: 'error',
        message: error.message,
        code: 'code' in error && typeof error.code === 'string' ? error.code : null,
      }),
    );
    this.#socket.on('close', () => this.#feed({ type: 'close' }));
  }

  reserve(cpus: number, priority: MbxPriority, min: number, name?: string): Reservation {
    const id = this.#nextId++;
    const granted = new Promise<Granted>((resolve) => this.#answers.set(id, resolve));
    this.#feed({ type: 'reserve', id, cpus, priority, min, name });
    return {
      granted,
      release: () => {
        // A grant given back is no longer one to lose.
        this.#losses.delete(id);
        this.#feed({ type: 'release', id });
      },
    };
  }

  #feed(event: ConnectionEvent): void {
    const [next, effects] = connectionStep(this.#connection, event);
    this.#connection = next;
    for (const effect of effects) this.#execute(effect);
  }

  #execute(effect: ConnectionEffect): void {
    switch (effect.do) {
      case 'ref':
        this.#socket.ref();
        return;
      case 'unref':
        this.#socket.unref();
        return;
      case 'write':
        this.#socket.write(effect.line);
        return;
      case 'answer': {
        const settle = this.#answers.get(effect.id);
        this.#answers.delete(effect.id);
        if (!effect.answer.ok) {
          settle?.(effect.answer);
          return;
        }
        const lost = new Promise<string>((resolve) => this.#losses.set(effect.id, resolve));
        settle?.({ ok: true, cpus: effect.answer.cpus, lost });
        return;
      }
      case 'lose':
        this.#losses.get(effect.id)?.(effect.why);
        this.#losses.delete(effect.id);
        return;
      case 'hang-up':
        this.forget();
        this.#socket.destroy();
        return;
    }
  }
}

/** This process's connection to each endpoint it has asked. */
const connections = new Map<string, EndpointSocket>();

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
    const fresh: EndpointSocket = new EndpointSocket(socket, () => {
      if (connections.get(socket) === fresh) connections.delete(socket);
    });
    connections.set(socket, fresh);
    connection = fresh;
  }
  return connection.reserve(cpus, priority, min, name);
}
