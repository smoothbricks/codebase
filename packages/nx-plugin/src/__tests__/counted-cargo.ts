import { expect } from 'bun:test';
import { subscribe, unsubscribe } from 'node:diagnostics_channel';
import { realpathSync } from 'node:fs';
import { chmod, mkdir, readFile, writeFile } from 'node:fs/promises';
import { createServer, type Server, type Socket } from 'node:net';
import { delimiter, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import type { LogSchema, OpContextBinding } from '@smoothbricks/lmao';
import { type QueryableSpan, querySpan } from '@smoothbricks/lmao/testing';
import { useTestSpan } from '@smoothbricks/lmao/testing/bun';
import { AggregateCreateNodesError } from 'nx/src/project-graph/error-types.js';
import { CargoMetadataError } from '../cargo-source-hash.js';
import { BROKER_HOST, encodeOrder, type ProcessReport, parseReport, readLines } from './counted-cargo-process.js';

/** What one fixture's `cargo metadata` children have done so far, in the order they did it. */
export interface CargoMetadataRuns {
  /** `cargo metadata` children started. */
  readonly spawned: number;
  /** Those whose Cargo has read the workspace, whether or not the answer has been handed over yet. */
  readonly read: number;
  /** The most that were alive at once; a child killed before it answered never counts as ended. */
  readonly peak: number;
  /** Each child's pid, in the order they started. */
  readonly pids: readonly number[];
  /** Processes the children left running behind them (see `leaveGrandchildren`). */
  readonly grandchildren: readonly number[];
}

/** One step of a `cargo metadata` child's life, as the broker received it. */
export interface CargoMetadataEvent {
  readonly kind: 'start' | 'read' | 'end' | 'grandchild';
  /** The child's pid; for `grandchild`, the pid of the process the child left behind. */
  readonly pid: number;
}

/** The events are in the order things happened, so a running count over them is the true concurrency. */
export function cargoMetadataRuns(events: readonly CargoMetadataEvent[]): CargoMetadataRuns {
  const pids: number[] = [];
  const grandchildren: number[] = [];
  let read = 0;
  let alive = 0;
  let peak = 0;
  for (const { kind, pid } of events) {
    switch (kind) {
      case 'start':
        pids.push(pid);
        alive += 1;
        peak = Math.max(peak, alive);
        break;
      case 'read':
        read += 1;
        break;
      case 'end':
        alive -= 1;
        break;
      case 'grandchild':
        grandchildren.push(pid);
        break;
    }
  }
  return { spawned: pids.length, read, peak, pids, grandchildren };
}

/**
 * How long an awaited event may take before its guard says what is still open: inside the tests'
 * own 20 s budgets, so the report reaches the test before the runner's timeout does. A guard only
 * ever fails a wait; nothing advances on it.
 */
const GUARD_MS = 15_000;

/**
 * The guard over the real Cargo: above the slowest real resolution seen behind a contended package
 * cache (21 s), below Bun's 30 s default test timeout, which those tests run under or exceed.
 */
const REAL_CARGO_GUARD_MS = 25_000;

/** The test spans still open under `span`, itself included: the work a hung test is still inside. */
function openSpans<T extends LogSchema>(span: QueryableSpan<T>, open: string[]): string[] {
  // Row 1 holds a span's completion; its timestamp stays 0 until the span ends.
  if (span.buffer.timestamp[1] === 0n) open.push(span.name);
  for (const child of span.children) openSpans(child, open);
  return open;
}

/**
 * `operation`, settling exactly as it does, unless no event settles it within `timeoutMs`: then it
 * rejects with `what`, the test spans still open and `diagnostics()`. The test span is taken now, so
 * call this from the test's own async context. `signal` disarms the guard; the result still follows
 * `operation`.
 */
export function guardEvent<T>(
  operation: Promise<T>,
  what: string,
  diagnostics: () => string | Promise<string>,
  signal?: AbortSignal,
  timeoutMs = GUARD_MS,
): Promise<T> {
  let spans: () => string;
  try {
    const test = querySpan(useTestSpan<OpContextBinding>().buffer);
    spans = () => {
      try {
        return `open test spans: [${openSpans(test, []).join(', ')}]`;
      } catch (error) {
        return `open test spans unreadable: ${error instanceof Error ? error.message : String(error)}`;
      }
    };
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    spans = () => `no test span to report: ${reason}`;
  }
  return new Promise<T>((resolve, reject) => {
    const report = async (): Promise<void> => {
      let details: string;
      try {
        details = await diagnostics();
      } catch (error) {
        details = `diagnostics failed: ${error instanceof Error ? (error.stack ?? error.message) : String(error)}`;
      }
      reject(new Error(`${what}: no event within ${timeoutMs} ms\n${spans()}\n${details}`));
    };
    // Kept referenced: when nothing else is left to wake the process, the report still comes.
    const timer = setTimeout(() => void report(), timeoutMs);
    const disarm = () => clearTimeout(timer);
    if (signal?.aborted) disarm();
    else signal?.addEventListener('abort', disarm, { once: true });
    operation.then(resolve, reject).finally(() => {
      disarm();
      signal?.removeEventListener('abort', disarm);
    });
  });
}

/** Where `readCargoResolve` says a call joined a resolution already in flight. */
const CARGO_RESOLVE_CHANNEL = '@smoothbricks/nx-plugin:cargo-resolve';

/**
 * Counts the calls that join a resolution of `manifest` already in flight in this process, from now:
 * subscribe before the calls start. `joined` resolves once `count` have joined. `dispose` stops
 * counting and disarms the guard; a `joined` not yet reached then never settles.
 */
export function waitForCargoJoins(manifest: string, count: number): { joined: Promise<void>; dispose(): void } {
  const canonical = realpathSync(manifest);
  const elsewhere: string[] = [];
  const malformed: string[] = [];
  const abort = new AbortController();
  let seen = 0;
  let listener: ((message: unknown) => void) | null = null;
  const stop = () => {
    if (listener !== null) unsubscribe(CARGO_RESOLVE_CHANNEL, listener);
    listener = null;
  };
  const counted = new Promise<void>((resolve) => {
    const onMessage = (message: unknown) => {
      if (typeof message !== 'object' || message === null || !('phase' in message) || !('manifest' in message)) {
        malformed.push(String(message));
        return;
      }
      if (message.phase !== 'join') return;
      if (message.manifest !== canonical) {
        elsewhere.push(String(message.manifest));
        return;
      }
      seen += 1;
      if (seen === count) {
        stop();
        resolve();
      }
    };
    listener = onMessage;
    subscribe(CARGO_RESOLVE_CHANNEL, onMessage);
  });
  const joined = guardEvent(
    counted,
    `waiting for ${count} calls to join the resolution of ${canonical}`,
    () =>
      `saw ${seen} of ${count} joins on ${CARGO_RESOLVE_CHANNEL}; ` +
      `joins of other manifests: [${elsewhere.join(', ')}]; malformed messages: [${malformed.join(', ')}]`,
    abort.signal,
  );
  return {
    joined,
    dispose: () => {
      stop();
      abort.abort();
    },
  };
}

/** A gate a test closes to hold every `cargo metadata` child at one point of its life, and opens to let them go on. */
export interface Latch {
  close(): Promise<void>;
  open(): Promise<void>;
}

export interface CountedCargo {
  /** The directory holding the `cargo` that goes ahead of the real one. */
  readonly bin: string;
  /**
   * What a process under test needs: this Cargo ahead of everything else on PATH, and the Nx
   * workspace-data directory (where Cargo resolutions are kept between processes) inside the
   * fixture, so the persistent cache is the fixture's own and is deleted with it, whatever
   * `NX_WORKSPACE_DATA_DIRECTORY` the test run itself inherited.
   */
  readonly environment: Readonly<Record<string, string>>;
  /** Hold every child before it reads the workspace: an edit made now is one its answer includes. */
  readonly beforeRead: Latch;
  /** Hold every child after it read the workspace: an edit made now is one its answer predates. */
  readonly beforeAnswer: Latch;
  /** While set, a child that has read answers with this on stderr and exit code 101 instead. */
  failWith(stderr: string): Promise<void>;
  recover(): Promise<void>;
  /** Whether every child leaves a background process running behind it, as a real Cargo does with rustc. */
  leaveGrandchildren(leave: boolean): Promise<void>;
  runs(): Promise<CargoMetadataRuns>;
  /**
   * The first runs `predicate` holds for: now, or after whichever child's report makes it hold. A
   * child counts as `read`, and its grandchild among `grandchildren`, only once that grandchild is
   * running and connected, so a predicate over both sees them together.
   */
  waitForRuns(predicate: (runs: CargoMetadataRuns) => boolean, what: string): Promise<CargoMetadataRuns>;
  /**
   * Resolves once every one of `pids` (children and grandchildren) has died: its connection to the
   * fixture closed without the child having finished. Rejects for a pid that never connected, or one
   * that ended normally or was let go by `release`, since none of those died.
   */
  waitForDisconnects(pids: readonly number[]): Promise<void>;
  /**
   * Open every latch, end every grandchild, stop accepting children, and wait until every process
   * the fixture started has gone, so nothing outlives it. Idempotent.
   */
  release(): Promise<void>;
}

export interface CannedCargo extends CountedCargo {
  /** What `cargo metadata` prints from now on: this document, as JSON. */
  answerWith(metadata: unknown): Promise<void>;
  /** What `cargo metadata` prints from now on: this text, whatever it is. */
  answerWithText(text: string): Promise<void>;
}

/** How a process's connection to the fixture ended. */
type Departure =
  /** A child that finished: it said `end` before hanging up. */
  | 'ended'
  /** A grandchild the fixture let go on `release`. */
  | 'released'
  /** Died: hung up without finishing, killed or crashed (its stderr says which). */
  | 'vanished';

/** Where a `cargo metadata` child is in its life: each step is taken when its gate lets it. */
type ChildPhase =
  | 'before-read'
  | 'reading'
  /** It has read; the grandchild it named has not connected yet. */
  | 'awaiting-grandchild'
  | 'before-answer'
  | 'answering'
  | 'ended';

interface MetadataChild {
  readonly role: 'metadata';
  readonly pid: number;
  readonly socket: Socket;
  phase: ChildPhase;
  grandchild: number | null;
  status: number | null;
  departure: Departure | null;
  error: string | null;
}

interface Grandchild {
  readonly role: 'grandchild';
  readonly pid: number;
  readonly parent: number;
  readonly socket: Socket;
  released: boolean;
  departure: Departure | null;
  error: string | null;
}

type Registered = MetadataChild | Grandchild;

const quote = (value: string): string => `'${value.replaceAll("'", `'"'"'`)}'`;

const PROGRAM = fileURLToPath(new URL('./counted-cargo-process.ts', import.meta.url));

function listen(server: Server): Promise<number> {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, BROKER_HOST, () => {
      server.off('error', reject);
      const address = server.address();
      if (address === null || typeof address === 'string') reject(new Error(`the broker has no port: ${address}`));
      else resolve(address.port);
    });
  });
}

/**
 * A `cargo` ahead of the real one on PATH, and the broker in this process it reports to. Every
 * subcommand but `metadata` runs the real Cargo (`forward` is the command line that does). A
 * `metadata` child is `counted-cargo-process.ts` under the same pid and process group; it reports
 * `start` as it begins, `read` once it has its answer, and `end` after the answer has been handed
 * over, so the broker orders each child's life against every other's. Its answer is the real
 * `cargo metadata`'s while `canned` is null, else `canned`, as it is when the child reads.
 */
async function shim(
  root: string,
  forward: string,
  initial: string | null,
  /** How long the fixture's waits may take: longer over the real Cargo. */
  guardMs: number,
): Promise<{ cargo: CountedCargo; answer(text: string): void }> {
  const real = Bun.which('cargo');
  if (real === null) throw new Error('cargo is not on PATH');
  const directory = join(root, 'counted-cargo');
  const bin = join(directory, 'bin');

  const events: CargoMetadataEvent[] = [];
  const processes = new Map<number, Registered>();
  const connections = new Set<Socket>();
  const listeners = new Set<() => void>();
  const faults: string[] = [];
  const torn: string[] = [];
  const closed = { read: false, answer: false };
  let canned = initial;
  let failure: string | null = null;
  let leave = false;
  let releasing = false;
  let listening = true;
  let released: Promise<void> | null = null;

  const changed = () => {
    for (const listener of [...listeners]) listener();
  };

  /** Settles with the first verdict `judge` gives: now, or after a later broker event; a fault fails it. */
  const verdict = <T>(judge: () => T | Error | null): Promise<T> =>
    new Promise((resolve, reject) => {
      const listener = () => {
        const result = faults.length > 0 ? new Error(`the counted Cargo broke: ${faults.join('; ')}`) : judge();
        if (result === null) return;
        listeners.delete(listener);
        if (result instanceof Error) reject(result);
        else resolve(result);
      };
      listeners.add(listener);
      listener();
    });

  /** Takes every step `child` may take now, in order; a child that has gone takes none. */
  const advance = (child: MetadataChild) => {
    if (child.departure !== null) return;
    if (child.phase === 'before-read' && !closed.read) {
      child.phase = 'reading';
      child.socket.write(encodeOrder({ kind: 'read', grandchild: leave, canned }));
    }
    if (child.phase === 'awaiting-grandchild') {
      const left = child.grandchild === null ? undefined : processes.get(child.grandchild);
      if (child.grandchild !== null && left?.role !== 'grandchild') return;
      if (child.grandchild !== null) events.push({ kind: 'grandchild', pid: child.grandchild });
      events.push({ kind: 'read', pid: child.pid });
      child.phase = 'before-answer';
    }
    if (child.phase === 'before-answer' && !closed.answer) {
      child.phase = 'answering';
      child.socket.write(encodeOrder({ kind: 'answer', failure }));
    }
  };
  const advanceAll = () => {
    for (const record of processes.values()) if (record.role === 'metadata') advance(record);
    changed();
  };

  /** What `report` makes of the connection that sent it: the process it registers, or the one it was. */
  const receive = (self: Registered | null, report: ProcessReport, socket: Socket): Registered | string => {
    if (self === null) {
      if (report.kind !== 'start' && report.kind !== 'grandchild') return `first report was ${report.kind}`;
      if (processes.has(report.pid)) return `pid ${report.pid} registered twice`;
      if (report.kind === 'start') {
        const child: MetadataChild = {
          role: 'metadata',
          pid: report.pid,
          socket,
          phase: 'before-read',
          grandchild: null,
          status: null,
          departure: null,
          error: null,
        };
        processes.set(child.pid, child);
        events.push({ kind: 'start', pid: child.pid });
        advance(child);
        return child;
      }
      const grandchild: Grandchild = {
        role: 'grandchild',
        pid: report.pid,
        parent: report.parent,
        socket,
        released: releasing,
        departure: null,
        error: null,
      };
      processes.set(grandchild.pid, grandchild);
      if (releasing) socket.end();
      const parent = processes.get(report.parent);
      if (parent?.role === 'metadata') advance(parent);
      return grandchild;
    }
    if (self.role === 'grandchild') return `grandchild ${self.pid} reported ${report.kind}`;
    if (report.kind === 'read' && self.phase === 'reading') {
      self.grandchild = report.grandchild;
      self.phase = 'awaiting-grandchild';
      advance(self);
      return self;
    }
    if (report.kind === 'end' && self.phase === 'answering') {
      self.phase = 'ended';
      self.status = report.status;
      events.push({ kind: 'end', pid: self.pid });
      return self;
    }
    return `child ${self.pid} reported ${report.kind} while ${self.phase}`;
  };

  const server = createServer((socket) => {
    connections.add(socket);
    let self: Registered | null = null;
    const unterminated = readLines(socket, (line) => {
      const report = parseReport(line);
      const outcome = report instanceof Error ? report.message : receive(self, report, socket);
      if (typeof outcome === 'string') {
        faults.push(outcome);
        socket.destroy();
      } else {
        self = outcome;
      }
      changed();
    });
    socket.on('error', (error) => {
      // A process killed while the broker writes to it resets the connection; its close says the rest.
      if (self !== null) self.error = error.message;
    });
    socket.on('close', () => {
      connections.delete(socket);
      // A process that died mid-write leaves part of a line: evidence of how it died, not a broken broker.
      const tail = unterminated();
      if (tail !== '') torn.push(`${self === null ? 'unregistered' : `${self.role} ${self.pid}`}: ${tail}`);
      if (self !== null) {
        if (self.role === 'metadata') self.departure = self.phase === 'ended' ? 'ended' : 'vanished';
        else self.departure = self.released ? 'released' : 'vanished';
      }
      changed();
    });
  });
  server.on('close', () => {
    listening = false;
    changed();
  });
  const port = await listen(server);
  // Only the processes it serves keep the test process alive, never the listener itself.
  server.unref();

  /** Everything the broker knows, for a guard that ran out of patience. */
  const describe = async (): Promise<string> => {
    const lines = [
      `counted Cargo broker ${BROKER_HOST}:${port}: ${listening ? 'listening' : 'closed'}${releasing ? ', releasing' : ''}`,
      `gates: before-read ${closed.read ? 'closed' : 'open'}, before-answer ${closed.answer ? 'closed' : 'open'}; ` +
        `failing: ${failure !== null}; leaving grandchildren: ${leave}`,
      `runs: ${JSON.stringify(cargoMetadataRuns(events))}`,
      `connections open: ${connections.size}`,
    ];
    for (const record of processes.values()) {
      const state = record.departure === null ? 'connected' : `gone (${record.departure})`;
      const socket = record.error === null ? '' : `, socket error: ${record.error}`;
      if (record.role === 'metadata') {
        lines.push(
          `metadata child ${record.pid}: ${record.phase}, ${state}, grandchild ${record.grandchild ?? 'none'}, ` +
            `status ${record.status ?? 'none'}${socket}`,
        );
      } else {
        let log: string;
        try {
          log = await readFile(join(directory, `grandchild-of-${record.parent}.log`), 'utf8');
        } catch (error) {
          log = `unreadable: ${error instanceof Error ? error.message : String(error)}`;
        }
        lines.push(`grandchild ${record.pid} of ${record.parent}: ${state}${socket}; log: ${JSON.stringify(log)}`);
      }
    }
    if (faults.length > 0) lines.push(`faults: ${faults.join('; ')}`);
    if (torn.length > 0) lines.push(`partial reports from processes that died: ${torn.join('; ')}`);
    return lines.join('\n');
  };

  /** SIGKILL `target` (a pid, or a negated process-group id); a target already gone is no failure. */
  const kill = (target: number, failures: string[]) => {
    try {
      process.kill(target, 'SIGKILL');
    } catch (error) {
      if (!(error instanceof Error && 'code' in error && error.code === 'ESRCH')) {
        failures.push(`kill ${target}: ${error instanceof Error ? error.message : String(error)}`);
      }
    }
  };

  const drain = async (): Promise<void> => {
    // Gates first, so every held child answers; then the grandchildren, which live until let go.
    releasing = true;
    closed.read = false;
    closed.answer = false;
    server.close();
    for (const record of processes.values()) {
      if (record.role === 'grandchild' && record.departure === null) {
        record.released = true;
        record.socket.end();
      }
    }
    advanceAll();
    try {
      await guardEvent(
        verdict(() => (connections.size === 0 ? true : null)),
        'releasing the counted Cargo',
        describe,
        undefined,
        guardMs,
      );
    } catch (error) {
      // A process still connected is still running, so its pid is still its own: kill it and, for a
      // child, the process group it leads (`readCargoResolve` starts each one detached), with whatever
      // real Cargo or grandchild is in it. Nothing the fixture started outlives a failed release.
      const failures: string[] = [];
      const killed: string[] = [];
      for (const record of processes.values()) {
        if (record.departure !== null) continue;
        if (record.role === 'metadata') kill(-record.pid, failures);
        kill(record.pid, failures);
        killed.push(`${record.role} ${record.pid}`);
      }
      for (const socket of connections) socket.destroy();
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(
        `${message}\nkilled: [${killed.join(', ')}]${failures.length > 0 ? `; kill failures: ${failures.join('; ')}` : ''}`,
        { cause: error },
      );
    }
  };

  await mkdir(bin, { recursive: true });
  await writeFile(
    join(bin, 'cargo'),
    [
      '#!/bin/sh',
      `real=${quote(real)}`,
      `if [ "$1" != metadata ]; then ${forward}; fi`,
      `exec ${quote(process.execPath)} ${quote(PROGRAM)} metadata ${port} "$real" ${quote(directory)} "$@"`,
      '',
    ].join('\n'),
  );
  await chmod(join(bin, 'cargo'), 0o755);

  const latch = (gate: 'read' | 'answer'): Latch => ({
    close: async () => {
      closed[gate] = true;
    },
    open: async () => {
      closed[gate] = false;
      advanceAll();
    },
  });
  const cargo: CountedCargo = {
    bin,
    environment: {
      PATH: `${bin}${delimiter}${process.env.PATH ?? ''}`,
      NX_WORKSPACE_DATA_DIRECTORY: join(root, '.nx/workspace-data'),
    },
    beforeRead: latch('read'),
    beforeAnswer: latch('answer'),
    failWith: async (stderr) => {
      failure = stderr;
    },
    recover: async () => {
      failure = null;
    },
    leaveGrandchildren: async (value) => {
      leave = value;
    },
    runs: async () => cargoMetadataRuns(events),
    waitForRuns: (predicate, what) =>
      guardEvent(
        verdict(() => {
          const runs = cargoMetadataRuns(events);
          return predicate(runs) ? runs : null;
        }),
        `waiting for ${what}`,
        describe,
        undefined,
        guardMs,
      ),
    waitForDisconnects: async (pids) => {
      await guardEvent(
        verdict(() => {
          const unknown = pids.filter((pid) => !processes.has(pid));
          if (unknown.length > 0) return new Error(`never connected to the counted Cargo: ${unknown.join(', ')}`);
          const records = pids.flatMap((pid) => processes.get(pid) ?? []);
          const survived = records.filter((record) => record.departure !== null && record.departure !== 'vanished');
          if (survived.length > 0) {
            const which = survived.map((record) => `${record.role} ${record.pid} (${record.departure})`);
            return new Error(`expected to die, but finished: ${which.join(', ')}`);
          }
          return records.every((record) => record.departure === 'vanished') ? true : null;
        }),
        `waiting for ${pids.join(', ')} to die`,
        describe,
        undefined,
        guardMs,
      );
    },
    release: () => {
      released ??= drain();
      return released;
    },
  };
  return {
    cargo,
    answer: (text) => {
      canned = text;
    },
  };
}

/** The real Cargo, counted: `metadata` prints what the real `cargo metadata` prints. */
export async function countedCargo(root: string): Promise<CountedCargo> {
  const { cargo } = await shim(root, 'exec "$real" "$@"', null, REAL_CARGO_GUARD_MS);
  return cargo;
}

/**
 * A Cargo whose `metadata` prints whatever the test says, whatever is on disk: the cache's own behavior,
 * alone. Its other subcommands run the real Cargo under no rustup selection, which the canned answers do
 * not depend on.
 */
export async function cannedCargo(root: string): Promise<CannedCargo> {
  const { cargo, answer } = await shim(root, 'exec env -u RUSTUP_TOOLCHAIN -u RUSTUP_HOME "$real" "$@"', '', GUARD_MS);
  return {
    ...cargo,
    answerWith: async (document) => answer(JSON.stringify(document)),
    answerWithText: async (text) => answer(text),
  };
}

/** `body` with `overrides` in this process's environment, which the module's Cargo children inherit. */
export async function withEnvironment<T>(
  overrides: Readonly<Record<string, string>>,
  body: () => Promise<T>,
): Promise<T> {
  const saved = Object.keys(overrides).map((key): [string, string | undefined] => [key, process.env[key]]);
  Object.assign(process.env, overrides);
  try {
    return await body();
  } finally {
    for (const [key, value] of saved) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

/** The error `promise` rejects with; a promise that resolves fails the test. */
export async function rejectionOf(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  throw new Error('expected a rejection, but the promise resolved');
}

/** The typed Cargo refusal, including the requested manifest and the original failure. */
export function expectCargoMetadataError(error: unknown, manifestPath: string): CargoMetadataError {
  expect(error).toBeInstanceOf(CargoMetadataError);
  if (!(error instanceof CargoMetadataError)) throw new Error(`expected a CargoMetadataError, got ${String(error)}`);
  expect(error.name).toBe('CargoMetadataError');
  expect(error.manifestPath).toBe(manifestPath);
  expect(error.cause).toBeInstanceOf(Error);
  return error;
}

/** `failure` is the whole refusal of a project-graph computation: Nx's aggregate of the per-file errors. */
export function aggregateOf(failure: unknown): AggregateCreateNodesError {
  if (!(failure instanceof AggregateCreateNodesError)) {
    throw new Error(`expected the graph computation to fail as an aggregate, got ${String(failure)}`);
  }
  return failure;
}

/**
 * `failure` is a project-graph computation refused for want of a Cargo resolution: an aggregate whose
 * every error is the typed `CargoMetadataError` for the workspace manifest `manifestPath`, and which
 * carries no partial result a runtime hash of the whole workspace could have filled.
 */
export function expectGraphRefusal(failure: unknown, manifestPath: string): AggregateCreateNodesError {
  const aggregate = aggregateOf(failure);
  expect(aggregate.errors.length).toBeGreaterThan(0);
  for (const [, error] of aggregate.errors) expectCargoMetadataError(error, manifestPath);
  expect(JSON.stringify(aggregate.partialResults)).not.toContain('--include-workspace');
  return aggregate;
}
