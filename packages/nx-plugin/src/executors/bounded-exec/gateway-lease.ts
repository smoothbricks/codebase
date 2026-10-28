import { connect } from 'node:net';
import typia from 'typia';
import { type CpuLeaseProgress, type CpuLeaseQueued, CpuLeaseWait, type CpuWaitRow } from './lease-wait.js';

const isCpuProgress = typia.createEquals<CpuLeaseProgress>();
const isCpuQueued = typia.createEquals<CpuLeaseQueued>();

/**
 * The client half of a lease held on a unix socket: the gateway's host disk-lifecycle lease
 * (`disk-lease.ts`) and a parent's CPU pool (`cpu-pool.ts`) speak the same protocol
 * (specs/cowshed/05_gateway.md, "Disk-lifecycle lease").
 *
 * The client writes one request line and keeps the connection open without half-closing it. The
 * gateway answers `queued` at once and `granted` when the lease is the client's, and the lease
 * lasts until the client closes the connection, so a client that dies holding one releases it with
 * its socket. The caller decides what a refusal means: disk work may run unleased, while CPU
 * admission fails rather than running outside its parent's count.
 */

/** The cowshed gateway's control socket, at the store root every peer agrees on. */
export const GATEWAY_SOCKET = '/private/cowshed/store/gateway.sock';

export interface LeaseBounds {
  /** For the `queued` answer a lease-scheduling gateway sends at once. */
  ackMs: number;
  /** For any answer once the request is half-closed. */
  afterCloseMs: number;
  /** For the grant, once queued; `null` waits for as long as the gateway keeps it queued. */
  grantMs: number | null;
}

/**
 * Why a lease is not held: no gateway listens, the gateway predates this kind of lease, the gateway
 * knows the lease but predates a field the request names (it refuses the field at once), or any
 * other refusal, silence or breakage.
 */
export type UnleasedCause = 'absent' | 'predates' | 'predates-field' | 'other';

/** A refusal: the lease is not held, and why. A CPU wait that expired carries its typed row. */
export type Unleased = { granted: false; cause: UnleasedCause; reason: string; code?: string; wait?: CpuWaitRow };

/**
 * A granted lease with its `granted` answer, held until released, or why there is none. `closed`
 * resolves when the connection really closes: after `release`, when the gateway ends it, or when
 * the client process dies.
 */
export type HeldLease =
  | { granted: true; answer: Readonly<Record<string, unknown>>; release: () => void; closed: Promise<void> }
  | Unleased;

/**
 * Ask the gateway at `socket` for the lease `request` names, `what` in its messages ("disk
 * leases", "the parent CPU pool"). A gateway from before one-line requests says nothing until it reads
 * EOF, so a disk request not queued within its ack bound is half-closed to read that gateway's
 * refusal. CPU admission instead uses its own control/silence policy: no compatibility half-close,
 * and no total queue bound (specs/cowshed/05_gateway.md, "Deadlines").
 */
export function holdLease(
  socket: string,
  request: Readonly<Record<string, unknown>>,
  what: string,
  bounds: LeaseBounds | CpuLeaseWait,
  onQueued?: () => void,
): Promise<HeldLease> {
  // `Promise.withResolvers` would read better but needs lib es2024; this package inherits lib
  // es2022 from tsconfig.base.json.
  let resolve!: (outcome: HeldLease) => void;
  const promise = new Promise<HeldLease>((settled) => {
    resolve = settled;
  });
  const cpu = bounds instanceof CpuLeaseWait ? bounds : null;
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: CPU connect/send is control-bounded and its answer heartbeat-silence-bounded by armCpu(); disk acknowledgement and half-close are bounded below, and a queued disk lease lasts until its holder releases it
  const connection = connect(socket);
  // `closed` is made when a consumer reads it, so a lease nobody watches allocates no promise. The
  // flag, not `connection.destroyed`, says the socket really closed: `destroy()` sets that flag before
  // the `close` event fires.
  let socketClosed = false;
  let closed: Promise<void> | undefined;
  let state: 'connecting' | 'asked' | 'half-closed' | 'queued' | 'settled' = 'connecting';
  let buffered = '';
  let timer: NodeJS.Timeout | undefined;
  const settle = (outcome: HeldLease) => {
    if (state === 'settled') {
      return;
    }
    state = 'settled';
    clearTimeout(timer);
    if (!outcome.granted) {
      connection.destroy();
    }
    resolve(outcome);
  };
  const unleased = (reason: string, cause: UnleasedCause = 'other', code?: string, wait?: CpuWaitRow) =>
    settle({
      granted: false,
      cause,
      reason,
      ...(code === undefined ? {} : { code }),
      ...(wait === undefined ? {} : { wait }),
    });
  const wait = (ms: number, onExpiry: () => void) => {
    clearTimeout(timer);
    timer = setTimeout(onExpiry, ms);
  };
  const armCpu = (): void => {
    if (cpu === null || state === 'settled') return;
    wait(cpu.remainingMs(), () => {
      cpu.look();
      const failure = cpu.expired();
      if (failure === null) {
        armCpu();
        return;
      }
      const peer = failure.server === null ? socket : `${socket} (pid ${failure.server})`;
      unleased(
        `${failure.code}: cpu-pool: the lease endpoint ${peer} exceeded ${failure.boundMs} ms (waited ${failure.elapsedMs} ms; last progress: ${failure.last})`,
        'other',
        failure.code,
        { ...failure, socket },
      );
    });
  };
  armCpu();
  const answer = (line: string) => {
    if (state === 'settled') return;
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch {
      unleased(`the lease endpoint ${socket} answered something that is not an answer: ${line}`);
      return;
    }
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
      unleased(`the lease endpoint ${socket} answered something that is not an answer: ${line}`);
      return;
    }
    const fields: Readonly<Record<string, unknown>> = Object.fromEntries(Object.entries(parsed));
    if (cpu !== null && isCpuProgress(parsed)) {
      if (!cpu.heartbeat(parsed.progress)) {
        unleased(`the lease endpoint ${socket} sent non-advancing CPU progress: ${line}`);
      } else {
        armCpu();
      }
      return;
    }
    const lease = fields.lease;
    if (fields.ok !== true) {
      const error = String(fields.error ?? '');
      const why = `${String(fields.code ?? 'refused')}: ${error}`;
      if (state === 'half-closed' || error.endsWith('unknown gateway control operation')) {
        unleased(
          `the lease endpoint ${socket} predates ${what} (${why}); restart it with \`cowshed setup\``,
          'predates',
        );
      } else if (error.endsWith('contains an unknown field')) {
        unleased(
          `the lease endpoint ${socket} predates a field of ${what} (${why}); restart it with \`cowshed setup\``,
          'predates-field',
        );
      } else {
        unleased(
          `the lease endpoint ${socket} refused the lease (${why})`,
          'other',
          typeof fields.code === 'string' ? fields.code : undefined,
        );
      }
    } else if (state === 'half-closed') {
      unleased(`the lease endpoint ${socket} did not queue the request in time; it is alive but overloaded`);
    } else if (state === 'asked' && lease === 'queued') {
      state = 'queued';
      onQueued?.();
      if (bounds instanceof CpuLeaseWait) {
        // A pool from before its acknowledgement named its process names none.
        bounds.queued(isCpuQueued(parsed) ? parsed.server : null);
        armCpu();
      } else if (bounds.grantMs === null) {
        clearTimeout(timer);
      } else {
        const grantMs = bounds.grantMs;
        wait(grantMs, () => unleased(`the lease endpoint ${socket} granted nothing within ${grantMs} ms`));
      }
    } else if (state === 'queued' && lease === 'granted') {
      settle({
        granted: true,
        answer: fields,
        release: () => connection.destroy(),
        get closed() {
          closed ??= socketClosed
            ? Promise.resolve()
            : // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a held lease's connection lasts as long as its holder keeps it; release() destroys it on this side, and the gateway's own end closes it too
              new Promise<void>((done) => connection.once('close', () => done()));
          return closed;
        },
      });
    } else {
      unleased(`the lease endpoint ${socket} answered out of turn: ${line}`);
    }
  };
  connection.on('connect', () => {
    state = 'asked';
    if (bounds instanceof CpuLeaseWait) {
      connection.write(`${JSON.stringify(request)}\n`, () => {
        bounds.sent();
        armCpu();
      });
    } else {
      connection.write(`${JSON.stringify(request)}\n`);
      wait(bounds.ackMs, () => {
        state = 'half-closed';
        connection.end();
        wait(bounds.afterCloseMs, () => unleased(`the lease endpoint ${socket} answered nothing`));
      });
    }
  });
  connection.on('data', (chunk: Buffer) => {
    buffered += chunk.toString('utf8');
    for (let newline = buffered.indexOf('\n'); newline !== -1; newline = buffered.indexOf('\n')) {
      const line = buffered.slice(0, newline);
      buffered = buffered.slice(newline + 1);
      answer(line);
    }
  });
  connection.on('error', (error) => {
    if (state === 'connecting') {
      unleased(`nothing answers for ${what} at ${socket} (${error.message})`, 'absent');
    } else {
      unleased(error.message);
    }
  });
  connection.on('close', () => {
    socketClosed = true;
    unleased(`the lease connection for ${what} at ${socket} closed`);
  });
  return promise;
}
