import { connect } from 'node:net';

/**
 * The client half of a lease held on the cowshed gateway's control socket: the host
 * disk-lifecycle lease (`disk-lease.ts`) and the host CPU budget (`cpu-tokens.ts`) speak the same
 * protocol (specs/cowshed/05_gateway.md, "Disk-lifecycle lease" and "Host CPU budget").
 *
 * The client writes one request line and keeps the connection open without half-closing it. The
 * gateway answers `queued` at once and `granted` when the lease is the client's, and the lease
 * lasts until the client closes the connection, so a client that dies holding one releases it with
 * its socket. A lease never decides whether a command runs: without a grant, it runs without one
 * and the caller says why.
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
 * Why a command runs without a lease: no gateway listens, the gateway predates this kind of lease,
 * or any other refusal, silence or breakage.
 */
export type UnleasedCause = 'absent' | 'predates' | 'other';

/** A granted lease with its `granted` answer, held until released, or why there is none. */
export type HeldLease =
  | { granted: true; answer: Readonly<Record<string, unknown>>; release: () => void }
  | { granted: false; cause: UnleasedCause; reason: string };

/**
 * Ask the gateway at `socket` for the lease `request` names, `what` in its messages ("disk
 * leases", "the CPU budget"). A gateway from before one-line requests says nothing until it reads
 * EOF, so a request not queued within `ackMs` is half-closed and that gateway's refusal read; a
 * gateway that reads the line but does not know the operation refuses it at once. Both predate the
 * lease.
 */
export function holdLease(
  socket: string,
  request: Readonly<Record<string, unknown>>,
  what: string,
  bounds: LeaseBounds,
): Promise<HeldLease> {
  // `Promise.withResolvers` would read better but needs lib es2024; this package inherits lib
  // es2022 from tsconfig.base.json.
  let resolve!: (outcome: HeldLease) => void;
  const promise = new Promise<HeldLease>((settled) => {
    resolve = settled;
  });
  const connection = connect(socket);
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
  const unleased = (reason: string, cause: UnleasedCause = 'other') => settle({ granted: false, cause, reason });
  const wait = (ms: number, onExpiry: () => void) => {
    clearTimeout(timer);
    timer = setTimeout(onExpiry, ms);
  };
  const answer = (line: string) => {
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch {
      unleased(`the gateway answered something that is not an answer: ${line}`);
      return;
    }
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
      unleased(`the gateway answered something that is not an answer: ${line}`);
      return;
    }
    const fields: Readonly<Record<string, unknown>> = Object.fromEntries(Object.entries(parsed));
    const lease = fields.lease;
    if (fields.ok !== true) {
      const error = String(fields.error ?? '');
      const why = `${String(fields.code ?? 'refused')}: ${error}`;
      if (state === 'half-closed' || error.endsWith('unknown gateway control operation')) {
        unleased(`the gateway predates ${what} (${why}); restart it with \`cowshed setup\``, 'predates');
      } else {
        unleased(`the gateway refused the lease (${why})`);
      }
    } else if (state === 'half-closed') {
      unleased('the gateway did not queue the request in time; it is alive but overloaded');
    } else if (state === 'asked' && lease === 'queued') {
      state = 'queued';
      const grantMs = bounds.grantMs;
      if (grantMs === null) {
        clearTimeout(timer);
      } else {
        wait(grantMs, () => unleased(`the gateway granted nothing within ${grantMs} ms`));
      }
    } else if (state === 'queued' && lease === 'granted') {
      settle({ granted: true, answer: fields, release: () => connection.destroy() });
    } else {
      unleased(`the gateway answered out of turn: ${line}`);
    }
  };
  connection.on('connect', () => {
    state = 'asked';
    connection.write(`${JSON.stringify(request)}\n`);
    wait(bounds.ackMs, () => {
      state = 'half-closed';
      connection.end();
      wait(bounds.afterCloseMs, () => unleased('the gateway answered nothing'));
    });
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
      unleased(`the gateway's control socket ${socket} does not answer (${error.message})`, 'absent');
    } else {
      unleased(error.message);
    }
  });
  connection.on('close', () => unleased('the gateway closed the lease connection'));
  return promise;
}
