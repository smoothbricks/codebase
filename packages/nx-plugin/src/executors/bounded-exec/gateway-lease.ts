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
 * Why a lease is not held: no gateway listens, the gateway predates this kind of lease, the gateway
 * knows the lease but predates a field the request names (it refuses the field at once), an install
 * restarted the gateway while the ask was queued (`install-restart`), or any other refusal, silence
 * or breakage.
 */
export type UnleasedCause = 'absent' | 'predates' | 'predates-field' | 'other' | 'install-restart';

type PlainCause = Exclude<UnleasedCause, 'install-restart'>;

/** The code of the one answer a gateway sends a cut-off ask before it closes the connection. */
export const INSTALL_RESTART_CODE = 'install-restart';

/**
 * The install that restarted the gateway and cut a queued ask off: its pid, the binary it runs, and
 * how long, in ms, the gateway it starts is bound to serve within (05_gateway.md, "A queued CPU ask outlives an install").
 */
export interface InstallRestart {
  pid: number;
  binary: string;
  servesWithinMs: number;
}

/** A refusal: the lease is not held, and why. `install-restart` names the install that cut the ask off. */
export type Unleased =
  | { granted: false; cause: PlainCause; reason: string; code?: string }
  | {
      granted: false;
      cause: 'install-restart';
      reason: string;
      code: typeof INSTALL_RESTART_CODE;
      install: InstallRestart;
    };

/**
 * A granted lease with its `granted` answer, held until released, or why there is none. `closed`
 * resolves when the connection really closes: after `release`, when the gateway ends it, or when
 * the client process dies.
 */
export type HeldLease =
  | { granted: true; answer: Readonly<Record<string, unknown>>; release: () => void; closed: Promise<void> }
  | Unleased;

const isCount = (value: unknown): value is number =>
  typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;

/** The `install` an `install-restart` answer names, or `null` when it does not name a whole install. */
function parseInstallRestart(value: unknown): InstallRestart | null {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return null;
  }
  const fields: Readonly<Record<string, unknown>> = Object.fromEntries(Object.entries(value));
  const { pid, binary, servesWithinMs } = fields;
  if (!isCount(pid) || typeof binary !== 'string' || !isCount(servesWithinMs)) {
    return null;
  }
  return { pid, binary, servesWithinMs };
}

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
  onQueued?: () => void,
): Promise<HeldLease> {
  // `Promise.withResolvers` would read better but needs lib es2024; this package inherits lib
  // es2022 from tsconfig.base.json.
  let resolve!: (outcome: HeldLease) => void;
  const promise = new Promise<HeldLease>((settled) => {
    resolve = settled;
  });
  // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a unix-socket connect completes or fails at once; the request's ack is bounded by bounds.ackMs and then bounds.afterCloseMs, and a queued lease's grant by bounds.grantMs, or by nothing for CPU tokens, which have no admission deadline (05_gateway.md, "Host CPU budget")
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
  const unleased = (reason: string, cause: PlainCause = 'other', code?: string) =>
    settle({ granted: false, cause, reason, ...(code === undefined ? {} : { code }) });
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
      if (fields.code === INSTALL_RESTART_CODE) {
        const install = parseInstallRestart(fields.install);
        if (install === null) {
          unleased(
            `the gateway refused the lease (${why}; the install it names is malformed: ${fields.install === undefined ? 'missing' : JSON.stringify(fields.install)})`,
            'other',
            INSTALL_RESTART_CODE,
          );
        } else {
          settle({
            granted: false,
            cause: 'install-restart',
            reason: `the gateway refused the lease (${why})`,
            code: INSTALL_RESTART_CODE,
            install,
          });
        }
      } else if (state === 'half-closed' || error.endsWith('unknown gateway control operation')) {
        unleased(`the gateway predates ${what} (${why}); restart it with \`cowshed setup\``, 'predates');
      } else if (error.endsWith('contains an unknown field')) {
        unleased(
          `the gateway predates a field of ${what} (${why}); restart it with \`cowshed setup\``,
          'predates-field',
        );
      } else {
        unleased(
          `the gateway refused the lease (${why})`,
          'other',
          typeof fields.code === 'string' ? fields.code : undefined,
        );
      }
    } else if (state === 'half-closed') {
      unleased('the gateway did not queue the request in time; it is alive but overloaded');
    } else if (state === 'asked' && lease === 'queued') {
      state = 'queued';
      onQueued?.();
      const grantMs = bounds.grantMs;
      if (grantMs === null) {
        clearTimeout(timer);
      } else {
        wait(grantMs, () => unleased(`the gateway granted nothing within ${grantMs} ms`));
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
  connection.on('close', () => {
    socketClosed = true;
    unleased('the gateway closed the lease connection');
  });
  return promise;
}
