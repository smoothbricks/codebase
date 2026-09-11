import { setTimeout as delay } from 'node:timers/promises';

/**
 * What a release knows about state it does not own: the version is on the
 * registry, it is not, or nobody answered.
 *
 * Every release decision starts by asking the outside world what it already
 * did -- is this version published, does this release exist on the forge. Two
 * of those answers are useful and the third is an honest non-answer, and a
 * probe that reports only two has to hide the third inside one of them. That
 * is not a formality: a reset packet read as `absent` republishes a version
 * that exists, and read as `exists` skips one that does not. `undetermined`
 * makes the non-answer a value the caller must handle.
 */
export type DurableState =
  | { kind: 'exists' }
  | { kind: 'absent' }
  /** No verdict was reached: `attempts` requests all died in transport. */
  | { kind: 'undetermined'; attempts: number; detail: string };

/** One attempt that never reached a verdict; `probeWithTransportRetry` accumulates the count. */
export function undetermined(detail: string): DurableState {
  return { kind: 'undetermined', attempts: 1, detail };
}

/**
 * Waits between transport retries, and therefore the attempt bound: three
 * attempts, 2.5s of waiting in the worst case, none at all when the first
 * attempt gets an answer.
 *
 * The clients' own retry ladders are deliberately off (npm gets
 * `--fetch-retries=0`) because a 404 is the common answer on the publish path
 * and paying a retry ladder for the common answer slowed every probe. The
 * cost was that one reset packet aborted a release that had nothing wrong with
 * it. This bound buys that back for failures that reached no verdict, and only
 * for those -- a 404 or a 401 still costs exactly one request.
 */
const TRANSPORT_RETRY_BACKOFF_MS = [500, 2_000] as const;

export interface TransportRetryShell {
  /** Backoff between attempts; injected so tests exercise the retry without waiting. */
  sleep(ms: number): Promise<void>;
}

const processRetryShell: TransportRetryShell = { sleep: (ms) => delay(ms) };

/**
 * Runs `attempt` until it reaches a verdict or the retry bound is spent, and
 * reports how many attempts that took.
 *
 * Only `undetermined` is retried. A refusal -- unauthorized, forbidden,
 * misconfigured -- throws out of `attempt` and past this loop untouched,
 * because retrying a verdict the remote already gave just delays the same
 * answer.
 */
export async function probeWithTransportRetry(
  attempt: () => Promise<DurableState>,
  shell: TransportRetryShell = processRetryShell,
): Promise<DurableState> {
  for (let attempts = 1; ; attempts += 1) {
    const probe = await attempt();
    if (probe.kind !== 'undetermined') {
      return probe;
    }
    const backoffMs = TRANSPORT_RETRY_BACKOFF_MS.at(attempts - 1);
    if (backoffMs === undefined) {
      return { ...probe, attempts };
    }
    await shell.sleep(backoffMs);
  }
}

/**
 * The release's answer to a non-answer: refuse, and say why in terms an
 * operator can act on.
 *
 * Refusing is the only defensible move. `absent` would republish a version
 * that exists; `exists` would skip a version that does not. So the message has
 * one job beyond naming the subject: make it unmistakable that this is the
 * network, not the release, so the run gets re-dispatched instead of
 * investigated.
 */
export function requireDurableState(probe: DurableState, subject: string, question: string): boolean {
  if (probe.kind === 'exists') {
    return true;
  }
  if (probe.kind === 'absent') {
    return false;
  }
  throw new Error(
    `${subject}: ${question} is undetermined -- ${probe.attempts} attempts all failed in the network transport, ` +
      'before the remote answered. Refusing: reading this as unpublished would republish a version that exists, ' +
      'and reading it as published would skip one that does not. This is a network failure, not a release ' +
      'problem -- nothing was published or skipped by this check, so re-dispatching this run is safe. ' +
      `Last transport failure: ${probe.detail}`,
  );
}

/**
 * Signatures of a request that died before the remote gave a verdict.
 *
 * Three client families appear here on purpose. npm and undici report
 * `errno`-style codes; `gh` reports Go's transport phrasing; Bun's own `fetch`
 * reports prose plus a CamelCase code, and a reset arrives as the useless
 * "The socket connection was closed unexpectedly" with the only real signal,
 * `ECONNRESET`, on the error's `code` property. A 5xx is the server admitting
 * it produced no verdict either. Permission and not-found verdicts are
 * deliberately absent -- matching one here would turn a terminal answer into
 * three retries of the same answer.
 */
const TRANSPORT_FAILURE =
  /\b(?:ECONNRESET|ECONNREFUSED|ECONNABORTED|ETIMEDOUT|ESOCKETTIMEDOUT|EAI_AGAIN|ENOTFOUND|EHOSTUNREACH|ENETUNREACH|ENETDOWN|EPIPE|EPROTO|ERR_SOCKET_CONNECTION_TIMEOUT|UND_ERR_(?:CONNECT_TIMEOUT|SOCKET|HEADERS_TIMEOUT|BODY_TIMEOUT))\b|\bConnection(?:Reset|Refused|Closed|Timeout)\b|\bFailedToOpenSocket\b|\bTimeoutError\b|socket hang up|socket connection was closed|network socket disconnected|connection (?:reset|refused|closed)|unable to connect|operation timed out|no such host|i\/o timeout|tls handshake timeout|unexpected eof|\bE5\d{2}\b|\bHTTP 5\d{2}\b|\b5\d{2} (?:Internal Server Error|Bad Gateway|Service Unavailable|Gateway Time-?out)\b/i;

/** Whether command output or a response body describes a failure that reached no verdict. */
export function isTransportFailure(output: string): boolean {
  return TRANSPORT_FAILURE.test(output);
}

/**
 * The transport failure `error` describes, or null when it describes something
 * else and must not be retried.
 *
 * The whole error is read, not its message. A reset reaches us as
 * `TypeError: The socket connection was closed unexpectedly` with the code on
 * a property, and under Node as `TypeError: fetch failed` with the reason one
 * `cause` down -- classify on the message alone and you either retry every bug
 * or retry no reset.
 */
export function transportFailureDetail(error: unknown): string | null {
  const chain = errorChainText(error, 4);
  return isTransportFailure(chain) ? chain : null;
}

function errorChainText(error: unknown, depth: number): string {
  if (depth <= 0 || error === undefined || error === null) {
    return '';
  }
  if (!(error instanceof Error)) {
    return String(error);
  }
  const code = 'code' in error && typeof error.code === 'string' ? error.code : '';
  const nested =
    error instanceof AggregateError
      ? error.errors.map((inner: unknown) => errorChainText(inner, depth - 1))
      : [errorChainText(error.cause, depth - 1)];
  return [`${error.name}: ${error.message}`, code, ...nested].filter(Boolean).join(': ');
}
