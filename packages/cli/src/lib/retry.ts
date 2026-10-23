import { setTimeout as delay } from 'node:timers/promises';

/**
 * How long a transient failure is retried: at most `attempts` tries in all (fewer than one still tries once),
 * waiting `baseDelayMs` doubling to `maxDelayMs` between them.
 */
export interface RetryPolicy {
  attempts: number;
  baseDelayMs: number;
  maxDelayMs: number;
  /** The longest wait a server's Retry-After may ask for: asking for more ends the retrying instead of waiting. */
  maxRetryAfterMs: number;
}

/** A retry about to happen: `attempt` of `attempts` just failed for `reason`, and the next one starts in `delayMs`. */
export interface RetryEvent {
  attempt: number;
  attempts: number;
  delayMs: number;
  reason: string;
}

/** The clock, the dice and the log of a retry loop, injected so a test neither waits nor depends on chance. */
export interface RetryShell {
  sleep(milliseconds: number): Promise<void>;
  /** Jitter factor in [0, 1): a wait is the backoff times (0.5 + 0.5 * random()). */
  random(): number;
  retrying(event: RetryEvent): void;
}

export const processRetryShell: RetryShell = {
  sleep: (milliseconds) => delay(milliseconds),
  random: () => Math.random(),
  retrying: ({ attempt, attempts, delayMs, reason }) => {
    console.warn(`Attempt ${attempt} of ${attempts} failed (${reason}); retrying in ${delayMs} ms.`);
  },
};

/**
 * One attempt's verdict. The step decides what is transient, because only the caller knows what its remote's answers
 * mean and whether repeating the call is safe; this loop only paces the repeats and bounds them. An error the step
 * throws instead of reporting is not transient and leaves the loop untouched.
 */
export type RetryStep<T> =
  | { kind: 'done'; value: T }
  | {
      kind: 'transient';
      /** Thrown as it is when the retrying ends, so callers keep their own error types and messages. */
      error: Error;
      reason: string;
      /** What the server asked to wait before the next attempt, from `Retry-After`. */
      retryAfterMs?: number;
    };

/**
 * Runs `step` until it is done or the policy's attempts are spent, then throws the last transient error. Waits
 * exponentially longer between attempts, jittered into the upper half of the backoff so parallel callers that failed
 * together do not retry together, and never less than the server asked for.
 */
export async function retryTransient<T>(
  step: () => Promise<RetryStep<T>>,
  policy: RetryPolicy,
  shell: RetryShell,
): Promise<T> {
  for (let attempt = 1; ; attempt += 1) {
    const outcome = await step();
    if (outcome.kind === 'done') return outcome.value;
    if (attempt >= policy.attempts) throw outcome.error;
    const { retryAfterMs } = outcome;
    // A server that asks for longer than the bound has told us the bound cannot outwait it.
    if (retryAfterMs !== undefined && retryAfterMs > policy.maxRetryAfterMs) throw outcome.error;
    const backoff = Math.min(policy.maxDelayMs, policy.baseDelayMs * 2 ** (attempt - 1));
    const jittered = Math.floor(backoff * (0.5 + 0.5 * shell.random()));
    const delayMs = Math.max(jittered, retryAfterMs ?? 0);
    shell.retrying({ attempt, attempts: policy.attempts, delayMs, reason: outcome.reason });
    await shell.sleep(delayMs);
  }
}

/**
 * The IMF-fixdate form (`Fri, 23 Oct 2026 12:00:30 GMT`), the one servers send; `Date.parse` alone also reads strings
 * like `1.5` as a date. The obsolete RFC 850 and asctime forms read as no hint, which only costs the backoff's wait.
 */
const HTTP_DATE = /^[A-Z][a-z]{2}, \d{2} [A-Z][a-z]{2} \d{4} \d{2}:\d{2}:\d{2} GMT$/;

/** A `Retry-After` header as milliseconds: delay-seconds or an HTTP date. Absent or unreadable is no hint. */
export function parseRetryAfter(header: string | null, nowMs: number): number | undefined {
  if (header === null) return undefined;
  if (/^\d+$/.test(header)) return Number(header) * 1000;
  if (!HTTP_DATE.test(header)) return undefined;
  const date = Date.parse(header);
  return Number.isNaN(date) ? undefined : Math.max(0, date - nowMs);
}
