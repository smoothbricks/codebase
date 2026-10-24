import { describe, expect, it } from 'bun:test';
import { recordingRetryShell } from './__tests__/recording-retry-shell.js';
import { parseRetryAfter, type RetryPolicy, type RetryStep, retryTransient } from './retry.js';

const POLICY: RetryPolicy = { attempts: 4, baseDelayMs: 1000, maxDelayMs: 5000, maxRetryAfterMs: 5000 };

/** A step answering from a script: a failure each time its entry is an Error, the value otherwise. */
function scripted(answers: Array<Error | { retryAfterMs: number } | string>): {
  step: () => Promise<RetryStep<string>>;
  calls: () => number;
} {
  let calls = 0;
  return {
    calls: () => calls,
    step: async () => {
      const answer = answers[Math.min(calls, answers.length - 1)];
      calls += 1;
      if (typeof answer === 'string') return { kind: 'done', value: answer };
      if (answer instanceof Error) return { kind: 'transient', error: answer, reason: answer.message };
      return { kind: 'transient', error: new Error('asked to wait'), reason: 'asked to wait', ...answer };
    },
  };
}

describe('retryTransient', () => {
  it('returns a first answer without waiting or announcing anything', async () => {
    const { shell, sleeps, retries } = recordingRetryShell();
    const { step, calls } = scripted(['done']);

    await expect(retryTransient(step, POLICY, shell)).resolves.toBe('done');

    expect(calls()).toBe(1);
    expect(sleeps).toEqual([]);
    expect(retries).toEqual([]);
  });

  it('doubles the wait after each transient failure up to the cap and announces every retry', async () => {
    const { shell, sleeps, retries } = recordingRetryShell();
    const { step } = scripted([new Error('first'), new Error('second'), new Error('third'), 'done']);

    await expect(retryTransient(step, { ...POLICY, attempts: 5 }, shell)).resolves.toBe('done');

    expect(sleeps).toEqual([1000, 2000, 4000]);
    expect(retries).toEqual([
      { attempt: 1, attempts: 5, delayMs: 1000, reason: 'first' },
      { attempt: 2, attempts: 5, delayMs: 2000, reason: 'second' },
      { attempt: 3, attempts: 5, delayMs: 4000, reason: 'third' },
    ]);
  });

  it('caps the wait at the policy maximum', async () => {
    const { shell, sleeps } = recordingRetryShell();
    const { step } = scripted([new Error('again')]);

    await retryTransient(step, { ...POLICY, attempts: 5, maxDelayMs: 3000 }, shell).catch(() => undefined);

    expect(sleeps).toEqual([1000, 2000, 3000, 3000]);
  });

  it('lets a server ask for a wait longer than the backoff cap, up to its own ceiling', async () => {
    const { shell, sleeps } = recordingRetryShell();
    const { step } = scripted([{ retryAfterMs: 8000 }, 'done']);

    await retryTransient(step, { ...POLICY, maxDelayMs: 3000, maxRetryAfterMs: 10_000 }, shell);

    expect(sleeps).toEqual([8000]);
  });

  it('spreads each wait between half the backoff and all of it', async () => {
    const waits: number[] = [];
    for (const random of [0, 0.5, 1]) {
      const { shell, sleeps } = recordingRetryShell();
      const { step } = scripted([new Error('once'), 'done']);
      await retryTransient(step, POLICY, { ...shell, random: () => random });
      waits.push(...sleeps);
    }

    expect(waits).toEqual([500, 750, 1000]);
  });

  it('throws the last transient error itself once the attempts are spent, with no wait after the last', async () => {
    const { shell, sleeps } = recordingRetryShell();
    const last = new Error('still down');
    const { step, calls } = scripted([new Error('down'), new Error('down'), new Error('down'), last]);

    const failure = await retryTransient(step, POLICY, shell).then(
      () => null,
      (error: unknown) => error,
    );

    expect(failure).toBe(last);
    expect(calls()).toBe(POLICY.attempts);
    expect(sleeps).toHaveLength(POLICY.attempts - 1);
  });

  it('waits at least as long as the server asked, even when that exceeds the backoff', async () => {
    const { shell, sleeps } = recordingRetryShell();
    const { step } = scripted([{ retryAfterMs: 4000 }, 'done']);

    await retryTransient(step, POLICY, shell);

    expect(sleeps).toEqual([4000]);
  });

  it('keeps the backoff when the server asked for less', async () => {
    const { shell, sleeps } = recordingRetryShell();
    const { step } = scripted([{ retryAfterMs: 10 }, 'done']);

    await retryTransient(step, POLICY, shell);

    expect(sleeps).toEqual([1000]);
  });

  it('stops at once when the server asks for more than the policy allows it to ask', async () => {
    const { shell, sleeps, retries } = recordingRetryShell();
    const { step, calls } = scripted([{ retryAfterMs: POLICY.maxRetryAfterMs + 1 }, 'done']);

    await expect(retryTransient(step, POLICY, shell)).rejects.toThrow('asked to wait');

    expect(calls()).toBe(1);
    expect(sleeps).toEqual([]);
    expect(retries).toEqual([]);
  });

  it('lets a failure the step throws through untouched: only reported transients are retried', async () => {
    const { shell, sleeps } = recordingRetryShell();
    const bug = new TypeError('Invalid URL');

    await expect(
      retryTransient(
        async () => {
          throw bug;
        },
        POLICY,
        shell,
      ),
    ).rejects.toBe(bug);

    expect(sleeps).toEqual([]);
  });
});

describe('parseRetryAfter', () => {
  const NOW = Date.parse('2026-10-23T12:00:00Z');

  it('reads delay-seconds', () => {
    expect(parseRetryAfter('7', NOW)).toBe(7000);
    expect(parseRetryAfter('0', NOW)).toBe(0);
  });

  it('reads an HTTP date relative to now, never negative', () => {
    expect(parseRetryAfter('Fri, 23 Oct 2026 12:00:30 GMT', NOW)).toBe(30_000);
    expect(parseRetryAfter('Fri, 23 Oct 2026 11:59:00 GMT', NOW)).toBe(0);
  });

  it('reads no hint out of an absent or unreadable header', () => {
    expect(parseRetryAfter(null, NOW)).toBeUndefined();
    expect(parseRetryAfter('', NOW)).toBeUndefined();
    expect(parseRetryAfter('soon', NOW)).toBeUndefined();
    expect(parseRetryAfter('-5', NOW)).toBeUndefined();
    expect(parseRetryAfter('1.5', NOW)).toBeUndefined();
    expect(parseRetryAfter('Fri, 99 Oct 2026 12:00:00 GMT', NOW)).toBeUndefined();
  });
});
