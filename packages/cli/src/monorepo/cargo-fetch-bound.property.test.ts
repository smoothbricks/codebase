import { describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import {
  boundedFetchEnv,
  CARGO_FETCH_DEADLINE_ENV,
  CARGO_FETCH_DEADLINE_MS,
  describeDeadline,
  fetchDeadline,
  GIT_HTTP_LOW_SPEED_LIMIT,
  GIT_HTTP_LOW_SPEED_TIME_S,
  judgeBound,
} from '../../../nx-plugin/managed/raw/tooling/direnv/cargo-fetch-bound.ts';

/**
 * The bound on a cargo fetch is decided by pure functions over facts, so these are properties of function calls: no
 * clock, no process, no environment. The shell that measures time and kills is cargo-fetch-run.ts, proved against real
 * processes in cargo-fetch-run.test.ts.
 */

/** Whole milliseconds, so that sums and differences of readings are exact. */
const deadlines = fc.integer({ min: 1, max: 10 * CARGO_FETCH_DEADLINE_MS });
const elapsed = fc.integer({ min: 0, max: 20 * CARGO_FETCH_DEADLINE_MS });
/** What a subtraction of two clock readings can hand over, including what is not a time. */
const anyReading = fc.oneof(
  fc.double({ min: -1e6, max: 1e9, noNaN: true }),
  fc.constantFrom(Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY),
);

describe('the bound on a cargo fetch', () => {
  it('leaves a finished command alone at any time, and judges a running one by its deadline alone', () => {
    fc.assert(
      fc.property(deadlines, elapsed, (deadlineMs, elapsedMs) => {
        expect(judgeBound({ deadlineMs, elapsedMs, finished: true })).toEqual({ kind: 'finished' });
        expect(judgeBound({ deadlineMs, elapsedMs, finished: false })).toEqual(
          elapsedMs < deadlineMs ? { kind: 'wait', forMs: deadlineMs - elapsedMs } : { kind: 'kill' },
        );
      }),
    );
  });

  it('never asks for a wait that is not positive and finite, whatever the facts', () => {
    fc.assert(
      fc.property(anyReading, anyReading, fc.boolean(), (deadlineMs, elapsedMs, finished) => {
        const verdict = judgeBound({ deadlineMs, elapsedMs, finished });
        if (verdict.kind === 'wait') {
          expect(Number.isFinite(verdict.forMs)).toBe(true);
          expect(verdict.forMs).toBeGreaterThan(0);
          expect(verdict.forMs).toBe(deadlineMs - elapsedMs);
        }
      }),
    );
  });

  it('kills at the first wake no earlier than the wait asked for, and at every later reading', () => {
    fc.assert(
      fc.property(deadlines, elapsed, elapsed, (deadlineMs, startMs, lateness) => {
        const running = (elapsedMs: number) => judgeBound({ deadlineMs, elapsedMs, finished: false });
        const first = running(startMs);
        const woken = first.kind === 'wait' ? startMs + first.forMs + lateness : startMs + lateness;
        expect(running(woken)).toEqual({ kind: 'kill' });
        expect(running(woken + lateness + 1)).toEqual({ kind: 'kill' });
      }),
    );
  });

  it('takes the default for no variable, and else only a positive whole number of milliseconds that shortens it', () => {
    expect(fetchDeadline(undefined)).toEqual({ kind: 'deadline', ms: CARGO_FETCH_DEADLINE_MS });
    // The edges of the range: the default itself is allowed, one millisecond more lengthens it, and what is not a
    // positive whole number in its plain spelling is refused.
    for (const [raw, kind] of [
      [String(CARGO_FETCH_DEADLINE_MS), 'deadline'],
      [String(CARGO_FETCH_DEADLINE_MS + 1), 'refused'],
      ['1', 'deadline'],
      ['0', 'refused'],
      ['', 'refused'],
      ['007', 'refused'],
    ] as const) {
      expect({ raw, kind: fetchDeadline(raw).kind }).toEqual({ raw, kind });
    }
    const spellings = fc.oneof(fc.string(), fc.stringMatching(/^[0-9]{1,8}$/));
    fc.assert(
      fc.property(spellings, (raw) => {
        const result = fetchDeadline(raw);
        if (result.kind === 'deadline') {
          expect(result.ms).toBeGreaterThan(0);
          expect(result.ms).toBeLessThanOrEqual(CARGO_FETCH_DEADLINE_MS);
          expect(String(result.ms)).toBe(raw);
        } else {
          expect(result.message).toContain(CARGO_FETCH_DEADLINE_ENV);
        }
      }),
    );
    fc.assert(
      fc.property(deadlines, (ms) => {
        expect(fetchDeadline(String(ms)).kind).toBe(ms <= CARGO_FETCH_DEADLINE_MS ? 'deadline' : 'refused');
      }),
    );
  });

  it('gives git its low-speed limits in the fetch environment and changes nothing else in it', () => {
    const names = fc.string({ minLength: 1 }).filter((name) => !name.includes('=') && !name.includes('\0'));
    const environment = fc.dictionary(names, fc.option(fc.string(), { nil: undefined }));
    const limits = ['GIT_HTTP_LOW_SPEED_LIMIT', 'GIT_HTTP_LOW_SPEED_TIME'];
    fc.assert(
      fc.property(environment, (env) => {
        const bounded = boundedFetchEnv(env);
        expect(bounded.GIT_HTTP_LOW_SPEED_LIMIT).toBe(String(GIT_HTTP_LOW_SPEED_LIMIT));
        expect(bounded.GIT_HTTP_LOW_SPEED_TIME).toBe(String(GIT_HTTP_LOW_SPEED_TIME_S));
        const others = (entries: Array<[string, string | undefined]>) =>
          entries.filter(([name, value]) => value !== undefined && !limits.includes(name));
        expect(others(Object.entries(bounded))).toEqual(others(Object.entries(env)));
        // A variable the caller left undefined stays out, as a string value only: the spawn takes no other.
        expect(Object.values(bounded).every((value) => typeof value === 'string')).toBe(true);
      }),
      // A variable called `__proto__` is an own property like any other; assigning it by name would drop it.
      { examples: [[Object.fromEntries([['__proto__', 'carried']])]] },
    );
  });

  it('names the command, the lock and both times in the failure of a command past its deadline', () => {
    fc.assert(
      fc.property(
        fc.string({ minLength: 1 }),
        fc.string({ minLength: 1 }),
        deadlines,
        fc.nat(),
        (command, lockPath, deadlineMs, elapsedMs) => {
          const message = describeDeadline({ command, lockPath, deadlineMs, elapsedMs });
          for (const fact of [command, lockPath, String(deadlineMs), String(elapsedMs)]) {
            expect(message).toContain(fact);
          }
        },
      ),
    );
  });
});
