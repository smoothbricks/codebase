import { describe, expect, it } from 'bun:test';
import { createTestTracerOptions, requireStringArray } from '../../__tests__/test-helpers.js';
import { defineOpContext } from '../../defineOpContext.js';
import { resolveEntryType } from '../../resolveMessage.js';
import { Err, Ok, type Result } from '../../result.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import { ENTRY_TYPE_SPAN_ERR, ENTRY_TYPE_SPAN_RETRY } from '../../schema/systemSchema.js';
import { TestTracer } from '../../tracers/TestTracer.js';
import { iterateSpanChildren } from '../../traceTopology.js';
import {
  type Bound,
  CONNECTED,
  Deadline,
  deadline,
  Idle,
  idle,
  PeerLostError,
  type Progress,
  received,
  SilentError,
  sent,
  TimeoutError,
  type Timers,
} from '../Deadline.js';

/** A clock and timers that move only when the test moves them. */
class ManualTimers implements Timers {
  private at = 1_000;
  private readonly armed: { due: number; fire: () => void; live: boolean }[] = [];

  now(): number {
    return this.at;
  }

  after(ms: number, fire: () => void): () => void {
    const timer = { due: this.at + ms, fire, live: true };
    this.armed.push(timer);
    return () => {
      timer.live = false;
    };
  }

  /** Move the clock by `ms` with no timer armed: the pure values' tests. */
  move(ms: number): void {
    this.at += ms;
  }

  /** Timers still armed. */
  get live(): number {
    return this.armed.filter((timer) => timer.live).length;
  }

  /** Move the clock by `ms`, firing every timer that comes due, earliest first, then let the settled promises run. */
  async advance(ms: number): Promise<void> {
    const until = this.at + ms;
    for (;;) {
      const next = this.armed.filter((timer) => timer.live && timer.due <= until).sort((a, b) => a.due - b.due)[0];
      if (next === undefined) break;
      this.at = next.due;
      next.live = false;
      next.fire();
      await settle();
    }
    this.at = until;
    await settle();
  }
}

/** Let every promise continuation queued so far run. */
async function settle(): Promise<void> {
  for (let turn = 0; turn < 8; turn += 1) await Promise.resolve();
}

/** The failure `result` carries, which must be a `Class`: a test that got anything else fails naming it. */
function failureOf<C>(result: Result<unknown, unknown>, Class: abstract new (...args: never[]) => C): C {
  if (result.isOk() || !(result.error instanceof Class)) {
    throw new Error(`expected ${Class.name}, got ${JSON.stringify(result)}`);
  }
  return result.error;
}

const CONTROL: Bound = { class: 'control', withinMs: 5_000 };

/** A body whose peer never answers: it settles only when its wait is aborted, as a socket destroyed on abort does. */
function silentPeer(signal: AbortSignal): Promise<Result<string, never>> {
  return new Promise((_, reject) => signal.addEventListener('abort', () => reject(signal.reason)));
}

describe('Deadline and Idle values', () => {
  it('spends one budget from its start', () => {
    const timers = new ManualTimers();
    const budget = Deadline.start('status', CONTROL, timers);
    const seen: (number | null)[] = [];
    for (const step of [0, 2_000, 2_999, 1, 60_000]) {
      timers.move(step);
      seen.push(budget.remaining(timers));
    }
    expect(seen).toEqual([5_000, 3_000, 1, null, null]);
    expect(budget.elapsed(timers)).toBe(65_000);
  });

  it('names op, peer, class, bound, elapsed and last progress on expiry', () => {
    const timers = new ManualTimers();
    const budget = Deadline.start('status', CONTROL, timers);
    timers.move(5_000);
    const expired = budget.expired('gateway /run/control.sock', { kind: 'sent', bytes: 18 }, timers);
    expect(expired.data).toEqual({
      op: 'status',
      peer: 'gateway /run/control.sock',
      class: 'control',
      boundMs: 5_000,
      elapsedMs: 5_000,
      last: { kind: 'sent', bytes: 18 },
    });
    expect(expired.code).toBe('TIMEOUT');
    expect(expired.message).toBe(
      'TIMEOUT: status on gateway /run/control.sock: no answer within its control bound of 5000ms; waited 5000ms, last progress: sent 18 bytes',
    );
  });

  it('restarts the silence on progress, never the total', () => {
    const timers = new ManualTimers();
    const silence = Idle.start('job terminal', 15_000, timers);
    const steps: [number, Progress | null, number | null][] = [
      [10_000, null, 5_000],
      [0, { kind: 'heartbeat', seq: 1 }, 15_000],
      [14_000, { kind: 'heartbeat', seq: 2 }, 15_000],
      [15_000, null, null],
    ];
    for (const [step, progress, remaining] of steps) {
      timers.move(step);
      if (progress !== null) silence.observe(progress, timers);
      expect(silence.remaining(timers)).toBe(remaining);
    }
    const silent = silence.silent('supervisor pid 81200', timers);
    expect([silent.data.silentForMs, silent.data.elapsedMs, silent.data.last]).toEqual([
      15_000,
      39_000,
      { kind: 'heartbeat', seq: 2 },
    ]);
    expect(silent.code).toBe('SILENT');
    expect(silent.message).toBe(
      'SILENT: job terminal: supervisor pid 81200 showed no progress for 15000ms (idle bound 15000ms); waited 39000ms in all, last progress: heartbeat 2',
    );
  });

  it('accumulates a stream counter within one direction', () => {
    const progress = sent(sent(CONNECTED, 10), 8);
    expect(progress).toEqual({ kind: 'sent', bytes: 18 });
    expect(received(received(progress, 4), 1)).toEqual({ kind: 'received', bytes: 5 });
  });
});

describe('deadline()', () => {
  it("passes the body's answer through and disarms its timer", async () => {
    const timers = new ManualTimers();
    const answer = await deadline('status', 'gateway', CONTROL, async () => new Ok('up'), timers);
    expect(answer.isOk() && answer.value).toBe('up');
    expect(timers.live).toBe(0);
  });

  it("answers TIMEOUT at the bound, aborts the body's signal with it, and never hears the body again", async () => {
    const timers = new ManualTimers();
    let signal: AbortSignal | undefined;
    const pending = deadline(
      'status',
      'gateway /run/control.sock',
      CONTROL,
      (wait) => {
        signal = wait.signal;
        wait.progress({ kind: 'sent', bytes: 18 });
        return silentPeer(wait.signal);
      },
      timers,
    );
    await timers.advance(4_999);
    expect(signal?.aborted).toBe(false);
    await timers.advance(1);
    const answer = await pending;
    const failure = failureOf(answer, TimeoutError);
    expect(failure.data).toEqual({
      op: 'status',
      peer: 'gateway /run/control.sock',
      class: 'control',
      boundMs: 5_000,
      elapsedMs: 5_000,
      last: { kind: 'sent', bytes: 18 },
    });
    expect(signal?.aborted).toBe(true);
    expect(signal?.reason).toBe(failure);
  });

  it("returns the body's PEER_LOST, naming the time waited and the last progress", async () => {
    const timers = new ManualTimers();
    let close: () => void = () => {};
    const pending = deadline(
      'status',
      'gateway',
      CONTROL,
      (wait) =>
        new Promise<Result<string, PeerLostError>>((resolve) => {
          wait.progress(CONNECTED);
          close = () => resolve(new Err(wait.lost()));
        }),
      timers,
    );
    await timers.advance(1_200);
    close();
    const answer = await pending;
    const failure = failureOf(answer, PeerLostError);
    expect(failure.message).toBe(
      'PEER_LOST: status on gateway: the peer ended the connection after 1200ms; last progress: connected',
    );
    expect(timers.live).toBe(0);
  });

  it('never starts a body whose budget is already spent', async () => {
    const timers = new ManualTimers();
    let started = false;
    const answer = await deadline(
      'status',
      'gateway',
      { class: 'control', withinMs: 0 },
      async () => {
        started = true;
        return new Ok(1);
      },
      timers,
    );
    expect(started).toBe(false);
    expect(failureOf(answer, TimeoutError).code).toBe('TIMEOUT');
  });

  it('rejects with a body that throws before its bound, as a bug and not a wait failure', async () => {
    const timers = new ManualTimers();
    const failure = new Error('bug');
    const pending = deadline('status', 'gateway', CONTROL, () => Promise.reject(failure), timers);
    await Promise.resolve(expect(pending).rejects.toBe(failure));
    expect(timers.live).toBe(0);
  });
});

describe('idle()', () => {
  it('waits as long as the holder shows progress, and answers SILENT once it stops', async () => {
    const timers = new ManualTimers();
    let beat: (seq: number) => void = () => {};
    const pending = idle(
      'job terminal',
      'supervisor pid 81200',
      15_000,
      (wait) => {
        beat = (seq) => wait.progress({ kind: 'heartbeat', seq });
        return silentPeer(wait.signal);
      },
      timers,
    );
    for (let seq = 1; seq <= 20; seq += 1) {
      await timers.advance(5_000);
      beat(seq);
    }
    await timers.advance(14_999);
    let done = false;
    void pending.then(() => {
      done = true;
    });
    await settle();
    expect(done).toBe(false);
    await timers.advance(1);
    const answer = await pending;
    const failure = failureOf(answer, SilentError);
    expect([failure.data.silentForMs, failure.data.elapsedMs, failure.data.last]).toEqual([
      15_000,
      115_000,
      { kind: 'heartbeat', seq: 20 },
    ]);
  });
});

describe('idle() silence bound', () => {
  it('holds a holder to a shorter silence once it has shown its heartbeat, from its last progress', async () => {
    const timers = new ManualTimers();
    let heartbeat: () => void = () => {};
    const pending = idle(
      'gateway answer',
      'gateway',
      60_000,
      (wait) => {
        heartbeat = () => {
          wait.progress({ kind: 'heartbeat', seq: 1 });
          wait.setSilence(15_000);
        };
        return silentPeer(wait.signal);
      },
      timers,
    );
    await timers.advance(40_000);
    heartbeat();
    await timers.advance(14_999);
    let done = false;
    void pending.then(() => {
      done = true;
    });
    await settle();
    expect(done).toBe(false);
    await timers.advance(1);
    const answer = await pending;
    const failure = failureOf(answer, SilentError);
    expect([failure.data.idleMs, failure.data.silentForMs, failure.data.elapsedMs]).toEqual([15_000, 15_000, 55_000]);
  });
});

describe('an Op that returns a wait failure', () => {
  const opContext = defineOpContext({ logSchema: defineLogSchema({ peer: S.category() }) });

  it('ends its span span-err with the code, and the span never retries it', async () => {
    const timers = new ManualTimers();
    const tracer = new TestTracer(opContext, createTestTracerOptions());
    const traced = tracer.trace('root', async (ctx) => {
      const status = await ctx.span('status', async (span) => {
        const answer = await deadline('status', 'gateway', CONTROL, (wait) => silentPeer(wait.signal), timers);
        return answer.isErr() ? span.err(answer.error) : span.ok(answer.value);
      });
      return status.isErr() ? ctx.err(status.error) : ctx.ok(status.value);
    });
    await timers.advance(5_000);
    const result = await traced;
    expect(failureOf(result, TimeoutError).code).toBe('TIMEOUT');
    const [span] = iterateSpanChildren(tracer.rootBuffers[0]);
    if (span === undefined) throw new Error('expected the status span');
    expect(resolveEntryType(span, 1)).toBe(ENTRY_TYPE_SPAN_ERR);
    expect(requireStringArray(span.getColumnIfAllocated('error_code'), 'error codes')[1]).toBe('TIMEOUT');
    const rows = Array.from({ length: span._writeIndex }, (_, row) => resolveEntryType(span, row));
    expect(rows).not.toContain(ENTRY_TYPE_SPAN_RETRY);
  });
});
