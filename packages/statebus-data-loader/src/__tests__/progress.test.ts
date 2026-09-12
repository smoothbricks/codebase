import { describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import type { ByteSample } from '../model.js';
import type { TimerPort } from '../ports.js';
import { createByteProgressReporter } from '../progress.js';
import { measureByteStream } from '../stream.js';

function clock() {
  let time = 0;
  const tasks = new Map<() => void, number>();
  const timer: TimerPort = {
    after(delay, callback) {
      tasks.set(callback, time + delay);
      return () => {
        tasks.delete(callback);
      };
    },
  };
  return {
    timer,
    size: () => tasks.size,
    advance(milliseconds: number) {
      time += milliseconds;
      for (const [callback, due] of tasks)
        if (due <= time) {
          tasks.delete(callback);
          callback();
        }
    },
  };
}

describe('periodic byte reporting', () => {
  it('coalesces each direction by interval, flushes the final measurement, and disposes all timers', () => {
    const time = clock();
    const events: ByteSample[] = [];
    const reporter = createByteProgressReporter({
      timer: time.timer,
      intervalMs: 100,
      emit: (sample) => events.push(sample),
    });
    reporter.report({ direction: 'download', transferred: 3 });
    time.advance(50);
    reporter.report({ direction: 'download', transferred: 9 });
    reporter.report({ direction: 'upload', transferred: 2, total: 8 });
    expect(events).toEqual([]);
    time.advance(50);
    expect(events).toEqual([
      { direction: 'upload', transferred: 2, total: 8 },
      { direction: 'download', transferred: 9 },
    ]);
    reporter.report({ direction: 'download', transferred: 10 });
    reporter.flush();
    expect(events.at(-1)).toEqual({ direction: 'download', transferred: 10 });
    expect(time.size()).toBe(0);
    reporter.report({ direction: 'download', transferred: 12 });
    reporter.dispose();
    time.advance(1000);
    reporter.report({ direction: 'download', transferred: 20 });
    expect(events.length).toBe(3);
    expect(time.size()).toBe(0);
  });

  it('counts the actual stream chunks and closes the source on early return', async () => {
    let closed = false;
    const first = new Uint8Array(3);
    const second = new Uint8Array(5);
    async function* source() {
      try {
        yield first;
        yield second;
        yield new Uint8Array(20);
      } finally {
        closed = true;
      }
    }
    const events: ByteSample[] = [];
    const stream = measureByteStream(
      source(),
      (direction, transferred, total) => events.push({ direction, transferred, total }),
      { direction: 'download' },
    );
    expect((await stream.next()).value).toBe(first);
    expect((await stream.next()).value).toBe(second);
    await stream.return();
    expect(events).toEqual([
      { direction: 'download', transferred: 3 },
      { direction: 'download', transferred: 8 },
    ]);
    expect(closed).toBe(true);
  });
});

it('ignores invalid and regressive samples before coalescing and retains immutable published samples', () => {
  const time = clock();
  const events: ByteSample[] = [];
  const reporter = createByteProgressReporter({
    timer: time.timer,
    intervalMs: 100,
    emit: (sample) => events.push(Object.freeze(sample)),
  });
  reporter.reportBytes('download', 100, 200);
  for (const bytes of [80, -1, 0.5, Number.NaN, Number.POSITIVE_INFINITY]) reporter.reportBytes('download', bytes);
  reporter.flush();
  expect(events).toEqual([{ direction: 'download', transferred: 100, total: 200 }]);
  reporter.reportBytes('download', 100, 200);
  reporter.flush();
  expect(time.size()).toBe(0);
  expect(events.length).toBe(1);
  const sample = { direction: 'download' as const, transferred: 200, total: 200 };
  reporter.report(sample);
  sample.transferred = 1;
  reporter.flush();
  expect(events[0]?.transferred).toBe(100);
  expect(events[1]?.transferred).toBe(200);
});

it('keeps reentrant reports for the next flush without emitting an older count after a newer one', () => {
  const time = clock();
  const events: ByteSample[] = [];
  const reporter = createByteProgressReporter({
    timer: time.timer,
    intervalMs: 100,
    emit(sample) {
      events.push(sample);
      if (sample.direction === 'upload') {
        reporter.reportBytes('download', 20);
        reporter.flush();
      }
    },
  });
  reporter.reportBytes('upload', 1);
  reporter.reportBytes('download', 10);
  reporter.flush();
  expect(events.map((s) => s.transferred)).toEqual([1, 10]);
  time.advance(100);
  expect(events.map((s) => s.transferred)).toEqual([1, 10, 20]);
  expect(time.size()).toBe(0);
});

it('stops the second direction when disposed by the first callback', () => {
  const time = clock();
  const events: ByteSample[] = [];
  const reporter = createByteProgressReporter({
    timer: time.timer,
    intervalMs: 100,
    emit(sample) {
      events.push(sample);
      reporter.dispose();
    },
  });
  reporter.reportBytes('upload', 1);
  reporter.reportBytes('download', 2);
  reporter.flush();
  expect(events.length).toBe(1);
  expect(time.size()).toBe(0);
});

it('preserves the greatest actual measurement across arbitrary coalescing boundaries', () => {
  fc.assert(
    fc.property(
      fc.array(fc.record({ bytes: fc.nat({ max: 1000000 }), flush: fc.boolean() }), { maxLength: 200 }),
      (samples) => {
        const time = clock();
        const events: ByteSample[] = [];
        const reporter = createByteProgressReporter({
          timer: time.timer,
          intervalMs: 100,
          emit: (sample) => events.push(sample),
        });
        let greatest = -1;
        for (const sample of samples) {
          greatest = Math.max(greatest, sample.bytes);
          reporter.reportBytes('download', sample.bytes);
          if (sample.flush) time.advance(100);
        }
        reporter.flush();
        if (greatest >= 0) expect(events.at(-1)?.transferred).toBe(greatest);
        for (let i = 1; i < events.length; i += 1)
          expect(events[i].transferred).toBeGreaterThan(events[i - 1].transferred);
        expect(time.size()).toBe(0);
      },
    ),
    { seed: 230923, numRuns: 500 },
  );
});

it('measures a truly empty stream and closes a source when its numeric reporter throws', async () => {
  const events: number[] = [];
  async function* empty() {
    yield* [];
  }
  for await (const _ of measureByteStream(empty(), (_direction, transferred) => events.push(transferred), {
    direction: 'download',
  })) {
    /* empty */
  }
  expect(events).toEqual([0]);
  let closed = false;
  async function* source() {
    try {
      yield new Uint8Array(3);
    } finally {
      closed = true;
    }
  }
  const stream = measureByteStream(
    source(),
    () => {
      throw new Error('consumer failure');
    },
    { direction: 'download' },
  );
  await expect(stream.next()).rejects.toThrow('consumer failure');
  expect(closed).toBe(true);
});
