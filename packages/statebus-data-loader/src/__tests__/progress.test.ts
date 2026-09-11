import { describe, expect, it } from 'bun:test';
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
    const stream = measureByteStream(source(), (sample) => events.push(sample), { direction: 'download' });
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
