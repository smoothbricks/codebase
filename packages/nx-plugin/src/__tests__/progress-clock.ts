import { jest } from 'bun:test';

/**
 * Runs `run` with the timers faked, so a no-progress bound (`idleTimeoutMs`) expires only when the clock moves, and
 * moves it `stepMs` each time this process writes a chunk of the command's output that `moves` accepts.
 *
 * A real-time window races the command's own start: under load `node` can take longer to print its first line than a
 * test's 400 ms window lasts, and the bound then ends a run that has said nothing yet. Here the window can end only
 * after the command has said what the test is about. bounded-exec re-arms the bound on a finished test before it
 * passes that output on, so a step taken on a chunk measures from that chunk's progress, as a real gap would. The
 * step is taken after the write returns: a bound that fires writes its own message, which must not move the clock.
 *
 * What the run writes to this process's stdout and stderr is read here and not printed; bounded-exec's result keeps
 * it. Returns that result and how far the clock moved.
 */
export async function onProgressClock<T>(
  moves: (chunk: string) => boolean,
  stepMs: number,
  run: () => Promise<T>,
): Promise<{ result: T; movedMs: number }> {
  const stdout = process.stdout.write;
  const stderr = process.stderr.write;
  let movedMs = 0;
  const watch = (chunk: string | Uint8Array): boolean => {
    if (moves(typeof chunk === 'string' ? chunk : Buffer.from(chunk).toString('utf8'))) {
      queueMicrotask(() => {
        movedMs += stepMs;
        jest.advanceTimersByTime(stepMs);
      });
    }
    return true;
  };
  jest.useFakeTimers();
  process.stdout.write = watch;
  process.stderr.write = watch;
  try {
    return { result: await run(), movedMs };
  } finally {
    process.stdout.write = stdout;
    process.stderr.write = stderr;
    jest.useRealTimers();
  }
}
