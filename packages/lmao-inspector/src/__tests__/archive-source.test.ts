import { afterEach, describe, expect, it } from 'bun:test';
import { REMOTE_HTTP_MS, SilentError, type Timers } from '@smoothbricks/lmao/errors/Deadline';
import { createArchiveSource } from '../sources/archive-source.js';

/** A clock that moves only when the test fires its armed timers. */
class ManualTimers implements Timers {
  private at = 0;
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

  /** Timers still armed. */
  get live(): number {
    return this.armed.filter((timer) => timer.live).length;
  }

  /** Move the clock by `ms`, firing every timer that comes due, earliest first. */
  advance(ms: number): void {
    const until = this.at + ms;
    for (;;) {
      const next = this.armed.filter((timer) => timer.live && timer.due <= until).sort((a, b) => a.due - b.due)[0];
      if (next === undefined) break;
      this.at = next.due;
      next.live = false;
      next.fire();
    }
    this.at = until;
  }
}

let stop: (() => void) | null = null;
afterEach(() => {
  stop?.();
  stop = null;
});

/** A stand-in archive that answers its head and `first`, then never writes again. */
function stalledArchive(first: Uint8Array): string {
  const server = Bun.serve({
    port: 0,
    fetch: () =>
      new Response(
        new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(first);
          },
        }),
      ),
  });
  stop = () => server.stop(true);
  return `http://127.0.0.1:${server.port}/archive`;
}

describe('archive source', () => {
  it('ends a read SILENT once the archive has sent nothing for the remote-http bound', async () => {
    const timers = new ManualTimers();
    const baseUrl = stalledArchive(new TextEncoder().encode('not arrow'));
    const batches = createArchiveSource(baseUrl, timers).fetchRange(0, 1)[Symbol.asyncIterator]();
    const first = await batches.next();
    expect(first.done).toBe(false);
    expect(new TextDecoder().decode(first.value ?? new Uint8Array())).toBe('not arrow');
    expect(timers.live).toBe(0);
    const second = batches.next();
    expect(timers.live).toBe(1);
    timers.advance(REMOTE_HTTP_MS);
    const failure = await second.then(
      () => null,
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(SilentError);
    expect(failure instanceof SilentError ? failure.message : '').toBe(
      `SILENT: archive read: archive ${baseUrl} showed no progress for 17s (idle bound 17s); waited 17s in all, last progress: received 9 bytes`,
    );
    expect(timers.live).toBe(0);
  });
});
