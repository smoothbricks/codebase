import { afterEach, describe, expect, it, vi } from 'bun:test';
import { mkdtempSync, rmSync, statSync, unlinkSync, utimesSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { type DirectoryEvent, type DirectoryWatch, pollingDirectoryWatch } from '../bun/directory-watch.js';

/** Never elapses: `poll` is driven by advancing the clock, not by waiting. */
const POLL_MS = 20;

type Delivered = [DirectoryEvent, string | null];

const watches: DirectoryWatch[] = [];
const roots: string[] = [];

afterEach(() => {
  for (const watch of watches.splice(0)) watch.close();
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
  vi.useRealTimers();
});

/**
 * A real directory under observation, with the events its listener was handed.
 *
 * The filesystem is real because the seam's whole job is reading it; only the
 * poll interval is faked, so one `poll()` runs per `advance()` and a test never
 * guesses how long a cycle takes.
 */
function observe(): { directory: string; events: Delivered[]; advance: () => void } {
  vi.useFakeTimers();
  const directory = mkdtempSync(join(tmpdir(), 'dirwatch-'));
  roots.push(directory);
  const events: Delivered[] = [];
  watches.push(
    pollingDirectoryWatch(POLL_MS)(
      directory,
      (event, entry) => events.push([event, entry]),
      () => undefined,
    ),
  );
  return { directory, events, advance: () => vi.advanceTimersByTime(POLL_MS) };
}

describe('polling directory watch', () => {
  it('reports an appearing entry as a membership change', () => {
    const { directory, events, advance } = observe();

    writeFileSync(join(directory, 'added.ts'), 'export const a = 1;\n');
    advance();

    expect(events).toEqual([['rename', 'added.ts']]);
  });

  it('reports a rewritten entry as a content change, not a membership change', () => {
    const { directory, events, advance } = observe();
    const file = join(directory, 'edited.ts');
    writeFileSync(file, 'export const a = 1;\n');
    const stamped = statSync(file);
    advance();
    events.length = 0;

    // Same name, same length, so only the timestamp separates these bytes — and
    // how finely is the filesystem's business, not this test's: APFS advances
    // per write, a Linux runner stamped two writes 0.3 ms apart identically and
    // this case failed there. Setting the mtime is what makes the rewrite a
    // rewrite on every filesystem, exactly as an editor's seconds-later save
    // would. A generation proved against the old bytes must not survive it.
    writeFileSync(file, 'export const a = 2;\n');
    utimesSync(file, stamped.atime, new Date(stamped.mtime.getTime() + 2_000));
    advance();

    expect(events).toEqual([['change', 'edited.ts']]);
  });

  it('reports a removed entry as a membership change', () => {
    const { directory, events, advance } = observe();
    const file = join(directory, 'removed.ts');
    writeFileSync(file, 'export const a = 1;\n');
    advance();
    events.length = 0;

    unlinkSync(file);
    advance();

    expect(events).toEqual([['rename', 'removed.ts']]);
  });

  it('reports a vanished directory once, not once per entry it held', () => {
    const { directory, events, advance } = observe();
    writeFileSync(join(directory, 'one.ts'), 'export const a = 1;\n');
    writeFileSync(join(directory, 'two.ts'), 'export const b = 2;\n');
    advance();
    events.length = 0;

    rmSync(directory, { recursive: true, force: true });
    advance();

    expect(events).toEqual([['rename', null]]);
  });

  it('delivers nothing after the watch is closed', () => {
    const { directory, events, advance } = observe();
    for (const watch of watches.splice(0)) watch.close();

    writeFileSync(join(directory, 'ignored.ts'), 'export const a = 1;\n');
    advance();

    expect(events).toEqual([]);
  });
});
