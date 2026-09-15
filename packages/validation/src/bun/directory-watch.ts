/**
 * A polling directory-watch seam for ttsc's transform cache under Bun.
 *
 * WHY this exists rather than ttsc's own `fs.watch` fallback: under Bun 1.4.2
 * on macOS every `fs.watch` registration after the first blocks for seconds.
 * Measured here, five directory watchers in one process: 2 ms, 49,711 ms,
 * 8,018 ms, 8,729 ms, 11,384 ms — 77,844 ms in total, against 0.57 ms for the
 * same five through `fs.watchFile`, and 0.7 ms for twelve under Node 24.
 * `{ recursive: true }`, which would need one registration, registers in
 * 2.2 ms and then delivers no events at all. ttsc opens one watcher per project
 * directory and per host-input directory to prove a transform generation is
 * reusable, so a package with dozens of directories paid minutes of blocking
 * time per compile before a single module was delivered.
 *
 * `fs.watchFile` alone is not the fix: it registers in 0.6 ms and does observe
 * directory membership, but it reports a stat transition rather than the entry
 * that moved, and ttsc filters host-input events by entry name. A nameless
 * event is recorded as a mutation of the whole directory, so unrelated writes
 * would manufacture unstable generations. This polls entries instead and
 * synthesizes the same `(event, entry)` pairs `fs.watch` would have delivered.
 *
 * One interval serves every observed directory, and it is unrefed so a watch
 * never keeps a `bun test` process alive. Fidelity differs from a working
 * `fs.watch` in one way worth naming: a mutation made and reverted entirely
 * between two polls is invisible here, where an event-based watcher would have
 * reported it. ttsc's hash comparison covers the surviving state either way.
 */

import { readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';

/**
 * ttsc's watch-seam event kinds: `rename` is a membership change (an entry
 * appeared or disappeared), `change` is a content change to an entry that
 * stayed. `null` means the seam could not name the entry.
 */
export type DirectoryEvent = 'rename' | 'change';

/** What ttsc's cache hands the seam for one directory. */
export type DirectoryListener = (event: DirectoryEvent, entry: string | null) => void;

/** The handle ttsc's cache expects back, so it can stop observing. */
export interface DirectoryWatch {
  close(): void;
}

/** The seam ttsc's transform cache calls to observe one directory. */
export type DirectoryWatchSeam = (
  directory: string,
  listener: DirectoryListener,
  onError: (error: unknown) => void,
) => DirectoryWatch;

/** One directory's polled state: entry name to content stamp. */
type Entries = Record<string, string>;

/** A directory under observation, and the state its listener was last told. */
interface Observed {
  readonly directory: string;
  readonly listener: DirectoryListener;
  entries: Entries | undefined;
}

/**
 * Read one directory's entries with a stamp that changes on any write.
 *
 * Inode and mode catch replacement and permission flips, size catches a
 * truncation, and nanosecond mtime catches an in-place rewrite inside the same
 * millisecond — which is exactly the write a generation proof must not miss.
 * `statSync` with `bigint` is the only stat carrying nanoseconds.
 *
 * Returns `undefined` when the directory itself is gone.
 */
function readEntries(directory: string): Entries | undefined {
  let names: readonly string[];
  try {
    names = readdirSync(directory);
  } catch {
    return undefined;
  }

  const entries: Entries = {};
  for (const name of names) {
    try {
      const stats = statSync(join(directory, name), { bigint: true });
      entries[name] = `${stats.ino}:${stats.mode}:${stats.size}:${stats.mtimeNs}`;
    } catch {
      // An entry that vanished between readdir and stat is a membership change
      // the next cycle reports. Recording no stamp keeps this cycle honest.
    }
  }
  return entries;
}

/** Build a seam that re-reads every observed directory every `intervalMs`. */
export function pollingDirectoryWatch(intervalMs: number): DirectoryWatchSeam {
  const observed = new Set<Observed>();
  let timer: ReturnType<typeof setInterval> | undefined;

  const poll = (): void => {
    for (const entry of observed) {
      const current = readEntries(entry.directory);
      const previous = entry.entries;
      entry.entries = current;
      if (current === undefined || previous === undefined) {
        // The directory appeared or disappeared. That is one membership change
        // for the directory, not one per entry it held or now holds.
        if (current !== previous) entry.listener('rename', null);
        continue;
      }

      for (const name of Object.keys(current)) {
        const before = previous[name];
        if (before === undefined) entry.listener('rename', name);
        else if (before !== current[name]) entry.listener('change', name);
      }
      for (const name of Object.keys(previous)) {
        if (current[name] === undefined) entry.listener('rename', name);
      }
    }
  };

  return (directory, listener) => {
    const entry: Observed = { directory, entries: readEntries(directory), listener };
    observed.add(entry);
    if (timer === undefined) {
      timer = setInterval(poll, intervalMs);
      timer.unref();
    }

    return {
      close: () => {
        observed.delete(entry);
        if (observed.size === 0 && timer !== undefined) {
          clearInterval(timer);
          timer = undefined;
        }
      },
    };
  };
}
