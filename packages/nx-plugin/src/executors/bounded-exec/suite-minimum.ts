/**
 * The minimum a whole-suite test leaf names on its reservation (specs/cowshed/05_gateway.md, "A minimum for a whole
 * suite"): "the fewest threads at which its last recorded test walls, longest first, never leave 180 s without a test
 * finishing". The 180 s there is the leaf's own no-progress bound, `idleMs`.
 *
 * A grant never grows, so a suite that starts on one core runs every test in turn for its whole life: a suite of
 * minutes-long cases granted one core beside seventeen short leaves ran 21 minutes where its longest case is 8.5.
 */

import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { TEST_RESULTS_FILE } from './test-results.js';

/**
 * Each test's wall as the task's last run closed it, by the test's id, from its results in `directory`; none for a
 * task that has not run, whose results are gone or cannot be read. They size a request and order a leaf's cases at its
 * gate (case-gate.ts), never a charge: a leaf with none names no minimum and starts as it did before it had any.
 */
export async function readLastWalls(directory: string): Promise<Map<string, number>> {
  let text: string;
  try {
    text = await readFile(join(directory, TEST_RESULTS_FILE), 'utf8');
  } catch {
    return new Map();
  }
  const walls = new Map<string, number>();
  for (const line of text.split('\n')) {
    if (line === '') continue;
    let row: unknown;
    try {
      row = JSON.parse(line);
    } catch {
      return new Map();
    }
    if (typeof row !== 'object' || row === null || !('durationMs' in row) || !('id' in row)) continue;
    const { id, durationMs: wall } = row;
    if (typeof id === 'string' && typeof wall === 'number' && Number.isFinite(wall) && wall >= 0) walls.set(id, wall);
  }
  return walls;
}

/** When each test finishes on `threads` threads: longest first, each on the thread that frees first. */
function finishTimes(wallsLongestFirst: readonly number[], threads: number): number[] {
  const free = new Array<number>(threads).fill(0);
  const finished: number[] = [];
  for (const wall of wallsLongestFirst) {
    let soonest = 0;
    for (let thread = 1; thread < threads; thread++) {
      if ((free[thread] ?? 0) < (free[soonest] ?? 0)) soonest = thread;
    }
    const end = (free[soonest] ?? 0) + wall;
    free[soonest] = end;
    finished.push(end);
  }
  return finished.sort((a, b) => a - b);
}

/** Whether no span longer than `idleMs` passes without a test finishing, from the start to the last finish. */
function keepsFinishing(finished: readonly number[], idleMs: number): boolean {
  let last = 0;
  for (const end of finished) {
    if (end - last > idleMs) return false;
    last = end;
  }
  return true;
}

/**
 * The fewest threads, from one to `want`, at which the recorded walls keep a test finishing within every `idleMs`;
 * `want` when none does (a single test longer than the bound finishes within it on no count). One, which is no
 * minimum, for a leaf with no recorded wall.
 */
export function suiteMinimum(recordedWallsMs: readonly number[], want: number, idleMs: number): number {
  if (recordedWallsMs.length === 0) return 1;
  const longestFirst = [...recordedWallsMs].sort((a, b) => b - a);
  const most = Math.min(want, longestFirst.length);
  for (let threads = 1; threads <= most; threads++) {
    if (keepsFinishing(finishTimes(longestFirst, threads), idleMs)) return threads;
  }
  return most;
}
