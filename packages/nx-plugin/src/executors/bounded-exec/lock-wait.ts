/**
 * Lock-wait evidence: the open-file table of the runner's tree (`lsof -p` on its pids) and the kernel's own lock record
 * of each lockable file in it (procinfo's `lockHolders`). Pure: no I/O and no clock; `hang-diagnostics.ts` reads both
 * and hands them here, so every decision is a function of what it was given.
 *
 * The holder comes from the lock record, never from a walk of the processes that might hold the file open: such a
 * census asks every process on the host, and one stalled in the kernel stalls it, while the lock record answers for
 * the file alone. Linux's record names every lock's process and the processes waiting for it. Darwin's names the first
 * conflicting lock alone, a POSIX holder by its pid and a flock(2) holder by none.
 */
import type { FileLock, LockHolders } from '@smoothbricks/procinfo';

/** One row of an `lsof -nP -p ...` table: a file a process holds open. */
export interface OpenFile {
  readonly pid: number;
  readonly fd: string;
  readonly type: string;
  readonly path: string;
}

/**
 * A lockable file of the run whose lock the kernel's record shows a process of the run waiting for (Linux), or held by
 * a process outside the run or shared among two or more of its processes (Darwin, whose record names no waiter).
 */
export interface LockWait {
  readonly path: string;
  /** The runner's tree's pids that hold the file open on numbered descriptors, ascending. */
  readonly inTree: readonly number[];
  /** The tree's pids the record shows waiting for the lock, ascending; empty where it names no waiter. */
  readonly waiting: readonly number[];
  /** The record's holders: a pid, or `null` for a lock whose holder the record does not name on this platform. */
  readonly holders: readonly (number | null)[];
}

/** COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME; NAME is everything from the ninth column on, spaces included. */
const TABLE_ROW = /^\S+\s+(?<pid>\d+)\s+\S+\s+(?<fd>\S+)\s+(?<type>\S+)\s+\S+\s+\S+\s+\S+\s+(?<path>.+)$/;

/** The rows of an `lsof -nP` table. The header and any row that does not parse are skipped. */
export function parseLsofTable(text: string): OpenFile[] {
  const rows: OpenFile[] = [];
  for (const line of text.split('\n')) {
    const row = toOpenFile(line);
    if (row !== null) rows.push(row);
  }
  return rows;
}

function toOpenFile(line: string): OpenFile | null {
  const groups: Partial<Record<'pid' | 'fd' | 'type' | 'path', string>> = TABLE_ROW.exec(line)?.groups ?? {};
  const { pid, fd, type, path } = groups;
  if (pid === undefined || fd === undefined || type === undefined || path === undefined) return null;
  return { pid: Number(pid), fd, type, path };
}

/**
 * A regular file on a numbered descriptor: the only kind a blocking flock or fcntl can wait on. `cwd`, `txt`, `rtd`,
 * `mem` and the other unnumbered descriptors name files a process runs or maps, not files it locks.
 */
function isLockable(row: OpenFile): boolean {
  return row.type === 'REG' && /^\d/.test(row.fd);
}

/** The distinct lockable paths in `tree`, sorted. */
export function lockCandidates(tree: readonly OpenFile[]): string[] {
  return [...new Set(tree.filter(isLockable).map((row) => row.path))].sort();
}

/** The distinct pids of `locks`, ascending, `null` (an unnamed holder) last. */
function pidsOf(locks: readonly FileLock[]): (number | null)[] {
  const named = [...new Set(locks.flatMap((entry) => (entry.pid === null ? [] : [entry.pid])))].sort((a, b) => a - b);
  return locks.some((entry) => entry.pid === null) ? [...named, null] : named;
}

/**
 * The lock waits of the run: for each lockable path in `tree` whose lock `records` shows held. Where the record names
 * waiters (Linux, whose every lock has a kind), a wait is a process of the tree waiting, and the holders are the
 * record's other entries. Where it names none (Darwin), a wait is a holder outside the tree, or unnamed, or a lock two
 * or more of the tree's processes hold the file for. An unlocked file, or one whose locks were not read, is no wait.
 * Sorted by path.
 */
export function lockWaits(tree: readonly OpenFile[], records: ReadonlyMap<string, LockHolders>): LockWait[] {
  const waits: LockWait[] = [];
  for (const path of lockCandidates(tree)) {
    const record = records.get(path);
    if (record?.state !== 'locked') continue;
    const inTree = [...new Set(tree.filter((row) => isLockable(row) && row.path === path).map((row) => row.pid))].sort(
      (a, b) => a - b,
    );
    if (record.locks.some((entry) => entry.kind !== null)) {
      const waiting = pidsOf(record.locks.filter((entry) => entry.waiting)).filter(
        (pid): pid is number => pid !== null && inTree.includes(pid),
      );
      if (waiting.length === 0) continue;
      waits.push({ path, inTree, waiting, holders: pidsOf(record.locks.filter((entry) => !entry.waiting)) });
      continue;
    }
    const holders = pidsOf(record.locks);
    if (holders.every((pid) => pid !== null && inTree.includes(pid)) && inTree.length < 2) continue;
    waits.push({ path, inTree, waiting: [], holders });
  }
  return waits;
}

/** A wait as a reader names it: the file, the run's processes holding and waiting, and its holder. */
export function describeLockWait(wait: LockWait): string {
  const named = wait.holders.filter((pid): pid is number => pid !== null);
  const held = [
    ...(named.length > 0 ? [`held by pid ${named.join(', ')}`] : []),
    ...(wait.holders.includes(null)
      ? [`locked, and the holder is not named by the kernel's lock record on this platform`]
      : []),
  ].join('; ');
  const waits = wait.waiting.length > 0 ? `, which ${wait.waiting.length > 1 ? 'wait' : 'waits'} for it` : '';
  const run = wait.waiting.length > 0 ? wait.waiting : wait.inTree;
  return `${wait.path}, open in the run by pid ${run.join(', ')}${waits}${held === '' ? '' : `; ${held}`}`;
}
