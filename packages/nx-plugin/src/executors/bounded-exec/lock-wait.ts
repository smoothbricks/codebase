/**
 * Lock-wait evidence from the open-file table of the runner's tree. Pure: no I/O and no clock. `hang-diagnostics.ts`
 * runs `lsof -p` on the tree's pids and hands its output here, so every decision is a function of the rows it was
 * given.
 *
 * A wait is open-handle evidence, not a kernel lock owner. flock(2) and fcntl(2) name no holder, and who else on the
 * host holds a file open is a question only a census of every process answers, which the snapshot never takes: a
 * process stalled in the kernel holds such a census with it. So a wait is named only where the tree shows one, a
 * regular file two or more of its processes hold on numbered descriptors, and a holder outside the tree is not looked
 * up. Nothing here observes the lock call itself; a waiter's stack sample does.
 */

/** One row of an `lsof -nP -p ...` table: a file a process holds open. */
export interface OpenFile {
  readonly pid: number;
  readonly fd: string;
  readonly type: string;
  readonly path: string;
}

/**
 * A regular file two or more processes of the runner's tree hold on numbered descriptors: one may hold its lock while
 * another waits on it. `holders` are those pids, ascending; which of them holds the lock the kernel does not say.
 */
export interface LockWait {
  readonly path: string;
  readonly holders: readonly number[];
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

/**
 * The lock waits in `tree`, the open files of the runner's tree: each lockable path two or more of its processes hold.
 * Sorted by path, holders ascending.
 */
export function lockWaits(tree: readonly OpenFile[]): LockWait[] {
  const waits: LockWait[] = [];
  for (const path of lockCandidates(tree)) {
    const holders = [...new Set(tree.filter((row) => isLockable(row) && row.path === path).map((row) => row.pid))];
    if (holders.length < 2) continue;
    waits.push({ path, holders: holders.sort((a, b) => a - b) });
  }
  return waits;
}

/** A wait as a reader names it: `/path open by pid 7, 9`. */
export function describeLockWait(wait: LockWait): string {
  return `${wait.path} open by pid ${wait.holders.join(', ')}`;
}
