/**
 * Lock-wait evidence from open-file tables. Pure: no I/O and no clock. `hang-diagnostics.ts` runs `lsof` and hands its
 * output here, so every decision is a function of the rows it was given.
 *
 * A wait is open-handle evidence, not a kernel lock owner. flock(2) and fcntl(2) name no holder, so a regular file the
 * run holds on a numbered descriptor, and that another process also holds open, is the only candidate for the lock a
 * run blocks on. Nothing here observes the lock call itself.
 */

/** One row of an `lsof -nP -p ...` table: a file a process holds open. */
export interface OpenFile {
  readonly pid: number;
  readonly fd: string;
  readonly type: string;
  readonly path: string;
}

/** A process that holds a file open. */
export interface Opener {
  readonly pid: number;
  readonly command: string;
}

/** An opener that `lsof -F` named for one path. */
export interface PathOpener extends Opener {
  readonly path: string;
}

/**
 * A regular file the run holds on a numbered descriptor while processes outside the run also hold it open. `waiters`
 * are the run's pids holding it; `holders` are the outside processes, never a run pid.
 */
export interface LockWait {
  readonly path: string;
  readonly waiters: readonly number[];
  readonly holders: readonly Opener[];
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
 * The lock waits in `tree`, the open files of the run's processes. A path is a wait only when some process outside
 * the run (`treePids` excludes the run) also holds it open. Sorted by path; holders deduplicated by pid and sorted.
 */
export function lockWaits(
  tree: readonly OpenFile[],
  treePids: ReadonlySet<number>,
  openers: readonly PathOpener[],
): LockWait[] {
  const waits: LockWait[] = [];
  for (const path of lockCandidates(tree)) {
    const holders = new Map<number, Opener>();
    for (const opener of openers) {
      if (opener.path === path && !treePids.has(opener.pid) && !holders.has(opener.pid)) {
        holders.set(opener.pid, { pid: opener.pid, command: opener.command });
      }
    }
    if (holders.size === 0) continue;
    const waiters = [...new Set(tree.filter((row) => isLockable(row) && row.path === path).map((row) => row.pid))];
    waits.push({
      path,
      waiters: waiters.sort((a, b) => a - b),
      holders: [...holders.values()].sort((a, b) => a.pid - b.pid),
    });
  }
  return waits;
}

/**
 * Parse `lsof -F pcn` output: `p<pid>` opens a process, `c<command>` names it, `f<fd>` opens a file (lsof always
 * emits one), and `n<path>` names the file. `null` for any other shape: the caller must then call the census
 * incomplete, never empty.
 */
export function parseLsofFields(text: string): PathOpener[] | null {
  const openers: PathOpener[] = [];
  let pid: number | null = null;
  let command: string | null = null;
  for (const line of text.split('\n')) {
    if (line === '') continue;
    const value = line.slice(1);
    switch (line.charAt(0)) {
      case 'p': {
        const next = Number(value);
        if (!Number.isSafeInteger(next) || next <= 0) return null;
        pid = next;
        command = null;
        break;
      }
      case 'c':
        if (pid === null || command !== null) return null;
        command = value;
        break;
      case 'f':
        if (pid === null) return null;
        break;
      case 'n':
        if (pid === null || command === null) return null;
        openers.push({ pid, command, path: value });
        break;
      default:
        return null;
    }
  }
  return openers;
}

/** The holders as a reader names them: `pid 51234 (cargo), pid 9 (rustc)`. */
export function describeHolders(holders: readonly Opener[]): string {
  return holders.map(({ pid, command }) => `pid ${pid} (${command})`).join(', ');
}
