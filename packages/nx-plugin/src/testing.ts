/**
 * Ownership and retirement of the temp workspaces a test suite runs real Nx
 * in, shared by every package whose tests do.
 *
 * A fixture that runs Nx with its daemon on leaves a daemon whose working
 * directory is the fixture: it idles, watches files and burns CPU until
 * someone stops it. A test that throws stops it in its `finally`; a test
 * process that is killed runs no `finally`. So every fixture root lives in a
 * run directory named for the test process that owns it,
 * `<tmpdir>/smoothbricks-fixtures/<suite>/run-<pid>`, and the first fixture a
 * process creates for a suite reclaims every run of that suite whose owner is
 * gone: each process working in it is stopped, then the run is deleted. A run
 * whose owner still exists is never touched, so nothing outside a dead run of
 * the same suite is ever signalled.
 *
 * Node APIs only: the module is built with the library and imported by Bun
 * tests in other packages.
 */
import { execFile } from 'node:child_process';
import { mkdir, mkdtemp, readdir, readFile, readlink, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir, userInfo } from 'node:os';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { promisify } from 'node:util';

const run = promisify(execFile);
const EXIT_TIMEOUT_MS = 10_000;
const FIXTURE_HOME = 'smoothbricks-fixtures';
const RUN_NAME = /^run-(\d+)$/;
const SWEEP_LOCK = 'sweep.lock';

/** One process as `ps` lists it. */
export interface ProcessEntry {
  readonly pid: number;
  readonly ppid: number;
  /** `ps` state; a leading `Z` is a zombie, which has exited. */
  readonly stat: string;
  readonly command: string;
}

/** Every process on the host, as `ps` lists it on macOS and Linux alike. */
export async function processTable(): Promise<ProcessEntry[]> {
  const { stdout } = await run('ps', ['-A', '-o', 'pid=', '-o', 'ppid=', '-o', 'stat=', '-o', 'command='], {
    maxBuffer: 64 * 1024 * 1024,
  });
  return stdout
    .split('\n')
    .filter((line) => line.trim() !== '')
    .map((line) => {
      const match = /^\s*(\d+)\s+(\d+)\s+(\S+)\s?(.*)$/.exec(line);
      if (match === null) {
        throw new Error(`Unable to parse ps line: ${line}`);
      }
      const [, pid, ppid, stat, command] = match;
      return { pid: Number(pid), ppid: Number(ppid), stat: stat ?? '', command: command ?? '' };
    });
}

/** The listed processes among `pids`, and every process descending from one. */
export function withDescendants(table: readonly ProcessEntry[], pids: readonly number[]): ProcessEntry[] {
  const found: ProcessEntry[] = [];
  const pending = table.filter((entry) => pids.includes(entry.pid));
  for (let entry = pending.pop(); entry !== undefined; entry = pending.pop()) {
    found.push(entry);
    const parent = entry.pid;
    pending.push(...table.filter((child) => child.ppid === parent));
  }
  return found;
}

function errorCode(error: unknown): unknown {
  return error instanceof Error && 'code' in error ? error.code : undefined;
}

/** Whether `pid` names a process, this user's or not. A zombie still answers. */
export function isRunning(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    switch (errorCode(error)) {
      case 'ESRCH':
        return false;
      case 'EPERM':
        return true;
      default:
        throw error;
    }
  }
}

/** Signal `pid`; one that has already exited is not an error. */
export function terminate(pid: number, signal: NodeJS.Signals = 'SIGTERM'): void {
  try {
    process.kill(pid, signal);
  } catch (error) {
    if (errorCode(error) !== 'ESRCH') {
      throw error;
    }
  }
}

/**
 * Wait until every one of `processes` has exited. One still running
 * {@link EXIT_TIMEOUT_MS} later is an error naming it and `owner`.
 */
export async function awaitExit(processes: readonly ProcessEntry[], owner: string): Promise<void> {
  const deadline = Date.now() + EXIT_TIMEOUT_MS;
  for (let waiting = processes.filter((entry) => isRunning(entry.pid)); waiting.length > 0; ) {
    if (Date.now() > deadline) {
      // A signal probe also answers for a zombie, which has already exited.
      const live = new Set(
        (await processTable()).filter((entry) => !entry.stat.startsWith('Z')).map((entry) => entry.pid),
      );
      const survivors = waiting.filter((entry) => live.has(entry.pid));
      if (survivors.length === 0) {
        return;
      }
      throw new Error(
        `${owner}: ${survivors.map((entry) => `${entry.pid} (${entry.command})`).join(', ')} still running ${EXIT_TIMEOUT_MS}ms after it was stopped`,
      );
    }
    await sleep(50);
    waiting = waiting.filter((entry) => isRunning(entry.pid));
  }
}

/** Every process of this user whose working directory is `root` or lies beneath it, deleted or not. */
export async function pidsWorkingIn(root: string): Promise<number[]> {
  const under = (cwd: string) => cwd === root || cwd.startsWith(`${root}/`);
  switch (process.platform) {
    case 'linux': {
      const found: number[] = [];
      for (const pid of (await readdir('/proc')).filter((name) => /^\d+$/.test(name))) {
        const cwd = await readlink(`/proc/${pid}/cwd`).catch((error: unknown) => {
          // Another user's process, or one that exited since the listing.
          if (errorCode(error) === 'EACCES' || errorCode(error) === 'ENOENT') {
            return null;
          }
          throw error;
        });
        if (cwd !== null && under(cwd.replace(/ \(deleted\)$/, ''))) {
          found.push(Number(pid));
        }
      }
      return found;
    }
    case 'darwin': {
      // lsof keeps naming a working directory after it has been deleted. It
      // exits 1 when a listed process vanished before it was read; what it
      // printed for the rest is still the answer.
      const fields = await run('lsof', ['-nP', '-a', '-d', 'cwd', '-u', String(userInfo().uid), '-Fpn'], {
        maxBuffer: 64 * 1024 * 1024,
      }).then(
        ({ stdout }) => stdout,
        (error: unknown) => {
          if (error instanceof Error && 'code' in error && error.code === 1 && 'stdout' in error) {
            return String(error.stdout);
          }
          throw error;
        },
      );
      const found: number[] = [];
      let pid = 0;
      for (const line of fields.split('\n')) {
        if (line.startsWith('p')) {
          pid = Number(line.slice(1));
        } else if (line.startsWith('n') && under(line.slice(1))) {
          found.push(pid);
        }
      }
      return found;
    }
    default:
      throw new Error(`No working-directory scan for ${process.platform}`);
  }
}

/**
 * The Nx daemon `workspace` runs, the daemon first, then every process it
 * started. Empty when the workspace records no daemon or the recorded one is
 * gone. The record is `d/server-process.json` under the workspace's own
 * `.nx/workspace-data`, where every fixture keeps its Nx state.
 */
export async function nxDaemonProcesses(workspace: string): Promise<ProcessEntry[]> {
  const record = join(workspace, '.nx', 'workspace-data', 'd', 'server-process.json');
  const text = await readFile(record, 'utf8').catch((error: unknown) => {
    if (errorCode(error) === 'ENOENT') {
      return null;
    }
    throw error;
  });
  if (text === null) {
    return [];
  }
  const parsed: unknown = JSON.parse(text);
  if (
    typeof parsed !== 'object' ||
    parsed === null ||
    !('processId' in parsed) ||
    typeof parsed.processId !== 'number' ||
    !Number.isSafeInteger(parsed.processId)
  ) {
    throw new Error(`Nx daemon record ${record} names no process id`);
  }
  return withDescendants(await processTable(), [parsed.processId]);
}

/**
 * Release the Nx daemon `workspace` started and wait until the daemon and
 * every process it ran have exited. `stop` runs `nx daemon --stop` against
 * the workspace, in the environment that started it. Nx deletes the daemon's
 * record before the daemon exits, so the record cannot say when the workspace
 * is free to delete; the processes can. The daemon also leaves what it
 * spawned to end on its own: plugin workers once they see it gone, an editor
 * extension probe never. Those get SIGTERM with it, the ones it started
 * while it was stopping included. A workspace without the record never had a
 * daemon, and `stop` does not run.
 */
export async function stopNxDaemon(workspace: string, stop: () => Promise<void>): Promise<void> {
  const [daemon, ...before] = await nxDaemonProcesses(workspace);
  if (daemon === undefined) {
    return;
  }
  await stop();
  const known = new Set([daemon.pid, ...before.map((entry) => entry.pid)]);
  const after = withDescendants(await processTable(), [daemon.pid]).filter((entry) => !known.has(entry.pid));
  const spawned = [...before, ...after];
  for (const entry of spawned) {
    terminate(entry.pid);
  }
  await awaitExit([daemon, ...spawned], `Nx daemon of ${workspace}`);
}

const runDirectories = new Map<string, Promise<string>>();

/**
 * A fresh fixture root for `suite`, owned by this process: a `prefix`-named
 * temp directory in this process's run directory. Canonical, because macOS
 * puts the temp directory behind a /private symlink and a daemon never sees
 * an edit made under a root named through one. The first root of a suite in
 * a process reclaims the suite's dead runs first.
 */
export async function ownedFixtureRoot(suite: string, prefix: string): Promise<string> {
  let directory = runDirectories.get(suite);
  if (directory === undefined) {
    directory = openRun(suite);
    runDirectories.set(suite, directory);
  }
  return mkdtemp(join(await directory, prefix));
}

async function openRun(suite: string): Promise<string> {
  const suiteDirectory = await reclaimDeadFixtureRuns(suite);
  const directory = join(suiteDirectory, `run-${process.pid}`);
  await mkdir(directory, { recursive: true });
  return directory;
}

/**
 * Stop every process working in a run of `suite` whose owner is gone, then
 * delete the run, and return the suite's directory. The first fixture root of a suite in
 * a process does this; a test that just watched a fixture-owning process die
 * may too. An owner's pid reused by an unrelated process makes its run look
 * alive; it is reclaimed once that process is gone too, never early.
 */
export async function reclaimDeadFixtureRuns(suite: string): Promise<string> {
  const suiteDirectory = join(await realpath(tmpdir()), FIXTURE_HOME, suite);
  await mkdir(suiteDirectory, { recursive: true });
  await reclaimDeadRuns(suiteDirectory);
  return suiteDirectory;
}

async function reclaimDeadRuns(suiteDirectory: string): Promise<void> {
  const dead = (await readdir(suiteDirectory)).flatMap((name) => {
    const owner = RUN_NAME.exec(name)?.[1];
    if (owner === undefined) {
      return [];
    }
    const pid = Number(owner);
    return pid !== process.pid && !isRunning(pid) ? [join(suiteDirectory, name)] : [];
  });
  if (dead.length === 0) {
    return;
  }
  const lock = await takeSweepLock(suiteDirectory);
  if (lock === null) {
    // Another process of the suite is reclaiming them right now.
    return;
  }
  try {
    for (const directory of dead) {
      await reclaimRun(directory);
    }
  } finally {
    await rm(lock, { force: true });
  }
}

async function reclaimRun(directory: string): Promise<void> {
  const roots = await readdir(directory).catch((error: unknown) => {
    if (errorCode(error) === 'ENOENT') {
      return null;
    }
    throw error;
  });
  if (roots === null) {
    return;
  }
  // A fixture deletes its root only once nothing runs in it, so an emptied
  // run holds no process and needs no scan.
  if (roots.length > 0) {
    const working = await pidsWorkingIn(directory);
    const processes = (await processTable()).filter((entry) => working.includes(entry.pid));
    if (processes.length > 0) {
      process.stderr.write(
        `reclaiming dead fixture run ${directory}: ${processes.map((entry) => `${entry.pid} (${entry.command})`).join(', ')}\n`,
      );
      for (const entry of processes) {
        terminate(entry.pid);
      }
      try {
        await awaitExit(processes, `processes of dead fixture run ${directory}`);
      } catch (error) {
        process.stderr.write(`${error instanceof Error ? error.message : String(error)}; sending SIGKILL\n`);
        for (const entry of processes) {
          terminate(entry.pid, 'SIGKILL');
        }
        await awaitExit(processes, `processes of dead fixture run ${directory} after SIGKILL`);
      }
    }
  }
  await rm(directory, { recursive: true, force: true });
}

/**
 * The suite's sweep lock, or null while a live process holds it. A lock left
 * by a process that died mid-sweep is taken over. Reclaiming is idempotent, so
 * the lock only keeps concurrent test processes from scanning the same runs.
 */
async function takeSweepLock(suiteDirectory: string): Promise<string | null> {
  const lock = join(suiteDirectory, SWEEP_LOCK);
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      await writeFile(lock, `${process.pid}\n`, { flag: 'wx' });
      return lock;
    } catch (error) {
      if (errorCode(error) !== 'EEXIST') {
        throw error;
      }
    }
    const holder = Number(
      (
        await readFile(lock, 'utf8').catch((error: unknown) => {
          if (errorCode(error) === 'ENOENT') {
            return '';
          }
          throw error;
        })
      ).trim(),
    );
    if (Number.isSafeInteger(holder) && holder > 0 && isRunning(holder)) {
      return null;
    }
    await rm(lock, { force: true });
  }
  return null;
}
