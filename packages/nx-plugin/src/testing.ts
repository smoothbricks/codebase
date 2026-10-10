/**
 * Ownership and retirement of the temp workspaces a test suite runs real Nx
 * in, shared by every package whose tests do.
 *
 * A fixture that runs Nx with its daemon on leaves a daemon whose working
 * directory is the fixture: it idles, watches files and burns CPU until
 * someone stops it. A test that throws stops it in its `finally`; a test
 * process that is killed runs no `finally`. So every fixture root lives in a
 * run directory named for the test process that owns it,
 * `<parent>/smoothbricks-fixtures/<suite>/run-<pid>`, where the caller names
 * the parent: the temp directory, or the directory of a volume the fixture has
 * to lie on. A process that gets SIGTERM, SIGINT or SIGHUP (a bounded test
 * leg's timeout sends SIGTERM, and SIGKILL 10 s later) first stops whatever
 * still works in every run it holds, then dies of that signal. One killed
 * outright cannot, so the first fixture a process creates for a suite under a
 * parent reclaims every run of that suite there whose owner is gone: each
 * process working in it is stopped, then the run is deleted. A run whose owner
 * still exists is never touched by another process, so nothing outside a dead
 * or dying run of the same suite is ever signalled.
 *
 * The module also says where a fixture's sockets and loopback listeners may
 * live, because a Cowshed sandbox admits only its own: a Unix socket under its
 * own tree or short runtime link, a TCP connect to a port of its own block. A
 * literal `/tmp/x.sock` or a port-0 listener still binds there, and then no
 * process of the workspace can reach it (specs/cowshed/04_sandbox.md).
 *
 * Node APIs and the library's public workspace-root finder only: this module
 * is built with the library and imported by Bun tests in other packages.
 */
import { execFile } from 'node:child_process';
import { createHash } from 'node:crypto';
import { lstatSync, readlinkSync, realpathSync, statSync } from 'node:fs';
import { lstat, mkdir, mkdtemp, readdir, readFile, readlink, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir, userInfo } from 'node:os';
import { dirname, isAbsolute, join, sep } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { promisify } from 'node:util';
import { findNxWorkspaceRoot } from './ensure-built.js';

const run = promisify(execFile);
const EXIT_TIMEOUT_MS = 10_000;
/** Under half of the SIGTERM-to-SIGKILL grace a bounded test leg gives, per round of signals. */
const SIGNALLED_GRACE_MS = 3_000;
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

/**
 * The listed processes among `pids`, and every process descending from one, each once: a pid that
 * descends from another in `pids` is not listed twice.
 */
export function withDescendants(table: readonly ProcessEntry[], pids: readonly number[]): ProcessEntry[] {
  const found = new Map<number, ProcessEntry>();
  const pending = table.filter((entry) => pids.includes(entry.pid));
  for (let entry = pending.pop(); entry !== undefined; entry = pending.pop()) {
    if (found.has(entry.pid)) {
      continue;
    }
    found.set(entry.pid, entry);
    const parent = entry.pid;
    pending.push(...table.filter((child) => child.ppid === parent));
  }
  return [...found.values()];
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
 * `timeoutMs` later is an error naming it and `owner`.
 */
export async function awaitExit(
  processes: readonly ProcessEntry[],
  owner: string,
  timeoutMs = EXIT_TIMEOUT_MS,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
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
        `${owner}: ${survivors.map((entry) => `${entry.pid} (${entry.command})`).join(', ')} still running ${timeoutMs}ms after it was stopped`,
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
 * spawned to end on its own: plugin workers once they see it gone, the editor
 * extension probe it starts at launch never. Those get SIGTERM with it, the
 * ones it started while it was stopping included. A child that outlives the
 * daemon is reparented to init and leaves the daemon's process tree, but it
 * still works in the workspace, so once the daemon has exited, the workspace
 * census ends what is left. A workspace without the record never had a
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
  await endWorkspaceProcesses(workspace, `Nx daemon of ${workspace}`);
}

/**
 * End every process still working in `workspace`, round after round, until a
 * census finds none. The daemon has exited by now, so a process can start
 * another under the workspace only while it is alive, and a child started
 * just before its parent was ended is in the next census. A zombie has
 * exited already, and this process never ends itself.
 */
async function endWorkspaceProcesses(workspace: string, owner: string): Promise<void> {
  for (;;) {
    const working = new Set(await pidsWorkingIn(workspace));
    const leftovers = (await processTable()).filter(
      (entry) => working.has(entry.pid) && entry.pid !== process.pid && !entry.stat.startsWith('Z'),
    );
    if (leftovers.length === 0) {
      return;
    }
    for (const entry of leftovers) {
      terminate(entry.pid);
    }
    await awaitExit(leftovers, owner);
  }
}

/**
 * The run this process holds in each suite directory, opened or still opening,
 * by that directory's canonical path: one for each parent and suite, however
 * the parent is spelled. Entries are kept for the life of the process, an open
 * that failed included.
 */
const runDirectories = new Map<string, Promise<string>>();

const RUN_SIGNALS = ['SIGTERM', 'SIGINT', 'SIGHUP'] as const;

/** The handlers this process made for {@link RUN_SIGNALS}, by signal: none until it opens its first run. */
const runSignalHandlers = new Map<NodeJS.Signals, () => void>();

/** Reclaiming every run this process holds, begun by the first of {@link RUN_SIGNALS} it received. */
let retirement: Promise<void> | undefined;

/**
 * A fresh fixture root for `suite`, owned by this process: a `prefix`-named
 * directory (mode 0700) in this process's run directory under
 * `parentDirectory`, `<parent>/smoothbricks-fixtures/<suite>/run-<pid>`. The
 * caller always names the parent: the temp directory, or the directory of the
 * volume the fixture has to lie on. The parent is resolved through every link,
 * so the root is canonical (macOS puts the temp directory behind a /private
 * symlink, and a daemon never sees an edit made under a root named through
 * one) and a parent spelled two ways is one run. A process holds one run for
 * each parent and suite, and the first root of a suite under a parent reclaims
 * that suite's dead runs there first. Once a signal has this process
 * reclaiming its runs, no root is made.
 */
export async function ownedFixtureRoot(parentDirectory: string, suite: string, prefix: string): Promise<string> {
  const suiteDirectory = join(await realpath(parentDirectory), FIXTURE_HOME, suite);
  refuseWhileRetiring();
  let opened = runDirectories.get(suiteDirectory);
  if (opened === undefined) {
    opened = openRun(suiteDirectory);
    runDirectories.set(suiteDirectory, opened);
  }
  const directory = await opened;
  refuseWhileRetiring();
  return mkdtemp(join(directory, prefix));
}

async function openRun(suiteDirectory: string): Promise<string> {
  await sweepSuite(suiteDirectory);
  const directory = join(suiteDirectory, `run-${process.pid}`);
  await mkdir(directory, { recursive: true });
  ownRunSignals();
  return directory;
}

/**
 * Own the signals that end this process, once however many runs it holds. The
 * first of them reclaims every run (`retireRuns`) before it takes its course. A
 * handler for each run would let the first to finish re-raise the signal over
 * the reclaim of the rest, and a signal repeated meanwhile would meet no handler
 * and kill the process before any reclaim ended.
 */
function ownRunSignals(): void {
  if (runSignalHandlers.size > 0) {
    return;
  }
  for (const signal of RUN_SIGNALS) {
    const handler = (): void => {
      retirement ??= retireRuns(signal);
    };
    runSignalHandlers.set(signal, handler);
    process.on(signal, handler);
  }
}

/**
 * Reclaim every run this process holds at once, then let `signal`, the first it
 * received, take its course: the handlers go and the process dies of it, as it
 * would have without them. Every run is awaited, one still opening or one whose
 * open failed included, and one failing cuts no other short; each failure is
 * written to stderr with the suite directory of its run. A repeated signal
 * meets a handler, not the default, so it waits for this and neither starts a
 * second reclaim nor kills the first. That wait is the reclaim's own: for each
 * run, after any sweep it was still opening with, the scans, two rounds of
 * `SIGNALLED_GRACE_MS` (SIGTERM, then SIGKILL) for its processes, and the
 * delete. A bounded test leg's SIGKILL ends it whatever it is.
 */
async function retireRuns(signal: NodeJS.Signals): Promise<void> {
  const runs = [...runDirectories];
  // Inside the 10 s a bounded leg grants between SIGTERM and SIGKILL.
  const outcomes = await Promise.allSettled(
    runs.map(async ([, opened]) => reclaimRun(await opened, 'signalled fixture run', SIGNALLED_GRACE_MS)),
  );
  for (const [index, outcome] of outcomes.entries()) {
    if (outcome.status === 'rejected') {
      const reason = outcome.reason instanceof Error ? outcome.reason.message : String(outcome.reason);
      process.stderr.write(`could not reclaim this process's run in ${runs[index][0]}: ${reason}\n`);
    }
  }
  for (const [name, handler] of runSignalHandlers) {
    process.removeListener(name, handler);
  }
  process.kill(process.pid, signal);
}

function refuseWhileRetiring(): void {
  if (retirement !== undefined) {
    throw new Error('no fixture root is made: a signal has this process reclaiming the fixture runs it owns');
  }
}

/**
 * Stop every process working in a run of `suite` under `parentDirectory` whose
 * owner is gone, then delete the run, and return the suite's directory there.
 * The first fixture root of a suite under a parent in a process does this; a
 * test that just watched a fixture-owning process die may too. An owner's pid
 * reused by an unrelated process makes its run look alive; it is reclaimed once
 * that process is gone too, never early.
 */
export async function reclaimDeadFixtureRuns(parentDirectory: string, suite: string): Promise<string> {
  const suiteDirectory = join(await realpath(parentDirectory), FIXTURE_HOME, suite);
  await sweepSuite(suiteDirectory);
  return suiteDirectory;
}

async function sweepSuite(suiteDirectory: string): Promise<void> {
  await mkdir(suiteDirectory, { recursive: true });
  await reclaimDeadRuns(suiteDirectory);
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
      await reclaimRun(directory, 'dead fixture run');
    }
  } finally {
    await rm(lock, { force: true });
  }
}

async function reclaimRun(directory: string, owner: string, graceMs = EXIT_TIMEOUT_MS): Promise<void> {
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
        `reclaiming ${owner} ${directory}: ${processes.map((entry) => `${entry.pid} (${entry.command})`).join(', ')}\n`,
      );
      for (const entry of processes) {
        terminate(entry.pid);
      }
      try {
        await awaitExit(processes, `processes of ${owner} ${directory}`, graceMs);
      } catch (error) {
        process.stderr.write(`${error instanceof Error ? error.message : String(error)}; sending SIGKILL\n`);
        for (const entry of processes) {
          terminate(entry.pid, 'SIGKILL');
        }
        await awaitExit(processes, `processes of ${owner} ${directory} after SIGKILL`, graceMs);
      }
    }
  }
  // A killed fixture may never have reached its own alias retirement, and its
  // workspace may be gone. The alias is its own record: one whose target is the
  // socket state of a workspace in this reclaimed run, and whose name is that
  // workspace's, is this sweep's to retire. Every other entry is left alone.
  const { directory: namespace } = socketNamespace();
  for (const entry of await readdir(namespace, { withFileTypes: true })) {
    if (!entry.isSymbolicLink()) continue;
    const target = await readlink(join(namespace, entry.name)).catch((error: unknown) => {
      if (errorCode(error) === 'ENOENT') return undefined;
      throw error;
    });
    if (target === undefined) continue;
    const workspace = dirname(dirname(target));
    if (
      workspace.startsWith(`${directory}${sep}`) &&
      target === fixtureSocketTarget(workspace) &&
      entry.name === fixtureSocketAliasName(workspace)
    ) {
      await retireFixtureSocketDirectory(workspace);
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

/**
 * The longest path a fixture binds a Unix socket at: the stricter of Nx's own
 * limit (`assertValidSocketPath` refuses over 95 characters) and the kernel's
 * (macOS's `sun_path` holds 104 bytes, the terminating NUL included), counted
 * in UTF-8 bytes, which is what the kernel limits.
 */
const SOCKET_PATH_BYTES = 95;

/**
 * The longest socket Nx binds in a configured socket directory: an isolated
 * plugin worker's `p<pid>-<counter>-<8 hex>.sock` (`getPluginSocketFileName`;
 * with a configured directory `getPluginSocketDir` is the daemon's own), which
 * outgrows the daemon's `d.sock` and a forked task's `fp<pid>-<n>.sock`. macOS
 * pids stop at 99999, Linux's have seven digits; two base-36 digits count 1295
 * workers.
 */
export const NX_LONGEST_SOCKET_NAME = `p${'9'.repeat(process.platform === 'darwin' ? 5 : 7)}-zz-ffffffff.sock`;

const RUNTIME_LINK_ENV = 'COWSHED_RUNTIME_LINK';
const PORT_BASE_ENV = 'COWSHED_PORT_BASE';
const PORT_BLOCK_SIZE_ENV = 'COWSHED_PORT_BLOCK_SIZE';
const MAX_PORT = 65_535;
const DECIMAL = /^[0-9]+$/;

/** Where a fixture's sockets are made, and the candidates passed over on the way there. */
export interface SocketNamespace {
  readonly directory: string;
  /** Each environment root not used, and why: carried into every refusal made in this namespace. */
  readonly passedOver: readonly string[];
}

/**
 * The directory a fixture's sockets are made in: a short root this process's
 * own environment supplies for the checkout it runs in. In order: the parent
 * of `NX_SOCKET_DIR`, which for a Cowshed job is the workspace's short runtime
 * link (its Nx socket directory is `nx` below it); `COWSHED_RUNTIME_LINK`,
 * that link; else the temp directory. Never `NX_SOCKET_DIR` itself: Nx deletes
 * that directory, recursively and whatever else is in it, whenever its daemon
 * stops, so a fixture's sockets live beside it. The sandbox admits Unix sockets
 * under the link by its literal path, and under the temp directory only for a
 * job's own `TMPDIR`. Spelled as the environment gives it and never resolved,
 * since the physical path is too long for a socket.
 *
 * As `nx-socket-dir.sh` does, a shell's variable is trusted only when it is
 * this checkout's. The public finder names the checkout from the working
 * directory; an `NX_SOCKET_DIR` also needs a matching `NX_WORKSPACE_ROOT_PATH`.
 * The link must resolve to that checkout's `.cowshed/run`, even when a test
 * preload has removed the inherited Nx variables. Unproven candidates are
 * passed over with their resolution or permission failure, so the fallback
 * directory, or the length refusal it leads to, names what it fell from. One
 * that is this checkout's but is not an existing directory of this user is an
 * error naming it.
 */
export function socketNamespace(): SocketNamespace {
  const passedOver: string[] = [];
  const socketDirectory = environmentValue('NX_SOCKET_DIR');
  const link = environmentValue(RUNTIME_LINK_ENV);
  if (socketDirectory !== undefined || link !== undefined) {
    const checkout = checkoutRoot();
    if (checkout.kind === 'unresolved') {
      passedOver.push(`NX_SOCKET_DIR and ${RUNTIME_LINK_ENV} cannot be tied to a checkout: ${checkout.why}`);
    } else {
      if (socketDirectory !== undefined) {
        const named = environmentValue('NX_WORKSPACE_ROOT_PATH');
        const binding: Resolution =
          named === undefined ? { kind: 'unresolved', why: 'NX_WORKSPACE_ROOT_PATH is not set' } : resolution(named);
        if (binding.kind === 'resolved' && binding.path === checkout.path) {
          const leaf = resolution(socketDirectory);
          if (leaf.kind === 'resolved' && isWithin(leaf.path, checkout.path)) {
            const source = `the parent of NX_SOCKET_DIR=${socketDirectory}`;
            return { directory: ownedDirectory(source, dirname(socketDirectory), passedOver, 'private'), passedOver };
          }
          passedOver.push(
            `NX_SOCKET_DIR=${socketDirectory} ${describeResolution(leaf)}, not inside the checkout ${checkout.path}`,
          );
        } else {
          passedOver.push(
            `NX_SOCKET_DIR=${socketDirectory} has no current checkout binding: ${describeResolution(binding)}`,
          );
        }
      }
      if (link !== undefined) {
        const run = join(checkout.path, '.cowshed', 'run');
        const runResolved = resolution(run);
        const linked = resolution(link);
        if (runResolved.kind === 'resolved' && linked.kind === 'resolved' && linked.path === runResolved.path) {
          return { directory: ownedDirectory(RUNTIME_LINK_ENV, link, passedOver, 'private'), passedOver };
        }
        passedOver.push(
          `${RUNTIME_LINK_ENV}=${link} ${describeResolution(linked)}, and the checkout's ${run} ${describeResolution(runResolved)}`,
        );
      }
    }
  }
  return { directory: ownedDirectory('the temp directory', tmpdir(), passedOver, 'process-temp'), passedOver };
}

/** The variable's value, or undefined when it is unset or empty. */
function environmentValue(name: string): string | undefined {
  const value = process.env[name];
  return value === undefined || value === '' ? undefined : value;
}

type Resolution =
  | { readonly kind: 'resolved'; readonly path: string }
  | { readonly kind: 'unresolved'; readonly why: string };

function describeResolution(resolved: Resolution): string {
  return resolved.kind === 'resolved' ? `resolves to ${resolved.path}` : `cannot be resolved (${resolved.why})`;
}

/** The current checkout, read from its marker rather than an inherited Nx override. */
function checkoutRoot(): Resolution {
  const here = resolution(process.cwd());
  if (here.kind === 'unresolved') {
    return { kind: 'unresolved', why: `the working directory cannot be resolved (${here.why})` };
  }
  const root = findNxWorkspaceRoot(here.path);
  return root === null
    ? { kind: 'unresolved', why: `no nx.json at or above the working directory ${here.path}` }
    : resolution(root);
}

/** `path` resolved through every link, or the errno of a path that does not exist or may not be looked at. */
function resolution(path: string): Resolution {
  try {
    return { kind: 'resolved', path: realpathSync(path) };
  } catch (error) {
    const code = errorCode(error);
    if (code === 'ENOENT' || code === 'ENOTDIR' || code === 'EACCES' || code === 'EPERM') {
      return { kind: 'unresolved', why: `${code} on ${path}` };
    }
    throw error;
  }
}

/** Whether the resolved `path` is `root` or lies beneath it. */
function isWithin(path: string, root: string): boolean {
  return path === root || path.startsWith(`${root}${sep}`);
}

function ownedDirectory(
  source: string,
  directory: string,
  passedOver: readonly string[],
  ownership: 'private' | 'process-temp',
): string {
  const stat = statSync(directory, { throwIfNoEntry: false });
  const user = process.getuid?.();
  const owned = user === undefined || stat?.uid === user;
  const publicTemp = ownership === 'process-temp' && stat?.uid === 0 && (stat.mode & 0o7777) === 0o1777;
  if (!isAbsolute(directory) || stat === undefined || !stat.isDirectory() || (!owned && !publicTemp)) {
    throw new Error(
      `${source} (${directory}, uid=${stat?.uid}, mode=${stat?.mode.toString(8)}) is not an existing absolute ` +
        'user-owned directory or an admitted sticky public temp directory, so no fixture socket can be ' +
        `made under it${passedOver.length === 0 ? '' : `; passed over: ${passedOver.join('; ')}`}`,
    );
  }
  return directory;
}

/**
 * `path`, or an error naming it, its length and the namespace it was made in
 * (with what that namespace passed over) when Nx and the kernel would refuse
 * to bind a socket there.
 */
export function assertSocketPath(path: string, namespace: SocketNamespace): string {
  const bytes = Buffer.byteLength(path, 'utf8');
  if (bytes > SOCKET_PATH_BYTES) {
    const { directory, passedOver } = namespace;
    throw new Error(
      `the socket path ${path} is ${bytes} UTF-8 bytes, over the ${SOCKET_PATH_BYTES} a Unix socket may have; ` +
        `the socket namespace ${directory} leaves too little room` +
        `${passedOver.length === 0 ? '' : `; passed over: ${passedOver.join('; ')}`}`,
    );
  }
  return path;
}

/**
 * A fresh directory, mode 0700 and named `prefix` plus six random characters,
 * in {@link socketNamespace}: the one owner of the sockets a fixture makes
 * there, so the fixture removes the directory and nothing else. `longestSocketName`
 * is the longest socket it will hold, and the call fails before creating
 * anything when that socket's path would be too long to bind.
 */
export async function ownedSocketDirectory(prefix: string, longestSocketName: string): Promise<string> {
  const namespace = socketNamespace();
  assertSocketPath(join(namespace.directory, `${prefix}XXXXXX`, longestSocketName), namespace);
  return mkdtemp(join(namespace.directory, prefix));
}

/** The alias's name: a digest of the workspace's physical path, which is the whole of its identity. */
function fixtureSocketAliasName(workspace: string): string {
  return `nx-${createHash('sha256').update(workspace).digest('hex').slice(0, 16)}`;
}

/** The real directory below the workspace that its alias names. */
function fixtureSocketTarget(workspace: string): string {
  return join(workspace, '.nx', 'sockets');
}

/** A colliding or retargeted alias is not this fixture's and must not be used or removed. */
function assertFixtureSocketAlias(alias: string, target: string): void {
  const entry = lstatSync(alias);
  if (!entry.isSymbolicLink() || readlinkSync(alias) !== target) {
    throw new Error(`fixture socket alias ${alias} does not name its owned target ${target}`);
  }
}

/**
 * The fixture's `NX_SOCKET_DIR`, named and nothing more: the real `nx` leaf
 * below the short parent alias {@link publishFixtureSocketDirectory} makes in
 * the job's admitted namespace. `workspace` is the physical path that
 * publication was given; the alias is named for a hash of it, so every client,
 * daemon stop and retirement of the workspace names the same leaf, and no two
 * fixtures share one. A long private `TMPDIR` affects neither the bind path nor
 * workspace ownership. A namespace too small for Nx's longest socket name is
 * refused here, so before any publication.
 */
export function fixtureSocketDirectory(workspace: string): string {
  const namespace = socketNamespace();
  const directory = join(namespace.directory, fixtureSocketAliasName(workspace), 'nx');
  assertSocketPath(join(directory, NX_LONGEST_SOCKET_NAME), namespace);
  return directory;
}

/**
 * Publish the fixture's short socket alias and return its
 * {@link fixtureSocketDirectory}. The workspace must be a physical directory in
 * a fixture run this process holds ({@link ownedFixtureRoot}): then the run's
 * sweep reclaims the alias even if this process dies before its
 * {@link retireFixtureSocketDirectory}. The alias points at the workspace's own
 * `.nx/sockets`, made mode 0700; a symlinked `.nx` or `.nx/sockets` is refused
 * before anything is written through it. Nx makes the `nx` leaf itself.
 */
export async function publishFixtureSocketDirectory(workspace: string): Promise<string> {
  refuseWhileRetiring();
  const runs = await Promise.allSettled(runDirectories.values());
  const owned = runs.some((run) => run.status === 'fulfilled' && workspace.startsWith(`${run.value}${sep}`));
  if (!owned || (await realpath(workspace)) !== workspace) {
    throw new Error(
      `no socket alias is published for ${workspace}: it is not a physical workspace in a fixture run this ` +
        'process owns, so no sweep could reclaim its alias',
    );
  }
  const directory = fixtureSocketDirectory(workspace);
  const alias = dirname(directory);
  const target = fixtureSocketTarget(workspace);
  for (const state of [dirname(target), target]) {
    try {
      await mkdir(state, { mode: 0o700 });
    } catch (error) {
      if (errorCode(error) !== 'EEXIST') throw error;
    }
    if (!(await lstat(state)).isDirectory()) {
      throw new Error(`fixture socket target ${state} is not a real directory in its physical workspace`);
    }
  }
  try {
    await symlink(target, alias, 'dir');
  } catch (error) {
    if (errorCode(error) !== 'EEXIST') throw error;
  }
  assertFixtureSocketAlias(alias, target);
  return directory;
}

/**
 * A second real leaf, `slot`, beside the `nx` one below the workspace's
 * published alias, for tests whose subject is two daemon socket directories.
 * A slot name is shorter than `nx`, so it fits wherever the published leaf
 * does. An existing slot is an error. Its caller stops the daemon bound in it
 * before removing it.
 */
export async function ownedWorkspaceSocketDirectory(workspace: string, slot: 'a' | 'b'): Promise<string> {
  const alias = dirname(fixtureSocketDirectory(workspace));
  assertFixtureSocketAlias(alias, fixtureSocketTarget(workspace));
  const directory = join(alias, slot);
  await mkdir(directory, { mode: 0o700 });
  return directory;
}

/**
 * After the daemon exits, unlink only this workspace's verified parent alias,
 * not its target. Named from the workspace's path alone, so a workspace already
 * deleted retires too. No alias is not an error; any other entry under its name
 * is refused and left in place.
 */
export async function retireFixtureSocketDirectory(workspace: string): Promise<void> {
  const alias = join(socketNamespace().directory, fixtureSocketAliasName(workspace));
  try {
    assertFixtureSocketAlias(alias, fixtureSocketTarget(workspace));
  } catch (error) {
    if (errorCode(error) === 'ENOENT') return;
    throw error;
  }
  await rm(alias);
}

/** The port block a Cowshed job's environment names, or why it names none a listener can use. */
type PortBlockEnvironment =
  /** Outside Cowshed, or on Linux: no block, so a listener binds port 0 as usual. */
  | { readonly kind: 'none' }
  /** The gateway listens on `base`; service ports are `base+1 … base+size-1`. */
  | { readonly kind: 'block'; readonly base: number; readonly size: number }
  | { readonly kind: 'broken'; readonly reason: string };

/**
 * The block `env` (`COWSHED_PORT_BASE`, `COWSHED_PORT_BLOCK_SIZE`) names. A
 * size without a base, a base without a size (never assumed), a value that is
 * not a decimal number, a size that is not a power of two of at least two, a
 * base not aligned to it, or a block outside the TCP ports 1–65535 is a broken
 * environment, refused rather than guessed around (04_sandbox.md).
 */
function portBlockEnvironment(env: NodeJS.ProcessEnv): PortBlockEnvironment {
  const baseText = env[PORT_BASE_ENV];
  const sizeText = env[PORT_BLOCK_SIZE_ENV];
  if (baseText === undefined && sizeText === undefined) {
    return { kind: 'none' };
  }
  if (baseText === undefined) {
    return { kind: 'broken', reason: `${PORT_BLOCK_SIZE_ENV}=${sizeText} is set without ${PORT_BASE_ENV}` };
  }
  if (sizeText === undefined) {
    return { kind: 'broken', reason: `${PORT_BASE_ENV}=${baseText} is set without ${PORT_BLOCK_SIZE_ENV}` };
  }
  if (!DECIMAL.test(baseText) || !DECIMAL.test(sizeText)) {
    return {
      kind: 'broken',
      reason: `${PORT_BASE_ENV}=${JSON.stringify(baseText)} and ${PORT_BLOCK_SIZE_ENV}=${JSON.stringify(sizeText)} are not both decimal numbers`,
    };
  }
  const base = Number(baseText);
  const size = Number(sizeText);
  if (!Number.isSafeInteger(size) || size < 2 || (size & (size - 1)) !== 0) {
    return {
      kind: 'broken',
      reason: `${PORT_BLOCK_SIZE_ENV}=${sizeText} is not a power of two of at least 2`,
    };
  }
  if (!Number.isSafeInteger(base) || base < 1 || base % size !== 0) {
    return {
      kind: 'broken',
      reason: `${PORT_BASE_ENV}=${baseText} is not a positive multiple of ${PORT_BLOCK_SIZE_ENV}=${sizeText}`,
    };
  }
  if (base + size - 1 > MAX_PORT) {
    return {
      kind: 'broken',
      reason: `the block ${base}+${size} (${PORT_BASE_ENV}, ${PORT_BLOCK_SIZE_ENV}) runs past TCP port ${MAX_PORT}`,
    };
  }
  return { kind: 'block', base, size };
}

/**
 * Bind a real listener where a Cowshed job's other processes can connect to
 * it. In a block, `bind` is called with each candidate `base+1 … base+size-1`,
 * lowest first (a long-lived test listener scans from the bottom), and the
 * first that binds is kept: only a real bind claims a port, so nothing probes
 * and closes. Only `EADDRINUSE` (by `code`) moves on to the next candidate;
 * any other failure is that bind's own and surfaces as is, and so does the
 * last candidate's, so an exhausted block reports `EADDRINUSE`, never a silent
 * port 0. Outside a block `bind` is called with 0, and the kernel picks.
 *
 * `bind` creates and binds one listener per call and returns it, and a failed
 * bind leaves nothing behind. It may throw (`Bun.serve` does) or reject (a
 * Node `net` server reports a failed `listen` as an `error` event, which the
 * caller turns into a rejection). The listener the winning call returns is the
 * caller's to keep and close.
 */
export async function bindListener<T>(bind: (port: number) => T | Promise<T>): Promise<T> {
  const block = portBlockEnvironment(process.env);
  switch (block.kind) {
    case 'none':
      return bind(0);
    case 'broken':
      throw new Error(`a listener cannot bind in a broken Cowshed port-block environment: ${block.reason}`);
    case 'block': {
      const last = block.base + block.size - 1;
      for (let port = block.base + 1; port < last; port += 1) {
        try {
          return await bind(port);
        } catch (error) {
          if (errorCode(error) !== 'EADDRINUSE') {
            throw error;
          }
        }
      }
      return bind(last);
    }
  }
}
