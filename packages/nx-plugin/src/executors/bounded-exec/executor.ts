import { spawn } from 'node:child_process';
import { isAbsolute, join } from 'node:path';
import { workspaceDataDirectoryForWorkspace } from 'nx/src/utils/cache-directory.js';

import {
  createHostCommands,
  describeRamTempError,
  RAM_TEMP_CAPACITY_BYTES,
  type RamTempLease,
  RamTempVolume,
  ramTempPaths,
} from './ram-temp.js';
import type { BoundedExecOptions } from './schema.js';
import {
  type BoundedExecTask,
  clearTaskDirectory,
  JUNIT_FILE,
  JUNIT_REPORT_ENV,
  judgeRun,
  readJunitReport,
  taskDirectory,
  withJunitReport,
  writeRecord,
} from './verdict.js';

const DEFAULT_KILL_AFTER_MS = 10_000;
const REAP_AFTER_FORCE_KILL_MS = 2_000;
const EXIT_CODE_BY_SIGNAL: Partial<Record<NodeJS.Signals, number>> = {
  SIGHUP: 129,
  SIGINT: 130,
  SIGTERM: 143,
};

export interface BoundedExecContext {
  root: string;
  /** The Nx task this run executes as, and the directory its verdict and report go to. */
  record?: { task: BoundedExecTask; directory: string };
}

/** The part of Nx's executor context that names the running task. */
export interface NxTaskContext {
  root: string;
  projectName?: string;
  targetName?: string;
  configurationName?: string;
  taskGraph?: { tasks: Record<string, { hash?: string }> };
}

export interface BoundedExecResult {
  success: boolean;
  terminalOutput: string;
}

/**
 * Which bound ended the run.
 *
 * `total` is a wall-clock ceiling: it answers "is this run unbounded?".
 * `idle` is a no-progress bound: it answers "is this run wedged?".
 *
 * These are different questions and one number cannot answer both. A ceiling
 * tight enough to catch a wedge promptly is also tight enough to fail a correct
 * run on a busy machine; a ceiling loose enough never to do that cannot catch a
 * wedge promptly. Keeping the two bounds separate is what makes each honest.
 */
type ExpiredBound = { kind: 'total' | 'idle'; limitMs: number };

interface RunState {
  settled: boolean;
  expiry: ExpiredBound | null;
  forceKillNeeded: boolean;
}

export interface ProcessTreeKiller {
  kill(pid: number, signal: NodeJS.Signals): Promise<void>;
}

/** Where the command's TMPDIR comes from; `null` leaves the inherited one. */
export type TempVolume = Pick<RamTempVolume, 'acquire' | 'release' | 'fullness'>;

export default function boundedExecExecutor(
  options: BoundedExecOptions,
  context: NxTaskContext,
): Promise<BoundedExecResult> {
  // Only macOS has `hdiutil ram://`; elsewhere the command keeps the inherited TMPDIR.
  const tempVolume =
    process.platform === 'darwin'
      ? new RamTempVolume(ramTempPaths(process.getuid?.() ?? 0), RAM_TEMP_CAPACITY_BYTES, createHostCommands())
      : null;
  return runBoundedExec(options, recordContext(context), createProcessTreeKiller(), tempVolume);
}

/**
 * Nx gives an executor the task's project, target and configuration, not its id; the id is the
 * key of the task graph entry they name. Outside a task graph the run records nothing.
 */
export function recordContext(context: NxTaskContext): BoundedExecContext {
  const { root, projectName, targetName, configurationName, taskGraph } = context;
  if (!projectName || !targetName) {
    return { root };
  }
  const id = `${projectName}:${targetName}${configurationName ? `:${configurationName}` : ''}`;
  const task = taskGraph?.tasks[id];
  if (task === undefined) {
    return { root };
  }
  return {
    root,
    record: {
      task: { id, hash: task.hash ?? null },
      directory: taskDirectory(workspaceDataDirectoryForWorkspace(root), id),
    },
  };
}

export async function runBoundedExec(
  options: BoundedExecOptions,
  context: BoundedExecContext,
  killer: ProcessTreeKiller,
  tempVolume: TempVolume | null,
): Promise<BoundedExecResult> {
  const cwd = resolveCwd(options.cwd, context.root);
  const directory = context.record?.directory ?? null;
  // A previous run's verdict and report go first, so neither can be read as this run's.
  if (directory !== null) {
    await clearTaskDirectory(directory);
  }
  const command = directory === null ? buildCommand(options) : await withJunitReport(buildCommand(options), directory);
  const timeoutMs = options.timeoutMs;
  const idleTimeoutMs = options.idleTimeoutMs;
  const killAfterMs = options.killAfterMs ?? DEFAULT_KILL_AFTER_MS;
  const outputChunks: string[] = [];
  const state: RunState = { settled: false, expiry: null, forceKillNeeded: false };
  // A runner bounded-exec cannot see in the command (a script that spawns `bun test`) writes
  // its JUnit report to this path when it is set.
  const env = directory === null ? options.env : { [JUNIT_REPORT_ENV]: join(directory, JUNIT_FILE), ...options.env };

  let lease: RamTempLease | null = null;
  if (tempVolume !== null) {
    const acquired = await tempVolume.acquire();
    if (!acquired.ok) {
      const message = `${describeRamTempError(acquired.error)}\n`;
      process.stderr.write(message);
      return { success: false, terminalOutput: message };
    }
    if (acquired.value.kind === 'leased') {
      lease = acquired.value.lease;
      for (const held of acquired.value.held) {
        const message = `RAM temp volume: dead lease ${held}\n`;
        outputChunks.push(message);
        process.stderr.write(message);
      }
    } else {
      const message = `RAM temp volume unavailable in this sandbox (${acquired.value.detail}); TMPDIR stays ${process.env.TMPDIR ?? '(unset)'}\n`;
      outputChunks.push(message);
      process.stderr.write(message);
    }
  }
  const releaseLease = async (): Promise<void> => {
    if (lease === null) {
      return;
    }
    const held = lease;
    lease = null;
    const released = await tempVolume?.release(held);
    if (released && !released.ok) {
      appendStderr(`${describeRamTempError(released.error)}\n`);
    }
  };

  const startedAt = Date.now();
  const child = spawn(command, [], {
    cwd,
    env: mergeEnv(lease, env),
    shell: true,
    detached: process.platform !== 'win32',
    windowsHide: true,
  });

  const appendStdout = (chunk: Buffer | string): void => {
    const text = chunk.toString();
    outputChunks.push(text);
    process.stdout.write(text);
  };
  const appendStderr = (chunk: Buffer | string): void => {
    const text = chunk.toString();
    outputChunks.push(text);
    process.stderr.write(text);
  };

  let idleTimer: NodeJS.Timeout | undefined;
  const armIdleTimer = (): void => {
    if (idleTimeoutMs === undefined || state.settled || state.expiry !== null) {
      return;
    }
    clearTimeout(idleTimer);
    idleTimer = setTimeout(() => expire({ kind: 'idle', limitMs: idleTimeoutMs }), idleTimeoutMs);
  };

  // Only the CHILD's own output counts as progress. The diagnostics below go
  // through appendStderr as well, and re-arming on those would let a teardown
  // message extend the very bound that just fired.
  child.stdout?.on('data', (chunk: Buffer | string) => {
    armIdleTimer();
    appendStdout(chunk);
  });
  child.stderr?.on('data', (chunk: Buffer | string) => {
    armIdleTimer();
    appendStderr(chunk);
  });

  const killChildTree = async (force: boolean): Promise<void> => {
    const pid = child.pid;
    if (!pid) {
      return;
    }
    await killer.kill(pid, force ? 'SIGKILL' : 'SIGTERM');
  };

  const onProcessExit = (): void => {
    void killChildTree(false);
  };
  const onTerminationSignal = (signal: NodeJS.Signals): void => {
    removeSignalHandlers();
    void killChildTree(false)
      .finally(releaseLease)
      .finally(() => process.kill(process.pid, signal));
  };
  const onSigint = (): void => onTerminationSignal('SIGINT');
  const onSigterm = (): void => onTerminationSignal('SIGTERM');
  const onSighup = (): void => onTerminationSignal('SIGHUP');
  const removeSignalHandlers = (): void => {
    process.removeListener('exit', onProcessExit);
    process.removeListener('SIGINT', onSigint);
    process.removeListener('SIGTERM', onSigterm);
    process.removeListener('SIGHUP', onSighup);
  };

  process.on('exit', onProcessExit);
  process.on('SIGINT', onSigint);
  process.on('SIGTERM', onSigterm);
  process.on('SIGHUP', onSighup);

  // `Promise.withResolvers` would read better here but needs lib es2024; this
  // package inherits lib es2022 from tsconfig.base.json.
  let resolveExit!: (value: { code: number | null; signal: NodeJS.Signals | null }) => void;
  const exitPromise = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) => {
    resolveExit = resolve;
    child.once('error', (error) => {
      appendStderr(`${error.message}\n`);
      state.settled = true;
      resolve({ code: 1, signal: null });
    });
    child.once('exit', (code, signal) => {
      state.settled = true;
      resolve({ code, signal });
    });
  });

  // Both bounds escalate through one path, so a run expires at most once and
  // the report always names which bound did it. A bare "timed out" would leave
  // the reader unable to tell a wedged toolchain from a loaded machine — the
  // two have opposite fixes.
  const expire = (bound: ExpiredBound): void => {
    if (state.settled || state.expiry !== null) {
      return;
    }
    state.expiry = bound;
    clearTimeout(totalTimer);
    clearTimeout(idleTimer);
    const elapsedMs = Date.now() - startedAt;
    appendStderr(
      bound.kind === 'idle'
        ? `\nCommand made no progress: no output for ${bound.limitMs}ms (idleTimeoutMs=${bound.limitMs}) after ${elapsedMs}ms of runtime (cwd=${cwd}): ${command}\n`
        : `\nCommand timed out after ${elapsedMs}ms (timeoutMs=${bound.limitMs}, cwd=${cwd}): ${command}\n`,
    );
    void (async () => {
      await ignoreKillError(killChildTree(false));
      if (!state.settled && killAfterMs > 0) {
        await delay(killAfterMs);
      }
      if (!state.settled) {
        state.forceKillNeeded = true;
        appendStderr(`Force-killing timed out command after killAfterMs=${killAfterMs}: ${command}\n`);
        await ignoreKillError(killChildTree(true));
        await delay(REAP_AFTER_FORCE_KILL_MS);
      }
      if (!state.settled) {
        resolveExit({ code: 1, signal: 'SIGKILL' });
      }
    })();
  };

  const totalTimer = setTimeout(() => expire({ kind: 'total', limitMs: timeoutMs }), timeoutMs);
  armIdleTimer();

  const { code: exitCode, signal: exitSignal } = await exitPromise;
  clearTimeout(totalTimer);
  clearTimeout(idleTimer);
  removeSignalHandlers();

  const code = exitCode ?? signalToExitCode(exitSignal);
  if (code !== 0 && state.expiry === null) {
    appendStderr(`Command exited with status ${code}: ${command}\n`);
  }

  if (state.expiry !== null && !state.forceKillNeeded) {
    appendStderr(`Timed out command exited after graceful termination: ${command}\n`);
  }

  // A test that dies of ENOSPC rarely says where it was writing; the volume can.
  const full = code !== 0 && lease !== null ? await tempVolume?.fullness(lease) : null;
  if (full) {
    appendStderr(`${describeRamTempError(full)}\n`);
  }
  await releaseLease();

  if (context.record !== undefined) {
    const { task, directory: recordDirectory } = context.record;
    const verdict = judgeRun({
      exitCode: code,
      elapsedMs: Date.now() - startedAt,
      expiry: state.expiry,
      report: await readJunitReport(recordDirectory),
    });
    if (verdict.outcome === 'bound' && verdict.bound === 'test') {
      appendStderr(`Every failing test failed only on its per-test timeout: ${verdict.tests.join(', ')}\n`);
    }
    await writeRecord(recordDirectory, { task: task.id, hash: task.hash, verdict });
  }

  return {
    success: state.expiry === null && code === 0,
    terminalOutput: outputChunks.join(''),
  };
}

export function createProcessTreeKiller(): ProcessTreeKiller {
  return {
    kill(pid, signal) {
      return killProcessGroup(pid, signal);
    },
  };
}

function killProcessGroup(pid: number, signal: NodeJS.Signals): Promise<void> {
  try {
    // Send signal to the entire process group (negative PID).
    // The executor spawns with detached: true on POSIX, which creates a
    // dedicated process group. Signaling -pid is atomic and catches all
    // descendants, including grandchildren that tree-walk libraries miss
    // when parents die and children get reparented to init.
    process.kill(-pid, signal);
  } catch {
    // ESRCH: process group already exited, or this pid is not a group leader.
  }
  try {
    process.kill(pid, signal);
  } catch {
    // Already reaped.
  }
  return Promise.resolve();
}

function resolveCwd(cwd: string | undefined, root: string): string {
  if (!cwd) {
    return root;
  }
  return isAbsolute(cwd) ? cwd : join(root, cwd);
}

function buildCommand(options: BoundedExecOptions): string {
  const parts = [options.command];
  if (Array.isArray(options.args)) {
    parts.push(...options.args);
  } else if (options.args) {
    parts.push(options.args);
  }
  if (options.forwardAllArgs !== false && options.__unparsed__?.length) {
    parts.push(...options.__unparsed__);
  }
  return parts.join(' ');
}

/** The lease's directory is the command's TMPDIR unless the target names one itself. */
function mergeEnv(lease: RamTempLease | null, env: Record<string, string> | undefined): NodeJS.ProcessEnv {
  if (lease === null) {
    return env ? { ...process.env, ...env } : process.env;
  }
  return { ...process.env, TMPDIR: lease.directory, ...env };
}

function signalToExitCode(signal: NodeJS.Signals | null): number {
  if (!signal) {
    return 1;
  }
  return EXIT_CODE_BY_SIGNAL[signal] ?? 1;
}

async function ignoreKillError(promise: Promise<void>): Promise<void> {
  try {
    await promise;
  } catch {
    // The process tree may have already exited between timeout and kill.
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
