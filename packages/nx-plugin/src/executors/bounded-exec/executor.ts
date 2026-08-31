import { spawn } from 'node:child_process';
import { isAbsolute, join } from 'node:path';

import type { BoundedExecOptions } from './schema.js';

const DEFAULT_KILL_AFTER_MS = 10_000;
const REAP_AFTER_FORCE_KILL_MS = 2_000;
const EXIT_CODE_BY_SIGNAL: Partial<Record<NodeJS.Signals, number>> = {
  SIGHUP: 129,
  SIGINT: 130,
  SIGTERM: 143,
};

export interface BoundedExecContext {
  root: string;
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

export default function boundedExecExecutor(
  options: BoundedExecOptions,
  context: BoundedExecContext,
): Promise<BoundedExecResult> {
  return runBoundedExec(options, context, createProcessTreeKiller());
}

export async function runBoundedExec(
  options: BoundedExecOptions,
  context: BoundedExecContext,
  killer: ProcessTreeKiller,
): Promise<BoundedExecResult> {
  const cwd = resolveCwd(options.cwd, context.root);
  const command = buildCommand(options);
  const timeoutMs = options.timeoutMs;
  const idleTimeoutMs = options.idleTimeoutMs;
  const killAfterMs = options.killAfterMs ?? DEFAULT_KILL_AFTER_MS;
  const startedAt = Date.now();
  const outputChunks: string[] = [];
  const state: RunState = { settled: false, expiry: null, forceKillNeeded: false };

  const child = spawn(command, [], {
    cwd,
    env: mergeEnv(options.env),
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
    void killChildTree(false).finally(() => process.kill(process.pid, signal));
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

function mergeEnv(env: Record<string, string> | undefined): NodeJS.ProcessEnv {
  return env ? { ...process.env, ...env } : process.env;
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
