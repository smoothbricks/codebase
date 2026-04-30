import { spawn } from 'node:child_process';
import { isAbsolute, join } from 'node:path';
import treeKill from 'tree-kill';

import type { BoundedExecOptions } from './schema.js';

const DEFAULT_KILL_AFTER_MS = 10_000;
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

interface RunState {
  settled: boolean;
  timedOut: boolean;
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
  const killAfterMs = options.killAfterMs ?? DEFAULT_KILL_AFTER_MS;
  const startedAt = Date.now();
  const outputChunks: string[] = [];
  const state: RunState = { settled: false, timedOut: false, forceKillNeeded: false };

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

  child.stdout?.on('data', appendStdout);
  child.stderr?.on('data', appendStderr);

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

  let timeout: NodeJS.Timeout | undefined;
  const timeoutPromise = new Promise<void>((resolve) => {
    timeout = setTimeout(() => {
      state.timedOut = true;
      const elapsedMs = Date.now() - startedAt;
      appendStderr(`\nCommand timed out after ${elapsedMs}ms (timeoutMs=${timeoutMs}, cwd=${cwd}): ${command}\n`);
      void (async () => {
        await ignoreKillError(killChildTree(false));
        if (!state.settled && killAfterMs > 0) {
          await delay(killAfterMs);
        }
        if (!state.settled) {
          state.forceKillNeeded = true;
          appendStderr(`Force-killing timed out command after killAfterMs=${killAfterMs}: ${command}\n`);
          await ignoreKillError(killChildTree(true));
        }
        resolve();
      })();
    }, timeoutMs);
  });

  const exitPromise = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) => {
    child.once('error', (error) => {
      appendStderr(`${error.message}\n`);
      resolve({ code: 1, signal: null });
    });
    child.once('exit', (code, signal) => {
      resolve({ code, signal });
    });
  });

  const exit = await exitPromise;
  const exitedGracefullyAfterTimeout = state.timedOut && exit.code === 0 && exit.signal === null;
  state.settled = !state.timedOut || exitedGracefullyAfterTimeout;
  if (timeout) {
    clearTimeout(timeout);
  }
  removeSignalHandlers();

  if (state.timedOut) {
    await timeoutPromise;
    state.settled = true;
  }

  const code = exit.code ?? signalToExitCode(exit.signal);
  if (code !== 0 && !state.timedOut) {
    appendStderr(`Command exited with status ${code}: ${command}\n`);
  }

  if (state.timedOut && !state.forceKillNeeded) {
    appendStderr(`Timed out command exited after graceful termination: ${command}\n`);
  }

  return {
    success: !state.timedOut && code === 0,
    terminalOutput: outputChunks.join(''),
  };
}

export function createProcessTreeKiller(): ProcessTreeKiller {
  return {
    kill(pid, signal) {
      return killTree(pid, signal);
    },
  };
}

function killTree(pid: number, signal: NodeJS.Signals): Promise<void> {
  return new Promise((resolve) => {
    treeKill(pid, signal, () => resolve());
  });
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
