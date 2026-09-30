/// <reference types="node" />

import { spawn } from 'node:child_process';
import { constants } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const FORWARDED_SIGNALS = ['SIGINT', 'SIGTERM', 'SIGHUP', 'SIGQUIT'] as const;

/** The package root holding `bin/`, from the URL of a module in `src/` or `dist/ts/`. */
export function packageRootFromModule(moduleUrl: string): string {
  const moduleDirectory = dirname(fileURLToPath(moduleUrl));
  if (basename(moduleDirectory) === 'ts' && basename(dirname(moduleDirectory)) === 'dist') {
    return resolve(moduleDirectory, '..', '..');
  }
  return resolve(moduleDirectory, '..');
}

/**
 * The package's `bin`: the one place that decides which native binary answers (06_cli.md
 * "Launcher"). The library runs the CLI through it rather than restating that rule.
 */
export function launcherPath(packageRoot: string): string {
  return join(packageRoot, 'bin', 'cowshed');
}

/**
 * Run the CLI with `argv` through the launcher of the package at `packageRoot` and resolve with
 * its exit status, `128 + n` for a CLI ended by signal `n` as a shell reports it. The signals
 * that end a command early are forwarded to it while it runs; this process is never killed on
 * its behalf.
 */
export function runLauncher(packageRoot: string, argv: readonly string[]): Promise<number> {
  const { promise, resolve: resolveExit, reject } = Promise.withResolvers<number>();
  const child = spawn(launcherPath(packageRoot), [...argv], { stdio: 'inherit' });
  const forwarded = FORWARDED_SIGNALS.map((signal) => {
    const forward = () => {
      child.kill(signal);
    };
    process.once(signal, forward);
    return [signal, forward] as const;
  });
  const settle = () => {
    for (const [signal, forward] of forwarded) {
      process.off(signal, forward);
    }
  };
  child.once('error', (error) => {
    settle();
    reject(error);
  });
  child.once('exit', (code, signal) => {
    settle();
    resolveExit(signal === null ? (code ?? 1) : 128 + constants.signals[signal]);
  });
  return promise;
}
