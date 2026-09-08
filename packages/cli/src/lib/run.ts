import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { $ } from 'bun';

export async function run(
  command: string,
  args: string[],
  cwd: string,
  env?: Record<string, string>,
  unsetEnv?: readonly string[],
): Promise<void> {
  const status = await runStatus(command, args, cwd, false, env, unsetEnv);
  if (status !== 0) {
    throw new Error(`${command} ${args.join(' ')} failed with exit code ${status}`);
  }
}

export async function runStatus(
  command: string,
  args: string[],
  cwd: string,
  quiet = false,
  env?: Record<string, string>,
  unsetEnv?: readonly string[],
): Promise<number> {
  const invocation = resolveCommandInvocation(cwd, command, args);
  let shell = $`${invocation.command} ${invocation.args}`.cwd(cwd).nothrow();
  if (env || unsetEnv) {
    shell = shell.env(mergeEnv(env, unsetEnv));
  }
  const result = quiet ? await shell.quiet() : await shell;
  return result.exitCode;
}

export async function runInteractiveStatus(
  command: string,
  args: string[],
  cwd: string,
  env?: Record<string, string>,
  unsetEnv?: readonly string[],
): Promise<number> {
  const invocation = resolveCommandInvocation(cwd, command, args);
  return new Promise((resolve, reject) => {
    const child = spawn(invocation.command, invocation.args, {
      cwd,
      env: mergeEnv(env, unsetEnv),
      stdio: 'inherit',
    });
    child.on('error', reject);
    child.on('close', (code, signal) => {
      if (signal) {
        reject(new Error(`${command} ${args.join(' ')} terminated by signal ${signal}`));
        return;
      }
      resolve(code ?? 1);
    });
  });
}

export async function runResult(
  command: string,
  args: string[],
  cwd: string,
  env?: Record<string, string>,
  unsetEnv?: readonly string[],
): Promise<{ exitCode: number; stdout: string; stderr: string }> {
  const invocation = resolveCommandInvocation(cwd, command, args);
  let shell = $`${invocation.command} ${invocation.args}`.cwd(cwd).nothrow().quiet();
  if (env || unsetEnv) {
    shell = shell.env(mergeEnv(env, unsetEnv));
  }
  const result = await shell;
  return {
    exitCode: result.exitCode,
    stdout: decode(result.stdout),
    stderr: decode(result.stderr),
  };
}

// Captured output MUST survive failure. A non-zero exit whose diagnostics were
// swallowed is unactionable in CI, and Bun's raw `$` is the trap: without
// `.nothrow()` it throws a ShellError whose entire message is "Failed with exit
// code 1" and whose captured stdout/stderr die with it. Anything that needs a
// command's output goes through here instead of a bare `.quiet()`/`.text()`
// template, so the diagnostics reach the log before the error does.
export async function runText(
  command: string,
  args: string[],
  cwd: string,
  env?: Record<string, string>,
  unsetEnv?: readonly string[],
): Promise<string> {
  const result = await runResult(command, args, cwd, env, unsetEnv);
  if (result.exitCode !== 0) {
    printCommandOutput(result.stdout, result.stderr);
    throw new Error(`${command} ${args.join(' ')} failed with exit code ${result.exitCode}`);
  }
  return result.stdout;
}

export function printCommandOutput(stdout: string, stderr: string): void {
  if (stdout.length > 0) {
    console.log(trimTrailingNewline(stdout));
  }
  if (stderr.length > 0) {
    console.error(trimTrailingNewline(stderr));
  }
}

/**
 * This process's environment with `env` overlaid and `unsetEnv` withheld: the one overlay every child the CLI
 * spawns is given. An overlay alone cannot unset a variable this process inherited, so withheld names are
 * deleted after the overlay. The result is a fresh object; `process.env` is never mutated.
 */
export function mergeEnv(env?: Record<string, string>, unsetEnv?: readonly string[]): Record<string, string> {
  const merged: Record<string, string> = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (value !== undefined) {
      merged[key] = value;
    }
  }
  if (env) {
    Object.assign(merged, env);
  }
  if (unsetEnv) {
    for (const name of unsetEnv) {
      delete merged[name];
    }
  }
  return merged;
}

function resolveCommandInvocation(root: string, command: string, args: string[]): { command: string; args: string[] } {
  const localCommand = join(root, 'node_modules', '.bin', command);
  if (existsSync(localCommand)) {
    return { command: localCommand, args };
  }
  const bundledCommand = resolveBundledCommand(command);
  if (bundledCommand) {
    return { command: 'bun', args: [bundledCommand, ...args] };
  }
  return { command, args };
}

function resolveBundledCommand(command: string): string | null {
  try {
    if (command === 'sherif') {
      return fileURLToPath(import.meta.resolve('sherif'));
    }
  } catch {
    return null;
  }
  return null;
}

export async function findRepoRoot(): Promise<string> {
  const result = await $`git rev-parse --show-toplevel`.cwd(process.cwd()).quiet().nothrow();
  if (result.exitCode === 0) {
    return decode(result.stdout).trim();
  }
  return process.cwd();
}

export function decode(bytes: Uint8Array): string {
  return new TextDecoder().decode(bytes);
}

function trimTrailingNewline(value: string): string {
  return value.endsWith('\n') ? value.slice(0, -1) : value;
}
