import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { $ } from 'bun';

export async function run(command: string, args: string[], cwd: string, env?: Record<string, string>): Promise<void> {
  const status = await runStatus(command, args, cwd, false, env);
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
): Promise<number> {
  const invocation = resolveCommandInvocation(cwd, command, args);
  let shell = $`${invocation.command} ${invocation.args}`.cwd(cwd).nothrow();
  if (env) {
    shell = shell.env(mergeEnv(env));
  }
  const result = quiet ? await shell.quiet() : await shell;
  return result.exitCode;
}

export async function runInteractiveStatus(
  command: string,
  args: string[],
  cwd: string,
  env?: Record<string, string>,
): Promise<number> {
  const invocation = resolveCommandInvocation(cwd, command, args);
  return new Promise((resolve, reject) => {
    const child = spawn(invocation.command, invocation.args, {
      cwd,
      env: env ? mergeEnv(env) : process.env,
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
): Promise<{ exitCode: number; stdout: string; stderr: string }> {
  const invocation = resolveCommandInvocation(cwd, command, args);
  let shell = $`${invocation.command} ${invocation.args}`.cwd(cwd).nothrow().quiet();
  if (env) {
    shell = shell.env(mergeEnv(env));
  }
  const result = await shell;
  return {
    exitCode: result.exitCode,
    stdout: decode(result.stdout),
    stderr: decode(result.stderr),
  };
}

function mergeEnv(env: Record<string, string>): Record<string, string> {
  const merged: Record<string, string> = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (value !== undefined) {
      merged[key] = value;
    }
  }
  return { ...merged, ...env };
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
    if (command === 'attw') {
      const packageJson = fileURLToPath(import.meta.resolve('@arethetypeswrong/cli/package.json'));
      return join(dirname(packageJson), 'dist', 'index.js');
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
