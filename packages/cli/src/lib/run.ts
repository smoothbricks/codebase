import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { $ } from 'bun';

export async function run(command: string, args: string[], cwd: string): Promise<void> {
  const status = await runStatus(command, args, cwd);
  if (status !== 0) {
    throw new Error(`${command} ${args.join(' ')} failed with exit code ${status}`);
  }
}

export async function runStatus(command: string, args: string[], cwd: string, quiet = false): Promise<number> {
  const invocation = resolveCommandInvocation(cwd, command, args);
  const shell = $`${invocation.command} ${invocation.args}`.cwd(cwd).nothrow();
  const result = quiet ? await shell.quiet() : await shell;
  return result.exitCode;
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
