#!/usr/bin/env node
import { existsSync } from 'node:fs';
import { dirname, isAbsolute, join, resolve } from 'node:path';
import { inspect } from 'node:util';

import { describeMiss, ensureBuilt, parseTargetSelector } from '../ensure-built.js';

const USAGE =
  'usage: smoothbricks-ensure-built <project:target[:configuration]> [--workspace-root <dir>] -- <binary> [args...]';

/** Argument and environment problems all exit 2, the shell's "usage" code. */
function usageError(message: string): never {
  process.stderr.write(`smoothbricks-ensure-built: ${message}\n${USAGE}\n`);
  process.exit(2);
}

function describeError(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  if (typeof error === 'object' && error !== null && 'message' in error && typeof error.message === 'string') {
    return error.message;
  }
  return typeof error === 'string'
    ? error
    : inspect(error, { breakLength: Number.POSITIVE_INFINITY, colors: false, depth: 5 });
}

/**
 * Split `--flag=value` into two tokens so both spellings parse identically.
 * Only leading `--` forms are split: a value may legitimately contain `=`.
 */
function tokenize(args: readonly string[]): string[] {
  const tokens: string[] = [];
  for (const arg of args) {
    const equals = arg.startsWith('--') ? arg.indexOf('=') : -1;
    if (equals === -1) {
      tokens.push(arg);
    } else {
      tokens.push(arg.slice(0, equals), arg.slice(equals + 1));
    }
  }
  return tokens;
}

function findWorkspaceRoot(from: string): string {
  let directory = from;
  for (;;) {
    if (existsSync(join(directory, 'nx.json'))) {
      return directory;
    }
    const parent = dirname(directory);
    if (parent === directory) {
      return usageError(`no nx.json at or above ${from}; pass --workspace-root`);
    }
    directory = parent;
  }
}

const argv = process.argv.slice(2);
const separator = argv.indexOf('--');
if (separator === -1) {
  usageError('missing `--` before the binary to exec');
}
const command = argv.slice(separator + 1);
if (command.length === 0) {
  usageError('no binary given after `--`');
}

let target: string | undefined;
let workspaceRootArg: string | undefined;
const tokens = tokenize(argv.slice(0, separator));
for (let index = 0; index < tokens.length; index += 1) {
  const token = tokens[index];
  if (token === '--workspace-root') {
    index += 1;
    workspaceRootArg = tokens[index] ?? usageError('--workspace-root needs a directory');
  } else if (token.startsWith('-')) {
    usageError(`unknown flag ${token}`);
  } else if (target !== undefined) {
    usageError(`more than one target given: ${target} and ${token}`);
  } else {
    target = token;
  }
}
if (target === undefined) {
  usageError('no project:target given');
}
if (parseTargetSelector(target) === null) {
  usageError(`'${target}' is not a project:target[:configuration] selector`);
}

// `execve` does no PATH lookup, so a bare name would fail as a missing file in
// the current directory. Say which mistake was made instead.
if (!command[0].includes('/')) {
  usageError(`'${command[0]}' must be a path to the binary, not a name to look up on PATH`);
}
// Resolved against the directory the user is standing in, not the workspace
// root: this is their command line, and the exec'd process inherits their cwd.
const binary = isAbsolute(command[0]) ? command[0] : resolve(process.cwd(), command[0]);

const workspaceRoot =
  workspaceRootArg === undefined ? findWorkspaceRoot(process.cwd()) : resolve(process.cwd(), workspaceRootArg);

const result = await ensureBuilt({ target, cwd: workspaceRoot }).catch((error: unknown) => {
  process.stderr.write(`smoothbricks-ensure-built: ${describeError(error)}\n`);
  process.exit(1);
});

if (result.disposition !== 'hit' && process.env.NX_VERBOSE_LOGGING === 'true') {
  process.stderr.write(`smoothbricks-ensure-built: ran ${target} because ${describeMiss(result.reason)}\n`);
}
if (result.disposition === 'failed') {
  if (result.signal !== null) {
    // Re-raise rather than translate, so a build killed by SIGINT leaves this
    // process looking killed by SIGINT to whatever is watching it.
    process.kill(process.pid, result.signal);
  }
  process.exit(result.exitCode);
}

if (process.execve === undefined) {
  throw new Error('smoothbricks-ensure-built needs process.execve, which requires a POSIX host on Node 24+ or Bun');
}
// `execve`, not spawn-and-wait: the built binary replaces this process, so it
// owns the terminal, the signals and the exit status directly, with no wrapper
// left behind to forward them.
process.execve(binary, [binary, ...command.slice(1)], process.env);
