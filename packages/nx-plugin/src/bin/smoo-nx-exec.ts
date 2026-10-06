#!/usr/bin/env node
import { isAbsolute, resolve } from 'node:path';

import { describeError, describeMiss, ensureBuilt, findNxWorkspaceRoot, parseExecArguments } from '../ensure-built.js';

const USAGE = 'usage: smoo-nx-exec <project:target[:configuration]> [--workspace-root <dir>] [-- <binary> [args...]]';

/** Argument and environment problems all exit 2, the shell's "usage" code. */
function usageError(message: string): never {
  process.stderr.write(`smoo-nx-exec: ${message}\n${USAGE}\n`);
  process.exit(2);
}

const parsed = parseExecArguments(process.argv.slice(2));
if (!parsed.ok) {
  usageError(parsed.usage);
}
const { target, command } = parsed.invocation;

const workspaceRoot =
  parsed.invocation.workspaceRoot === undefined
    ? (findNxWorkspaceRoot(process.cwd()) ??
      usageError(`no nx.json at or above ${process.cwd()}; pass --workspace-root`))
    : resolve(process.cwd(), parsed.invocation.workspaceRoot);

const result = await ensureBuilt({ target, cwd: workspaceRoot }).catch((error: unknown) => {
  process.stderr.write(`smoo-nx-exec: ${describeError(error)}\n`);
  process.exit(1);
});

if (result.disposition !== 'hit' && process.env.NX_VERBOSE_LOGGING === 'true') {
  process.stderr.write(`smoo-nx-exec: ran ${target} because ${describeMiss(result.reason)}\n`);
}
if (result.disposition === 'failed' || command.length === 0) {
  // This process ends here rather than becoming the binary, and `process.exit`
  // drops whatever a pipe has not taken yet: stdio pipes are asynchronous on
  // macOS, and the tail of a build log is the part that explains it. Under
  // Node an empty write completes once everything queued before it has.
  await Promise.all(
    [process.stdout, process.stderr].map((stream) => new Promise<void>((settle) => stream.write('', () => settle()))),
  );
}
if (result.disposition === 'failed') {
  if (result.signal !== null) {
    // Re-raise rather than translate, so a build killed by SIGINT leaves this
    // process looking killed by SIGINT to whatever is watching it.
    process.kill(process.pid, result.signal);
  }
  process.exit(result.exitCode);
}
if (command.length === 0) {
  process.exit(0);
}

// Resolved against the directory the user is standing in, not the workspace
// root: this is their command line, and the exec'd process inherits their cwd.
const binary = isAbsolute(command[0]) ? command[0] : resolve(process.cwd(), command[0]);

if (process.execve === undefined) {
  throw new Error('smoo-nx-exec needs process.execve, which requires a POSIX host on Node 24+ or Bun');
}
// Node puts a stdio pipe into O_NONBLOCK on macOS, and that flag belongs to the
// open file description, so it outlives execve. A binary that writes as if its
// stdout blocked would then lose everything past the first full pipe buffer
// (EAGAIN) whenever its reader falls behind: a `<cli> list | jq` read
// exactly 65536 bytes. Hand the binary the blocking stdio every program expects.
for (const stream of [process.stdin, process.stdout, process.stderr]) {
  const handle: unknown = Reflect.get(stream, '_handle');
  if (
    typeof handle === 'object' &&
    handle !== null &&
    'setBlocking' in handle &&
    typeof handle.setBlocking === 'function'
  ) {
    handle.setBlocking(true);
  }
}
// `execve`, not spawn-and-wait: the built binary replaces this process, so it
// owns the terminal, the signals and the exit status directly, with no wrapper
// left behind to forward them.
process.execve(binary, [binary, ...command.slice(1)], process.env);
