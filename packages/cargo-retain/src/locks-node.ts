/**
 * Cargo's locks taken from Node, which has no flock(2) of its own.
 *
 * macOS holds the lock in-process: `open(2)` with O_EXLOCK | O_NONBLOCK is `flock(LOCK_EX | LOCK_NB)` made with the open,
 * so a sandbox that refuses to exec an interpreter costs the pass nothing. Everywhere else a perl child holds it, for as
 * long as its stdin pipe lives.
 */
import { spawn } from 'node:child_process';
import { constants } from 'node:fs';
import { type FileHandle, open } from 'node:fs/promises';
import { createInterface } from 'node:readline';
import { describeCause, errorCode, type Result } from './errors.js';
import type { HoldAttempt, LockMode, LockProvider } from './locks.js';
import { O_EXLOCK } from './open-flags.js';

const LOCK_HELPER = [
  'use strict; use Fcntl qw(:flock O_RDONLY O_CREAT); $| = 1;',
  'my $mode = shift @ARGV; my @held;',
  'for my $path (@ARGV) {',
  '  my $flags = $mode eq "create" ? O_RDONLY | O_CREAT : O_RDONLY;',
  '  sysopen(my $fh, $path, $flags, 0644) or do { print "error\\t$path\\t$!\\n"; exit 0 };',
  '  if (!flock($fh, LOCK_EX | LOCK_NB)) { print(($!{EWOULDBLOCK} ? "busy\\t$path\\n" : "error\\t$path\\t$!\\n")); exit 0 }',
  '  push @held, $fh;',
  '}',
  'print "held\\n";',
  '1 while defined(my $line = <STDIN>);',
].join('\n');

export const nodeLocks: LockProvider = {
  tryHold: (paths, mode) => (process.platform === 'darwin' ? holdInProcess(paths, mode) : holdThroughPerl(paths, mode)),
};

/** `open(2)` takes each flock(2) itself: contention fails the open with EAGAIN, and closing the descriptor lets go. */
async function holdInProcess(paths: readonly string[], mode: LockMode): Promise<HoldAttempt> {
  const flags =
    (mode === 'create' ? constants.O_RDONLY | constants.O_CREAT : constants.O_RDONLY) | constants.O_NONBLOCK | O_EXLOCK;
  // WHY lock: the other writer is Cargo, which holds these flock(2)s shared for a whole build and has no other way to
  // exclude a pass (see locks.ts). The handles stay open in this process until `release`, across the pass's awaits, so a
  // build cannot start under a half-moved generation; the kernel drops them if this process dies.
  const held: FileHandle[] = [];
  const letGo = async (): Promise<void> => {
    await Promise.all(held.map((handle) => handle.close()));
  };
  for (const path of paths) {
    try {
      held.push(await open(path, flags, 0o644));
    } catch (error) {
      await letGo();
      return errorCode(error) === 'EAGAIN'
        ? { kind: 'busy', lock: path }
        : { kind: 'unavailable', reason: describeCause(error) };
    }
  }
  return { kind: 'held', hold: { release: letGo } };
}

function startHelper(mode: LockMode, paths: readonly string[]) {
  return spawn('perl', ['-e', LOCK_HELPER, mode, ...paths], { stdio: ['pipe', 'pipe', 'pipe'] });
}

/** A perl child takes each flock(2) on its own descriptor and holds it for as long as its stdin pipe lives. */
async function holdThroughPerl(paths: readonly string[], mode: LockMode): Promise<HoldAttempt> {
  // WHY lock: the other writer is Cargo (see locks.ts). A perl child holds the flock(2)s because Node has no flock(2)
  // here, so the hold spans the whole pass inside a child process: it lives exactly as long as its stdin pipe, which
  // `release` closes, and if this process dies the pipe closes, the child exits and the kernel drops the locks. The child
  // answers once and then only waits on that pipe; it runs nothing of the pass.
  let child: ReturnType<typeof startHelper>;
  try {
    child = startHelper(mode, paths);
  } catch (error) {
    // Node throws, not emits 'error', for exec failures it does not class as run-time, such as EPERM from a sandbox.
    return { kind: 'unavailable', reason: `cannot run perl: ${describeCause(error)}` };
  }
  const exited = new Promise<void>((resolve) => child.once('close', () => resolve()));
  let stderr = '';
  child.stderr.setEncoding('utf8').on('data', (chunk: string) => {
    stderr += chunk;
  });
  const lines = createInterface({ input: child.stdout });
  const first = await new Promise<Result<string, string>>((resolve) => {
    child.once('error', (error) => resolve({ ok: false, error: `cannot run perl: ${error.message}` }));
    lines.once('line', (line) => resolve({ ok: true, value: line }));
    lines.once('close', () => resolve({ ok: false, error: `perl ended before answering: ${stderr.trim()}` }));
  });
  if (!first.ok) {
    child.stdin.destroy();
    return { kind: 'unavailable', reason: first.error };
  }
  const [verdict, path, detail] = first.value.split('\t');
  if (verdict === 'held') {
    return {
      kind: 'held',
      hold: {
        async release() {
          child.stdin.end();
          await exited;
        },
      },
    };
  }
  child.stdin.end();
  await exited;
  if (verdict === 'busy' && path !== undefined) return { kind: 'busy', lock: path };
  return {
    kind: 'unavailable',
    reason: verdict === 'error' ? `${path}: ${detail}` : `unexpected answer ${first.value}`,
  };
}
