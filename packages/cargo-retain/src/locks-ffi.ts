/// <reference path="./ffi.d.ts" />
/**
 * Cargo's locks taken by the compiled CLI, which calls flock(2) itself: `tryLockFd` is bound to `native/flock.c` by the
 * FFI manifest beside the package (`ffi.json`) and exists nowhere else, so only the native entry may import this
 * module. Node and the library take their locks through `locks-node.ts`.
 *
 * Only EWOULDBLOCK is `busy`. Any other failure of flock(2), such as a file system that has none, is `unavailable` with
 * its cause, as the Node provider answers, so the pass is refused with the reason instead of skipped for ever.
 */
import { closeSync } from 'node:fs';
import { describeLockFailure, type HoldAttempt, type LockMode, type LockProvider } from './locks.js';

/** What `tryLockFd` answers besides a negative failure. */
const HELD = 0;
const CONTENDED = 1;

/** The errno and OS cause the C wrapper wrote, up to its NUL. */
function failureText(cause: Uint8Array): string {
  return new TextDecoder().decode(cause.subarray(0, cause.indexOf(0)));
}

async function tryHold(paths: readonly string[], mode: LockMode): Promise<HoldAttempt> {
  // WHY lock: Cargo builds are the other writers and coordinate only through these flock(2)s (each holds one shared for
  // the whole build), so no actor of ours can stand in for them. The descriptors stay open in this process until
  // `release` because the pass reads fingerprints and renames generations across awaits, and a build must not start in
  // between. No child process holds them: the hold ends with this process, which the kernel enforces if it dies.
  const held: number[] = [];
  const letGo = async (): Promise<void> => {
    for (let fd = held.pop(); fd !== undefined; fd = held.pop()) closeSync(fd);
  };
  for (const path of paths) {
    const cause = new Uint8Array(256);
    const fd = openLockFd(new TextEncoder().encode(`${path}\0`), mode === 'create' ? 1 : 0, cause);
    if (fd < 0) {
      await letGo();
      return { kind: 'unavailable', reason: describeLockFailure('open', path, failureText(cause)) };
    }
    const verdict = tryLockFd(fd, cause);
    if (verdict !== HELD) {
      closeSync(fd);
      await letGo();
      return verdict === CONTENDED
        ? { kind: 'busy', lock: path }
        : { kind: 'unavailable', reason: describeLockFailure('flock', path, failureText(cause)) };
    }
    held.push(fd);
  }
  return { kind: 'held', hold: { release: letGo } };
}

export const ffiLocks: LockProvider = { tryHold };
