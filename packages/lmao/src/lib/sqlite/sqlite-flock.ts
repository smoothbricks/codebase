/// <reference types="bun" />
/// <reference types="node" />

/**
 * A sink's write lock as flock(2) on a persistent sibling file.
 *
 * flock blocks in the kernel until the holder unlocks or its process ends, so a waiter neither polls nor carries a
 * bound, and a killed holder cannot strand the lock. The lock file is separate from the sink and from the publication
 * gate: on BSD kernels flock and SQLite's fcntl locks on one file exclude each other. It is never unlinked, for the
 * publication gate's reason: its inode is the lock.
 *
 * Node has no advisory-lock call, and neither runtime has one on Windows; there the sink has no write lock and its
 * writers wait the way SQLite waits. Bun reaches flock in the C library it already has loaded, through `bun:ffi`.
 *
 * @module sqlite-flock
 */

import { closeSync, openSync } from 'node:fs';
import { createRequire } from 'node:module';
import type { SinkWriteLock } from './sqlite-write-lock.js';

const LOCK_EX = 2;
const LOCK_UN = 8;
const EINTR = 4;

interface Flock {
  /** flock(2): 0, or -1 with the reason in `errno()`. */
  flock(fd: number, operation: number): number;
  errno(): number;
}

/** The C library of the running process and the function that returns the address of its `errno`. */
function cLibraries(): { readonly names: readonly string[]; readonly errnoAddress: '__error' | '__errno_location' } {
  if (process.platform === 'darwin') {
    return { names: ['libSystem.B.dylib'], errnoAddress: '__error' };
  }
  // glibc, then musl, by the name each is loaded under.
  const muslArch = process.arch === 'x64' ? 'x86_64' : process.arch === 'arm64' ? 'aarch64' : process.arch;
  return { names: ['libc.so.6', `libc.musl-${muslArch}.so.1`], errnoAddress: '__errno_location' };
}

function loadFlock(): Flock {
  const ffi: typeof import('bun:ffi') = createRequire(import.meta.url)('bun:ffi');
  const { names, errnoAddress } = cLibraries();
  const failures: unknown[] = [];
  for (const name of names) {
    try {
      const { symbols } = ffi.dlopen(name, {
        flock: { args: [ffi.FFIType.i32, ffi.FFIType.i32], returns: ffi.FFIType.i32 },
        [errnoAddress]: { args: [], returns: ffi.FFIType.ptr },
      });
      const errnoPointer = symbols[errnoAddress];
      return {
        flock: (fd, operation) => symbols.flock(fd, operation),
        errno() {
          const address = errnoPointer();
          if (address === null) throw new Error(`${errnoAddress}() returned no address`);
          return ffi.read.i32(address, 0);
        },
      };
    } catch (error) {
      failures.push(error);
    }
  }
  throw new AggregateError(failures, `Cannot load flock from the C library (${names.join(', ')})`);
}

let loaded: Flock | undefined;

/**
 * Open the write lock at `lockPath`, creating its file. Returns undefined where the runtime has no flock (see the
 * module comment).
 */
export function openFlockWriteLock(lockPath: string): SinkWriteLock | undefined {
  if (process.versions.bun === undefined || process.platform === 'win32') {
    return undefined;
  }
  loaded ??= loadFlock();
  const { flock, errno } = loaded;
  // One open file description per writer: flock excludes descriptions, so two writers of one process exclude too.
  const fd = openSync(lockPath, 'a');
  return {
    hold(write) {
      // A signal handler can end the wait without the lock (EINTR); the wait then starts again. Nothing else can.
      while (flock(fd, LOCK_EX) !== 0) {
        const reason = errno();
        if (reason !== EINTR) throw new Error(`flock(LOCK_EX) on ${lockPath} failed with errno ${reason}`);
      }
      try {
        return write();
      } finally {
        flock(fd, LOCK_UN);
      }
    },
    close() {
      closeSync(fd);
    },
  };
}
