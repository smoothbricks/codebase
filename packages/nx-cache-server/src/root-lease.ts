import { dlopen, type Pointer, read } from 'bun:ffi';
import { type FileHandle, open } from 'node:fs/promises';
import { getSystemErrorName } from 'node:util';

/**
 * The one approved native call in this package: `flock(2)` through libc, because Bun has no portable
 * file-lock API. The lock is advisory and lives on an open file description, so it is released by
 * `close` or by process death, including SIGKILL. The lockfile itself is persistent and never unlinked.
 */
const LOCK_EX = 2;
const LOCK_NB = 4;

type FlockFunction = (fd: number, operation: number) => number;
type ErrnoLocation = () => Pointer | bigint | null;

/** Returns 0 when the lock is held, otherwise the positive errno of the failed `flock`. */
function flockErrno(fd: number, flock: FlockFunction, errnoLocation: ErrnoLocation): number {
  if (flock(fd, LOCK_EX | LOCK_NB) === 0) return 0;
  const location = errnoLocation();
  if (!location) throw new Error('root lease: libc returned a null errno location');
  const errno = read.i32(location, 0);
  if (errno === 0) throw new Error('root lease: flock failed but errno is 0; refusing to treat the lock as held');
  return errno;
}

function lockErrno(fd: number): number {
  if (process.platform === 'darwin') {
    const libc = dlopen('libc.dylib', {
      flock: { args: ['i32', 'i32'], returns: 'i32' },
      __error: { args: [], returns: 'ptr' },
    });
    try {
      return flockErrno(fd, libc.symbols.flock, libc.symbols.__error);
    } finally {
      libc.close();
    }
  }
  if (process.platform === 'linux') {
    const libc = dlopen('libc.so.6', {
      flock: { args: ['i32', 'i32'], returns: 'i32' },
      __errno_location: { args: [], returns: 'ptr' },
    });
    try {
      return flockErrno(fd, libc.symbols.flock, libc.symbols.__errno_location);
    } finally {
      libc.close();
    }
  }
  throw new Error(`root lease is unsupported on platform ${process.platform}`);
}

/**
 * Takes the exclusive, non-blocking root lease on `path`. Returns the open handle, which holds the lock
 * until it is closed, or `undefined` when another process holds it. Never truncates or unlinks `path`.
 */
export async function tryAcquireRootLease(path: string): Promise<FileHandle | undefined> {
  const handle = await open(path, 'a', 0o600);
  let owned = false;
  try {
    const errno = lockErrno(handle.fd);
    if (errno === 0) {
      owned = true;
      return handle;
    }
    // EWOULDBLOCK on Darwin; on Linux EAGAIN and EWOULDBLOCK are both 11.
    if (errno === (process.platform === 'darwin' ? 35 : 11)) return undefined;
    throw new Error(`flock ${path} failed: ${getSystemErrorName(-errno)}`);
  } finally {
    if (!owned) await handle.close();
  }
}
