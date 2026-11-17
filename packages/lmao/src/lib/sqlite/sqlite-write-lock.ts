/**
 * The lock that serializes the write transactions of the processes sharing one file-backed trace sink.
 *
 * SQLite's own wait for its writer lock is a poll against a fixed bound (`busy_timeout`): a waiter sleeps, retries,
 * and fails with SQLITE_BUSY when the bound passes, however alive the holder is, and a writer that commits again right
 * after its commit takes the lock from every sleeper. A sink written by all test processes of a package at once needs
 * a wait that ends when the holder is done, so its writers queue on a lock the kernel hands over
 * (`./sqlite-flock.ts`) and reach SQLite's lock with no cooperating process holding it.
 *
 * This module is the runtime-neutral half: the writer reads the lock from the connection it was given, and the opener
 * that knows the sink's path and the runtime (`openWalDatabase`) attaches it. A connection nobody attached one to
 * (an in-memory database, a driver opened by hand, a runtime with no lock call) is written with SQLite's wait alone.
 *
 * @module sqlite-write-lock
 */

import type { SyncSQLiteDatabase } from './sqlite-db.js';

export interface SinkWriteLock {
  /**
   * Run `write` while no other cooperating process writes the sink. Blocks until the lock is free, with no bound: the
   * holder is inside a synchronous write, and a holder that dies releases the lock with its process.
   */
  hold<T>(write: () => T): T;
  /** Give up this writer's handle on the lock. */
  close(): void;
}

/** Opens a connection's write lock, or reports that its runtime has none. */
export type SinkWriteLockOpener = () => SinkWriteLock | undefined;

// Keyed by the connection object the writer is handed. The opener is kept, not an open lock: a connection that never
// reaches a writer then holds no descriptor.
const openers = new WeakMap<SyncSQLiteDatabase, SinkWriteLockOpener>();

/** Name the write lock of the sink `db` is a connection to. */
export function shareSinkWriteLock(db: SyncSQLiteDatabase, open: SinkWriteLockOpener): void {
  openers.set(db, open);
}

/** Give an adapter the write lock of the connection it wraps. */
export function carrySinkWriteLock(from: SyncSQLiteDatabase, to: SyncSQLiteDatabase): void {
  const open = openers.get(from);
  if (open) {
    openers.set(to, open);
  }
}

/** Open the write lock shared for `db`, if one was. The caller closes it. */
export function openSinkWriteLock(db: SyncSQLiteDatabase): SinkWriteLock | undefined {
  return openers.get(db)?.();
}
