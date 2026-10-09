/// <reference types="node" />

/**
 * Open a SQLite file that several processes share, created in WAL from the moment its path exists.
 *
 * WAL because many writers share such a file while readers assert over it: WAL keeps readers off the writer's lock —
 * measured at ~20ms worst-case writer stall against 200-1900ms under the rollback journal with twelve concurrent
 * writers.
 *
 * WAL is a property of the file (bytes 18-19 of its header), so it is set once, when the file is made. Setting it later
 * on a shared path is the race this module exists to remove: on a connection that has not yet read a WAL header,
 * `PRAGMA journal_mode = WAL` begins a read transaction and then upgrades it to an exclusive one to rewrite the header
 * (vdbe.c OP_JournalMode -> btree.c sqlite3BtreeSetVersion). SQLite never runs the busy handler for an upgrade out of
 * a held read transaction — waiting could deadlock against another connection doing the same — so an opener whose
 * upgrade meets another connection's read lock gets SQLITE_BUSY at once, whatever `busy_timeout` says. Measured: 32
 * processes opening one fresh path and issuing the setter together, with `busy_timeout = 10000`, fail 24-27 times; the
 * same 32 against a file already in WAL never fail, because the setter then finds WAL in the header and changes
 * nothing.
 *
 * Coordinated lmao openers serialize publication with a persistent sibling SQLite gate, then recheck `dbPath`.
 * The gate holder builds a private sibling draft, switches it to WAL, closes it, and publishes it with rename(2).
 * Thus the visible file is already WAL, and no coordinated opener replaces or converts an existing sink. Other
 * creators must not write the sink path during publication. The gate is never unlinked: doing so splits its lock.
 *
 * @module sqlite-wal
 */

import { closeSync, existsSync, mkdirSync, openSync, readSync, renameSync, rmSync } from 'node:fs';
import { basename, dirname, join } from 'node:path';
import type { SyncSQLiteDatabase } from './sqlite-db.js';

const JOURNAL_MODE_WAL_SQL = 'PRAGMA journal_mode = WAL';
const PUBLISH_GATE_TIMEOUT_MS = 10_000;

/**
 * Open `dbPath` with `open`, first creating it in WAL if it does not exist.
 *
 * `open` is the driver's own constructor (`bun:sqlite`, `node:sqlite`, better-sqlite3), called for the publication
 * gate, private draft and then `dbPath`. A file already at `dbPath` is opened as it is, in whatever journal mode it
 * has. A fileless name (`:memory:`, `''`) has no path to publish, so it is opened directly. Publication assumes no
 * simultaneous same-path openers in different SQLite libraries on threads of one process (POSIX locks are per process).
 */
export function openWalDatabase<D extends SyncSQLiteDatabase>(dbPath: string, open: (path: string) => D): D {
  // SQLite's names for a database with no shared file: `:memory:`, and `''` for a private temporary one.
  if (dbPath === ':memory:' || dbPath === '') {
    return open(dbPath);
  }
  // The parent may be a directory SQLite will not create, such as the trace sink's `.cache/lmao`.
  mkdirSync(dirname(dbPath), { recursive: true });
  if (!existsSync(dbPath)) {
    publishWalDatabase(dbPath, open);
  }
  return open(dbPath);
}

function publishWalDatabase(dbPath: string, open: (path: string) => SyncSQLiteDatabase): void {
  const directory = dirname(dbPath);
  const name = basename(dbPath);
  const gatePath = join(directory, `.${name}.publish`);
  const gate = open(gatePath);
  try {
    gate.exec(`PRAGMA busy_timeout = ${PUBLISH_GATE_TIMEOUT_MS}`);
    try {
      // From autocommit, not a held read transaction: SQLite's busy handler may wait for the other publisher.
      gate.exec('BEGIN EXCLUSIVE');
    } catch (error) {
      throw new Error(
        `Cannot acquire SQLite WAL publication gate ${gatePath} (busy_timeout ${PUBLISH_GATE_TIMEOUT_MS}ms)`,
        { cause: error },
      );
    }
    if (existsSync(dbPath)) return;
    // Only the gate holder touches this fixed path, including a killed holder's leftover sidecars.
    const draft = join(directory, `.${name}.draft`);
    try {
      removeDraft(draft);
      const db = open(draft);
      try {
        // exec leaves no prepared statement alive: Bun's close_v2 can release the draft immediately.
        db.exec(JOURNAL_MODE_WAL_SQL);
      } finally {
        db.close();
      }
      requireWalHeader(draft);
      renameSync(draft, dbPath);
    } finally {
      // Cleanup must finish before releasing the gate, or it could delete the next holder's draft.
      removeDraft(draft);
    }
  } finally {
    // Closing rolls back the gate's unwritten transaction and releases its kernel lock, also on failure.
    // Never unlink the gate: another opener may already hold its inode while waiting for this lock.
    gate.close();
  }
}

function removeDraft(draft: string): void {
  rmSync(draft, { force: true });
  rmSync(`${draft}-wal`, { force: true });
  rmSync(`${draft}-shm`, { force: true });
  rmSync(`${draft}-journal`, { force: true });
}

function requireWalHeader(draft: string): void {
  const header = Buffer.alloc(20);
  const descriptor = openSync(draft, 'r');
  try {
    const bytes = readSync(descriptor, header, 0, header.length, 0);
    // SQLite header bytes 18/19 are the file's write/read versions: 2 means WAL for both.
    if (bytes !== header.length || header[18] !== 2 || header[19] !== 2) {
      throw new Error(
        `SQLite WAL draft ${draft} has ${bytes} header bytes and write/read versions ${header[18]}/${header[19]}`,
      );
    }
  } finally {
    closeSync(descriptor);
  }
}
