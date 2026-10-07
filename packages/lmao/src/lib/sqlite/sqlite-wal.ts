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
 * So the file is built where no other process can see it — a private sibling path — switched to WAL there, closed,
 * and published with link(2), which either creates `dbPath` naming the finished file or fails with `EEXIST` because
 * another opener published first. Either way `dbPath` only ever names a file whose header already says WAL, and an
 * existing file is never replaced or converted.
 *
 * @module sqlite-wal
 */

import { randomUUID } from 'node:crypto';
import { existsSync, linkSync, mkdirSync, rmSync } from 'node:fs';
import { basename, dirname, join } from 'node:path';
import { readJournalMode } from './sqlite-common.js';
import type { SyncSQLiteDatabase } from './sqlite-db.js';

const JOURNAL_MODE_WAL_SQL = 'PRAGMA journal_mode = WAL';

/**
 * Open `dbPath` with `open`, first creating it in WAL if it does not exist.
 *
 * `open` is the driver's own constructor (`bun:sqlite`, `node:sqlite`, better-sqlite3), called for the private draft
 * and then for `dbPath`. A file already at `dbPath` is opened as it is, in whatever journal mode it has. A fileless
 * name (`:memory:`, `''`) has no path to publish — doing so would leave a literal file nothing reads — so it is opened
 * directly.
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
  // Same directory, so link(2) never crosses a filesystem; dot-prefixed, so it reads as nobody's database.
  const draft = join(dirname(dbPath), `.${basename(dbPath)}.${randomUUID()}.draft`);
  try {
    const db = open(draft);
    try {
      // Nothing else can open the draft, so this conversion holds the only lock on it and cannot be refused for
      // contention. Closing the last connection checkpoints and removes the draft's `-wal` and `-shm`.
      const mode = readJournalMode(db, JOURNAL_MODE_WAL_SQL);
      if (mode !== 'wal') {
        throw new Error(`${JOURNAL_MODE_WAL_SQL} on ${draft} settled on journal_mode=${mode}`);
      }
    } finally {
      db.close();
    }
    try {
      linkSync(draft, dbPath);
    } catch (error) {
      // Another opener published first; its file is just as WAL as this draft.
      if (!(error instanceof Error && 'code' in error && error.code === 'EEXIST')) {
        throw error;
      }
    }
  } finally {
    rmSync(draft, { force: true });
  }
}
