/**
 * SQLite persistence for LMAO span buffers.
 *
 * General-purpose sink and query API for writing span buffer trees to SQLite.
 * Platform-agnostic — accepts any SyncSQLiteDatabase (bun:sqlite or better-sqlite3).
 *
 * @module sqlite
 */

export { TraceSQLiteAsync } from './sqlite-async.js';
export { createD1SQLiteDatabase, type D1LikeDatabase, type D1LikePreparedStatement } from './sqlite-d1.js';
export type {
  AsyncSQLiteDatabase,
  AsyncSQLiteStatement,
  SyncSQLiteDatabase,
  SyncSQLiteStatement,
} from './sqlite-db.js';
export { TraceSQLite, type TraceSQLiteConfig, TraceSQLiteSink } from './sqlite-sink.js';
