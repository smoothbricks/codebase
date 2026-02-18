/**
 * SQLite persistence for LMAO span buffers.
 *
 * General-purpose sink and query API for writing span buffer trees to SQLite.
 * Platform-agnostic — accepts any SyncSQLiteDatabase (bun:sqlite or better-sqlite3).
 *
 * @module sqlite
 */

export type { SyncSQLiteDatabase, SyncSQLiteStatement } from './sqlite-db.js';
export { type TraceSQLiteConfig, TraceSQLiteSink } from './sqlite-sink.js';
