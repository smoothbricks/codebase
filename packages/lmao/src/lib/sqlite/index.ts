/**
 * SQLite persistence for LMAO span buffers.
 *
 * General-purpose writer API for persisting span buffer trees to SQLite.
 * Platform-agnostic — accepts any SyncSQLiteDatabase (bun:sqlite or better-sqlite3).
 *
 * @module sqlite
 */

export { SQLiteAsyncTraceWriter } from './sqlite-async-writer.js';
export {
  buildAddColumnSql,
  extractSqliteColumnsFromTableInfo,
  getMissingSqliteColumns,
  quoteSqlIdentifier,
  SPANS_TABLE_INFO_SQL,
  SPANS_TABLE_INIT_SQL,
} from './sqlite-common.js';
export { createD1SQLiteDatabase, type D1LikeDatabase, type D1LikePreparedStatement } from './sqlite-d1.js';
export type {
  AsyncSQLiteDatabase,
  AsyncSQLiteStatement,
  SyncSQLiteDatabase,
  SyncSQLiteStatement,
} from './sqlite-db.js';
export { SQLiteTraceWriter, type SQLiteWriterConfig } from './sqlite-writer.js';
