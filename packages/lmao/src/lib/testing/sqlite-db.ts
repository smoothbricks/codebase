/**
 * Minimal sync SQLite interface — satisfied by both bun:sqlite and better-sqlite3.
 *
 * No runtime dependency — just types. Each harness (bun, vitest) provides
 * the concrete implementation from its platform.
 *
 * @module sqlite-db
 */

export interface SyncSQLiteDatabase {
  exec(sql: string): void;
  prepare(sql: string): SyncSQLiteStatement;
  close(): void;
}

export interface SyncSQLiteStatement {
  run(...params: unknown[]): void;
  all(...params: unknown[]): unknown[];
  get(...params: unknown[]): unknown;
}
