/**
 * Minimal SQLite interfaces used by trace persistence.
 *
 * Sync APIs match bun:sqlite / better-sqlite3 style drivers.
 * Async APIs match worker-style drivers (for example D1 adapters).
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

export interface AsyncSQLiteDatabase {
  exec(sql: string): Promise<void>;
  prepare(sql: string): AsyncSQLiteStatement;
  close(): Promise<void>;
}

export interface AsyncSQLiteStatement {
  run(...params: unknown[]): Promise<void>;
  all(...params: unknown[]): Promise<unknown[]>;
  get(...params: unknown[]): Promise<unknown>;
}
