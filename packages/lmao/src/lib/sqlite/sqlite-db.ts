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
  /**
   * Execute repeated bindings in order, atomically, on this statement's connection.
   * Resolution acknowledges every row; rejection leaves none of this batch applied.
   * An existing caller-owned transaction remains open and retains its prior writes.
   * Drivers without an atomic bulk operation omit this capability.
   */
  runMany?(rows: readonly (readonly unknown[])[]): Promise<void>;
  all(...params: unknown[]): Promise<unknown[]>;
  get(...params: unknown[]): Promise<unknown>;
}
