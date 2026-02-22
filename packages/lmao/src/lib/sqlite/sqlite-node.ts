/// <reference types="node" />

import { DatabaseSync } from 'node:sqlite';
import type {
  AsyncSQLiteDatabase,
  AsyncSQLiteStatement,
  SyncSQLiteDatabase,
  SyncSQLiteStatement,
} from './sqlite-db.js';

function toSyncStatement(statement: {
  run(...params: unknown[]): unknown;
  all(...params: unknown[]): unknown[];
  get(...params: unknown[]): unknown;
}): SyncSQLiteStatement {
  return {
    run(...params: unknown[]): void {
      statement.run(...params);
    },
    all(...params: unknown[]): unknown[] {
      return statement.all(...params);
    },
    get(...params: unknown[]): unknown {
      return statement.get(...params);
    },
  };
}

/**
 * Create a Node built-in sqlite database adapter.
 *
 * Use this in Node Vitest setup when you want trace output persisted
 * to a deterministic file path.
 */
export function createNodeSQLiteDatabase(dbPath: string): SyncSQLiteDatabase {
  const db = new DatabaseSync(dbPath);
  return {
    exec(sql: string): void {
      db.exec(sql);
    },
    prepare(sql: string): SyncSQLiteStatement {
      return toSyncStatement(db.prepare(sql));
    },
    close(): void {
      db.close();
    },
  };
}

function toAsyncStatement(statement: SyncSQLiteStatement): AsyncSQLiteStatement {
  return {
    async run(...params: unknown[]): Promise<void> {
      statement.run(...params);
    },
    async all(...params: unknown[]): Promise<unknown[]> {
      return statement.all(...params);
    },
    async get(...params: unknown[]): Promise<unknown> {
      return statement.get(...params);
    },
  };
}

/**
 * Create an async adapter around Node built-in sqlite.
 *
 * Useful when wiring into `createAsyncDatabase` callbacks while still
 * writing to a deterministic file path on Node.
 */
export function createNodeSQLiteAsyncDatabase(dbPath: string): AsyncSQLiteDatabase {
  const syncDb = createNodeSQLiteDatabase(dbPath);
  return {
    async exec(sql: string): Promise<void> {
      syncDb.exec(sql);
    },
    prepare(sql: string): AsyncSQLiteStatement {
      return toAsyncStatement(syncDb.prepare(sql));
    },
    async close(): Promise<void> {
      syncDb.close();
    },
  };
}
