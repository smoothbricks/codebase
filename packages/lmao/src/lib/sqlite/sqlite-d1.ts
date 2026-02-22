/**
 * D1 adapter for LMAO async SQLite interfaces.
 *
 * This keeps D1-specific behavior in one place while exposing the generic
 * AsyncSQLiteDatabase contract used by SQLiteAsyncTracer/SQLiteAsyncTraceWriter.
 *
 * @module sqlite-d1
 */

import type { AsyncSQLiteDatabase, AsyncSQLiteStatement } from './sqlite-db.js';

type D1BindValue = null | number | string | ArrayBuffer | ArrayBufferView;

export interface D1LikePreparedStatement {
  bind(...values: D1BindValue[]): D1LikePreparedStatement;
  run(): Promise<unknown>;
  all<T = unknown>(): Promise<{ results?: T[] }>;
  first<T = unknown>(): Promise<T | null>;
}

export interface D1LikeDatabase {
  prepare(query: string): D1LikePreparedStatement;
  exec(query: string): Promise<unknown>;
}

function toD1BindValue(value: unknown): D1BindValue {
  if (value == null) {
    return null;
  }

  if (typeof value === 'boolean') {
    return value ? 1 : 0;
  }

  if (typeof value === 'bigint') {
    return Number(value);
  }

  if (typeof value === 'number' || typeof value === 'string') {
    return value;
  }

  if (value instanceof ArrayBuffer) {
    return value;
  }

  if (ArrayBuffer.isView(value)) {
    return value;
  }

  throw new Error(`Unsupported D1 bind param type: ${typeof value}`);
}

class D1StatementAdapter implements AsyncSQLiteStatement {
  constructor(
    private readonly db: D1LikeDatabase,
    private readonly query: string,
  ) {}

  private bind(params: unknown[]): D1LikePreparedStatement {
    const statement = this.db.prepare(this.query);
    return statement.bind(...params.map(toD1BindValue));
  }

  async run(...params: unknown[]): Promise<void> {
    await this.bind(params).run();
  }

  async all(...params: unknown[]): Promise<unknown[]> {
    const result = await this.bind(params).all();
    return result.results ?? [];
  }

  async get(...params: unknown[]): Promise<unknown> {
    return (await this.bind(params).first()) ?? undefined;
  }
}

class D1DatabaseAdapter implements AsyncSQLiteDatabase {
  constructor(private readonly db: D1LikeDatabase) {}

  async exec(sql: string): Promise<void> {
    const statements = sql
      .split(';')
      .map((statement) => statement.trim())
      .filter((statement) => statement.length > 0);

    for (const statement of statements) {
      await this.db.prepare(statement).run();
    }
  }

  prepare(sql: string): AsyncSQLiteStatement {
    return new D1StatementAdapter(this.db, sql);
  }

  async close(): Promise<void> {
    // D1 bindings are managed by the worker runtime.
  }
}

export function createD1SQLiteDatabase(db: D1LikeDatabase): AsyncSQLiteDatabase {
  return new D1DatabaseAdapter(db);
}
