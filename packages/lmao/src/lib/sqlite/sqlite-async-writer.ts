/**
 * SQLiteAsyncTraceWriter - Async SQLite persistence for span buffer trees.
 *
 * Works with async SQLite drivers (for example Cloudflare D1 adapters) while
 * preserving the same `spans` table shape as the sync SQLiteTraceWriter.
 *
 * @module sqlite-async-writer
 */

import type { LogSchema } from '../schema/LogSchema.js';
import type { AnySpanBuffer } from '../types.js';
import {
  buildAddColumnSql,
  buildInsertParams,
  buildInsertSql,
  extractSqliteColumnsFromTableInfo,
  getActiveUserFields,
  getInsertStatementCacheKey,
  getMissingSchemaColumns,
  isSqliteDuplicateColumnError,
  parseSqliteTableInfoRows,
  SPANS_TABLE_INFO_SQL,
  SPANS_TABLE_INIT_SQL,
  walkSpanSegments,
} from './sqlite-common.js';
import type { AsyncSQLiteDatabase, AsyncSQLiteStatement } from './sqlite-db.js';

export class SQLiteAsyncTraceWriter {
  private knownColumns = new Set<string>();
  private insertStmtCache = new Map<string, AsyncSQLiteStatement>();
  private initialized = false;

  constructor(private db: AsyncSQLiteDatabase) {}

  private async init(): Promise<void> {
    if (this.initialized) {
      return;
    }

    await this.db.exec(SPANS_TABLE_INIT_SQL);

    await this.refreshKnownColumns();

    this.initialized = true;
  }

  private async refreshKnownColumns(): Promise<void> {
    this.knownColumns.clear();
    // WHY: async drivers are still a real schema boundary; validate PRAGMA rows before caching column names that drive
    // later DDL and inserts.
    const rows = parseSqliteTableInfoRows(await this.db.prepare(SPANS_TABLE_INFO_SQL).all());
    for (const column of extractSqliteColumnsFromTableInfo(rows)) {
      this.knownColumns.add(column.name);
    }
  }

  private async ensureColumns(schema: LogSchema): Promise<void> {
    for (const column of getMissingSchemaColumns(schema, this.knownColumns)) {
      try {
        await this.db.exec(buildAddColumnSql(column));
      } catch (error) {
        if (!isSqliteDuplicateColumnError(error, column.name)) {
          throw error;
        }
      }

      await this.refreshKnownColumns();
      this.knownColumns.add(column.name);
    }
  }

  private getInsertStatement(activeUserFields: readonly string[]): AsyncSQLiteStatement {
    const key = getInsertStatementCacheKey(activeUserFields);
    const cached = this.insertStmtCache.get(key);
    if (cached) {
      return cached;
    }

    const stmt = this.db.prepare(buildInsertSql(activeUserFields));
    this.insertStmtCache.set(key, stmt);
    return stmt;
  }

  async flush(rootBuffer: AnySpanBuffer): Promise<void> {
    await this.init();
    for (const segment of walkSpanSegments(rootBuffer)) {
      await this.ensureColumns(segment.buffer._logSchema);

      const activeUserFields = getActiveUserFields(segment.buffer._logSchema, this.knownColumns);
      const insertStmt = this.getInsertStatement(activeUserFields);

      for (let row = 0; row < segment.buffer._writeIndex; row++) {
        await insertStmt.run(...buildInsertParams(segment, row, activeUserFields));
      }
    }
  }

  async close(): Promise<void> {
    await this.db.close();
  }
}
