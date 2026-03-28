/**
 * SQLiteTraceWriter - General-purpose writer that persists span buffer trees to SQLite.
 *
 * Accepts a SyncSQLiteDatabase instance (from bun:sqlite or better-sqlite3)
 * and persists the full span tree as a flat `spans` table with dynamic column
 * evolution based on the LogSchema fields found at flush time.
 *
 * Architecture:
 * - Single `spans` table — no runs/log_entries decomposition
 * - trace_id IS the run identifier (one root span per trace)
 * - Tree structure via span_id / parent_span_id (no depth column)
 * - User schema columns added dynamically via ALTER TABLE
 * - Per-buffer ensureColumns for cross-library schema merging
 *
 * @module sqlite-writer
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
  type SpanSegment,
  walkSpanSegments,
} from './sqlite-common.js';
import type { SyncSQLiteDatabase, SyncSQLiteStatement } from './sqlite-db.js';

export interface SQLiteWriterConfig {
  /** Path to SQLite file. Default: '.trace-results.db' */
  dbPath?: string;
}

export class SQLiteTraceWriter {
  private knownColumns = new Set<string>();
  private insertStmtCache = new Map<string, SyncSQLiteStatement>();

  constructor(private db: SyncSQLiteDatabase) {
    this.init();
  }

  private init(): void {
    this.db.exec(SPANS_TABLE_INIT_SQL);

    this.refreshKnownColumns();
  }

  private refreshKnownColumns(): void {
    this.knownColumns.clear();
    // WHY: PRAGMA row shape changes would make later ALTER/INSERT logic drift silently, so validate here before
    // mutating the writer's cached schema view.
    const rows = parseSqliteTableInfoRows(this.db.prepare(SPANS_TABLE_INFO_SQL).all());
    for (const column of extractSqliteColumnsFromTableInfo(rows)) {
      this.knownColumns.add(column.name);
    }
  }

  /** Ensure user-defined schema columns exist in the spans table */
  private ensureColumns(schema: LogSchema): void {
    for (const column of getMissingSchemaColumns(schema, this.knownColumns)) {
      try {
        this.db.exec(buildAddColumnSql(column));
      } catch (error) {
        if (!isSqliteDuplicateColumnError(error, column.name)) {
          throw error;
        }
      }

      this.refreshKnownColumns();
      this.knownColumns.add(column.name);
    }
  }

  private writeSegmentRows(segment: SpanSegment): void {
    this.ensureColumns(segment.buffer._logSchema);

    const activeUserFields = getActiveUserFields(segment.buffer._logSchema, this.knownColumns);
    const insertStmt = this.getInsertStatement(activeUserFields);

    for (let row = 0; row < segment.buffer._writeIndex; row++) {
      insertStmt.run(...buildInsertParams(segment, row, activeUserFields));
    }
  }

  private flushAllSegments(rootBuffer: AnySpanBuffer): void {
    for (const segment of walkSpanSegments(rootBuffer)) {
      this.writeSegmentRows(segment);
    }
  }

  private getInsertStatement(activeUserFields: readonly string[]): SyncSQLiteStatement {
    const key = getInsertStatementCacheKey(activeUserFields);
    const cached = this.insertStmtCache.get(key);
    if (cached) {
      return cached;
    }

    const stmt = this.db.prepare(buildInsertSql(activeUserFields));
    this.insertStmtCache.set(key, stmt);
    return stmt;
  }

  /** Write a root SpanBuffer tree to the database */
  flush(rootBuffer: AnySpanBuffer): void {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      this.flushAllSegments(rootBuffer);
      this.db.exec('COMMIT');
    } catch (error) {
      this.db.exec('ROLLBACK');
      throw error;
    }
  }

  close(): void {
    this.db.close();
  }
}
