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

import { hasOwnString, isRecord } from '@smoothbricks/validation';
import { cleanupDebug } from '../cleanupDiagnostics.js';
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

const JOURNAL_MODE_WAL_SQL = 'PRAGMA journal_mode = WAL';
const DATABASE_LIST_SQL = 'PRAGMA database_list';

/** Journal modes that keep no rollback record, so a killed writer leaves partially applied pages behind. */
const UNRECOVERABLE_JOURNAL_MODES: Record<string, true> = { memory: true, off: true };

export interface SQLiteWriterConfig {
  /** Path to SQLite file. Defaults to `DEFAULT_TRACE_DB_PATH` from `./trace-db-path.js`. */
  dbPath?: string;
}

export class SQLiteTraceWriter {
  private knownColumns = new Set<string>();
  private insertStmtCache = new Map<string, SyncSQLiteStatement>();

  constructor(private db: SyncSQLiteDatabase) {
    this.init();
  }

  private init(): void {
    // Parallel Bun test workers share one trace DB, so wait for short SQLite writer locks instead of failing setup.
    this.db.exec('PRAGMA busy_timeout = 10000');
    this.requireRecoverableJournal();
    this.db.exec(SPANS_TABLE_INIT_SQL);

    this.refreshKnownColumns();
  }

  /**
   * Put a file-backed sink in WAL, and refuse to write one that has no rollback record.
   *
   * Several test-worker processes write this database at once and tests assert over what lands in it, so it is an
   * oracle rather than a log. WAL keeps those readers off the writer's lock — measured at ~20ms worst-case writer
   * stall against 200-1900ms under the rollback journal with twelve concurrent writers. Its `-wal` and `-shm`
   * sidecars are also created once and then stay, where a rollback journal creates and unlinks one per transaction.
   *
   * `PRAGMA journal_mode` reports the mode it settled on instead of failing, so a refused conversion is silent.
   * `memory` and `off` keep no rollback record at all: a worker killed mid-transaction then leaves partially applied
   * rows that `PRAGMA integrity_check` still calls "ok", turning a crashed run into wrong assertion input rather than
   * a missing one. An in-memory database has no file to protect and legitimately settles on `memory`.
   */
  private requireRecoverableJournal(): void {
    const modeRow = this.db.prepare(JOURNAL_MODE_WAL_SQL).get();
    if (!isRecord(modeRow) || !hasOwnString(modeRow, 'journal_mode')) {
      throw new Error(`${JOURNAL_MODE_WAL_SQL} returned no journal_mode`);
    }

    const mode = modeRow.journal_mode.toLowerCase();
    if (!UNRECOVERABLE_JOURNAL_MODES[mode]) {
      return;
    }

    const file = this.readMainDatabaseFile();
    if (file === '') {
      return;
    }

    throw new Error(
      `Trace database at ${file} settled on journal_mode=${mode}, which cannot roll back a killed writer`,
    );
  }

  /** Backing file of the `main` schema, or '' for an in-memory database. */
  private readMainDatabaseFile(): string {
    for (const row of this.db.prepare(DATABASE_LIST_SQL).all()) {
      if (isRecord(row) && hasOwnString(row, 'name') && row.name === 'main' && hasOwnString(row, 'file')) {
        return row.file;
      }
    }

    throw new Error(`${DATABASE_LIST_SQL} returned no main schema`);
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
    cleanupDebug('sqliteWriter.flush:start', { statementCacheSize: this.insertStmtCache.size });
    this.db.exec('BEGIN IMMEDIATE');
    try {
      this.flushAllSegments(rootBuffer);
      this.db.exec('COMMIT');
      cleanupDebug('sqliteWriter.flush:end', { statementCacheSize: this.insertStmtCache.size });
    } catch (error) {
      this.db.exec('ROLLBACK');
      cleanupDebug('sqliteWriter.flush:error', { statementCacheSize: this.insertStmtCache.size });
      throw error;
    }
  }

  close(): void {
    cleanupDebug('sqliteWriter.close:start', { statementCacheSize: this.insertStmtCache.size });
    this.db.close();
    cleanupDebug('sqliteWriter.close:end');
  }
}
