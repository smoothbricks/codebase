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
  readJournalMode,
  SPANS_TABLE_INFO_SQL,
  SPANS_TABLE_INIT_SQL,
  type SpanSegment,
  walkSpanSegments,
} from './sqlite-common.js';
import type { SyncSQLiteDatabase, SyncSQLiteStatement } from './sqlite-db.js';
import { openSinkWriteLock, type SinkWriteLock } from './sqlite-write-lock.js';

const JOURNAL_MODE_SQL = 'PRAGMA journal_mode';
const DATABASE_LIST_SQL = 'PRAGMA database_list';

/** Journal modes that keep no rollback record, so a killed writer leaves partially applied pages behind. */
const UNRECOVERABLE_JOURNAL_MODES: Record<string, true> = { memory: true, off: true };

/**
 * Rows a writer of a shared sink commits per turn at its write lock. A whole test run is one trace, flushed when the
 * run ends: a single transaction over it (270,000 rows in one measured run, 1.7 s into a 2.7 GB sink) keeps every
 * sibling's next row waiting for that long. A turn's commit pays the WAL checkpoint its pages bring due, so smaller
 * turns cost the flush more in total: measured on that sink, 1,024 rows per turn take 3.6 s, 4,096 take 2.3 s with
 * no turn over 132 ms, and 16,384 take 1.6 s with turns up to 186 ms.
 */
const SHARED_FLUSH_ROWS_PER_TURN = 4096;

export interface SQLiteWriterConfig {
  /** Path to SQLite file. Defaults to `DEFAULT_TRACE_DB_PATH` from `./trace-db-path.js`. */
  dbPath?: string;
}

export class SQLiteTraceWriter {
  private knownColumns = new Set<string>();
  private insertStmtCache = new Map<string, SyncSQLiteStatement>();
  /** Present when `db` is a connection to a sink other processes write too (`./sqlite-write-lock.ts`). */
  private readonly writeLock: SinkWriteLock | undefined;

  constructor(private db: SyncSQLiteDatabase) {
    this.writeLock = openSinkWriteLock(db);
    this.init();
  }

  /** Run one write of this writer, alone among the processes sharing the sink when it is shared. */
  private exclusive<T>(write: () => T): T {
    return this.writeLock ? this.writeLock.hold(write) : write();
  }

  private init(): void {
    // Parallel Bun test workers share one trace DB, so wait for short SQLite writer locks instead of failing setup.
    this.db.exec('PRAGMA busy_timeout = 10000');
    this.requireRecoverableJournal();
    // Creating the table writes the schema: a write like any other on a shared sink.
    this.exclusive(() => this.db.exec(SPANS_TABLE_INIT_SQL));

    this.refreshKnownColumns();
  }

  /**
   * Refuse to write a file-backed sink that has no rollback record, without changing its journal mode.
   *
   * Tests assert over what lands in this database, so it is an oracle rather than a log. `memory` and `off` keep no
   * rollback record at all: a worker killed mid-transaction then leaves partially applied rows that
   * `PRAGMA integrity_check` still calls "ok", turning a crashed run into wrong assertion input rather than a missing
   * one. An in-memory database has no file to protect and legitimately reports `memory`.
   *
   * The mode is only read. Changing it here is what failed: the writer opens a database other processes have open
   * too, and converting a shared file to WAL is refused with SQLITE_BUSY without waiting (see `./sqlite-wal.ts`). WAL
   * is set by whoever creates the file — `openWalDatabase` and the openers built on it.
   */
  private requireRecoverableJournal(): void {
    const mode = readJournalMode(this.db, JOURNAL_MODE_SQL);
    if (!UNRECOVERABLE_JOURNAL_MODES[mode]) {
      return;
    }

    const file = this.readMainDatabaseFile();
    if (file === '') {
      return;
    }

    throw new Error(`Trace database at ${file} is in journal_mode=${mode}, which cannot roll back a killed writer`);
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

  /** Write rows `from` (inclusive) to `to` (exclusive) of one segment. */
  private writeSegmentRows(segment: SpanSegment, from: number, to: number): void {
    this.ensureColumns(segment.buffer._logSchema);

    const activeUserFields = getActiveUserFields(segment.buffer._logSchema, this.knownColumns);
    const insertStmt = this.getInsertStatement(activeUserFields);

    for (let row = from; row < to; row++) {
      insertStmt.run(...buildInsertParams(segment, row, activeUserFields));
    }
  }

  private transaction(write: () => void): void {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      write();
      this.db.exec('COMMIT');
    } catch (error) {
      this.db.exec('ROLLBACK');
      throw error;
    }
  }

  /**
   * Write the tree in turns at the write lock, each one transaction of at most `SHARED_FLUSH_ROWS_PER_TURN` rows, so
   * a sibling process waits for one turn and never for the trace.
   */
  private flushInTurns(rootBuffer: AnySpanBuffer, writeLock: SinkWriteLock): void {
    const segments = walkSpanSegments(rootBuffer);
    let next = segments.next();
    let row = 0;
    while (!next.done) {
      writeLock.hold(() =>
        this.transaction(() => {
          let budget = SHARED_FLUSH_ROWS_PER_TURN;
          while (budget > 0 && !next.done) {
            const segment = next.value;
            const end = Math.min(segment.buffer._writeIndex, row + budget);
            this.writeSegmentRows(segment, row, end);
            budget -= end - row;
            if (end === segment.buffer._writeIndex) {
              next = segments.next();
              row = 0;
            } else {
              row = end;
            }
          }
        }),
      );
    }
  }

  /**
   * Write one row of a span's first buffer now, in its own transaction, while its trace still runs.
   *
   * Row 0 is the span's start and row 1 its completion (specs/lmao/01h). A process that is killed before its trace
   * ends has then left the spans it started and the ones it finished; `flush` writes every row again when the trace
   * ends, so a tag set after the start replaces the early row.
   */
  writeRow(buffer: AnySpanBuffer, row: 0 | 1): void {
    this.exclusive(() => {
      this.ensureColumns(buffer._logSchema);
      const activeUserFields = getActiveUserFields(buffer._logSchema, this.knownColumns);
      const segment: SpanSegment = {
        buffer,
        traceId: buffer.trace_id,
        spanId: buffer.span_id,
        parentSpanId: buffer.parent_span_id,
        rowOffset: 0,
      };
      this.getInsertStatement(activeUserFields).run(...buildInsertParams(segment, row, activeUserFields));
    });
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

  /**
   * Write a root SpanBuffer tree to the database.
   *
   * One transaction, unless the sink is shared with other writer processes: then the tree is written in bounded
   * turns (`flushInTurns`), and a reader of another process can see a trace whose later rows are not written yet,
   * as it already can under `liveChildRows`. A failed turn rolls back alone and the error is thrown.
   */
  flush(rootBuffer: AnySpanBuffer): void {
    cleanupDebug('sqliteWriter.flush:start', { statementCacheSize: this.insertStmtCache.size });
    try {
      if (this.writeLock) {
        this.flushInTurns(rootBuffer, this.writeLock);
      } else {
        this.transaction(() => {
          for (const segment of walkSpanSegments(rootBuffer)) {
            this.writeSegmentRows(segment, 0, segment.buffer._writeIndex);
          }
        });
      }
      cleanupDebug('sqliteWriter.flush:end', { statementCacheSize: this.insertStmtCache.size });
    } catch (error) {
      cleanupDebug('sqliteWriter.flush:error', { statementCacheSize: this.insertStmtCache.size });
      throw error;
    }
  }

  close(): void {
    cleanupDebug('sqliteWriter.close:start', { statementCacheSize: this.insertStmtCache.size });
    this.writeLock?.close();
    this.db.close();
    cleanupDebug('sqliteWriter.close:end');
  }
}
