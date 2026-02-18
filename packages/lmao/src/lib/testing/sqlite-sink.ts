/**
 * TraceSQLiteSink - Writes span buffer trees to SQLite.
 *
 * Accepts a SyncSQLiteDatabase instance (from bun:sqlite or better-sqlite3)
 * and persists the full span tree with dynamic column evolution based on the
 * LogSchema fields found at flush time.
 *
 * @module sqlite-sink
 */

import type { SchemaType } from '@smoothbricks/arrow-builder';
import type { OpContextBinding } from '../opContext/types.js';
import type { LogSchema } from '../schema/LogSchema.js';
import { ENTRY_TYPE_SPAN_ERR, ENTRY_TYPE_SPAN_EXCEPTION, ENTRY_TYPE_SPAN_OK } from '../schema/systemSchema.js';
import type { TestTracer } from '../tracers/TestTracer.js';
import type { AnySpanBuffer, SpanBuffer } from '../types.js';
import type { SyncSQLiteDatabase } from './sqlite-db.js';

export interface TraceSQLiteConfig {
  /** Path to SQLite file. Default: '.trace-results.db' */
  dbPath?: string;
  /** Run ID. Default: auto-generated timestamp + random */
  runId?: string;
}

/** Map arrow-builder SchemaType to SQLite column type */
function schemaTypeToSqlite(schemaType: SchemaType): string {
  switch (schemaType) {
    case 'category':
    case 'text':
    case 'enum':
      return 'TEXT';
    case 'number':
      return 'REAL';
    case 'bigUint64':
    case 'boolean':
      return 'INTEGER';
    case 'binary':
      return 'BLOB';
  }
}

function entryTypeToStatus(entryType: number): string {
  switch (entryType) {
    case ENTRY_TYPE_SPAN_OK:
      return 'ok';
    case ENTRY_TYPE_SPAN_ERR:
      return 'err';
    case ENTRY_TYPE_SPAN_EXCEPTION:
      return 'exception';
    default:
      return 'running';
  }
}

export class TraceSQLiteSink {
  readonly runId: string;
  private knownColumns = new Set<string>();
  private startedAt: number;

  constructor(
    private db: SyncSQLiteDatabase,
    config?: Omit<TraceSQLiteConfig, 'dbPath'>,
  ) {
    this.runId = config?.runId ?? `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    this.startedAt = Date.now();
    this.init();
  }

  private init(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS runs (
        run_id TEXT PRIMARY KEY,
        started_at INTEGER NOT NULL,
        completed_at INTEGER,
        test_count INTEGER,
        pass_count INTEGER,
        fail_count INTEGER
      );

      CREATE TABLE IF NOT EXISTS spans (
        run_id TEXT NOT NULL REFERENCES runs(run_id),
        span_name TEXT NOT NULL,
        parent_span_name TEXT,
        status TEXT NOT NULL,
        error_code TEXT,
        duration_ns INTEGER,
        started_at INTEGER NOT NULL,
        trace_id TEXT NOT NULL,
        span_id INTEGER NOT NULL,
        depth INTEGER NOT NULL,
        PRIMARY KEY (run_id, trace_id, span_id)
      );

      CREATE TABLE IF NOT EXISTS log_entries (
        run_id TEXT NOT NULL,
        trace_id TEXT NOT NULL,
        span_id INTEGER NOT NULL,
        row_index INTEGER NOT NULL,
        entry_type INTEGER NOT NULL,
        timestamp_ns INTEGER,
        message TEXT,
        FOREIGN KEY (run_id, trace_id, span_id) REFERENCES spans(run_id, trace_id, span_id)
      );

      CREATE INDEX IF NOT EXISTS idx_spans_name ON spans(run_id, span_name);
      CREATE INDEX IF NOT EXISTS idx_spans_status ON spans(run_id, status);
      CREATE INDEX IF NOT EXISTS idx_log_entries_span ON log_entries(run_id, trace_id, span_id);
    `);

    // Insert the run record
    this.db.prepare('INSERT OR REPLACE INTO runs (run_id, started_at) VALUES (?, ?)').run(this.runId, this.startedAt);

    // Cache existing columns from log_entries
    const cols = this.db.prepare('PRAGMA table_info(log_entries)').all() as { name: string }[];
    for (const col of cols) {
      this.knownColumns.add(col.name);
    }
  }

  /** Ensure user-defined schema columns exist in the log_entries table */
  private ensureColumns(schema: LogSchema): void {
    const fields = schema.fields as Record<string, { __schema_type?: SchemaType }>;

    for (const [name, field] of Object.entries(fields)) {
      if (this.knownColumns.has(name)) continue;

      const schemaType = field.__schema_type;
      if (!schemaType) continue;

      const sqliteType = schemaTypeToSqlite(schemaType);

      // Check for conflict (same name but that shouldn't happen with schema validation)
      this.db.exec(`ALTER TABLE log_entries ADD COLUMN ${name} ${sqliteType}`);
      this.knownColumns.add(name);
    }
  }

  /** Write a root SpanBuffer tree to the database */
  flush<T extends LogSchema>(rootBuffer: SpanBuffer<T>): void {
    this.ensureColumns(rootBuffer._logSchema);
    this.flushBuffer(rootBuffer, undefined, 0);
  }

  /** Write all rootBuffers from a TestTracer */
  flushAll(tracer: TestTracer<OpContextBinding>): void {
    let testCount = 0;
    let passCount = 0;
    let failCount = 0;

    for (const rootBuffer of tracer.rootBuffers) {
      this.flush(rootBuffer as SpanBuffer<LogSchema>);
      testCount++;

      // Check root buffer completion status
      if (rootBuffer._writeIndex >= 2) {
        const status = entryTypeToStatus(rootBuffer.entry_type[1]);
        if (status === 'ok') passCount++;
        else failCount++;
      }
    }

    // Update run summary
    this.db
      .prepare('UPDATE runs SET completed_at = ?, test_count = ?, pass_count = ?, fail_count = ? WHERE run_id = ?')
      .run(Date.now(), testCount, passCount, failCount, this.runId);
  }

  private flushBuffer(buffer: AnySpanBuffer, parentSpanName: string | undefined, depth: number): void {
    const spanName = buffer.message_values[0];
    const traceId = buffer.trace_id;
    const spanId = buffer.span_id;
    const writeIndex = buffer._writeIndex;

    // Determine span status from entry_type[1] (completion row)
    const status = writeIndex >= 2 ? entryTypeToStatus(buffer.entry_type[1]) : 'running';

    // Error code from error_code_values[1] if span errored
    const errorCode = status === 'err' && buffer.error_code_nulls?.[1] === 0 ? buffer.error_code_values[1] : null;

    // Duration from timestamps
    let durationNs: bigint | null = null;
    if (writeIndex >= 2) {
      const startNanos = buffer.timestamp[0];
      const endNanos = buffer.timestamp[writeIndex - 1];
      durationNs = endNanos - startNanos;
    }

    const startedAtNs = writeIndex > 0 ? buffer.timestamp[0] : 0n;

    // Insert span record
    this.db
      .prepare(
        `INSERT OR REPLACE INTO spans
         (run_id, span_name, parent_span_name, status, error_code, duration_ns, started_at, trace_id, span_id, depth)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        this.runId,
        spanName,
        parentSpanName ?? null,
        status,
        errorCode,
        durationNs !== null ? Number(durationNs) : null,
        Number(startedAtNs),
        traceId,
        spanId,
        depth,
      );

    // Insert log entries for each row in the buffer
    const schema = buffer._logSchema;
    const userFields = schema._columnNames;

    // Build column list for insertion (base + user fields that exist in knownColumns)
    const activeUserFields = userFields.filter((f: string) => this.knownColumns.has(f));
    const userColsSql = activeUserFields.length > 0 ? `, ${activeUserFields.join(', ')}` : '';
    const userPlaceholders = activeUserFields.length > 0 ? `, ${activeUserFields.map(() => '?').join(', ')}` : '';

    const insertStmt = this.db.prepare(
      `INSERT INTO log_entries (run_id, trace_id, span_id, row_index, entry_type, timestamp_ns, message${userColsSql})
       VALUES (?, ?, ?, ?, ?, ?, ?${userPlaceholders})`,
    );

    for (let row = 0; row < writeIndex; row++) {
      const entryType = buffer.entry_type[row];
      const timestampNs = Number(buffer.timestamp[row]);
      const message = buffer.message_nulls && buffer.message_nulls[row] === 0 ? buffer.message_values[row] : null;

      // Collect user field values
      const userValues: unknown[] = [];
      for (const fieldName of activeUserFields) {
        const nulls = (buffer as any)[`${fieldName}_nulls`] as Uint8Array | undefined;
        const values = (buffer as any)[`${fieldName}_values`] as unknown[] | undefined;

        if (nulls && values && nulls[row] === 0) {
          const val = values[row];
          // Convert bigint to number for SQLite compatibility
          userValues.push(typeof val === 'bigint' ? Number(val) : val);
        } else {
          userValues.push(null);
        }
      }

      insertStmt.run(this.runId, traceId, spanId, row, entryType, timestampNs, message, ...userValues);
    }

    // Recurse into overflow buffers (same span, extended rows)
    if (buffer._overflow) {
      this.flushOverflow(buffer._overflow, traceId, spanId, writeIndex);
    }

    // Recurse into children
    for (const child of buffer._children) {
      this.flushBuffer(child, spanName, depth + 1);
    }
  }

  private flushOverflow(buffer: AnySpanBuffer, traceId: string, spanId: number, rowOffset: number): void {
    const writeIndex = buffer._writeIndex;
    const schema = buffer._logSchema;
    const userFields = schema._columnNames;
    const activeUserFields = userFields.filter((f: string) => this.knownColumns.has(f));
    const userColsSql = activeUserFields.length > 0 ? `, ${activeUserFields.join(', ')}` : '';
    const userPlaceholders = activeUserFields.length > 0 ? `, ${activeUserFields.map(() => '?').join(', ')}` : '';

    const insertStmt = this.db.prepare(
      `INSERT INTO log_entries (run_id, trace_id, span_id, row_index, entry_type, timestamp_ns, message${userColsSql})
       VALUES (?, ?, ?, ?, ?, ?, ?${userPlaceholders})`,
    );

    for (let row = 0; row < writeIndex; row++) {
      const entryType = buffer.entry_type[row];
      const timestampNs = Number(buffer.timestamp[row]);
      const message = buffer.message_nulls && buffer.message_nulls[row] === 0 ? buffer.message_values[row] : null;

      const userValues: unknown[] = [];
      for (const fieldName of activeUserFields) {
        const nulls = (buffer as any)[`${fieldName}_nulls`] as Uint8Array | undefined;
        const values = (buffer as any)[`${fieldName}_values`] as unknown[] | undefined;
        if (nulls && values && nulls[row] === 0) {
          const val = values[row];
          userValues.push(typeof val === 'bigint' ? Number(val) : val);
        } else {
          userValues.push(null);
        }
      }

      insertStmt.run(this.runId, traceId, spanId, rowOffset + row, entryType, timestampNs, message, ...userValues);
    }

    // Continue into chained overflow
    if (buffer._overflow) {
      this.flushOverflow(buffer._overflow, traceId, spanId, rowOffset + writeIndex);
    }
  }

  close(): void {
    this.db.close();
  }
}
