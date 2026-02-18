/**
 * TraceSQLiteSink - General-purpose sink that writes span buffer trees to SQLite.
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
 * @module sqlite-sink
 */

import type { SchemaType } from '@smoothbricks/arrow-builder';
import type { LogSchema } from '../schema/LogSchema.js';
import type { AnySpanBuffer } from '../types.js';
import type { SyncSQLiteDatabase } from './sqlite-db.js';

/** Check if bit at index is set in Arrow null bitmap (1 = valid/present, 0 = null) */
function isBitSet(bitmap: Uint8Array, idx: number): boolean {
  return (bitmap[idx >>> 3] & (1 << (idx & 7))) !== 0;
}

/**
 * Read a user column value at the given row.
 *
 * Typed arrays (Float64Array, BigUint64Array, Uint8Array) are pre-allocated with
 * zeros — the only way to distinguish "set to 0" from "never written" is the
 * bit-packed null bitmap.  JS arrays (string[], unknown[]) store `undefined` for
 * unwritten slots, so a plain != null check suffices.
 */
type DynamicUserColumnBuffer = Record<string, unknown>;

function readUserValue(buffer: DynamicUserColumnBuffer, fieldName: string, row: number): unknown {
  const values = buffer[`${fieldName}_values`];
  if (!values) return undefined;

  if (ArrayBuffer.isView(values)) {
    // Typed array — need null bitmap to distinguish zero from unset
    const nulls = buffer[`${fieldName}_nulls`] as Uint8Array | undefined;
    if (!('length' in values)) {
      return undefined;
    }
    const typedValues = values as unknown as { [index: number]: unknown; length: number };
    return nulls && isBitSet(nulls, row) ? typedValues[row] : undefined;
  }
  // JS array (string[], unknown[]) — undefined means unset
  return (values as unknown[])[row];
}

export interface TraceSQLiteConfig {
  /** Path to SQLite file. Default: '.trace-results.db' */
  dbPath?: string;
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

export class TraceSQLiteSink {
  private knownColumns = new Set<string>();

  constructor(private db: SyncSQLiteDatabase) {
    this.init();
  }

  private init(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS spans (
        trace_id TEXT NOT NULL,
        span_id INTEGER NOT NULL,
        parent_span_id INTEGER NOT NULL,
        row_index INTEGER NOT NULL,
        entry_type INTEGER NOT NULL,
        timestamp_ns INTEGER NOT NULL,
        message TEXT,
        PRIMARY KEY (trace_id, span_id, row_index)
      );

      CREATE INDEX IF NOT EXISTS idx_spans_trace ON spans(trace_id);
      CREATE INDEX IF NOT EXISTS idx_spans_parent ON spans(trace_id, parent_span_id);
    `);

    // Cache existing user columns from spans table
    const cols = this.db.prepare('PRAGMA table_info(spans)').all() as { name: string }[];
    for (const col of cols) {
      this.knownColumns.add(col.name);
    }
  }

  /** Ensure user-defined schema columns exist in the spans table */
  private ensureColumns(schema: LogSchema): void {
    const fields = schema.fields as Record<string, { __schema_type?: SchemaType }>;

    for (const [name, field] of Object.entries(fields)) {
      if (this.knownColumns.has(name)) continue;

      const schemaType = field.__schema_type;
      if (!schemaType) continue;

      const sqliteType = schemaTypeToSqlite(schemaType);
      this.db.exec(`ALTER TABLE spans ADD COLUMN ${name} ${sqliteType}`);
      this.knownColumns.add(name);
    }
  }

  /** Write a root SpanBuffer tree to the database */
  flush(rootBuffer: AnySpanBuffer): void {
    this.flushBuffer(rootBuffer);
  }

  private flushBuffer(buffer: AnySpanBuffer): void {
    // Per-buffer schema merging — handles cross-library ops with different schemas
    this.ensureColumns(buffer._logSchema);

    const traceId = buffer.trace_id;
    const spanId = buffer.span_id;
    const parentSpanId = buffer.parent_span_id;
    const writeIndex = buffer._writeIndex;

    const schema = buffer._logSchema;
    const userFields = schema._columnNames;
    const activeUserFields = userFields.filter((f: string) => this.knownColumns.has(f));
    const userColsSql = activeUserFields.length > 0 ? `, ${activeUserFields.join(', ')}` : '';
    const userPlaceholders = activeUserFields.length > 0 ? `, ${activeUserFields.map(() => '?').join(', ')}` : '';

    const insertStmt = this.db.prepare(
      `INSERT INTO spans (trace_id, span_id, parent_span_id, row_index, entry_type, timestamp_ns, message${userColsSql})
       VALUES (?, ?, ?, ?, ?, ?, ?${userPlaceholders})`,
    );

    for (let row = 0; row < writeIndex; row++) {
      const entryType = buffer.entry_type[row];
      const timestampNs = Number(buffer.timestamp[row]);
      const message = buffer.message_values[row] ?? null;

      const userValues: unknown[] = [];
      for (const fieldName of activeUserFields) {
        const val = readUserValue(buffer as unknown as DynamicUserColumnBuffer, fieldName, row);
        if (val !== undefined) {
          // Convert bigint to number for SQLite compatibility
          userValues.push(typeof val === 'bigint' ? Number(val) : val);
        } else {
          userValues.push(null);
        }
      }

      insertStmt.run(traceId, spanId, parentSpanId, row, entryType, timestampNs, message, ...userValues);
    }

    // Overflow: same span, extended rows
    if (buffer._overflow) {
      this.flushOverflow(buffer._overflow, traceId, spanId, parentSpanId, writeIndex);
    }

    // Children
    for (const child of buffer._children) {
      this.flushBuffer(child);
    }
  }

  private flushOverflow(
    buffer: AnySpanBuffer,
    traceId: string,
    spanId: number,
    parentSpanId: number,
    rowOffset: number,
  ): void {
    // Per-buffer schema merging for overflow too
    this.ensureColumns(buffer._logSchema);

    const writeIndex = buffer._writeIndex;
    const schema = buffer._logSchema;
    const userFields = schema._columnNames;
    const activeUserFields = userFields.filter((f: string) => this.knownColumns.has(f));
    const userColsSql = activeUserFields.length > 0 ? `, ${activeUserFields.join(', ')}` : '';
    const userPlaceholders = activeUserFields.length > 0 ? `, ${activeUserFields.map(() => '?').join(', ')}` : '';

    const insertStmt = this.db.prepare(
      `INSERT INTO spans (trace_id, span_id, parent_span_id, row_index, entry_type, timestamp_ns, message${userColsSql})
       VALUES (?, ?, ?, ?, ?, ?, ?${userPlaceholders})`,
    );

    for (let row = 0; row < writeIndex; row++) {
      const entryType = buffer.entry_type[row];
      const timestampNs = Number(buffer.timestamp[row]);
      const message = buffer.message_values[row] ?? null;

      const userValues: unknown[] = [];
      for (const fieldName of activeUserFields) {
        const val = readUserValue(buffer as unknown as DynamicUserColumnBuffer, fieldName, row);
        if (val !== undefined) {
          userValues.push(typeof val === 'bigint' ? Number(val) : val);
        } else {
          userValues.push(null);
        }
      }

      insertStmt.run(traceId, spanId, parentSpanId, rowOffset + row, entryType, timestampNs, message, ...userValues);
    }

    // Continue into chained overflow
    if (buffer._overflow) {
      this.flushOverflow(buffer._overflow, traceId, spanId, parentSpanId, rowOffset + writeIndex);
    }
  }

  close(): void {
    this.db.close();
  }
}
