/**
 * TraceSQLiteAsync - Async SQLite persistence for span buffer trees.
 *
 * Works with async SQLite drivers (for example Cloudflare D1 adapters) while
 * preserving the same `spans` table shape as the sync TraceSQLite writer.
 *
 * @module sqlite-async
 */

import type { SchemaType } from '@smoothbricks/arrow-builder';
import type { LogSchema } from '../schema/LogSchema.js';
import type { AnySpanBuffer } from '../types.js';
import type { AsyncSQLiteDatabase, AsyncSQLiteStatement } from './sqlite-db.js';

/** Check if bit at index is set in Arrow null bitmap (1 = valid/present, 0 = null) */
function isBitSet(bitmap: Uint8Array, idx: number): boolean {
  return (bitmap[idx >>> 3] & (1 << (idx & 7))) !== 0;
}

type DynamicUserColumnBuffer = Record<string, unknown>;

function readUserValue(buffer: DynamicUserColumnBuffer, fieldName: string, row: number): unknown {
  const values = buffer[`${fieldName}_values`];
  if (!values) return undefined;

  if (ArrayBuffer.isView(values)) {
    const nulls = buffer[`${fieldName}_nulls`] as Uint8Array | undefined;
    if (!('length' in values)) {
      return undefined;
    }
    const typedValues = values as unknown as { [index: number]: unknown; length: number };
    return nulls && isBitSet(nulls, row) ? typedValues[row] : undefined;
  }

  return (values as unknown[])[row];
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

export class TraceSQLiteAsync {
  private knownColumns = new Set<string>();
  private insertStmtCache = new Map<string, AsyncSQLiteStatement>();
  private initialized = false;

  constructor(private db: AsyncSQLiteDatabase) {}

  private async init(): Promise<void> {
    if (this.initialized) {
      return;
    }

    await this.db.exec(`
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

    const cols = (await this.db.prepare('PRAGMA table_info(spans)').all()) as { name: string }[];
    for (const col of cols) {
      this.knownColumns.add(col.name);
    }

    this.initialized = true;
  }

  private async ensureColumns(schema: LogSchema): Promise<void> {
    const fields = schema.fields as Record<string, { __schema_type?: SchemaType }>;

    for (const [name, field] of Object.entries(fields)) {
      if (this.knownColumns.has(name)) continue;

      const schemaType = field.__schema_type;
      if (!schemaType) continue;

      const sqliteType = schemaTypeToSqlite(schemaType);
      await this.db.exec(`ALTER TABLE spans ADD COLUMN ${name} ${sqliteType}`);
      this.knownColumns.add(name);
    }
  }

  private getInsertStatement(activeUserFields: readonly string[]): AsyncSQLiteStatement {
    const key = activeUserFields.join(',');
    const cached = this.insertStmtCache.get(key);
    if (cached) {
      return cached;
    }

    const userColsSql = activeUserFields.length > 0 ? `, ${activeUserFields.join(', ')}` : '';
    const userPlaceholders = activeUserFields.length > 0 ? `, ${activeUserFields.map(() => '?').join(', ')}` : '';
    const stmt = this.db.prepare(
      `INSERT INTO spans (trace_id, span_id, parent_span_id, row_index, entry_type, timestamp_ns, message${userColsSql})
       VALUES (?, ?, ?, ?, ?, ?, ?${userPlaceholders})`,
    );
    this.insertStmtCache.set(key, stmt);
    return stmt;
  }

  async flush(rootBuffer: AnySpanBuffer): Promise<void> {
    await this.init();
    await this.flushBuffer(rootBuffer);
  }

  private async flushBuffer(buffer: AnySpanBuffer): Promise<void> {
    await this.ensureColumns(buffer._logSchema);

    const traceId = buffer.trace_id;
    const spanId = buffer.span_id;
    const parentSpanId = buffer.parent_span_id;
    const writeIndex = buffer._writeIndex;

    const schema = buffer._logSchema;
    const userFields = schema._columnNames;
    const activeUserFields = userFields.filter((f: string) => this.knownColumns.has(f));
    const insertStmt = this.getInsertStatement(activeUserFields);

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

      await insertStmt.run(traceId, spanId, parentSpanId, row, entryType, timestampNs, message, ...userValues);
    }

    if (buffer._overflow) {
      await this.flushOverflow(buffer._overflow, traceId, spanId, parentSpanId, writeIndex);
    }

    for (const child of buffer._children) {
      await this.flushBuffer(child);
    }
  }

  private async flushOverflow(
    buffer: AnySpanBuffer,
    traceId: string,
    spanId: number,
    parentSpanId: number,
    rowOffset: number,
  ): Promise<void> {
    await this.ensureColumns(buffer._logSchema);

    const writeIndex = buffer._writeIndex;
    const schema = buffer._logSchema;
    const userFields = schema._columnNames;
    const activeUserFields = userFields.filter((f: string) => this.knownColumns.has(f));
    const insertStmt = this.getInsertStatement(activeUserFields);

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

      await insertStmt.run(
        traceId,
        spanId,
        parentSpanId,
        rowOffset + row,
        entryType,
        timestampNs,
        message,
        ...userValues,
      );
    }

    if (buffer._overflow) {
      await this.flushOverflow(buffer._overflow, traceId, spanId, parentSpanId, rowOffset + writeIndex);
    }
  }

  async close(): Promise<void> {
    await this.db.close();
  }
}
