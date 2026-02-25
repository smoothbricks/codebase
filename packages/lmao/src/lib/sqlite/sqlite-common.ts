import type { SchemaType } from '@smoothbricks/arrow-builder';
import type { LogSchema } from '../schema/LogSchema.js';
import type { AnySpanBuffer } from '../types.js';

export const SPANS_TABLE_INIT_SQL = `
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
`;

export const SPANS_TABLE_INFO_SQL = 'PRAGMA table_info(spans)';

type DynamicUserColumnBuffer = Record<string, unknown>;

export interface SQLiteColumnDefinition {
  name: string;
  sqliteType: string;
}

interface SQLiteTableInfoColumn {
  name: string;
  type: string;
}

export interface SpanSegment {
  buffer: AnySpanBuffer;
  traceId: string;
  spanId: number;
  parentSpanId: number;
  rowOffset: number;
}

/** Check if bit at index is set in Arrow null bitmap (1 = valid/present, 0 = null) */
function isBitSet(bitmap: Uint8Array, idx: number): boolean {
  return (bitmap[idx >>> 3] & (1 << (idx & 7))) !== 0;
}

/**
 * Read a user column value at the given row.
 *
 * Typed arrays are pre-allocated with zeros, so null bitmaps distinguish
 * "set to zero" from "never written". JS arrays use `undefined` for unwritten slots.
 */
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

function readScopeValue(buffer: AnySpanBuffer, fieldName: string): unknown {
  const scopeValues = buffer._scopeValues as Record<string, unknown> | undefined;
  return scopeValues?.[fieldName];
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

  throw new Error(`Unsupported schema type: ${String(schemaType)}`);
}

export function normalizeSqliteColumnType(type: string): string {
  return type.trim() === '' ? 'TEXT' : type;
}

export function quoteSqlIdentifier(name: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
    throw new Error(`Unsafe SQLite identifier: ${name}`);
  }
  return `"${name}"`;
}

export function buildAddColumnSql(column: SQLiteColumnDefinition): string {
  return `ALTER TABLE spans ADD COLUMN ${quoteSqlIdentifier(column.name)} ${normalizeSqliteColumnType(column.sqliteType)}`;
}

export function getMissingSqliteColumns(
  columns: readonly SQLiteColumnDefinition[],
  knownColumns: ReadonlySet<string>,
): SQLiteColumnDefinition[] {
  const missing: SQLiteColumnDefinition[] = [];
  for (const column of columns) {
    if (!knownColumns.has(column.name)) {
      missing.push(column);
    }
  }
  return missing;
}

export function extractSqliteColumnsFromTableInfo(columns: readonly SQLiteTableInfoColumn[]): SQLiteColumnDefinition[] {
  return columns.map((column) => ({
    name: column.name,
    sqliteType: normalizeSqliteColumnType(column.type),
  }));
}

function* walkOverflowSegments(
  buffer: AnySpanBuffer,
  traceId: string,
  spanId: number,
  parentSpanId: number,
  rowOffset: number,
): Generator<SpanSegment> {
  yield {
    buffer,
    traceId,
    spanId,
    parentSpanId,
    rowOffset,
  };

  if (buffer._overflow) {
    yield* walkOverflowSegments(buffer._overflow, traceId, spanId, parentSpanId, rowOffset + buffer._writeIndex);
  }
}

function* walkTree(buffer: AnySpanBuffer): Generator<SpanSegment> {
  yield* walkOverflowSegments(buffer, buffer.trace_id, buffer.span_id, buffer.parent_span_id, 0);

  for (const child of buffer._children) {
    yield* walkTree(child);
  }
}

export function* walkSpanSegments(rootBuffer: AnySpanBuffer): Generator<SpanSegment> {
  yield* walkTree(rootBuffer);
}

export function getMissingSchemaColumns(
  schema: LogSchema,
  knownColumns: ReadonlySet<string>,
): SQLiteColumnDefinition[] {
  const fields = schema.fields as Record<string, { __schema_type?: SchemaType }>;
  const schemaColumns: SQLiteColumnDefinition[] = [];

  for (const [name, field] of Object.entries(fields)) {
    const schemaType = field.__schema_type;
    if (!schemaType) continue;

    schemaColumns.push({ name, sqliteType: schemaTypeToSqlite(schemaType) });
  }

  return getMissingSqliteColumns(schemaColumns, knownColumns);
}

export function getActiveUserFields(schema: LogSchema, knownColumns: ReadonlySet<string>): string[] {
  return schema._columnNames.filter((fieldName) => knownColumns.has(fieldName));
}

export function getInsertStatementCacheKey(activeUserFields: readonly string[]): string {
  return activeUserFields.join(',');
}

export function buildInsertSql(activeUserFields: readonly string[]): string {
  const userColsSql = activeUserFields.length > 0 ? `, ${activeUserFields.join(', ')}` : '';
  const userPlaceholders = activeUserFields.length > 0 ? `, ${activeUserFields.map(() => '?').join(', ')}` : '';
  return `INSERT INTO spans (trace_id, span_id, parent_span_id, row_index, entry_type, timestamp_ns, message${userColsSql})
   VALUES (?, ?, ?, ?, ?, ?, ?${userPlaceholders})`;
}

export function buildInsertParams(segment: SpanSegment, row: number, activeUserFields: readonly string[]): unknown[] {
  const { buffer, traceId, spanId, parentSpanId, rowOffset } = segment;
  const entryType = buffer.entry_type[row];
  const timestampNs = Number(buffer.timestamp[row]);
  const message = buffer.message_values[row] ?? null;

  const userValues = activeUserFields.map((fieldName) => {
    const directValue = readUserValue(buffer as unknown as DynamicUserColumnBuffer, fieldName, row);
    const val = directValue ?? readScopeValue(buffer, fieldName);
    if (val === undefined || val === null) {
      return null;
    }
    return typeof val === 'bigint' ? Number(val) : val;
  });

  return [traceId, spanId, parentSpanId, rowOffset + row, entryType, timestampNs, message, ...userValues];
}
