import type { Column, Table } from '@uwdata/flechette';
import type { AnySpanBuffer } from '../types.js';

function isObject(value: unknown): value is object {
  return typeof value === 'object' && value !== null;
}

export function requireColumn(table: Table, columnName: string): Column<unknown> {
  const column = table.getChild(columnName);
  if (!column) {
    throw new Error(`Column not found: ${columnName}`);
  }
  return column;
}

export function getColumnValue(table: Table, columnName: string, rowIndex: number): unknown {
  return requireColumn(table, columnName).get(rowIndex);
}

export function getRawTimestamp(table: Table, rowIndex: number): bigint {
  const column = requireColumn(table, 'timestamp');
  const batch = column.data[0];
  if (!batch || !(batch.values instanceof BigInt64Array)) {
    throw new Error('timestamp column does not expose raw BigInt64Array values');
  }
  return batch.values[rowIndex];
}

export function requireBinaryCell(column: Column<unknown>, rowIndex: number): Uint8Array {
  const value = column.at(rowIndex);
  if (!(value instanceof Uint8Array)) {
    throw new Error(`Expected binary cell at row ${rowIndex}`);
  }
  return value;
}

export function nextWriterRow(writer: { nextRow(): unknown }): object {
  const row = writer.nextRow();
  if (!isObject(row)) {
    throw new Error('Expected createColumnWriter().nextRow() to return a row writer object');
  }
  return row;
}

export function invokeWriterMethod(writer: object, methodName: string, value: unknown): object {
  const method = Reflect.get(writer, methodName);
  if (typeof method !== 'function') {
    throw new Error(`Expected row writer method '${methodName}'`);
  }
  const next = Reflect.apply(method, writer, [value]);
  if (!isObject(next)) {
    throw new Error(`Expected row writer method '${methodName}' to return a row writer object`);
  }
  return next;
}

export function callBufferWriter(buffer: object, methodName: string, rowIndex: number, value: unknown): void {
  const method = Reflect.get(buffer, methodName);
  if (typeof method !== 'function') {
    throw new Error(`Expected buffer writer '${methodName}'`);
  }
  Reflect.apply(method, buffer, [rowIndex, value]);
}

export function isAnySpanBuffer(value: unknown): value is AnySpanBuffer {
  return isObject(value) && 'timestamp' in value && 'entry_type' in value && '_writeIndex' in value;
}

export function requireAnySpanBuffer(value: unknown, message: string): AnySpanBuffer {
  if (!isAnySpanBuffer(value)) {
    throw new Error(message);
  }
  return value;
}
