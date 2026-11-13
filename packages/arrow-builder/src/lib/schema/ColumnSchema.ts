/**
 * ColumnSchema class for efficient schema field access
 *
 * Generic schema class that provides cached field access for any columnar data use case.
 * This is a generic utility that can be used by metrics collectors, CSV parsers, etc.
 *
 * Provides:
 * - Cached field names and column entries (computed once at construction)
 * - Field count getter
 * - Schema fields accessible via .fields property
 */

import type { SchemaFields } from '../schema-types.js';
import type { Field } from './field.js';

/** Column entry tuple: [columnName, field] */
export type ColumnEntry = readonly [string, Field];

/**
 * ColumnSchema class - generic schema with efficient cached field access
 *
 * The schema fields are stored in `.fields` property for efficient access.
 * This class is generic and can be used for any columnar data use case.
 *
 * @template T - The schema fields type (SchemaFields)
 */
export class ColumnSchema<T extends SchemaFields = SchemaFields> {
  /** The schema fields object - readonly reference with exact type */
  readonly fields: T;

  /** Cached column names (computed once at construction) */
  readonly _columnNames: readonly string[];

  /** Cached column entries as [columnName, field] tuples */
  readonly _columns: readonly ColumnEntry[];

  constructor(fields: T) {
    this.fields = fields;
    this._columnNames = Object.keys(fields);
    this._columns = Object.entries<Field>(fields);
  }

  /** Number of columns in the schema */
  get _columnCount(): number {
    return this._columnNames.length;
  }

  /** Check if this is a ColumnSchema instance */
  static isColumnSchema(value: unknown): value is ColumnSchema {
    return value instanceof ColumnSchema || (value != null && typeof value === 'object' && '_columns' in value);
  }
}

/** Type guard to check if a value is a ColumnSchema instance */
export function isColumnSchema(value: unknown): value is ColumnSchema {
  return ColumnSchema.isColumnSchema(value);
}
