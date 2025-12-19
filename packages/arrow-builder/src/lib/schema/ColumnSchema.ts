/**
 * ColumnSchema class for efficient schema field access
 *
 * Generic schema class that provides cached field access for any columnar data use case.
 * This is a generic utility that can be used by metrics collectors, CSV parsers, etc.
 *
 * Provides:
 * - Cached field names and entries (computed once at construction)
 * - Generator for efficient field iteration
 * - Field count getter
 * - Schema fields accessible via .fields property
 */

import type * as Sury from '@sury/sury';
import type { SchemaFields } from '../schema-types.js';

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

  /** Cached field names (computed once at construction) */
  private readonly _fieldNames: readonly string[];

  /** Cached field entries (computed once at construction) */
  private readonly _fieldEntries: ReadonlyArray<[string, Sury.Schema<unknown, unknown>]>;

  /**
   * Create a new ColumnSchema instance
   *
   * @param fields - The schema fields object
   */
  constructor(fields: T) {
    this.fields = fields;

    // Cache field names and entries once at construction
    this._fieldNames = Object.keys(fields);
    this._fieldEntries = Object.entries(fields);
  }

  /**
   * Get cached field names
   *
   * @returns Readonly array of field names
   */
  get fieldNames(): readonly string[] {
    return this._fieldNames;
  }

  /**
   * Get the number of fields in the schema
   *
   * @returns The number of fields
   */
  get fieldCount(): number {
    return this._fieldNames.length;
  }

  /**
   * Generator for iterating over field entries
   *
   * @yields [fieldName, fieldSchema] tuples
   *
   * @example
   * ```typescript
   * for (const [name, schema] of columnSchema.fieldEntries()) {
   *   console.log(name, schema);
   * }
   * ```
   */
  *fieldEntries(): Generator<[string, Sury.Schema<unknown, unknown>]> {
    for (const entry of this._fieldEntries) {
      yield entry;
    }
  }

  /**
   * Check if this is a ColumnSchema instance
   *
   * Used for type guards and instanceof checks
   */
  static isColumnSchema(value: unknown): value is ColumnSchema {
    return (
      value instanceof ColumnSchema ||
      (value != null && typeof value === 'object' && 'fieldNames' in value && 'fieldEntries' in value)
    );
  }
}

/**
 * Type guard to check if a value is a ColumnSchema instance
 *
 * @param value - Value to check
 * @returns True if value is a ColumnSchema instance
 */
export function isColumnSchema(value: unknown): value is ColumnSchema {
  return ColumnSchema.isColumnSchema(value);
}
