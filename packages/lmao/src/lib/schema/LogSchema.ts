/**
 * LogSchema class for logging-specific schema functionality
 *
 * Extends ColumnSchema from arrow-builder with what a log schema is besides its columns: the fields of one
 * library, whose name a field's identity is written under.
 */

import { ColumnSchema } from '@smoothbricks/arrow-builder';
import type { SchemaFields } from './types.js';

/** The library name of a log schema that names none: it traces, and publishes no field list. */
export const UNNAMED_LIBRARY = 'log';

/**
 * LogSchema class - extends ColumnSchema with logging-specific features
 *
 * @template T - The schema fields type (SchemaFields)
 */
export class LogSchema<T extends SchemaFields = SchemaFields> extends ColumnSchema<T> {
  /** The library whose fields these are. */
  readonly library: string;

  constructor(fields: T, library: string = UNNAMED_LIBRARY) {
    super(fields);
    this.library = library;
  }

  /**
   * Extend this schema with additional fields
   *
   * Creates a new LogSchema instance with merged fields.
   * Validates that extension fields don't conflict with existing fields.
   *
   * @param extension - Additional schema fields to add
   * @returns New LogSchema instance with merged fields
   * @throws Error if field names conflict
   *
   * @example
   * ```typescript
   * const base = new LogSchema({ requestId: S.category() });
   * const extended = base.extend({ duration: S.number() });
   * // extended.fields = { requestId: ..., duration: ... }
   * ```
   */
  extend<U extends SchemaFields>(extension: U): LogSchema<T & U> {
    // Check for field name conflicts
    const extensionKeys = Object.keys(extension);
    for (const key of extensionKeys) {
      if (this._columnNames.includes(key)) {
        throw new Error(
          `Schema conflict: attribute '${key}' already exists in base schema. ` +
            `Base schema has: ${this._columnNames.join(', ')}`,
        );
      }
    }

    // Perform shallow merge
    // Create merged via explicit property assignment to preserve property order for V8 hidden class stability
    const merged = Object.create(null);
    for (const key of this._columnNames) {
      // Use prototypeless object for stable prototype, prevents hidden class deopt
      merged[key] = this.fields[key];
    }
    for (const key of extensionKeys) {
      merged[key] = extension[key];
    }

    // Return new LogSchema instance
    return new LogSchema(merged, this.library);
  }

  /**
   * Check if this is a LogSchema instance
   *
   * Used for type guards and instanceof checks
   */
  static isLogSchema(value: unknown): value is LogSchema {
    return value instanceof LogSchema;
  }
}

/**
 * Type guard to check if a value is a LogSchema instance
 *
 * @param value - Value to check
 * @returns True if value is a LogSchema instance
 */
export function isLogSchema(value: unknown): value is LogSchema {
  return LogSchema.isLogSchema(value);
}
