/**
 * LogSchema class for logging-specific schema functionality
 *
 * Extends ColumnSchema from arrow-builder with logging-specific features:
 * - Schema extension with reserved name validation
 * - Integration with lmao's logging system
 *
 * The base ColumnSchema provides generic cached field access.
 * LogSchema adds logging-specific extension capabilities.
 */

import { ColumnSchema } from '@smoothbricks/arrow-builder';
import type { SchemaFields } from './types.js';

/**
 * Reserved method names that cannot be used as attribute names
 *
 * These names are reserved to prevent conflicts or confusion:
 * - Technically conflicting: Names that would shadow actual methods/properties
 *   on TagWriter, SpanLogger, FluentLogEntry, or result objects
 * - Confusing: Names that would create confusing code patterns even though
 *   they don't technically conflict (e.g., ctx.tag.tag() or ctx.scope.scope)
 */
const RESERVED_NAMES = new Set([
  // Technically conflicting - methods on TagWriter/FluentLogEntry/result objects
  'with', // Used on TagWriter, FluentLogEntry, FluentSuccessResult, FluentErrorResult
  'message', // Used on FluentSuccessResult, FluentErrorResult

  // Technically conflicting - log level methods on SpanLogger
  'info', // ctx.log.info()
  'debug', // ctx.log.debug()
  'warn', // ctx.log.warn()
  'error', // ctx.log.error()
  'trace', // ctx.log.trace()

  // Internal SpanLogger methods (prefixed with _ but included for completeness)
  '_nextRow', // Internal row advancement
  '_buffer', // Internal buffer reference
  '_writeIndex', // Internal write position
  '_getNextBuffer', // Internal buffer chaining
  '_setScope', // Internal scope setter
  '_prefillScopedAttributes', // Internal scope prefill

  // Internal FF logging methods (called by evaluator, not on public type)
  'ffAccess', // Feature flag access logging
  'ffUsage', // Feature flag usage logging

  // Confusing - properties on SpanContext (not technically conflicting)
  'tag', // ctx.tag is TagWriter; ctx.tag.tag() would be confusing
  'scope', // ctx.scope is readonly; ctx.scope.scope would be confusing

  // Confusing - methods on SpanContext (not technically conflicting)
  'ok', // ctx.ok() is method; ctx.tag.ok() or ctx.ok(result).ok() would be confusing
  'err', // ctx.err() is method; ctx.tag.err() or ctx.err(code, details).err() would be confusing
  'span', // ctx.span() is method; ctx.tag.span() would be confusing
]);

/**
 * LogSchema class - extends ColumnSchema with logging-specific features
 *
 * Provides all the generic schema functionality from ColumnSchema plus
 * logging-specific extension with reserved name validation.
 *
 * @template T - The schema fields type (SchemaFields)
 */
export class LogSchema<T extends SchemaFields = SchemaFields> extends ColumnSchema<T> {
  /**
   * Assert that user-provided field names don't conflict with reserved names
   * and don't start with underscore (reserved for system properties)
   *
   * @throws Error if any field name is reserved or starts with _
   */
  static assertUserFieldNames(fieldNames: readonly string[]): void {
    for (const name of fieldNames) {
      if (name.startsWith('_')) {
        throw new Error(`Field name '${name}' cannot start with '_' - this prefix is reserved for system properties`);
      }
      if (RESERVED_NAMES.has(name)) {
        throw new Error(
          `Attribute name '${name}' is reserved and cannot be used. ` +
            `Reserved names: ${Array.from(RESERVED_NAMES).join(', ')}`,
        );
      }
    }
  }

  /**
   * Extend this schema with additional fields
   *
   * Creates a new LogSchema instance with merged fields.
   * Validates that extension fields don't conflict with existing fields
   * and don't use reserved names (logging-specific validation).
   *
   * @param extension - Additional schema fields to add
   * @returns New LogSchema instance with merged fields
   * @throws Error if field names conflict or reserved names are used
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
      if (this.fieldNames.includes(key)) {
        throw new Error(
          `Schema conflict: attribute '${key}' already exists in base schema. ` +
            `Base schema has: ${this.fieldNames.join(', ')}`,
        );
      }
    }

    // Assert extension doesn't use reserved names (logging-specific)
    LogSchema.assertUserFieldNames(extensionKeys);

    // Perform shallow merge
    // Create merged via explicit property assignment to preserve property order for V8 hidden class stability
    const merged = Object.create(null);
    for (const key of this.fieldNames) {
      // Use prototypeless object for stable prototype, prevents hidden class deopt
      merged[key] = this.fields[key];
    }
    for (const key of extensionKeys) {
      merged[key] = extension[key];
    }

    // Return new LogSchema instance
    return new LogSchema(merged);
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
