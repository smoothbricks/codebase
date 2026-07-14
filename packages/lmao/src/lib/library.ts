/**
 * Library Prefix/Remapping Utilities
 *
 * WHY: Libraries need to write clean, domain-focused code (ctx.tag.status(200))
 * while avoiding naming conflicts when composed with other libraries.
 *
 * HOW: At composition time (cold path), we create immutable Arrow remap descriptors.
 * Child buffers retain canonical schema/layout.
 *
 * Per specs/lmao/01e_library_integration_pattern.md:
 * - Libraries define clean schemas without prefixes (status, method, url)
 * - Prefixing happens at composition time (http_status, http_method, http_url)
 * - Zero hot path overhead - remapping is composed at module creation
 *
 * WHAT: This module provides schema/prefix mapping utilities and immutable remap
 * descriptors for cold Arrow conversion.
 */

import { isRecord } from '@smoothbricks/validation';
import type { RemapDescriptor, RemappedColumn } from './logBinding.js';
import { LogSchema, type SchemaFields } from './schema/types.js';

function isSchemaField(value: unknown): value is SchemaFields[string] {
  return isRecord(value);
}

/**
 * Prefix a tag attribute schema
 * Renames all fields with a prefix to avoid conflicts
 *
 * 01j "Library Compilation (Prefixed)": a library defines clean field names
 * (status, method); the consumer applies a prefix (http) at wire time. The utilities
 * below create the prefixed schema, clean-to-prefixed mapping, and immutable Arrow
 * remap descriptor.
 *
 * Example:
 * - Input: { status: S.number(), method: S.enum(['GET', 'POST']) }
 * - Prefix: 'http'
 * - Output: { http_status: S.number(), http_method: S.enum(['GET', 'POST']) }
 */
export function prefixSchema(schema: LogSchema, prefix: string): LogSchema & Record<string, unknown> {
  const prefixedFields: Record<string, unknown> = {};
  const constructorFields: SchemaFields = {};

  for (const [fieldName, fieldSchema] of schema._columns) {
    const prefixedName = `${prefix}_${fieldName}`;
    prefixedFields[prefixedName] = fieldSchema;
    if (isSchemaField(fieldSchema)) {
      constructorFields[prefixedName] = fieldSchema;
    }
  }

  // Create LogSchema and spread fields directly onto result for direct access
  const logSchema = new LogSchema(constructorFields);
  return Object.assign(logSchema, prefixedFields);
}

/**
 * Prefix mapping: maps clean names to prefixed column names
 *
 * WHY: Library authors write ctx.tag.status() but buffer column is http_status
 * HOW: At module creation time, we build this mapping once (cold path)
 *
 * @example
 * // Input: schema = { status: S.number(), method: S.enum([...]) }, prefix = 'http'
 * // Output: { status: 'http_status', method: 'http_method' }
 */
export interface PrefixMapping {
  [cleanName: string]: string; // cleanName -> prefixedColumnName
}

/**
 * Create a mapping from clean field names to prefixed column names
 *
 * WHY: This mapping exposes the relationship between clean library field names and
 * their prefixed column names.
 *
 * @param schema - Clean schema (LogSchema instance)
 * @param prefix - Prefix to apply (e.g., 'http')
 * @returns Mapping from clean name to prefixed name
 *
 * @example
 * const schema = defineLogSchema({ status: S.number() });
 * const mapping = createPrefixMapping(schema, 'http');
 * // Returns: { status: 'http_status' }
 */
export function createPrefixMapping<T extends LogSchema>(schema: T, prefix: string): PrefixMapping {
  const mapping: PrefixMapping = {};

  // Use LogSchema.fieldNames directly (no conditional needed)
  for (const fieldName of schema._columnNames) {
    mapping[fieldName] = `${prefix}_${fieldName}`;
  }

  return mapping;
}

/**
 * Create immutable output-to-source metadata for cold Arrow traversal.
 *
 * The mapping is already fully composed by OpGroup prefix/map operations. Keeping
 * both the frozen lookup and output schema entries avoids wrapping child buffers
 * or reconstructing schema metadata during each traversal.
 */
export function createRemapDescriptor(
  sourceSchema: LogSchema,
  outputToSourceMapping: Record<string, string>,
): RemapDescriptor {
  const sourceNames: Record<string, string> = {};
  const columns: RemappedColumn[] = [];

  for (const [outputName, sourceName] of Object.entries(outputToSourceMapping)) {
    sourceNames[outputName] = sourceName;
    const fieldSchema = sourceSchema.fields[sourceName];
    if (fieldSchema !== undefined) {
      const column: RemappedColumn = Object.freeze([
        outputName,
        sourceName,
        fieldSchema,
        `${sourceName}_values`,
        `${sourceName}_nulls`,
      ]);
      columns.push(column);
    }
  }

  return Object.freeze({
    sourceNames: Object.freeze(sourceNames),
    columns: Object.freeze(columns),
  });
}
