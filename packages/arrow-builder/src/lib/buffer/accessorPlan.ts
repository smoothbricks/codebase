/**
 * Schema -> accessor plan: the pure-data description of a ColumnBuffer/ColumnWriter
 * class shape, shared by both materializers.
 *
 * The plan captures everything the generators need to know about a schema —
 * field list, storage kind (TypedArray constructor, bytes per element,
 * bit-packing), enum values, and access mode (eager / preallocated / lazy) —
 * so the two materializers stay in lockstep:
 * - Compiled (columnBufferGenerator/columnWriterGenerator): renders class
 *   source and evaluates it with `new Function` where string codegen is allowed.
 * - Closure-composed (closureMaterializers): builds the same class from
 *   per-field closures where string codegen is forbidden (e.g. workerd).
 */

import type { ColumnSchema, SchemaType, SchemaWithMetadata } from '../schema-types.js';

//#region smoo/lmao!n/buffer-arch-eager-vs-lazy
/**
 * Storage info for a schema field - determines how values are stored
 */
export interface ColumnStorageInfo {
  constructorName: string;
  bytesPerElement: number;
  isBitPacked: boolean;
  schemaType: SchemaType | undefined;
  enumValues?: readonly string[];
  /** When true, column is allocated eagerly in constructor (no null bitmap). */
  isEager: boolean;
}

/**
 * How a column's storage is allocated and guarded:
 * - eager: allocated in the constructor, no null bitmap
 * - preallocated: allocated in the constructor WITH a null bitmap (runtime-selected)
 * - lazy: allocated on first access via the `_nulls` getter
 */
export type ColumnAccessMode = 'eager' | 'preallocated' | 'lazy';

/** One column of the accessor plan. */
export interface ColumnPlanEntry {
  readonly name: string;
  readonly storage: ColumnStorageInfo;
  readonly mode: ColumnAccessMode;
}

/** Pure-data accessor plan for a schema (+ optional preallocated column set). */
export interface ColumnAccessorPlan {
  readonly columns: readonly ColumnPlanEntry[];
}

/**
 * Get TypedArray constructor name and byte size for a schema field
 */
export function getTypedArrayInfo(schema: ColumnSchema, fieldName: string): ColumnStorageInfo {
  const fieldSchema = schema.fields[fieldName];
  const schemaWithMetadata = fieldSchema as SchemaWithMetadata;
  const schemaType = schemaWithMetadata?.__schema_type;
  const isEager = schemaWithMetadata?.__eager === true;

  if (schemaType === 'enum') {
    const enumValues = schemaWithMetadata.__enum_values;
    const enumCount = enumValues?.length ?? 0;

    // Uint8Array can hold 0-255 indices (256 values total)
    if (enumCount === 0 || enumCount <= 256) {
      return { constructorName: 'Uint8Array', bytesPerElement: 1, isBitPacked: false, schemaType, enumValues, isEager };
    }
    if (enumCount <= 65536) {
      return {
        constructorName: 'Uint16Array',
        bytesPerElement: 2,
        isBitPacked: false,
        schemaType,
        enumValues,
        isEager,
      };
    }
    // >65536 values: Uint32Array (4 bytes)
    return { constructorName: 'Uint32Array', bytesPerElement: 4, isBitPacked: false, schemaType, enumValues, isEager };
  }

  if (schemaType === 'category') {
    return { constructorName: 'Array', bytesPerElement: 0, isBitPacked: false, schemaType, isEager };
  }

  if (schemaType === 'text') {
    return { constructorName: 'Array', bytesPerElement: 0, isBitPacked: false, schemaType, isEager };
  }

  if (schemaType === 'number') {
    return { constructorName: 'Float64Array', bytesPerElement: 8, isBitPacked: false, schemaType, isEager };
  }

  if (schemaType === 'boolean') {
    return { constructorName: 'Uint8Array', bytesPerElement: 1, isBitPacked: true, schemaType, isEager };
  }

  if (schemaType === 'bigUint64') {
    return { constructorName: 'BigUint64Array', bytesPerElement: 8, isBitPacked: false, schemaType, isEager };
  }

  if (schemaType === 'binary') {
    // Binary columns use Array storage (same as category/text) to hold frozen object references
    return { constructorName: 'Array', bytesPerElement: 0, isBitPacked: false, schemaType, isEager };
  }

  // Default to Uint32Array for unknown types
  return { constructorName: 'Uint32Array', bytesPerElement: 4, isBitPacked: false, schemaType, isEager };
}
//#endregion smoo/lmao!n/buffer-arch-eager-vs-lazy

/**
 * Build the accessor plan for a schema.
 *
 * `preallocatedColumns` upgrades lazy columns to constructor allocation while
 * retaining nullable semantics (extension.preallocatedColumns); eager columns
 * are unaffected by it.
 */
export function buildColumnAccessorPlan(
  schema: ColumnSchema,
  preallocatedColumns?: readonly string[],
): ColumnAccessorPlan {
  const preallocated = new Set(preallocatedColumns ?? []);
  const columns: ColumnPlanEntry[] = [];

  for (const fieldName of schema._columnNames) {
    const storage = getTypedArrayInfo(schema, fieldName);
    const mode: ColumnAccessMode = storage.isEager ? 'eager' : preallocated.has(fieldName) ? 'preallocated' : 'lazy';
    columns.push({ name: fieldName, storage, mode });
  }

  return { columns };
}
