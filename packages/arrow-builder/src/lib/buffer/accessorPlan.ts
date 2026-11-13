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

import type { ColumnSchema } from '../schema/ColumnSchema.js';
import { enumIndexBits } from '../schema/field.js';
import { getEnumValues, storageLaneOf } from '../schema/typeGuards.js';
import type { StorageLane } from '../schema/types.js';

//#region smoo/lmao!n/buffer-arch-eager-vs-lazy
/**
 * Storage info for a schema field - determines how values are stored
 */
export interface ColumnStorageInfo {
  constructorName: string;
  bytesPerElement: number;
  isBitPacked: boolean;
  schemaType: StorageLane | undefined;
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
  const field = schema.fields[fieldName];
  const schemaType = storageLaneOf(field);
  const isEager = field?.hints.eager === true;

  switch (schemaType) {
    case 'enum': {
      const enumValues = getEnumValues(field);
      const bits = enumIndexBits(enumValues?.length ?? 0);
      const constructorName = bits === 8 ? 'Uint8Array' : bits === 16 ? 'Uint16Array' : 'Uint32Array';
      return { constructorName, bytesPerElement: bits / 8, isBitPacked: false, schemaType, enumValues, isEager };
    }
    case 'category':
    case 'text':
    // Binary columns use Array storage (same as category/text) to hold frozen object references
    case 'binary':
      return { constructorName: 'Array', bytesPerElement: 0, isBitPacked: false, schemaType, isEager };
    case 'number':
      return { constructorName: 'Float64Array', bytesPerElement: 8, isBitPacked: false, schemaType, isEager };
    case 'boolean':
      return { constructorName: 'Uint8Array', bytesPerElement: 1, isBitPacked: true, schemaType, isEager };
    case 'bigUint64':
      return { constructorName: 'BigUint64Array', bytesPerElement: 8, isBitPacked: false, schemaType, isEager };
    case undefined:
      // A field with no storage lane: its buffer owner refuses it by name before a row is written.
      return { constructorName: 'Uint32Array', bytesPerElement: 4, isBitPacked: false, schemaType, isEager };
  }
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
