import type { LogSchema } from './schema/LogSchema.js';
import { getEnumValues, getSchemaType } from './schema/typeGuards.js';

export type EnumIndexArray = Uint8Array | Uint16Array | Uint32Array;
export type EnumIndexArrayConstructor =
  | Uint8ArrayConstructor
  | Uint16ArrayConstructor
  | Uint32ArrayConstructor;

/** Immutable schema-order metadata for one enum column. */
export interface EnumLookupDescriptor {
  readonly fieldName: string;
  readonly values: readonly string[];
  readonly indexArrayConstructor: EnumIndexArrayConstructor;
  /** Encode a declared value, preserving the runtime writer policy of index 0 for invalid values. */
  readonly encode: (value: string) => number;
}

/** Immutable enum metadata shared by every physical plan for one LogSchema. */
export interface SchemaEnumLookupDescriptor {
  readonly ordered: readonly EnumLookupDescriptor[];
  readonly byField: Readonly<Record<string, EnumLookupDescriptor>>;
}

const EMPTY_ENUM_LOOKUP: SchemaEnumLookupDescriptor = Object.freeze({
  ordered: Object.freeze([]),
  byField: Object.freeze(Object.create(null) as Record<string, EnumLookupDescriptor>),
});

const enumLookupBySchema = new WeakMap<LogSchema, SchemaEnumLookupDescriptor>();
interface EnumLookupCore {
  readonly values: readonly string[];
  readonly indexArrayConstructor: EnumIndexArrayConstructor;
  readonly encode: (value: string) => number;
}

const enumLookupCoreByFieldSchema = new WeakMap<object, EnumLookupCore>();
const enumLookupDescriptorsByFieldSchema = new WeakMap<object, Map<string, EnumLookupDescriptor>>();

function indexArrayConstructorForCount(count: number): EnumIndexArrayConstructor {
  // The maximum stored index is count - 1, so 256 and 65,536 values still fit
  // exactly in Uint8 and Uint16 respectively.
  if (count <= 0x100) return Uint8Array;
  if (count <= 0x1_0000) return Uint16Array;
  return Uint32Array;
}

function isEnumEncoder(value: unknown): value is (enumValue: string) => number {
  return typeof value === 'function';
}

function compileEnumEncoder(values: readonly string[]): (value: string) => number {
  const cases = values
    .map((value, index) => `case ${JSON.stringify(value)}: return ${index};`)
    .join('\n');
  const encoder: unknown = new Function('value', `'use strict'; switch (value) { ${cases} default: return 0; }`);
  if (!isEnumEncoder(encoder)) throw new TypeError('Failed to compile enum encoder');
  return encoder;
}

function createEnumLookupDescriptor(
  fieldName: string,
  fieldSchema: object,
  sourceValues: readonly string[],
): EnumLookupDescriptor {
  let descriptorsByName = enumLookupDescriptorsByFieldSchema.get(fieldSchema);
  const cached = descriptorsByName?.get(fieldName);
  if (cached) return cached;

  let core = enumLookupCoreByFieldSchema.get(fieldSchema);
  if (!core) {
    const values = Object.freeze(Array.from(sourceValues));
    core = Object.freeze({
      values,
      indexArrayConstructor: indexArrayConstructorForCount(values.length),
      encode: compileEnumEncoder(values),
    });
    enumLookupCoreByFieldSchema.set(fieldSchema, core);
  }

  const descriptor = Object.freeze({
    fieldName,
    values: core.values,
    indexArrayConstructor: core.indexArrayConstructor,
    encode: core.encode,
  });
  descriptorsByName ??= new Map();
  descriptorsByName.set(fieldName, descriptor);
  enumLookupDescriptorsByFieldSchema.set(fieldSchema, descriptorsByName);
  return descriptor;
}

/** Resolve and cache schema-order enum lookup metadata exactly once per LogSchema. */
export function resolveEnumLookupDescriptor(schema: LogSchema): SchemaEnumLookupDescriptor {
  const cached = enumLookupBySchema.get(schema);
  if (cached) return cached;

  const ordered: EnumLookupDescriptor[] = [];
  const byField = Object.create(null) as Record<string, EnumLookupDescriptor>;
  for (const [fieldName, fieldSchema] of schema._columns) {
    if (getSchemaType(fieldSchema) !== 'enum') continue;
    const values = getEnumValues(fieldSchema);
    if (!values || (typeof fieldSchema !== 'object' && typeof fieldSchema !== 'function') || fieldSchema === null) {
      continue;
    }
    const descriptor = createEnumLookupDescriptor(fieldName, fieldSchema, values);
    ordered.push(descriptor);
    byField[fieldName] = descriptor;
  }

  const descriptor =
    ordered.length === 0
      ? EMPTY_ENUM_LOOKUP
      : Object.freeze({
          ordered: Object.freeze(ordered),
          byField: Object.freeze(byField),
        });
  enumLookupBySchema.set(schema, descriptor);
  return descriptor;
}
