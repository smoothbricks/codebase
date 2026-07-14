import type { LogSchema } from './schema/LogSchema.js';
import { getEnumValues, getSchemaType } from './schema/typeGuards.js';

export type EnumIndexArray = Uint8Array | Uint16Array | Uint32Array;
export type EnumIndexArrayConstructor = Uint8ArrayConstructor | Uint16ArrayConstructor | Uint32ArrayConstructor;

/** Immutable schema-order metadata for one enum column. */
export interface EnumLookupDescriptor {
  readonly fieldName: string;
  readonly values: readonly string[];
  readonly indexArrayConstructor: EnumIndexArrayConstructor;
  /** Encode a declared value, preserving strict string matching and index 0 for every invalid value. */
  readonly encode: (value: unknown) => number;
}

/** Immutable enum metadata shared by every physical plan for one LogSchema. */
export interface SchemaEnumLookupDescriptor {
  readonly ordered: readonly EnumLookupDescriptor[];
  readonly byField: Readonly<Record<string, EnumLookupDescriptor>>;
}

function createNullPrototypeRecord<Value>(): Record<string, Value> {
  return Object.create(null);
}

const EMPTY_ENUM_DESCRIPTORS: readonly EnumLookupDescriptor[] = Object.freeze([]);
const EMPTY_ENUM_LOOKUP: SchemaEnumLookupDescriptor = Object.freeze({
  ordered: EMPTY_ENUM_DESCRIPTORS,
  byField: Object.freeze(createNullPrototypeRecord<EnumLookupDescriptor>()),
});

const enumLookupBySchema = new WeakMap<LogSchema, SchemaEnumLookupDescriptor>();
interface EnumLookupCore {
  readonly values: readonly string[];
  readonly indexArrayConstructor: EnumIndexArrayConstructor;
  readonly encode: (value: unknown) => number;
}

const enumLookupCoreByFieldSchema = new WeakMap<object, EnumLookupCore>();
const enumLookupDescriptorsByFieldSchema = new WeakMap<object, Record<string, EnumLookupDescriptor>>();

function indexArrayConstructorForCount(count: number): EnumIndexArrayConstructor {
  // The maximum stored index is count - 1, so 256 and 65,536 values still fit
  // exactly in Uint8 and Uint16 respectively.
  if (count <= 0x100) return Uint8Array;
  if (count <= 0x1_0000) return Uint16Array;
  return Uint32Array;
}

function createEnumEncoder(values: readonly string[]): (value: unknown) => number {
  const ordinalByValue = createNullPrototypeRecord<number>();
  for (let index = 0; index < values.length; index++) {
    const value = values[index];
    // Switch cases historically resolve duplicate declarations to the first
    // matching case. Preserve that byte-level behavior in the fallback table.
    if (!Object.hasOwn(ordinalByValue, value)) ordinalByValue[value] = index;
  }
  const immutableOrdinalByValue = Object.freeze(ordinalByValue);
  return (value: unknown): number => (typeof value === 'string' ? (immutableOrdinalByValue[value] ?? 0) : 0);
}

function createEnumLookupDescriptor(
  fieldName: string,
  fieldSchema: object,
  sourceValues: readonly string[],
): EnumLookupDescriptor {
  let descriptorsByName = enumLookupDescriptorsByFieldSchema.get(fieldSchema);
  const cached = descriptorsByName?.[fieldName];
  if (cached) return cached;

  let core = enumLookupCoreByFieldSchema.get(fieldSchema);
  if (!core) {
    const values = Object.freeze(Array.from(sourceValues));
    core = Object.freeze({
      values,
      indexArrayConstructor: indexArrayConstructorForCount(values.length),
      encode: createEnumEncoder(values),
    });
    enumLookupCoreByFieldSchema.set(fieldSchema, core);
  }

  const descriptor = Object.freeze({
    fieldName,
    values: core.values,
    indexArrayConstructor: core.indexArrayConstructor,
    encode: core.encode,
  });
  descriptorsByName ??= createNullPrototypeRecord<EnumLookupDescriptor>();
  descriptorsByName[fieldName] = descriptor;
  enumLookupDescriptorsByFieldSchema.set(fieldSchema, descriptorsByName);
  return descriptor;
}

/** Resolve and cache schema-order enum lookup metadata exactly once per LogSchema. */
export function resolveEnumLookupDescriptor(schema: LogSchema): SchemaEnumLookupDescriptor {
  const cached = enumLookupBySchema.get(schema);
  if (cached) return cached;

  const ordered: EnumLookupDescriptor[] = [];
  const byField = createNullPrototypeRecord<EnumLookupDescriptor>();
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
