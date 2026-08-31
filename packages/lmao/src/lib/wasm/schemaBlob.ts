/**
 * Compact schema blob for `thread_span_buffer_new_with_schema`.
 *
 * Each field is `[kind:u8][name_len:u8][name bytes]`; enum fields append
 * `[variant_count:u16 LE]` and `[len:u8][variant bytes]` for each variant.
 * System columns from `THREAD_SYSTEM_COLUMNS` are omitted — they are implicit
 * in the native row store.
 */

import type { LogSchema } from '../schema/LogSchema.js';
import { THREAD_ATTRIBUTE_KINDS, THREAD_SYSTEM_COLUMNS } from '../schema/systemSchema.js';
import { getEnumValues, getSchemaType } from '../schema/typeGuards.js';

const SYSTEM_NAMES: Readonly<Record<string, true>> = Object.freeze(
  Object.fromEntries(THREAD_SYSTEM_COLUMNS.map((column) => [column.name, true as const])),
);
const KIND_BY_NAME: Readonly<Record<string, number>> = Object.freeze(
  Object.fromEntries(THREAD_ATTRIBUTE_KINDS.map((kind) => [kind.name, kind.discriminant])),
);

export const THREAD_SYSTEM_COLUMN_COUNT = THREAD_SYSTEM_COLUMNS.length;

export function isThreadSystemColumn(name: string): boolean {
  return SYSTEM_NAMES[name] === true;
}

export function attributeKindForSchemaType(type: string): number | undefined {
  switch (type) {
    case 'number':
      return KIND_BY_NAME.number;
    case 'bigUint64':
      return KIND_BY_NAME.uint64;
    case 'boolean':
      return KIND_BY_NAME.boolean;
    case 'category':
    case 'text':
      return KIND_BY_NAME.text;
    case 'enum':
      return KIND_BY_NAME.enum;
    default:
      return undefined;
  }
}

const utf8 = new TextEncoder();

export function encodeSchemaBlob(schema: LogSchema): Uint8Array {
  const chunks: number[] = [];
  for (const name of schema._columnNames) {
    if (SYSTEM_NAMES[name] === true) continue;
    const field = schema.fields[name];
    const type = getSchemaType(field);
    if (type === undefined) {
      throw new TypeError(`schema field '${name}' has no type`);
    }
    const kind = attributeKindForSchemaType(type);
    if (kind === undefined) {
      throw new TypeError(`schema field '${name}' type '${type}' is not a thread-buffer attribute`);
    }
    const nameBytes = utf8.encode(name);
    if (nameBytes.length > 255) {
      throw new RangeError(`schema field name '${name}' exceeds 255 bytes`);
    }
    chunks.push(kind, nameBytes.length);
    for (const byte of nameBytes) chunks.push(byte);
    if (type !== 'enum') continue;
    const variants = getEnumValues(field);
    if (variants === undefined || variants.length === 0) {
      throw new TypeError(`enum field '${name}' has no variants`);
    }
    if (variants.length > 0xffff) {
      throw new RangeError(`enum field '${name}' has too many variants`);
    }
    chunks.push(variants.length & 0xff, (variants.length >>> 8) & 0xff);
    for (const variant of variants) {
      const variantBytes = utf8.encode(variant);
      if (variantBytes.length > 255) {
        throw new RangeError(`enum variant of '${name}' exceeds 255 bytes`);
      }
      chunks.push(variantBytes.length);
      for (const byte of variantBytes) chunks.push(byte);
    }
  }
  return Uint8Array.from(chunks);
}

export function schemaAttributeOrdinals(schema: LogSchema): ReadonlyMap<string, number> {
  const ordinals = new Map<string, number>();
  let ordinal = THREAD_SYSTEM_COLUMN_COUNT;
  for (const name of schema._columnNames) {
    if (SYSTEM_NAMES[name] === true) continue;
    ordinals.set(name, ordinal);
    ordinal += 1;
  }
  return ordinals;
}
