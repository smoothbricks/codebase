/**
 * Reading a field node from the buffer layer and its consumers: the storage lane, an enum's variants and
 * dictionary bytes, the mask, the encoder of a binary column.
 */

import { intern } from '../arrow/interner.js';
import { type EnumParams, type Field, isField, type MaskTransform } from './field.js';
import type { BinaryEncoder, EnumField, EnumUtf8Precomputed, StorageLane } from './types.js';

/**
 * The lane the buffer layer stores a field in, or `undefined` when it has none: a kind whose column the buffer
 * layer cannot hold yet. The caller that builds buffers refuses such a field by name.
 */
export function storageLaneOf(value: unknown): StorageLane | undefined {
  if (!isField(value)) return undefined;
  switch (value.kind) {
    case 'enum':
    case 'category':
    case 'text':
    case 'boolean':
      return value.kind;
    case 'f64':
      return 'number';
    case 'u64':
      return 'bigUint64';
    default:
      return value.physical.type === 'binary' ? 'binary' : undefined;
  }
}

function isEnumParams(params: unknown): params is EnumParams {
  if (typeof params !== 'object' || params === null) return false;
  const variants: unknown = Reflect.get(params, 'variants');
  return Array.isArray(variants) && variants.every((variant) => typeof variant === 'string');
}

/** Whether a value is an enum field. */
export function isEnumField(value: unknown): value is EnumField {
  return isField(value) && value.kind === 'enum' && isEnumParams(value.params);
}

/** An enum field's variants in ordinal order; `undefined` for anything else. */
export function getEnumValues(value: unknown): readonly string[] | undefined {
  return isEnumField(value) ? value.params.variants : undefined;
}

const enumUtf8ByVariants = new WeakMap<readonly string[], EnumUtf8Precomputed>();

function precomputeEnumUtf8(values: readonly string[]): EnumUtf8Precomputed {
  // intern() keeps every variant's UTF-8 bytes in the process-wide table the string lanes reuse.
  const bytes = values.map((v) => intern(v));
  const offsets = new Int32Array(values.length + 1);
  let totalSize = 0;
  for (let i = 0; i < bytes.length; i++) {
    totalSize += bytes[i].length;
    offsets[i + 1] = totalSize;
  }
  const concatenated = new Uint8Array(totalSize);
  let offset = 0;
  for (const b of bytes) {
    concatenated.set(b, offset);
    offset += b.length;
  }
  return { bytes, concatenated, offsets };
}

/**
 * The dictionary bytes of an enum field: its variants' UTF-8 in ordinal order. Derived from the declaration once
 * per variant list and kept for its life, so a flush copies ready bytes.
 */
export function getEnumUtf8(value: unknown): EnumUtf8Precomputed | undefined {
  if (!isEnumField(value)) return undefined;
  const { variants } = value.params;
  let utf8 = enumUtf8ByVariants.get(variants);
  if (utf8 === undefined) {
    utf8 = precomputeEnumUtf8(variants);
    enumUtf8ByVariants.set(variants, utf8);
  }
  return utf8;
}

/** The mask a field's column is written out through, if any. */
export function getMaskTransform(value: unknown): MaskTransform | undefined {
  return isField(value) ? value.hints.mask : undefined;
}

function bytesOf(field: Field, value: unknown): Uint8Array {
  const cell = field.codec.encode(field.params, value);
  if (cell instanceof Uint8Array) return cell;
  throw new TypeError(`field kind '${field.kind}' lowers to a binary column but encoded a ${typeof cell}`);
}

/**
 * The encoder of a binary column whose values are not raw bytes: the field kind's own `encode`. `undefined` for a
 * raw `binary` field and for a field that is not a binary column.
 */
export function getBinaryEncoder(value: unknown): BinaryEncoder | undefined {
  if (!isField(value) || value.kind === 'binary' || value.physical.type !== 'binary') return undefined;
  return { encode: (cell) => bytesOf(value, cell) };
}
