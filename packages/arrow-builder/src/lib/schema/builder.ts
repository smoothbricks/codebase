/**
 * The field builder: `S`.
 *
 * One builder for every type a column can hold. Each function returns a field node (field.ts) of a shared kind; a
 * package's private kinds come from `defineFieldKind` and stand beside these.
 *
 * Strings are three kinds, each with one meaning:
 * - S.enum(['A', 'B']): a closed set the declaration lists; the column is a dictionary of the variants.
 * - S.category(): a bounded set of repeated values the data brings; the column is a dictionary of the stream.
 * - S.text(): everything else; a plain string column.
 */

import {
  binaryKind,
  booleanKind,
  categoryKind,
  durationKind,
  type EnumParams,
  enumKind,
  type Field,
  f64Kind,
  fixedBinaryKind,
  i8Kind,
  i16Kind,
  i32Kind,
  i64Kind,
  type MaskTransform,
  type OptionalField,
  optional,
  textKind,
  timestampKind,
  u8Kind,
  u16Kind,
  u32Kind,
  u64Kind,
  withFieldMethods,
} from './field.js';
import type { TimeUnit } from './physical.js';
import type {
  EagerField,
  EnumField,
  LazyBinarySchema,
  LazyBooleanSchema,
  LazyCategorySchema,
  LazyEnumSchema,
  LazyField,
  LazyNumberSchema,
  LazyTextSchema,
  LazyU64Schema,
  MaskableField,
  MaskPreset,
} from './types.js';

/**
 * Masking preset implementations
 * Applied during Arrow table serialization (cold path, NOT hot path)
 */
export const maskingTransforms: Record<MaskPreset, MaskTransform> = {
  /**
   * Hash masking - creates deterministic hash for IDs
   * Maintains referential integrity while hiding actual values
   */
  hash: (value: string): string => {
    // Simple hash for demo - use crypto.subtle.digest in production
    let hash = 0;
    for (let i = 0; i < value.length; i++) {
      hash = (hash << 5) - hash + value.charCodeAt(i);
      hash = hash & hash; // Convert to 32-bit integer
    }
    return `0x${Math.abs(hash).toString(16).padStart(16, '0')}`;
  },

  /**
   * URL masking - hides domain while preserving path structure
   * Useful for HTTP request logging
   */
  url: (value: string): string => {
    try {
      const url = new URL(value);
      return `${url.protocol}//*****${url.pathname}${url.search}`;
    } catch {
      return '*****';
    }
  },

  /**
   * SQL masking - replaces literals with placeholders
   * Preserves query structure for analysis
   */
  sql: (value: string): string => {
    // Replace string literals and numbers with placeholders
    return value.replace(/'[^']*'/g, '?').replace(/\b\d+\b/g, '?');
  },

  /**
   * Email masking - shows first character and domain only
   * Maintains uniqueness while protecting privacy
   */
  email: (value: string): string => {
    const [local, domain] = value.split('@');
    if (!domain) return '*****';
    // Guard against empty local part (e.g., "@domain.com")
    if (!local || local.length === 0) return `*****@${domain}`;
    return `${local[0]}*****@${domain}`;
  },
};

function resolveMaskTransform(preset: MaskPreset | MaskTransform): MaskTransform {
  return typeof preset === 'function' ? preset : maskingTransforms[preset];
}

function assertEager<F extends Field>(field: Field): asserts field is EagerField<F> {
  if (!field.hints.eager) throw new TypeError(`expected an eager '${field.kind}' field`);
}

function assertLazy<F extends Field>(field: Field): asserts field is F & { readonly hints: { readonly eager: false } } {
  if (field.hints.eager) throw new TypeError(`expected a lazy '${field.kind}' field`);
}

function eagerOf<F extends Field>(field: F, mask: MaskTransform | undefined): EagerField<F> {
  const made = withFieldMethods(field, { eager: true, mask }, {});
  assertEager<F>(made);
  return made;
}

/**
 * The field as a lazy column with `.eager()`. A builder outside this package gives its own kinds the same two
 * forms through this.
 */
export function lazyField<T, K extends string, P>(field: Field<T, K, P>): LazyField<Field<T, K, P>> {
  const made = withFieldMethods(field, { eager: false, mask: undefined }, { eager: () => eagerOf(field, undefined) });
  assertLazy<typeof made>(made);
  return made;
}

function maskableField<T, K extends string, P>(
  field: Field<T, K, P>,
  mask: MaskTransform | undefined,
): MaskableField<Field<T, K, P>> {
  const made = withFieldMethods(
    field,
    { eager: false, mask },
    {
      eager: () => eagerOf(field, mask),
      mask: (preset: MaskPreset | MaskTransform) => maskableField(field, resolveMaskTransform(preset)),
    },
  );
  assertLazy<typeof made>(made);
  return made;
}

function assertEnumOf<T extends string>(
  field: Field<string, 'enum', EnumParams>,
  values: readonly T[],
): asserts field is EnumField<T> {
  if (field.params.variants.length !== values.length) throw new TypeError('enum field lost its variants');
}

/** The field builder's shape. */
export interface FieldBuilder {
  boolean(): LazyBooleanSchema;

  i8(): Field<number, 'i8', null>;
  i16(): Field<number, 'i16', null>;
  i32(): Field<number, 'i32', null>;
  i64(): Field<bigint, 'i64', null>;
  u8(): Field<number, 'u8', null>;
  u16(): Field<number, 'u16', null>;
  u32(): Field<number, 'u32', null>;
  u64(): LazyU64Schema;

  /** A 64-bit float. */
  f64(): LazyNumberSchema;
  /** `f64` under the name a JavaScript value has. */
  number(): LazyNumberSchema;

  /**
   * A closed set of strings the declaration lists. A variant's ordinal is its position; the list is append-only,
   * and a variant no longer written keeps its place.
   *
   * @throws TypeError for an empty list or a repeated variant.
   */
  enum<const T extends readonly string[]>(values: T): LazyEnumSchema<T[number]>;

  /** A bounded set of repeated strings the data brings (a region, a model, a table name). */
  category(): LazyCategorySchema;

  /** Any other string: ids, keys, free text. A plain string column. */
  text(): LazyTextSchema;

  /** Raw bytes. */
  binary(): LazyBinarySchema;
  /** Raw bytes of one width. */
  fixedBinary(width: number): Field<Uint8Array, 'fixedBinary', number>;

  /** A point in time: a count of `unit` since the Unix epoch, UTC. */
  timestamp(unit: TimeUnit): Field<bigint, 'timestamp', TimeUnit>;
  /** A span of time: a count of `unit`. */
  duration(unit: TimeUnit): Field<bigint, 'duration', TimeUnit>;

  /**
   * The field, optional: the same kind and column; empty is a clear validity bit. There is no other empty state.
   *
   * @throws TypeError for an eager field.
   */
  optional<F extends Field>(field: F): OptionalField<F>;
}

export const S: FieldBuilder = {
  boolean: () => lazyField(booleanKind(null)),
  i8: () => i8Kind(null),
  i16: () => i16Kind(null),
  i32: () => i32Kind(null),
  i64: () => i64Kind(null),
  u8: () => u8Kind(null),
  u16: () => u16Kind(null),
  u32: () => u32Kind(null),
  u64: () => lazyField(u64Kind(null)),
  f64: () => lazyField(f64Kind(null)),
  number: () => lazyField(f64Kind(null)),
  enum: <const T extends readonly string[]>(values: T): LazyEnumSchema<T[number]> => {
    if (values.length === 0) throw new TypeError('Enum must have at least one value');
    if (new Set(values).size !== values.length) throw new TypeError(`Enum repeats a variant: [${values.join(', ')}]`);
    // The list is the caller's and is held as given: a declaration is not copied per field.
    const field = enumKind(Object.freeze({ variants: values }));
    assertEnumOf<T[number]>(field, values);
    return lazyField(field);
  },
  category: () => maskableField(categoryKind(null), undefined),
  text: () => maskableField(textKind(null), undefined),
  binary: () => lazyField(binaryKind(null)),
  fixedBinary: (width) => {
    if (!Number.isInteger(width) || width <= 0)
      throw new TypeError(`fixedBinary needs a positive integer width, got ${width}`);
    return fixedBinaryKind(width);
  },
  timestamp: (unit) => timestampKind(unit),
  duration: (unit) => durationKind(unit),
  optional,
};
