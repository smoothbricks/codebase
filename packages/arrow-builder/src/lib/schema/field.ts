/**
 * A field: one named, typed entry of a library's schema.
 *
 * A field node is frozen plain data: `kind` says what the value is, `params` holds what the kind was declared with,
 * `physical` is the column type the two determine, `optional` is the one empty state (the validity bitmap), and
 * `hints` are storage and output choices that are no part of the type. The kind's functions (`codec`) and the
 * builder's chain methods are non-enumerable, so a node's data clones and compares by value.
 *
 * The kinds are open: `defineFieldKind` gives an outside package a kind with its own identity, validation and
 * codec. The physical vocabulary (physical.ts) is closed.
 */

import { blake3 } from '@noble/hashes/blake3.js';
import type { Schema, SchemaResult } from './core.js';
import type { PhysicalType, PhysicalValue, TimeUnit } from './physical.js';

/** Why a value is not a value of a field: where, what was expected, what was there. */
export interface FieldViolation {
  /** Path of the offending value: the field's name, then the kind's own path inside the value. */
  readonly path: string;
  readonly expected: string;
  readonly observed: string;
}

/**
 * A kind of field: its identity, its column type, its validation and its codec.
 *
 * `T` is the TypeScript value, `P` the declaration's parameters. Every function is pure and total over its domain:
 * `encode` and `decode` are called only with a value `validate` accepted and a cell `encode` produced.
 */
export interface FieldKind<T, P, Id extends string = string> {
  /** Namespaced by its owner (`<owner>.<name>`) for an extension; part of the type's identity. */
  readonly id: Id;
  /** The column type: a function of the parameters alone. */
  physical(params: P): PhysicalType;
  /** Canonical text of the parameters: equal text, equal type. */
  digest(params: P): string;
  /** Judge a value. A violation's `path` is relative to the value (`''` for the value itself). */
  validate(params: P, value: unknown): SchemaResult<T, FieldViolation>;
  /** The cell of `physical` that holds `value`. */
  encode(params: P, value: T): PhysicalValue;
  /** The value a cell holds. */
  decode(params: P, cell: PhysicalValue): T;
}

/** A transform applied to a string where a column is written out, never where it is stored. */
export type MaskTransform = (value: string) => string;

/** Storage and output choices of a column. They are not part of a field's type. */
export interface FieldHints {
  /** Written on every row: allocated up front, no validity bitmap. */
  readonly eager: boolean;
  /** Applied to each string when the column is written out. */
  readonly mask: MaskTransform | undefined;
}

export interface Field<T = unknown, K extends string = string, P = unknown> extends Schema<T> {
  readonly kind: K;
  readonly params: P;
  /** May be empty; emptiness is a clear validity bit and nothing else. */
  readonly optional: boolean;
  readonly physical: PhysicalType;
  readonly hints: FieldHints;
  /** The kind's functions. Non-enumerable: not part of the node's data. */
  readonly codec: FieldKind<T, P, K>;
}

const NO_HINTS: FieldHints = Object.freeze({ eager: false, mask: undefined });

type Methods = Readonly<Record<string, (...args: never[]) => unknown>>;

function hidden(value: unknown): PropertyDescriptor {
  return { value, enumerable: false, writable: false, configurable: false };
}

/** Build a frozen node. `methods` become non-enumerable own properties. */
function node<T, K extends string, P, M extends Methods>(
  codec: FieldKind<T, P, K>,
  params: P,
  optional: boolean,
  hints: FieldHints,
  methods: M,
): Field<T, K, P> & M {
  const data = { kind: codec.id, params, optional, physical: codec.physical(params), hints };
  const descriptors: PropertyDescriptorMap = { codec: hidden(codec) };
  for (const [name, method] of Object.entries(methods)) descriptors[name] = hidden(method);
  Object.defineProperties(data, descriptors);
  assertNode<T, K, P, M>(data, codec, methods);
  return Object.freeze(data);
}

function assertNode<T, K extends string, P, M extends Methods>(
  value: object,
  codec: FieldKind<T, P, K>,
  methods: M,
): asserts value is Field<T, K, P> & M {
  if (Reflect.get(value, 'codec') !== codec) throw new TypeError(`field node of kind '${codec.id}' lost its codec`);
  for (const name of Object.keys(methods)) {
    if (typeof Reflect.get(value, name) !== 'function') throw new TypeError(`field node lost its method '${name}'`);
  }
}

/** Whether a value is a field node. */
export function isField(value: unknown): value is Field {
  if (typeof value !== 'object' || value === null) return false;
  const codec: unknown = Reflect.get(value, 'codec');
  return (
    typeof Reflect.get(value, 'kind') === 'string' &&
    typeof codec === 'object' &&
    codec !== null &&
    Reflect.get(codec, 'id') === Reflect.get(value, 'kind')
  );
}

/**
 * Define a kind of field. The returned builder makes a field of that kind from its parameters.
 *
 * This is the one way a kind exists: the shared kinds below are defined through it too, so nothing that handles a
 * field can tell an extension from a shared kind.
 */
export function defineFieldKind<T, P, const Id extends string>(
  kind: FieldKind<T, P, Id>,
): (params: P) => Field<T, Id, P> {
  if (kind.id.length === 0) throw new TypeError('a field kind needs a non-empty id');
  const codec = Object.freeze({ ...kind });
  return (params) => node(codec, params, false, NO_HINTS, {});
}

/** The same field with the given hints and chain methods. Used by builders that add their own methods. */
export function withFieldMethods<T, K extends string, P, M extends Methods>(
  field: Field<T, K, P>,
  hints: FieldHints,
  methods: M,
): Field<T, K, P> & M {
  return node(field.codec, field.params, field.optional, hints, methods);
}

function assertExtended<F extends Field, M extends Methods>(value: Field, methods: M): asserts value is F & M {
  for (const name of Object.keys(methods)) {
    if (typeof Reflect.get(value, name) !== 'function') throw new TypeError(`field node lost its method '${name}'`);
  }
}

/**
 * The same field with more chain methods: its data, hints and existing methods are kept. A builder outside this
 * package adds its own vocabulary to a shared field through this.
 */
export function extendField<F extends Field, M extends Methods>(field: F, methods: M): F & M {
  const kept: Record<string, (...args: never[]) => unknown> = {};
  for (const [name, descriptor] of Object.entries(Object.getOwnPropertyDescriptors(field))) {
    if (!descriptor.enumerable && typeof descriptor.value === 'function') kept[name] = descriptor.value;
  }
  const made = node(field.codec, field.params, field.optional, field.hints, { ...kept, ...methods });
  assertExtended<F, M>(made, methods);
  return made;
}

/** A field that may be empty. The inner field's type with `undefined` admitted, and no other change. */
export type OptionalField<F extends Field> =
  F extends Field<infer T, infer K, infer P> ? Field<T | undefined, K, P> & { readonly optional: true } : never;

/**
 * The field, optional: the same kind, parameters and column; empty is a clear validity bit.
 *
 * @throws TypeError for an eager field: an eager column has no validity bitmap, so it cannot be empty.
 */
export function optional<F extends Field>(field: F): OptionalField<F> {
  if (field.hints.eager)
    throw new TypeError(`an eager '${field.kind}' field cannot be optional: it has no validity bitmap`);
  const made = node(field.codec, field.params, true, field.hints, {});
  assertOptional<F>(made);
  return made;
}

function assertOptional<F extends Field>(value: Field): asserts value is OptionalField<F> {
  if (!value.optional) throw new TypeError('optional() produced a required field');
}

// ---------------------------------------------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------------------------------------------

/**
 * Canonical text of a field's type: `kind(digest(params))`. Equal text, equal type. `optional` and the hints are
 * not part of it: two fields that differ only there hold the same values in the same column.
 */
export function typeText(field: Field): string {
  return `${field.kind}(${field.codec.digest(field.params)})`;
}

const HEX = '0123456789abcdef';
const utf8 = new TextEncoder();

/** The type digest: lowercase hex of the first 16 bytes of BLAKE3 over `typeText`. */
export function typeDigest(field: Field): string {
  const hash = blake3(utf8.encode(typeText(field)), { dkLen: 16 });
  let text = '';
  for (const byte of hash) text += HEX[byte >> 4] + HEX[byte & 15];
  return text;
}

// ---------------------------------------------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------------------------------------------

function observed(value: unknown): string {
  if (value === null) return 'null';
  if (value instanceof Uint8Array) return `Uint8Array(${value.length})`;
  if (typeof value === 'string') return JSON.stringify(value.length > 64 ? `${value.slice(0, 64)}…` : value);
  if (typeof value === 'number' || typeof value === 'bigint' || typeof value === 'boolean')
    return `${typeof value} ${value}`;
  return typeof value;
}

function accept<T>(value: T): SchemaResult<T, FieldViolation> {
  return { ok: true, value };
}

function refuse(expected: string, value: unknown): SchemaResult<never, FieldViolation> {
  return { ok: false, error: { path: '', expected, observed: observed(value) } };
}

/**
 * Judge a value against a field. An optional field accepts `undefined` and `null` as empty; every other judgement
 * is the kind's own.
 */
export function validateField<T>(
  name: string,
  field: Field<T>,
  value: unknown,
): SchemaResult<T | undefined, FieldViolation> {
  if (value === undefined || value === null) {
    return field.optional
      ? accept(undefined)
      : { ok: false, error: { path: name, expected: typeText(field), observed: observed(value) } };
  }
  const judged = field.codec.validate(field.params, value);
  if (judged.ok) return judged;
  const { path, expected, observed: seen } = judged.error;
  return { ok: false, error: { path: path === '' ? name : `${name}.${path}`, expected, observed: seen } };
}

// ---------------------------------------------------------------------------------------------------------------
// The shared kinds
// ---------------------------------------------------------------------------------------------------------------

const none = (): string => '';
const identity = <T extends PhysicalValue>(_params: unknown, value: T): T => value;

function decodeAs<T extends PhysicalValue>(is: (cell: PhysicalValue) => cell is T, kind: string) {
  return (_params: unknown, cell: PhysicalValue): T => {
    if (is(cell)) return cell;
    throw new TypeError(`a '${kind}' cell cannot hold ${observed(cell)}`);
  };
}

const isBoolean = (cell: PhysicalValue): cell is boolean => typeof cell === 'boolean';
const isNumber = (cell: PhysicalValue): cell is number => typeof cell === 'number';
const isBigint = (cell: PhysicalValue): cell is bigint => typeof cell === 'bigint';
const isString = (cell: PhysicalValue): cell is string => typeof cell === 'string';
const isBytes = (cell: PhysicalValue): cell is Uint8Array => cell instanceof Uint8Array;

export const booleanKind = defineFieldKind<boolean, null, 'boolean'>({
  id: 'boolean',
  physical: () => ({ type: 'bool' }),
  digest: none,
  validate: (_params, value) => (typeof value === 'boolean' ? accept(value) : refuse('boolean', value)),
  encode: identity,
  decode: decodeAs(isBoolean, 'boolean'),
});

export const f64Kind = defineFieldKind<number, null, 'f64'>({
  id: 'f64',
  physical: () => ({ type: 'float64' }),
  digest: none,
  validate: (_params, value) => (typeof value === 'number' ? accept(value) : refuse('number', value)),
  encode: identity,
  decode: decodeAs(isNumber, 'f64'),
});

function narrowInt<const Id extends string>(id: Id, bits: 8 | 16 | 32, signed: boolean) {
  const min = signed ? -(2 ** (bits - 1)) : 0;
  const max = signed ? 2 ** (bits - 1) - 1 : 2 ** bits - 1;
  const expected = `integer in [${min}, ${max}]`;
  return defineFieldKind<number, null, Id>({
    id,
    physical: () => ({ type: 'int', bits, signed }),
    digest: none,
    validate: (_params, value) =>
      typeof value === 'number' && Number.isInteger(value) && value >= min && value <= max
        ? accept(value)
        : refuse(expected, value),
    encode: identity,
    decode: decodeAs(isNumber, id),
  });
}

function wideInt<const Id extends string>(id: Id, signed: boolean) {
  const min = signed ? -(2n ** 63n) : 0n;
  const max = signed ? 2n ** 63n - 1n : 2n ** 64n - 1n;
  const expected = `bigint in [${min}, ${max}]`;
  return defineFieldKind<bigint, null, Id>({
    id,
    physical: () => ({ type: 'int', bits: 64, signed }),
    digest: none,
    validate: (_params, value) =>
      typeof value === 'bigint' && value >= min && value <= max ? accept(value) : refuse(expected, value),
    encode: identity,
    decode: decodeAs(isBigint, id),
  });
}

export const i8Kind = narrowInt('i8', 8, true);
export const i16Kind = narrowInt('i16', 16, true);
export const i32Kind = narrowInt('i32', 32, true);
export const i64Kind = wideInt('i64', true);
export const u8Kind = narrowInt('u8', 8, false);
export const u16Kind = narrowInt('u16', 16, false);
export const u32Kind = narrowInt('u32', 32, false);
export const u64Kind = wideInt('u64', false);

const I64_MIN = -(2n ** 63n);
const I64_MAX = 2n ** 63n - 1n;

/**
 * A count of a time unit. JavaScript holds nanoseconds and microseconds as bigint and milliseconds and seconds as
 * number (the `Date` domain); the cell is an Int64 for every unit.
 */
function timeKind<const Id extends string>(id: Id, physical: (unit: TimeUnit) => PhysicalType) {
  const wide = (unit: TimeUnit) => unit === 'ns' || unit === 'us';
  return defineFieldKind<number | bigint, TimeUnit, Id>({
    id,
    physical,
    digest: (unit) => unit,
    validate: (unit, value) => {
      if (wide(unit)) {
        return typeof value === 'bigint' && value >= I64_MIN && value <= I64_MAX
          ? accept(value)
          : refuse(`bigint of ${unit} in the int64 range`, value);
      }
      return typeof value === 'number' && Number.isSafeInteger(value)
        ? accept(value)
        : refuse(`integer number of ${unit}`, value);
    },
    encode: (_unit, value) => BigInt(value),
    decode: (unit, cell) => {
      if (typeof cell !== 'bigint') throw new TypeError(`a '${id}' cell cannot hold ${observed(cell)}`);
      return wide(unit) ? cell : Number(cell);
    },
  });
}

/** A point in time: a count of `unit` since the Unix epoch, UTC. */
export const timestampKind = timeKind('timestamp', (unit) => ({ type: 'timestamp', unit, timezone: 'UTC' }));
/** A span of time: a count of `unit`. */
export const durationKind = timeKind('duration', (unit) => ({ type: 'duration', unit }));

/** The string kinds. Text is the one plain kind: everything that is neither an enum nor a category. */
export const textKind = defineFieldKind<string, null, 'text'>({
  id: 'text',
  physical: () => ({ type: 'utf8' }),
  digest: none,
  validate: (_params, value) => (typeof value === 'string' ? accept(value) : refuse('string', value)),
  encode: identity,
  decode: decodeAs(isString, 'text'),
});

/**
 * A bounded set of repeated values the data brings. The column is a dictionary that belongs to the stream; the
 * cell handed to the column's writer is the value, and the writer assigns the ordinal.
 */
export const categoryKind = defineFieldKind<string, null, 'category'>({
  id: 'category',
  physical: () => ({ type: 'dictionary', indexBits: 32, dictionary: 'stream' }),
  digest: none,
  validate: (_params, value) => (typeof value === 'string' ? accept(value) : refuse('string', value)),
  encode: identity,
  decode: decodeAs(isString, 'category'),
});

/** An enum's declaration: its variants, a variant's ordinal being its position. */
export interface EnumParams {
  readonly variants: readonly string[];
}

/** The index width of an enum's dictionary: a function of the number of declared variants. */
export function enumIndexBits(variantCount: number): 8 | 16 | 32 {
  return variantCount <= 256 ? 8 : variantCount <= 65536 ? 16 : 32;
}

/**
 * A closed set the declaration lists. The column is a dictionary of the variants and the cell is the ordinal.
 * The list is append-only: a variant no longer written keeps its place.
 */
export const enumKind = defineFieldKind<string, EnumParams, 'enum'>({
  id: 'enum',
  physical: ({ variants }) => ({ type: 'dictionary', indexBits: enumIndexBits(variants.length), dictionary: 'schema' }),
  digest: ({ variants }) => JSON.stringify(variants),
  validate: ({ variants }, value) =>
    typeof value === 'string' && variants.includes(value)
      ? accept(value)
      : refuse(`one of [${variants.join(', ')}]`, value),
  encode: ({ variants }, value) => variants.indexOf(value),
  decode: ({ variants }, cell) => {
    const variant = typeof cell === 'number' ? variants[cell] : undefined;
    if (variant === undefined)
      throw new TypeError(`an enum of ${variants.length} variants has no ordinal ${observed(cell)}`);
    return variant;
  },
});

export const binaryKind = defineFieldKind<Uint8Array, null, 'binary'>({
  id: 'binary',
  physical: () => ({ type: 'binary' }),
  digest: none,
  validate: (_params, value) => (value instanceof Uint8Array ? accept(value) : refuse('Uint8Array', value)),
  encode: identity,
  decode: decodeAs(isBytes, 'binary'),
});

export const fixedBinaryKind = defineFieldKind<Uint8Array, number, 'fixedBinary'>({
  id: 'fixedBinary',
  physical: (width) => ({ type: 'fixedBinary', width }),
  digest: (width) => String(width),
  validate: (width, value) =>
    value instanceof Uint8Array && value.length === width ? accept(value) : refuse(`Uint8Array(${width})`, value),
  encode: identity,
  decode: decodeAs(isBytes, 'fixedBinary'),
});
