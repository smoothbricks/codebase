/** Narrows to Record<string, unknown> -- the universal "is an object with string keys" check. */
export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function isString(value: unknown): value is string {
  return typeof value === 'string';
}

export function isNumber(value: unknown): value is number {
  return typeof value === 'number' && !Number.isNaN(value);
}

export function isBoolean(value: unknown): value is boolean {
  return typeof value === 'boolean';
}

export function isBigInt(value: unknown): value is bigint {
  return typeof value === 'bigint';
}

/** Like isRecord but also rejects class instances (only plain objects). */
export function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

/** Type-safe hasOwnProperty check. */
export function hasOwn<K extends string>(
  obj: Record<string, unknown>,
  key: K,
): obj is Record<string, unknown> & Record<K, unknown> {
  return Object.hasOwn(obj, key);
}

/** Check obj has key with string value. */
export function hasOwnString<K extends string>(
  obj: Record<string, unknown>,
  key: K,
): obj is Record<string, unknown> & Record<K, string> {
  return hasOwn(obj, key) && typeof obj[key] === 'string';
}

/** Check obj has key with number value. */
export function hasOwnNumber<K extends string>(
  obj: Record<string, unknown>,
  key: K,
): obj is Record<string, unknown> & Record<K, number> {
  return hasOwn(obj, key) && typeof obj[key] === 'number';
}

/** Check obj has key with boolean value. */
export function hasOwnBoolean<K extends string>(
  obj: Record<string, unknown>,
  key: K,
): obj is Record<string, unknown> & Record<K, boolean> {
  return hasOwn(obj, key) && typeof obj[key] === 'boolean';
}

/** Check obj has key with bigint value. */
export function hasOwnBigInt<K extends string>(
  obj: Record<string, unknown>,
  key: K,
): obj is Record<string, unknown> & Record<K, bigint> {
  return hasOwn(obj, key) && typeof obj[key] === 'bigint';
}
