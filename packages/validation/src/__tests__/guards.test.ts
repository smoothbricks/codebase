import { describe, expect, it } from 'bun:test';

import {
  hasOwn,
  hasOwnBigInt,
  hasOwnBoolean,
  hasOwnNumber,
  hasOwnString,
  isBigInt,
  isBoolean,
  isNumber,
  isPlainObject,
  isRecord,
  isString,
} from '../guards.js';

// ---------------------------------------------------------------------------
// isRecord
// ---------------------------------------------------------------------------

describe('isRecord', () => {
  it('returns true for plain objects', () => {
    expect(isRecord({})).toBe(true);
    expect(isRecord({ a: 1 })).toBe(true);
  });

  it('returns true for Object.create(null)', () => {
    expect(isRecord(Object.create(null))).toBe(true);
  });

  it('returns true for class instances (isRecord does not check prototype)', () => {
    expect(isRecord(new Date())).toBe(true);
    expect(isRecord(new Map())).toBe(true);
  });

  it('returns false for arrays', () => {
    expect(isRecord([])).toBe(false);
    expect(isRecord([1, 2, 3])).toBe(false);
  });

  it('returns false for null', () => {
    expect(isRecord(null)).toBe(false);
  });

  it('returns false for undefined', () => {
    expect(isRecord(undefined)).toBe(false);
  });

  it('returns false for primitives', () => {
    expect(isRecord('')).toBe(false);
    expect(isRecord(0)).toBe(false);
    expect(isRecord(false)).toBe(false);
    expect(isRecord(42n)).toBe(false);
    expect(isRecord(Symbol())).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// isString
// ---------------------------------------------------------------------------

describe('isString', () => {
  it('returns true for strings', () => {
    expect(isString('')).toBe(true);
    expect(isString('hello')).toBe(true);
  });

  it('returns false for non-strings', () => {
    expect(isString(0)).toBe(false);
    expect(isString(null)).toBe(false);
    expect(isString(undefined)).toBe(false);
    expect(isString(true)).toBe(false);
    expect(isString({})).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// isNumber
// ---------------------------------------------------------------------------

describe('isNumber', () => {
  it('returns true for finite numbers', () => {
    expect(isNumber(0)).toBe(true);
    expect(isNumber(42)).toBe(true);
    expect(isNumber(-3.14)).toBe(true);
    expect(isNumber(Number.POSITIVE_INFINITY)).toBe(true);
  });

  it('returns false for NaN', () => {
    expect(isNumber(Number.NaN)).toBe(false);
  });

  it('returns false for non-numbers', () => {
    expect(isNumber('42')).toBe(false);
    expect(isNumber(null)).toBe(false);
    expect(isNumber(undefined)).toBe(false);
    expect(isNumber(42n)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// isBoolean
// ---------------------------------------------------------------------------

describe('isBoolean', () => {
  it('returns true for booleans', () => {
    expect(isBoolean(true)).toBe(true);
    expect(isBoolean(false)).toBe(true);
  });

  it('returns false for non-booleans', () => {
    expect(isBoolean(0)).toBe(false);
    expect(isBoolean(1)).toBe(false);
    expect(isBoolean('')).toBe(false);
    expect(isBoolean(null)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// isBigInt
// ---------------------------------------------------------------------------

describe('isBigInt', () => {
  it('returns true for bigints', () => {
    expect(isBigInt(0n)).toBe(true);
    expect(isBigInt(42n)).toBe(true);
    expect(isBigInt(-100n)).toBe(true);
  });

  it('returns false for non-bigints', () => {
    expect(isBigInt(0)).toBe(false);
    expect(isBigInt('42')).toBe(false);
    expect(isBigInt(null)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// isPlainObject
// ---------------------------------------------------------------------------

describe('isPlainObject', () => {
  it('returns true for plain objects', () => {
    expect(isPlainObject({})).toBe(true);
    expect(isPlainObject({ a: 1, b: 'two' })).toBe(true);
  });

  it('returns true for Object.create(null)', () => {
    expect(isPlainObject(Object.create(null))).toBe(true);
  });

  it('rejects class instances', () => {
    expect(isPlainObject(new Date())).toBe(false);
    expect(isPlainObject(new Map())).toBe(false);
    expect(isPlainObject(new Set())).toBe(false);
    expect(isPlainObject(new Error('test'))).toBe(false);
  });

  it('rejects arrays', () => {
    expect(isPlainObject([])).toBe(false);
  });

  it('rejects null', () => {
    expect(isPlainObject(null)).toBe(false);
  });

  it('rejects undefined', () => {
    expect(isPlainObject(undefined)).toBe(false);
  });

  it('rejects primitives', () => {
    expect(isPlainObject('')).toBe(false);
    expect(isPlainObject(0)).toBe(false);
    expect(isPlainObject(false)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// hasOwn
// ---------------------------------------------------------------------------

describe('hasOwn', () => {
  it('returns true for own properties', () => {
    const obj: Record<string, unknown> = { name: 'test', count: 42 };
    expect(hasOwn(obj, 'name')).toBe(true);
    expect(hasOwn(obj, 'count')).toBe(true);
  });

  it('returns false for missing keys', () => {
    const obj: Record<string, unknown> = { name: 'test' };
    expect(hasOwn(obj, 'missing')).toBe(false);
  });

  it('returns false for prototype properties', () => {
    const obj: Record<string, unknown> = {};
    expect(hasOwn(obj, 'toString')).toBe(false);
  });

  it('returns true even if value is undefined', () => {
    const obj: Record<string, unknown> = { key: undefined };
    expect(hasOwn(obj, 'key')).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// hasOwnString
// ---------------------------------------------------------------------------

describe('hasOwnString', () => {
  it('returns true for own string properties', () => {
    const obj: Record<string, unknown> = { name: 'hello' };
    expect(hasOwnString(obj, 'name')).toBe(true);
  });

  it('returns false if key is missing', () => {
    const obj: Record<string, unknown> = {};
    expect(hasOwnString(obj, 'name')).toBe(false);
  });

  it('returns false if value is not a string', () => {
    const obj: Record<string, unknown> = { name: 42 };
    expect(hasOwnString(obj, 'name')).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// hasOwnNumber
// ---------------------------------------------------------------------------

describe('hasOwnNumber', () => {
  it('returns true for own number properties', () => {
    const obj: Record<string, unknown> = { count: 42 };
    expect(hasOwnNumber(obj, 'count')).toBe(true);
  });

  it('returns false if key is missing', () => {
    const obj: Record<string, unknown> = {};
    expect(hasOwnNumber(obj, 'count')).toBe(false);
  });

  it('returns false if value is not a number', () => {
    const obj: Record<string, unknown> = { count: '42' };
    expect(hasOwnNumber(obj, 'count')).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// hasOwnBoolean
// ---------------------------------------------------------------------------

describe('hasOwnBoolean', () => {
  it('returns true for own boolean properties', () => {
    const obj: Record<string, unknown> = { active: true };
    expect(hasOwnBoolean(obj, 'active')).toBe(true);
  });

  it('returns false if key is missing', () => {
    const obj: Record<string, unknown> = {};
    expect(hasOwnBoolean(obj, 'active')).toBe(false);
  });

  it('returns false if value is not a boolean', () => {
    const obj: Record<string, unknown> = { active: 1 };
    expect(hasOwnBoolean(obj, 'active')).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// hasOwnBigInt
// ---------------------------------------------------------------------------

describe('hasOwnBigInt', () => {
  it('returns true for own bigint properties', () => {
    const obj: Record<string, unknown> = { amount: 100n };
    expect(hasOwnBigInt(obj, 'amount')).toBe(true);
  });

  it('returns false if key is missing', () => {
    const obj: Record<string, unknown> = {};
    expect(hasOwnBigInt(obj, 'amount')).toBe(false);
  });

  it('returns false if value is not a bigint', () => {
    const obj: Record<string, unknown> = { amount: 100 };
    expect(hasOwnBigInt(obj, 'amount')).toBe(false);
  });
});
