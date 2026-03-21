import { describe, expect, it } from 'bun:test';

import { assertDefined, assertNever, assertRecord } from '../assert.js';

// ---------------------------------------------------------------------------
// assertDefined
// ---------------------------------------------------------------------------

describe('assertDefined', () => {
  it('passes for defined values', () => {
    expect(() => assertDefined(0)).not.toThrow();
    expect(() => assertDefined('')).not.toThrow();
    expect(() => assertDefined(false)).not.toThrow();
    expect(() => assertDefined({})).not.toThrow();
    expect(() => assertDefined([])).not.toThrow();
    expect(() => assertDefined(0n)).not.toThrow();
  });

  it('throws for null', () => {
    expect(() => assertDefined(null)).toThrow('Expected value to be defined, got null');
  });

  it('throws for undefined', () => {
    expect(() => assertDefined(undefined)).toThrow('Expected value to be defined, got undefined');
  });

  it('uses custom message when provided', () => {
    expect(() => assertDefined(null, 'User must exist')).toThrow('User must exist');
  });

  it('narrows the type after assertion', () => {
    const value: string | null | undefined = 'hello';
    assertDefined(value);
    // If the assertion didn't narrow, this line would be a type error
    const _check: string = value;
    expect(_check).toBe('hello');
  });
});

// ---------------------------------------------------------------------------
// assertRecord
// ---------------------------------------------------------------------------

describe('assertRecord', () => {
  it('passes for plain objects', () => {
    expect(() => assertRecord({})).not.toThrow();
    expect(() => assertRecord({ a: 1 })).not.toThrow();
  });

  it('passes for class instances', () => {
    expect(() => assertRecord(new Date())).not.toThrow();
    expect(() => assertRecord(new Map())).not.toThrow();
  });

  it('throws for null', () => {
    expect(() => assertRecord(null)).toThrow('Expected a record object, got object');
  });

  it('throws for undefined', () => {
    expect(() => assertRecord(undefined)).toThrow('Expected a record object, got undefined');
  });

  it('throws for arrays', () => {
    expect(() => assertRecord([])).toThrow('Expected a record object, got object');
  });

  it('throws for primitives', () => {
    expect(() => assertRecord('string')).toThrow('Expected a record object, got string');
    expect(() => assertRecord(42)).toThrow('Expected a record object, got number');
    expect(() => assertRecord(true)).toThrow('Expected a record object, got boolean');
  });

  it('uses custom message when provided', () => {
    expect(() => assertRecord(null, 'Payload must be an object')).toThrow('Payload must be an object');
  });
});

// ---------------------------------------------------------------------------
// assertNever
// ---------------------------------------------------------------------------

describe('assertNever', () => {
  it('always throws', () => {
    // We cast to never in test-only context to exercise the runtime path
    expect(() => assertNever('unexpected' as never)).toThrow('Unexpected value: unexpected');
  });

  it('uses custom message when provided', () => {
    expect(() => assertNever(42 as never, 'Unhandled case: 42')).toThrow('Unhandled case: 42');
  });

  it('works in an exhaustive switch pattern', () => {
    type Status = 'active' | 'inactive';
    const check = (s: Status): string => {
      switch (s) {
        case 'active':
          return 'on';
        case 'inactive':
          return 'off';
        default:
          assertNever(s);
      }
    };
    expect(check('active')).toBe('on');
    expect(check('inactive')).toBe('off');
  });
});
