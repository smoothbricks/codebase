import { describe, expect, it } from 'bun:test';

import {
  expectJsonBoundary,
  expectJsonRecord,
  parseJsonArray,
  parseJsonBoundary,
  parseJsonBoundaryValue,
  parseJsonRecord,
  parseJsonValue,
  safeJsonParse,
  validateJsonValue,
} from '../json.js';

// ---------------------------------------------------------------------------
// safeJsonParse
// ---------------------------------------------------------------------------

describe('safeJsonParse', () => {
  it('parses valid JSON object', () => {
    const result = safeJsonParse('{"a":1}');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value).toEqual({ a: 1 });
    }
  });

  it('parses valid JSON array', () => {
    const result = safeJsonParse('[1,2,3]');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value).toEqual([1, 2, 3]);
    }
  });

  it('parses valid JSON string', () => {
    const result = safeJsonParse('"hello"');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value).toBe('hello');
    }
  });

  it('parses valid JSON number', () => {
    const result = safeJsonParse('42');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value).toBe(42);
    }
  });

  it('parses valid JSON null', () => {
    const result = safeJsonParse('null');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value).toBe(null);
    }
  });

  it('parses valid JSON boolean', () => {
    const result = safeJsonParse('true');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value).toBe(true);
    }
  });

  it('returns error for invalid JSON', () => {
    const result = safeJsonParse('{bad json}');
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error).toBeString();
      expect(result.error.length).toBeGreaterThan(0);
    }
  });

  it('returns error for empty string', () => {
    const result = safeJsonParse('');
    expect(result.ok).toBe(false);
  });

  it('returns error for trailing commas', () => {
    const result = safeJsonParse('{"a":1,}');
    expect(result.ok).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// parseJsonRecord
// ---------------------------------------------------------------------------

describe('parseJsonRecord', () => {
  it('parses valid JSON object', () => {
    const result = parseJsonRecord('{"name":"test","count":42}');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value).toEqual({ name: 'test', count: 42 });
    }
  });

  it('parses empty JSON object', () => {
    const result = parseJsonRecord('{}');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value).toEqual({});
    }
  });

  it('returns error for JSON array', () => {
    const result = parseJsonRecord('[1,2]');
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error).toContain('Expected JSON object');
    }
  });

  it('returns error for JSON string', () => {
    const result = parseJsonRecord('"hello"');
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error).toContain('Expected JSON object');
    }
  });

  it('returns error for JSON number', () => {
    const result = parseJsonRecord('42');
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error).toContain('Expected JSON object');
    }
  });

  it('returns error for JSON null', () => {
    const result = parseJsonRecord('null');
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error).toContain('Expected JSON object');
    }
  });

  it('returns error for invalid JSON', () => {
    const result = parseJsonRecord('not json');
    expect(result.ok).toBe(false);
  });
});

describe('parseJsonValue', () => {
  it('returns typed values when the parser succeeds', () => {
    const result = parseJsonValue(
      '{"kind":"signal"}',
      (value): { kind: string } => {
        if (typeof value === 'object' && value !== null && 'kind' in value && typeof value.kind === 'string') {
          return { kind: value.kind };
        }

        throw new Error('Expected signal envelope');
      },
      'signal envelope',
    );

    expect(result).toEqual({ ok: true, value: { kind: 'signal' } });
  });

  it('returns a structured error when the parser rejects the value', () => {
    const result = parseJsonValue(
      '{"kind":1}',
      (value): { kind: string } => {
        if (typeof value === 'object' && value !== null && 'kind' in value && typeof value.kind === 'string') {
          return { kind: value.kind };
        }

        throw new Error('Expected signal envelope');
      },
      'signal envelope',
    );

    expect(result).toEqual({ ok: false, error: 'Expected signal envelope' });
  });

  it('uses the fallback expected message when the parser throws a non-error value', () => {
    const result = parseJsonValue(
      '{"kind":1}',
      (): { kind: string } => {
        throw 'bad parser';
      },
      'signal envelope',
    );

    expect(result).toEqual({ ok: false, error: 'Expected signal envelope' });
  });
});

describe('parseJsonBoundaryValue', () => {
  it('returns the parsed value when the boundary parser succeeds', () => {
    const result = parseJsonBoundaryValue(
      { kind: 'signal' },
      {
        parse: (value): { kind: string } => {
          if (typeof value === 'object' && value !== null && 'kind' in value && typeof value.kind === 'string') {
            return { kind: value.kind };
          }

          throw new Error('Expected signal envelope');
        },
        expected: 'signal envelope',
      },
    );

    expect(result).toEqual({ ok: true, value: { kind: 'signal' } });
  });

  it('returns the expected fallback when the parser throws a non-error value', () => {
    const result = parseJsonBoundaryValue(
      { kind: 1 },
      {
        parse: (): { kind: string } => {
          throw 'bad parser';
        },
        expected: 'signal envelope',
      },
    );

    expect(result).toEqual({ ok: false, error: 'Expected signal envelope' });
  });
});

describe('parseJsonBoundary', () => {
  it('parses JSON without a boundary parser', () => {
    const result = parseJsonBoundary('{"kind":"signal"}');

    expect(result).toEqual({ ok: true, value: { kind: 'signal' } });
  });

  it('returns the boundary parser error for malformed payloads', () => {
    const result = parseJsonBoundary('{"kind":1}', {
      parse: (value): { kind: string } => {
        if (typeof value === 'object' && value !== null && 'kind' in value && typeof value.kind === 'string') {
          return { kind: value.kind };
        }

        throw new Error('Expected signal envelope');
      },
      expected: 'signal envelope',
    });

    expect(result).toEqual({ ok: false, error: 'Expected signal envelope' });
  });
});

describe('expectJsonBoundary', () => {
  it('returns parsed JSON without a boundary parser', () => {
    expect(expectJsonBoundary('{"kind":"signal"}', undefined, 'signal payload')).toEqual({ kind: 'signal' });
  });

  it('throws a contextual parse error when the boundary parser rejects the payload', () => {
    expect(() =>
      expectJsonBoundary(
        '{"kind":1}',
        {
          parse: (value): { kind: string } => {
            if (typeof value === 'object' && value !== null && 'kind' in value && typeof value.kind === 'string') {
              return { kind: value.kind };
            }

            throw new Error('Expected signal envelope');
          },
          expected: 'signal envelope',
        },
        'signal payload',
      ),
    ).toThrow('Failed to parse signal envelope: Expected signal envelope');
  });
});

describe('expectJsonRecord', () => {
  it('returns a parsed JSON object', () => {
    expect(expectJsonRecord('{"kind":"signal"}', 'signal payload')).toEqual({ kind: 'signal' });
  });

  it('throws a contextual parse error when the payload is not an object', () => {
    expect(() => expectJsonRecord('[1,2,3]', 'signal payload')).toThrow('signal payload: Expected JSON object');
  });
});

describe('validateJsonValue', () => {
  it('returns typed values when the validator succeeds', () => {
    const result = validateJsonValue('{"kind":"signal"}', (value) => {
      if (typeof value === 'object' && value !== null && 'kind' in value && typeof value.kind === 'string') {
        return { ok: true, value: { kind: value.kind } };
      }

      return { ok: false, error: 'Expected signal envelope' };
    });

    expect(result).toEqual({ ok: true, value: { kind: 'signal' } });
  });

  it('returns the validator error when validation fails', () => {
    const result = validateJsonValue('{"kind":1}', (value) => {
      if (typeof value === 'object' && value !== null && 'kind' in value && typeof value.kind === 'string') {
        return { ok: true, value: { kind: value.kind } };
      }

      return { ok: false, error: 'Expected signal envelope' };
    });

    expect(result).toEqual({ ok: false, error: 'Expected signal envelope' });
  });
});

// ---------------------------------------------------------------------------
// parseJsonArray
// ---------------------------------------------------------------------------

describe('parseJsonArray', () => {
  it('parses valid JSON array', () => {
    const result = parseJsonArray('[1,"two",true]');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value).toEqual([1, 'two', true]);
    }
  });

  it('parses empty JSON array', () => {
    const result = parseJsonArray('[]');
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value).toEqual([]);
    }
  });

  it('returns error for JSON object', () => {
    const result = parseJsonArray('{"a":1}');
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error).toContain('Expected JSON array');
    }
  });

  it('returns error for JSON string', () => {
    const result = parseJsonArray('"hello"');
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error).toContain('Expected JSON array');
    }
  });

  it('returns error for JSON null', () => {
    const result = parseJsonArray('null');
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error).toContain('Expected JSON array');
    }
  });

  it('returns error for invalid JSON', () => {
    const result = parseJsonArray('{not array}');
    expect(result.ok).toBe(false);
  });
});
