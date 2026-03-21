import { describe, expect, it } from 'bun:test';

import { parseJsonArray, parseJsonRecord, safeJsonParse } from '../json.js';

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
