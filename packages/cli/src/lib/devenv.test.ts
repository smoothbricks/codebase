import { describe, expect, it } from 'bun:test';
import { parseNulEnv } from './devenv.js';

describe('devenv environment loader', () => {
  it('parses NUL-separated env output without splitting values on newlines or equals signs', () => {
    const bytes = new TextEncoder().encode('SIMPLE=value\0MULTILINE=line 1\nline 2\0TOKEN=a=b=c\0');

    expect(parseNulEnv(bytes)).toEqual({
      SIMPLE: 'value',
      MULTILINE: 'line 1\nline 2',
      TOKEN: 'a=b=c',
    });
  });
});
