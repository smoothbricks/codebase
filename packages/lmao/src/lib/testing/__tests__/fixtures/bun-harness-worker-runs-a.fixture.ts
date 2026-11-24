import { describe, expect, it } from 'bun:test';
import { useTestSpan } from '../../bun-harness.js';

describe('worker runs file a', () => {
  it('file a first', () => {
    expect(useTestSpan().spanName).toBe('file a first');
  });

  it('file a second', () => {
    expect(useTestSpan().spanName).toBe('file a second');
  });
});
