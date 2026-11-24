import { describe, expect, it } from 'bun:test';
import { useTestSpan } from '../../bun-harness.js';

describe('worker runs file b', () => {
  it('file b first', () => {
    expect(useTestSpan().spanName).toBe('file b first');
  });

  it('file b second', () => {
    expect(useTestSpan().spanName).toBe('file b second');
  });
});
