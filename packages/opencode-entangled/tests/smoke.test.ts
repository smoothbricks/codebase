import { describe, expect, it } from 'bun:test';

describe('opencode-entangled', () => {
  it('should be importable', async () => {
    const mod = await import('../src/index.js');
    expect(mod).toBeDefined();
  });
});
