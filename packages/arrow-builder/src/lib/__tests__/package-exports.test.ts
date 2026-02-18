import { describe, expect, it } from 'bun:test';

describe('Package exports', () => {
  it('resolves root export', async () => {
    const mod = await import('@smoothbricks/arrow-builder');
    expect(typeof mod.createColumnBuffer).toBe('function');
    expect(typeof mod.createColumnWriter).toBe('function');
    expect(typeof mod.createTableFromBatches).toBe('function');
    expect(typeof mod.S).toBe('object');
  });

  it('resolves package metadata export', async () => {
    const pkg = await import('@smoothbricks/arrow-builder/package.json');
    // nodenext module resolution wraps JSON imports in { default: ... }
    const meta = pkg.default;
    expect(meta.name).toBe('@smoothbricks/arrow-builder');
    expect(meta.exports).toBeDefined();
  });
});
