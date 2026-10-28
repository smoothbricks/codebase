import { describe, expect, it } from 'bun:test';
import { mbxManagesTargets } from './cargo-retention-hook.js';

describe('Cargo retention beside mbx', () => {
  // specs/cowshed/16_build_volumes.md, "Migration" step 4: an mbx project's managed targets are mbx's to collect.
  it("is mbx's wherever the canonical mbx environment names the store and managed targets", () => {
    for (const [environment, managed] of [
      [{}, false],
      [{ MBX_CACHE_DIR: '/bc/mbx' }, false],
      [{ MBX_TARGET_VIEWS: '1' }, false],
      [{ MBX_CACHE_DIR: '', MBX_TARGET_VIEWS: '1' }, false],
      [{ MBX_CACHE_DIR: '/bc/mbx', MBX_TARGET_VIEWS: '0' }, false],
      [{ MBX_CACHE_DIR: '/bc/mbx', MBX_TARGET_VIEWS: '1' }, true],
    ] as const) {
      expect(mbxManagesTargets(environment)).toBe(managed);
    }
  });
});
