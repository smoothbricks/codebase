import { describe, expect, it } from 'bun:test';
import { withPackWorkspace } from '../release/__tests__/helpers/pack-workspace.js';
import { validatePackedPublishablePackageManifest } from './packed-package.js';

describe('packed package validation', () => {
  // Bun 1.4.4 writes a relative `bun pm pack --filename` into the package directory it packs, where 1.4.3 wrote it
  // into the workspace root the validation read it from: Publish run 41187136961 failed with ENOENT on that path.
  it('judges the tarball bun pm pack wrote for the selected package', async () => {
    await withPackWorkspace(async (root) => {
      expect(await validatePackedPublishablePackageManifest(root, ['alpha', 'beta'])).toBe(0);
    });
  });
});
