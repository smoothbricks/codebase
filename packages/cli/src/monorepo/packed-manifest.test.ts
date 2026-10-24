import { describe, expect, it } from 'bun:test';
import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { listPublishablePackages, readPackageJson, workspaceDependencyFields } from '../lib/workspace.js';
import { withPackWorkspace } from '../release/__tests__/helpers/pack-workspace.js';
import { bunBinary } from '../release/__tests__/helpers/private-registry.js';
import {
  readPackedPackageJson,
  validatePackedDependencyFreshness,
  validatePackedWorkspaceDependencies,
} from './packed-manifest.js';

const HELPER = '@priv.test/internal';

function untaggedRefusal(field: string): string {
  return `packages/beta: packed ${field}.${HELPER} pins 0.0.0, which no release tagged (internal@0.0.0); release internal in the same run.`;
}

describe('packed runtime dependency freshness', () => {
  // One pack covers every dependency field: each validator walks all of them in a single call.
  it('converts a private helper in each dependency field and refuses only untagged runtime pins', async () => {
    await withPackWorkspace(async (root) => {
      const manifestPath = join(root, 'packages/beta/package.json');
      const manifest = readPackageJson(manifestPath);
      if (!manifest) throw new Error('fixture beta manifest is missing');
      for (const field of workspaceDependencyFields) {
        manifest.json[field] = { [HELPER]: 'workspace:*' };
      }
      await writeFile(manifestPath, `${JSON.stringify(manifest.json, null, 2)}\n`);
      const source = listPublishablePackages(root).find((pkg) => pkg.projectName === 'beta');
      if (!source) throw new Error('fixture beta publisher is missing');
      const tarball = join(root, 'beta.tgz');
      const packed = await Bun.$`${bunBinary()} pm pack --filename ${tarball} --ignore-scripts --quiet`
        .cwd(join(root, source.path))
        .quiet()
        .nothrow();
      expect(packed.exitCode, packed.stderr.toString()).toBe(0);
      const packedManifest = await readPackedPackageJson(root, tarball, source.name);

      // devDependencies: converted to the workspace version and never demanded a publication.
      expect(source.json.devDependencies?.[HELPER]).toBe('workspace:*');
      expect(packedManifest.devDependencies?.[HELPER]).toBe('0.0.0');
      expect(validatePackedWorkspaceDependencies(root, source, packedManifest, { mode: 'install' })).toEqual([]);
      // Runtime fields: the untagged pin is refused in each of them, and nothing else is refused.
      expect(await validatePackedDependencyFreshness(root, source, packedManifest)).toEqual([
        untaggedRefusal('dependencies'),
        untaggedRefusal('optionalDependencies'),
        untaggedRefusal('peerDependencies'),
      ]);
    });
  });
});
