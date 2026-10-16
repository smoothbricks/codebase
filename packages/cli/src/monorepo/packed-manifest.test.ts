import { describe, expect, it } from 'bun:test';
import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import {
  listPublishablePackages,
  type PackageInfo,
  type PackageJson,
  readPackageJson,
  type WorkspaceDependencyField,
  workspaceDependencyFields,
} from '../lib/workspace.js';
import { withPackWorkspace } from '../release/__tests__/helpers/pack-workspace.js';
import { bunBinary } from '../release/__tests__/helpers/private-registry.js';
import {
  readPackedPackageJson,
  validatePackedDependencyFreshness,
  validatePackedWorkspaceDependencies,
} from './packed-manifest.js';

const HELPER = '@priv.test/internal';

async function withPackedHelper(
  field: WorkspaceDependencyField,
  check: (root: string, source: PackageInfo, packed: PackageJson) => Promise<void>,
): Promise<void> {
  await withPackWorkspace(async (root) => {
    const manifestPath = join(root, 'packages/beta/package.json');
    const manifest = readPackageJson(manifestPath);
    if (!manifest) throw new Error('fixture beta manifest is missing');
    manifest.json[field] = { [HELPER]: 'workspace:*' };
    await writeFile(manifestPath, `${JSON.stringify(manifest.json, null, 2)}\n`);
    const source = listPublishablePackages(root).find((pkg) => pkg.projectName === 'beta');
    if (!source) throw new Error('fixture beta publisher is missing');
    const tarball = join(root, 'beta.tgz');
    const packed = await Bun.$`${bunBinary()} pm pack --filename ${tarball} --ignore-scripts --quiet`
      .cwd(join(root, source.path))
      .quiet()
      .nothrow();
    expect(packed.exitCode, packed.stderr.toString()).toBe(0);
    await check(root, source, await readPackedPackageJson(root, tarball, source.name));
  });
}

describe('packed runtime dependency freshness', () => {
  it('converts private development workspace helpers without demanding their publication', async () => {
    await withPackedHelper('devDependencies', async (root, source, packed) => {
      expect(source.json.devDependencies?.[HELPER]).toBe('workspace:*');
      expect(packed.devDependencies?.[HELPER]).toBe('0.0.0');
      expect(validatePackedWorkspaceDependencies(root, source, packed, { mode: 'install' })).toEqual([]);
      expect(await validatePackedDependencyFreshness(root, source, packed)).toEqual([]);
    });
  });

  for (const field of workspaceDependencyFields) {
    if (field === 'devDependencies') continue;
    it(`still refuses an untagged helper in ${field}`, async () => {
      await withPackedHelper(field, async (root, source, packed) => {
        expect(await validatePackedDependencyFreshness(root, source, packed)).toEqual([
          `packages/beta: packed ${field}.${HELPER} pins 0.0.0, which no release tagged (internal@0.0.0); release internal in the same run.`,
        ]);
      });
    });
  }
});
