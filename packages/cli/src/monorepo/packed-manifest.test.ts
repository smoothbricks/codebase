import { describe, expect, it } from 'bun:test';
import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { listPublishablePackages, readPackageJson, workspaceDependencyFields } from '../lib/workspace.js';
import { git } from '../release/__tests__/helpers/fixture-repo.js';
import { withPackWorkspace } from '../release/__tests__/helpers/pack-workspace.js';
import { bunBinary, withPrivateNpmFixture } from '../release/__tests__/helpers/private-registry.js';
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

  it('refuses a tagged pin the registry does not hold, unless the run repairs pending releases first', async () => {
    await withPrivateNpmFixture(async (fixture) => {
      await withPackWorkspace(
        async (root) => {
          // Registry reads land on the loopback public stand-in, which holds nothing.
          await writeFile(join(root, '.npmrc'), `registry=${fixture.publicRegistry.registry}\n`);
          await git(root, ['add', '.npmrc']);
          await git(root, ['commit', '-qm', 'read the loopback registry']);
          // lib@1.0.0 is tagged at HEAD, so app's pin on it is fresh: what a publish that failed after tagging leaves.
          await git(root, ['tag', 'lib@1.0.0']);
          const source = listPublishablePackages(root).find((pkg) => pkg.projectName === 'app');
          if (!source) throw new Error('fixture app publisher is missing');
          const tarball = join(root, 'app.tgz');
          const packed = await Bun.$`${bunBinary()} pm pack --filename ${tarball} --ignore-scripts --quiet`
            .cwd(join(root, source.path))
            .quiet()
            .nothrow();
          expect(packed.exitCode, packed.stderr.toString()).toBe(0);
          const packedManifest = await readPackedPackageJson(root, tarball, source.name);
          expect(await validatePackedDependencyFreshness(root, source, packedManifest)).toEqual([
            'packages/app: packed dependencies.@pub.test/lib pins 1.0.0, which the registry does not hold; publish lib@1.0.0 first (release repair-pending) or release lib in the same run.',
          ]);
          expect(await validatePackedDependencyFreshness(root, source, packedManifest, [], true)).toEqual([]);
        },
        {
          packages: [
            { name: '@pub.test/lib', projectName: 'lib', path: 'packages/lib', version: '1.0.0', tags: ['npm:public'] },
            {
              name: '@pub.test/app',
              projectName: 'app',
              path: 'packages/app',
              version: '2.0.0',
              tags: ['npm:public'],
              dependencies: { '@pub.test/lib': 'workspace:*' },
            },
          ],
        },
      );
    });
  });

  it('refuses to publish a runtime pin on a dependency that has no stable release', async () => {
    await withPackWorkspace(
      async (root) => {
        const source = listPublishablePackages(root).find((pkg) => pkg.projectName === 'app');
        if (!source) throw new Error('fixture app publisher is missing');
        const tarball = join(root, 'app.tgz');
        const packed = await Bun.$`${bunBinary()} pm pack --filename ${tarball} --ignore-scripts --quiet`
          .cwd(join(root, source.path))
          .quiet()
          .nothrow();
        expect(packed.exitCode, packed.stderr.toString()).toBe(0);
        const packedManifest = await readPackedPackageJson(root, tarball, source.name);
        expect(packedManifest.dependencies?.['@pub.test/lib']).toBe('0.0.1-next.0');
        expect(validatePackedWorkspaceDependencies(root, source, packedManifest, { mode: 'install' })).toEqual([]);
        expect(validatePackedWorkspaceDependencies(root, source, packedManifest, { mode: 'publish' })).toEqual([
          'packages/app: packed dependencies.@pub.test/lib would ship 0.0.1-next.0, a workspace-internal -next version no registry holds: lib has no stable release. Release lib in the same run (a first release needs --bump patch).',
        ]);
      },
      {
        packages: [
          {
            name: '@pub.test/lib',
            projectName: 'lib',
            path: 'packages/lib',
            version: '0.0.1-next.0',
            tags: ['npm:public'],
          },
          {
            name: '@pub.test/app',
            projectName: 'app',
            path: 'packages/app',
            version: '2.0.0',
            tags: ['npm:public'],
            dependencies: { '@pub.test/lib': 'workspace:*' },
          },
        ],
      },
    );
  });
});
