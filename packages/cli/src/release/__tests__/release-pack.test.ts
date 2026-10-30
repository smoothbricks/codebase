/* biome-ignore-all lint/suspicious/noTemplateCurlyInString: Workflow expressions in packed fixtures are literal. */
import { describe, expect, it } from 'bun:test';
import { createHash } from 'node:crypto';
import { cp, mkdir, readdir, readFile, writeFile } from 'node:fs/promises';
import { isAbsolute, join } from 'node:path';
import { RELEASE_CONFIGURATION } from '@smoothbricks/nx-plugin/workspace-config-policy';
import {
  listPublicPackages,
  listPublishablePackages,
  listReleasePackages,
  type PackageInfo,
} from '../../lib/workspace.js';
import {
  assertPackedArtifact,
  packReleaseTarball,
  type ReleasePackManifest,
  type ReleasePackNx,
  releaseCheckGateRefusal,
  releasePack,
  verifyReleasePackManifest,
} from '../pack.js';
import { git, gitOutput } from './helpers/fixture-repo.js';
import { type PackWorkspacePackage, packedManifestJson, withPackWorkspace } from './helpers/pack-workspace.js';
import { FIXTURE_SCOPE, withPrivateNpmFixture } from './helpers/private-registry.js';

describe('release pack', () => {
  /**
   * Selection is the boundary that keeps private artifacts off npmjs. Release and
   * packing operate on both publishable classes; bootstrap and trust-publisher
   * talk to npmjs and must keep reading the public-only list. A private package
   * appearing in the public list would create a public npmjs package named after
   * it, and a package owned by another repository must not be released from here
   * at all. One workspace holds every shape the classification must tell apart,
   * so each selection is checked against all of them at once.
   */
  it('selects only owned, singly tagged, non-private packages and refuses closures it cannot pack', async () => {
    await withPackWorkspace(
      async (root) => {
        const names = (packages: readonly PackageInfo[]): string[] => packages.map((pkg) => pkg.name);
        const npmjsOnly = names(listPublicPackages(root));
        const publishable = names(listPublishablePackages(root));
        const releasable = names(listReleasePackages(root));

        // npmjs bootstrap and trust-publisher consume the public list: no private-tagged package may be on it.
        expect(npmjsOnly).toEqual(['@priv.test/public-face']);
        // Ownership is what makes a release ours to cut; the tag alone is not.
        expect(publishable).toEqual([
          '@priv.test/alpha',
          '@priv.test/beta',
          '@priv.test/public-face',
          '@priv.test/uses-internal',
          '@priv.test/vendored',
        ]);
        expect(releasable).toEqual([
          '@priv.test/alpha',
          '@priv.test/beta',
          '@priv.test/public-face',
          '@priv.test/uses-internal',
        ]);
        // A package with both tags, or private:true with or without a tag, must be absent
        // from every selection so it can never reach a registry.
        for (const selected of [npmjsOnly, publishable, releasable]) {
          expect(selected).not.toContain('@priv.test/internal');
          expect(selected).not.toContain('@priv.test/tagged-both');
          expect(selected).not.toContain('@priv.test/private-true');
        }

        const output = artifactsDir(root, 'refused');
        // A runtime edge on a package that can never be published.
        await expect(releasePack(root, { projects: 'uses-internal', output }, fixtureNx())).rejects.toThrow(
          /@priv\.test\/internal/,
        );
        // A public package depending on a private one.
        await expect(releasePack(root, { projects: 'public-face', output }, fixtureNx())).rejects.toThrow(
          /@priv\.test\/alpha/,
        );
        // A foreign-owned package, even when named directly.
        await expect(releasePack(root, { projects: 'vendored', output }, fixtureNx())).rejects.toThrow(/vendored/);
      },
      {
        publicFace: true,
        foreignPackage: true,
        ambiguousTagPackages: true,
        packages: [
          privatePackage('uses-internal', '0.7.0', { dependencies: { '@priv.test/internal': 'workspace:*' } }),
        ],
      },
    );
  });

  /**
   * `release pack` is the artifact half of private distribution: it must produce
   * installable tarballs and a verifiable manifest while touching no registry and
   * no Git ref. Both non-effects are load-bearing — a pack that quietly queried
   * registry status would need credentials wherever artifacts are built, and one
   * that moved a ref would turn artifact production into a release.
   */
  it('writes a verifiable artifact directory and refuses to mix, trust or rebind one', async () => {
    await withPrivateNpmFixture(async (fixture) => {
      await withPackWorkspace(async (root) => {
        // The pack must reach no registry. npm reads .npmrc and bun reads bunfig.toml, so both name the loopback
        // stand-ins (the private scope its own endpoint): a request to either is recorded below instead of
        // leaving the machine.
        await writeFile(
          join(root, '.npmrc'),
          `registry=${fixture.publicRegistry.registry}\n${FIXTURE_SCOPE}:registry=${fixture.privateRegistry.registry}\n`,
        );
        await writeFile(
          join(root, 'bunfig.toml'),
          `[install]\nregistry = ${JSON.stringify(fixture.publicRegistry.registry)}\n`,
        );

        const output = artifactsDir(root, 'closure');
        const headBefore = await gitOutput(root, ['rev-parse', 'HEAD']);
        const tagsBefore = await gitOutput(root, ['tag', '--list']);
        const statusBefore = await gitOutput(root, ['status', '--porcelain', '--untracked-files=no']);
        const runs: string[][] = [];

        await releasePack(root, { projects: 'alpha', output }, fixtureNx(runs));
        // One build of the whole closure, in the configuration that ships.
        expect(runs).toEqual([
          ['run-many', '-t', 'build', '--projects=alpha,beta', `--configuration=${RELEASE_CONFIGURATION}`],
        ]);

        const manifest = await readManifest(output);
        expect(manifest.schemaVersion).toBe(1);
        // The invocation named only alpha; beta is pulled in as its runtime edge.
        expect(manifest.packages.map((entry) => entry.name)).toEqual(['@priv.test/alpha', '@priv.test/beta']);
        expect(manifest.packages.map((entry) => entry.projectName)).toEqual(['alpha', 'beta']);
        expect(manifest.packages.map((entry) => entry.version)).toEqual(['0.1.0', '0.2.0']);
        expect(manifest.packages[0]?.runtimeDependencies).toEqual(['@priv.test/beta']);
        expect(manifest.packages[1]?.runtimeDependencies).toEqual([]);

        for (const entry of manifest.packages) {
          // Consumption resolves the tarball against the manifest's own
          // directory, so an absolute or escaping path is unusable.
          expect(isAbsolute(entry.tarball)).toBe(false);
          expect(entry.tarball.split('/')).not.toContain('..');
          const bytes = await readFile(join(output, entry.tarball));
          expect(createHash('sha256').update(bytes).digest('hex')).toBe(entry.sha256);
        }

        // A surviving `workspace:*` would be an uninstallable artifact.
        const packedAlpha = await packedManifestJson(join(output, manifest.packages[0]?.tarball ?? ''));
        expect(packedAlpha).toContain('"@priv.test/beta": "0.2.0"');
        expect(packedAlpha).not.toContain('workspace:');

        expect(fixture.privateRegistry.requests).toEqual([]);
        expect(fixture.publicRegistry.requests).toEqual([]);
        expect(await gitOutput(root, ['rev-parse', 'HEAD'])).toBe(headBefore);
        expect(await gitOutput(root, ['tag', '--list'])).toBe(tagsBefore);
        expect(await gitOutput(root, ['status', '--porcelain', '--untracked-files=no'])).toBe(statusBefore);

        // A nonempty output directory is refused instead of mixing releases.
        const stale = artifactsDir(root, 'stale');
        await mkdir(stale, { recursive: true });
        await writeFile(join(stale, 'stale.tgz'), 'previous release');

        await expect(releasePack(root, { projects: 'alpha', output: stale }, fixtureNx())).rejects.toThrow();

        expect(await readdir(stale)).toEqual(['stale.tgz']);

        // Each refusal below damages its own copy of one verified artifact directory.
        const pristine = artifactsDir(root, 'beta');
        await releasePack(root, { projects: 'beta', output: pristine }, fixtureNx());

        const tampered = artifactsDir(root, 'tampered');
        await cp(pristine, tampered, { recursive: true });
        const verified = await verifyReleasePackManifest(tampered);
        expect(verified.packages.map((entry) => entry.name)).toEqual(['@priv.test/beta']);

        // The digest is the only thing standing between a consumer and a swapped
        // tarball, so verification must actually re-hash the bytes on disk.
        const tarball = join(tampered, verified.packages[0]?.tarball ?? '');
        const tarballBytes = await readFile(tarball);
        tarballBytes[tarballBytes.length - 1] ^= 0xff;
        await writeFile(tarball, tarballBytes);

        await expect(verifyReleasePackManifest(tampered)).rejects.toThrow(/beta/);

        const unbound = artifactsDir(root, 'unbound');
        await cp(pristine, unbound, { recursive: true });
        // `release pack` writes into an empty directory, so an extra tarball is a
        // mixed or tampered release, not an artifact this manifest describes.
        await writeFile(join(unbound, 'priv.test-beta-9.9.9.tgz'), 'unbound artifact');

        await expect(verifyReleasePackManifest(unbound)).rejects.toThrow(/priv\.test-beta-9\.9\.9\.tgz/);

        const rebound = artifactsDir(root, 'rebound');
        await cp(pristine, rebound, { recursive: true });
        const first = (await readManifest(rebound)).packages[0];
        if (!first) {
          throw new Error('pack produced no manifest entries');
        }

        // The manifest is the name/version map a consumer pins against: two
        // entries for one name make the pinned version ambiguous.
        await writeFile(
          join(rebound, 'manifest.json'),
          `${JSON.stringify({ schemaVersion: 1, packages: [first, { ...first, version: '9.9.9' }] }, null, 2)}\n`,
        );
        await expect(verifyReleasePackManifest(rebound)).rejects.toThrow(/appears twice/);

        // A path is validated before it is opened, so no read can escape the
        // artifact directory.
        await writeFile(
          join(rebound, 'manifest.json'),
          `${JSON.stringify({ schemaVersion: 1, packages: [{ ...first, tarball: `../${first.tarball}` }] }, null, 2)}\n`,
        );
        await expect(verifyReleasePackManifest(rebound)).rejects.toThrow(/bare file name/);
      });
    });
  });

  /**
   * One packer makes every tarball `release pack` and `release publish` ship: workspace
   * pins become exact versions, no lifecycle script runs, every declared asset must be
   * inside, and the workspace's own bytes are back as they were afterwards.
   */
  it('packs tarballs with exact pins, declared assets and a resolved registry, and restores the workspace', async () => {
    const wasmExports = { '.': './dist/index.js', './reducer.wasm': './dist/reducer.wasm' };
    await withPackWorkspace(
      async (root) => {
        // The private publish path carries the destination inside the tarball: the
        // resolved literal registry goes into publishConfig for the pack and the
        // workspace bytes come back afterwards. An unresolved placeholder shipping in
        // a published package.json would send every consumer's install to a
        // nonexistent host, so it must fail before bytes leave.
        const registry = 'https://forge.example.test/api/packages/priv-owner/npm/';
        const betaManifestPath = join(root, 'packages/beta/package.json');
        const betaManifestBefore = await readFile(betaManifestPath, 'utf8');

        await withPackedTarball(root, 'beta', { publishConfigRegistry: registry }, async ({ tarball }) => {
          const manifest = await packedManifestJson(tarball);
          expect(manifest).toContain(`"registry": "${registry}"`);
          expect(manifest).not.toContain('${');
        });
        expect(await readFile(betaManifestPath, 'utf8')).toBe(betaManifestBefore);

        await withPackedTarball(
          root,
          'beta',
          { publishConfigRegistry: '${PRIV_NPM_REGISTRY}' },
          async ({ tarball, pkg }) => {
            await expect(assertPackedArtifact(root, tarball, pkg)).rejects.toThrow(/environment placeholder/);
          },
        );

        // A missing exported Wasm asset is refused and leaves no partial artifact behind.
        const wasmMissingOutput = artifactsDir(root, 'wasm-missing');
        await expect(
          releasePack(root, { projects: 'wasm-missing', output: wasmMissingOutput }, fixtureNx()),
        ).rejects.toThrow(/reducer\.wasm/);
        expect(await readdir(wasmMissingOutput)).toEqual([]);

        // A declared Wasm export that is present in the files allowlist ships.
        await withPackedTarball(root, 'wasm-shipped', {}, async ({ tarball }) => {
          const extraction = await Bun.$`tar -xzOf ${tarball} package/dist/reducer.wasm`.quiet();
          expect(extraction.stdout.toString()).toBe('wasm-bytes');
        });

        // A declared types entry missing from the packed tarball is refused.
        await withPackedTarball(root, 'types-missing', {}, async ({ tarball, pkg }) => {
          await expect(assertPackedArtifact(root, tarball, pkg)).rejects.toThrow(/index\.d\.ts/);
        });

        // Packing normalizes workspace protocol dependencies to exact versions and runs no lifecycle script.
        await withPackedTarball(root, 'scripted', {}, async ({ tarball }) => {
          const manifest = await packedManifestJson(tarball);
          expect(manifest).toContain('"@priv.test/beta": "0.2.0"');
          expect(manifest).not.toContain('workspace:');
          // The script would create this file; packing must not execute it.
          await expect(readFile(join(root, 'packages/scripted/prepack-ran'), 'utf8')).rejects.toThrow();
        });

        // next-dep is an unpublished prerelease whose last stable release is tagged 0.2.0.
        await git(root, ['tag', 'next-dep@0.2.0']);
        const lockPath = join(root, 'bun.lock');
        const lockBefore = await readFile(lockPath);
        const consumerManifestPath = join(root, 'packages/next-consumer/package.json');
        const consumerManifestBefore = await readFile(consumerManifestPath);

        await withPackedTarball(root, 'next-consumer', {}, async ({ tarball }) => {
          // publish-mode sync rewrote the -next lock entry to the stable tag
          // mid-flight; the packed dependency must embed the stable version...
          expect(await packedManifestJson(tarball)).toContain('"@priv.test/next-dep": "0.2.0"');
          // ...and both workspace files must be back to their original bytes
          // after packing, including the manifest the export prune rewrote.
          expect(await readFile(lockPath)).toEqual(lockBefore);
          expect(await readFile(consumerManifestPath)).toEqual(consumerManifestBefore);
        });
      },
      {
        packages: [
          privatePackage('wasm-missing', '0.8.0', { exports: wasmExports }),
          privatePackage('wasm-shipped', '0.9.0', {
            exports: wasmExports,
            files: { 'dist/reducer.wasm': 'wasm-bytes' },
          }),
          privatePackage('types-missing', '1.0.0', { types: 'dist/index.d.ts' }),
          privatePackage('scripted', '1.1.0', {
            dependencies: { '@priv.test/beta': 'workspace:*' },
            scripts: { prepack: 'touch prepack-ran' },
          }),
          privatePackage('next-consumer', '1.2.0', {
            dependencies: { '@priv.test/next-dep': 'workspace:*' },
            exports: { '.': { development: './src/index.ts', default: './dist/index.js' } },
          }),
          privatePackage('next-dep', '0.2.1-next.0'),
        ],
      },
    );
  });
});

describe('release-check gate', () => {
  it('runs only a dependency-free gate cached on a runtime digest of the packed bytes', () => {
    const digest = { runtime: 'bun scripts/verify-packaged-bin-bits.ts --digest' };
    const refusals = [
      { cache: true, inputs: [digest], dependsOn: ['build'] },
      { cache: false },
      {},
      { cache: true, inputs: ['{projectRoot}/dist/**/*'] },
      { cache: true, inputs: [digest] },
    ].map((gate) => releaseCheckGateRefusal('pkg', gate));
    expect(refusals.map((refusal) => refusal !== null)).toEqual([true, true, true, true, false]);
    expect(refusals[0]).toContain('empty dependsOn');
    for (const refusal of refusals.slice(1, 4)) expect(refusal).toContain('runtime input');
  });
});

/**
 * The Nx a pack sees here: a graph in which no fixture project declares a
 * release-check gate, and a run-many that `runs` records instead of running.
 * The fixture writes every package's built files itself.
 */
function fixtureNx(runs: string[][] = []): ReleasePackNx {
  return {
    loadProjects: async () => ({}),
    runNx: async (args) => {
      runs.push(args);
    },
  };
}

/** The package as classification produces it — the same value the commands pack. */
function packagedProject(root: string, projectName: string): PackageInfo {
  const pkg = listPublishablePackages(root).find((candidate) => candidate.projectName === projectName);
  if (!pkg) {
    throw new Error(`fixture workspace has no publishable project ${projectName}`);
  }
  return pkg;
}

/**
 * Packs the named project the way both release commands do, hands the tarball to
 * `body`, and removes it again whether `body` passes or throws.
 */
async function withPackedTarball(
  root: string,
  projectName: string,
  options: { publishConfigRegistry?: string },
  body: (packed: { tarball: string; pkg: PackageInfo }) => Promise<void>,
): Promise<void> {
  const pkg = packagedProject(root, projectName);
  const packed = await packReleaseTarball(root, pkg, options, fixtureNx());
  try {
    await body({ tarball: packed.tarball, pkg });
  } finally {
    await packed.cleanup();
  }
}

/** A private-tagged package of the fixture's repository, built like the fixture's own packages. */
function privatePackage(
  projectName: string,
  version: string,
  rest: Omit<PackWorkspacePackage, 'name' | 'projectName' | 'path' | 'version' | 'tags'> = {},
): PackWorkspacePackage {
  return {
    name: `@priv.test/${projectName}`,
    projectName,
    path: `packages/${projectName}`,
    version,
    tags: ['npm:private'],
    ...rest,
  };
}

/** The fixture's .gitignore already excludes `artifacts`, so no scenario's output shows as a working-tree change. */
function artifactsDir(root: string, scenario: string): string {
  return join(root, 'artifacts', scenario);
}

/**
 * Validating read: the manifest is a consumption contract, so a missing or
 * mistyped field must fail here with the offending payload named, not surface
 * as an `undefined` in an assertion far from the cause.
 */
async function readManifest(output: string): Promise<ReleasePackManifest> {
  const parsed: unknown = JSON.parse(await readFile(join(output, 'manifest.json'), 'utf8'));
  if (!isReleasePackManifest(parsed)) {
    throw new Error(`pack manifest is not a ReleasePackManifest: ${JSON.stringify(parsed)}`);
  }
  return parsed;
}

function isReleasePackManifest(value: unknown): value is ReleasePackManifest {
  if (!value || typeof value !== 'object' || !('schemaVersion' in value) || !('packages' in value)) {
    return false;
  }
  return value.schemaVersion === 1 && Array.isArray(value.packages) && value.packages.every(isPackEntry);
}

function isPackEntry(value: unknown): boolean {
  if (!value || typeof value !== 'object') {
    return false;
  }
  const stringFields = ['projectName', 'name', 'version', 'tarball', 'sha256'];
  return (
    stringFields.every((field) => field in value && typeof Reflect.get(value, field) === 'string') &&
    'runtimeDependencies' in value &&
    Array.isArray(value.runtimeDependencies) &&
    value.runtimeDependencies.every((dependency: unknown) => typeof dependency === 'string')
  );
}
