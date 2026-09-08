import { describe, expect, it } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdir, readdir, readFile, writeFile } from 'node:fs/promises';
import { isAbsolute, join } from 'node:path';
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
  releasePack,
  verifyReleasePackManifest,
} from '../pack.js';
import { gitOutput } from './helpers/fixture-repo.js';
import { packedManifestJson, withPackWorkspace } from './helpers/pack-workspace.js';
import { withPrivateNpmFixture } from './helpers/private-registry.js';

/**
 * Selection is the boundary that keeps private artifacts off npmjs. Release and
 * packing operate on both publishable classes; bootstrap and trust-publisher
 * talk to npmjs and must keep reading the public-only list. A private package
 * appearing in the public list would create a public npmjs package named after
 * it, and a package owned by another repository must not be released from here
 * at all.
 */
describe('publishable closure classification', () => {
  it('separates the npmjs-only public list from the publishable release list', async () => {
    await withPackWorkspace(
      async (root) => {
        expect(listPublicPackages(root).map((pkg) => pkg.name)).toEqual(['@priv.test/public-face']);
        expect(listPublishablePackages(root).map((pkg) => pkg.name)).toEqual([
          '@priv.test/alpha',
          '@priv.test/beta',
          '@priv.test/public-face',
        ]);
        expect(listReleasePackages(root).map((pkg) => pkg.name)).toEqual([
          '@priv.test/alpha',
          '@priv.test/beta',
          '@priv.test/public-face',
        ]);
      },
      { publicFace: true },
    );
  });

  it('never selects a private:true package for publication', async () => {
    await withPackWorkspace(async (root) => {
      for (const selected of [listPublishablePackages(root), listReleasePackages(root), listPublicPackages(root)]) {
        expect(selected.map((pkg) => pkg.name)).not.toContain('@priv.test/internal');
      }
      expect(listPublishablePackages(root).map((pkg) => pkg.name)).toEqual(['@priv.test/alpha', '@priv.test/beta']);
      expect(listPublicPackages(root)).toEqual([]);
    });
  });

  it('excludes a publishable package owned by another repository from the release list', async () => {
    await withPackWorkspace(
      async (root) => {
        // Ownership is what makes a release ours to cut; the tag alone is not.
        expect(listPublishablePackages(root).map((pkg) => pkg.name)).toContain('@priv.test/vendored');
        expect(listReleasePackages(root).map((pkg) => pkg.name)).toEqual(['@priv.test/alpha', '@priv.test/beta']);
      },
      { foreignPackage: true },
    );
  });

  it('never selects an ambiguously tagged package for any publishable list', async () => {
    await withPackWorkspace(
      async (root) => {
        // The public list is what npmjs bootstrap and trust-publisher consume;
        // a package with both tags, or private:true plus a tag, must be absent
        // from every publishable selection so it can never reach a registry.
        for (const selected of [listPublicPackages(root), listPublishablePackages(root), listReleasePackages(root)]) {
          expect(selected.map((pkg) => pkg.name)).not.toContain('@priv.test/tagged-both');
          expect(selected.map((pkg) => pkg.name)).not.toContain('@priv.test/private-true');
        }
      },
      { ambiguousTagPackages: true },
    );
  });
});

/**
 * `release pack` is the artifact half of private distribution: it must produce
 * installable tarballs and a verifiable manifest while touching no registry and
 * no Git ref. Both non-effects are load-bearing — a pack that quietly queried
 * registry status would need credentials wherever artifacts are built, and one
 * that moved a ref would turn artifact production into a release.
 */
describe('release pack artifacts', () => {
  it('packs the runtime closure into a verifiable manifest without contacting a registry or moving a ref', async () => {
    await withPrivateNpmFixture(async (fixture) => {
      await withPackWorkspace(async (root) => {
        const output = join(root, 'artifacts');
        const headBefore = await gitOutput(root, ['rev-parse', 'HEAD']);
        const tagsBefore = await gitOutput(root, ['tag', '--list']);
        const statusBefore = await gitOutput(root, ['status', '--porcelain', '--untracked-files=no']);

        await releasePack(root, { projects: 'alpha', output });

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
      });
    });
  });

  it('refuses a nonempty output directory instead of mixing releases', async () => {
    await withPackWorkspace(async (root) => {
      const output = join(root, 'artifacts');
      await mkdir(output, { recursive: true });
      await writeFile(join(output, 'stale.tgz'), 'previous release');

      await expect(releasePack(root, { projects: 'alpha', output })).rejects.toThrow();

      expect(await readdir(output)).toEqual(['stale.tgz']);
    });
  });

  it('refuses a runtime edge on a package that can never be published', async () => {
    await withPackWorkspace(
      async (root) => {
        await expect(releasePack(root, { projects: 'alpha', output: join(root, 'artifacts') })).rejects.toThrow(
          /@priv\.test\/internal/,
        );
      },
      { alphaDependencies: { '@priv.test/internal': 'workspace:*' } },
    );
  });

  it('refuses a public package depending on a private one', async () => {
    await withPackWorkspace(
      async (root) => {
        await expect(releasePack(root, { projects: 'public-face', output: join(root, 'artifacts') })).rejects.toThrow(
          /@priv\.test\/alpha/,
        );
      },
      { publicFace: true },
    );
  });

  it('refuses to pack a foreign-owned package even when named directly', async () => {
    await withPackWorkspace(
      async (root) => {
        await expect(releasePack(root, { projects: 'vendored', output: join(root, 'artifacts') })).rejects.toThrow(
          /vendored/,
        );
      },
      { foreignPackage: true },
    );
  });

  it('refuses a missing exported Wasm asset and leaves no partial artifact behind', async () => {
    await withPackWorkspace(
      async (root) => {
        const output = join(root, 'artifacts');
        await expect(releasePack(root, { projects: 'alpha', output })).rejects.toThrow(/reducer\.wasm/);
        expect(await readdir(output)).toEqual([]);
      },
      {
        alphaExports: { '.': './dist/index.js', './reducer.wasm': './dist/reducer.wasm' },
      },
    );
  });

  it('ships a declared Wasm export that is present in the files allowlist', async () => {
    await withPackWorkspace(
      async (root) => {
        const packed = await packReleaseTarball(root, packagedProject(root, 'alpha'));
        try {
          const extraction = await Bun.$`tar -xzOf ${packed.tarball} package/dist/reducer.wasm`.quiet();
          expect(extraction.stdout.toString()).toBe('wasm-bytes');
        } finally {
          await packed.cleanup();
        }
      },
      {
        alphaExports: { '.': './dist/index.js', './reducer.wasm': './dist/reducer.wasm' },
        alphaFiles: { 'dist/reducer.wasm': 'wasm-bytes' },
      },
    );
  });

  it('refuses when the declared types entry is missing from the packed tarball', async () => {
    await withPackWorkspace(
      async (root) => {
        const pkg = packagedProject(root, 'alpha');
        const packed = await packReleaseTarball(root, pkg);
        try {
          await expect(assertPackedArtifact(root, packed.tarball, pkg)).rejects.toThrow(/index\.d\.ts/);
        } finally {
          await packed.cleanup();
        }
      },
      { alphaTypes: 'dist/index.d.ts' },
    );
  });

  it('rejects a tampered artifact when the manifest is verified', async () => {
    await withPackWorkspace(async (root) => {
      const output = join(root, 'artifacts');
      await releasePack(root, { projects: 'beta', output });

      const manifest = await verifyReleasePackManifest(output);
      expect(manifest.packages.map((entry) => entry.name)).toEqual(['@priv.test/beta']);

      // The digest is the only thing standing between a consumer and a swapped
      // tarball, so verification must actually re-hash the bytes on disk.
      const tarball = join(output, manifest.packages[0]?.tarball ?? '');
      const bytes = await readFile(tarball);
      bytes[bytes.length - 1] ^= 0xff;
      await writeFile(tarball, bytes);

      await expect(verifyReleasePackManifest(output)).rejects.toThrow(/beta/);
    });
  });

  it('refuses an artifact directory holding a tarball the manifest does not bind', async () => {
    await withPackWorkspace(async (root) => {
      const output = join(root, 'artifacts');
      await releasePack(root, { projects: 'beta', output });

      // `release pack` writes into an empty directory, so an extra tarball is a
      // mixed or tampered release, not an artifact this manifest describes.
      await writeFile(join(output, 'priv.test-beta-9.9.9.tgz'), 'unbound artifact');

      await expect(verifyReleasePackManifest(output)).rejects.toThrow(/priv\.test-beta-9\.9\.9\.tgz/);
    });
  });

  it('refuses a manifest that binds one package name twice or escapes the directory', async () => {
    await withPackWorkspace(async (root) => {
      const output = join(root, 'artifacts');
      await releasePack(root, { projects: 'beta', output });
      const manifest = await readManifest(output);
      const entry = manifest.packages[0];
      if (!entry) {
        throw new Error('pack produced no manifest entries');
      }

      // The manifest is the name/version map a consumer pins against: two
      // entries for one name make the pinned version ambiguous.
      await writeFile(
        join(output, 'manifest.json'),
        `${JSON.stringify({ schemaVersion: 1, packages: [entry, { ...entry, version: '9.9.9' }] }, null, 2)}\n`,
      );
      await expect(verifyReleasePackManifest(output)).rejects.toThrow(/appears twice/);

      // A path is validated before it is opened, so no read can escape the
      // artifact directory.
      await writeFile(
        join(output, 'manifest.json'),
        `${JSON.stringify({ schemaVersion: 1, packages: [{ ...entry, tarball: `../${entry.tarball}` }] }, null, 2)}\n`,
      );
      await expect(verifyReleasePackManifest(output)).rejects.toThrow(/bare file name/);
    });
  });
});

/**
 * The private publish path carries the destination inside the tarball: the
 * resolved literal registry goes into publishConfig for the pack and the
 * workspace bytes come back afterwards. An unresolved placeholder shipping in
 * a published package.json would send every consumer's install to a
 * nonexistent host, so it must fail before bytes leave.
 */
describe('private publish manifest transform', () => {
  const registry = 'https://forge.example.test/api/packages/priv-owner/npm/';

  it('packs the resolved literal registry and restores the workspace manifest', async () => {
    await withPackWorkspace(async (root) => {
      const manifestPath = join(root, 'packages/beta/package.json');
      const before = await readFile(manifestPath, 'utf8');

      const packed = await packReleaseTarball(root, packagedProject(root, 'beta'), {
        publishConfigRegistry: registry,
      });
      try {
        const manifest = await packedManifestJson(packed.tarball);
        expect(manifest).toContain(`"registry": "${registry}"`);
        expect(manifest).not.toContain('${');
      } finally {
        await packed.cleanup();
      }

      expect(await readFile(manifestPath, 'utf8')).toBe(before);
    });
  });

  it('refuses a packed artifact whose registry is still an environment placeholder', async () => {
    await withPackWorkspace(async (root) => {
      const pkg = packagedProject(root, 'beta');
      const packed = await packReleaseTarball(root, pkg, { publishConfigRegistry: '${PRIV_NPM_REGISTRY}' });
      try {
        await expect(assertPackedArtifact(root, packed.tarball, pkg)).rejects.toThrow(/environment placeholder/);
      } finally {
        await packed.cleanup();
      }
    });
  });
});

describe('release pack tarball production', () => {
  it('normalizes workspace protocol dependencies to exact versions', async () => {
    await withPackWorkspace(async (root) => {
      const packed = await packReleaseTarball(root, packagedProject(root, 'alpha'));
      try {
        const manifest = await packedManifestJson(packed.tarball);
        expect(manifest).toContain('"@priv.test/beta": "0.2.0"');
        expect(manifest).not.toContain('workspace:');
      } finally {
        await packed.cleanup();
      }
    });
  });

  it('does not run package lifecycle scripts while packing', async () => {
    await withPackWorkspace(
      async (root) => {
        const packed = await packReleaseTarball(root, packagedProject(root, 'alpha'));
        try {
          // The script would create this file; packing must not execute it.
          await expect(readFile(join(root, 'packages/alpha/prepack-ran'), 'utf8')).rejects.toThrow();
        } finally {
          await packed.cleanup();
        }
      },
      { alphaScripts: { prepack: 'touch prepack-ran' } },
    );
  });

  it('restores bun.lock and package.json byte-exactly around publish-mode packing', async () => {
    await withPackWorkspace(
      async (root) => {
        const lockPath = join(root, 'bun.lock');
        const lockBefore = await readFile(lockPath);
        const manifestPath = join(root, 'packages/alpha/package.json');
        const manifestBefore = await readFile(manifestPath);

        const packed = await packReleaseTarball(root, packagedProject(root, 'alpha'));
        try {
          // publish-mode sync rewrote the -next lock entry to the stable tag
          // mid-flight; the packed dependency must embed the stable version...
          expect(await packedManifestJson(packed.tarball)).toContain('"@priv.test/beta": "0.2.0"');
          // ...and both workspace files must be back to their original bytes
          // after packing, including the manifest the export prune rewrote.
          expect(await readFile(lockPath)).toEqual(lockBefore);
          expect(await readFile(manifestPath)).toEqual(manifestBefore);
        } finally {
          await packed.cleanup();
        }
      },
      {
        betaVersion: '0.2.1-next.0',
        stableTags: ['beta@0.2.0'],
        alphaExports: { '.': { development: './src/index.ts', default: './dist/index.js' } },
      },
    );
  });
});

/** The package as classification produces it — the same value the commands pack. */
function packagedProject(root: string, projectName: string): PackageInfo {
  const pkg = listPublishablePackages(root).find((candidate) => candidate.projectName === projectName);
  if (!pkg) {
    throw new Error(`fixture workspace has no publishable project ${projectName}`);
  }
  return pkg;
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
