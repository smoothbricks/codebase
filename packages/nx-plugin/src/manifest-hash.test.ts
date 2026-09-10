import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, rename, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { parse as parseToml } from 'smol-toml';

import {
  hashVersionlessManifests,
  hashVersionlessWorkspaceManifests,
  stripCargoTomlVersion,
  stripLockfileWorkspaceVersions,
  stripPackageJsonVersion,
} from './manifest-hash.js';

const PACKAGE_MANIFEST = {
  name: '@acme/app',
  version: '1.2.3',
  exports: { '.': './dist/index.js' },
  dependencies: { '@acme/lib': 'workspace:*', biome: '^2.5.11' },
};

const CRATE_MANIFEST = `[package]
name = "acme-core"
version = "1.2.3"
edition = "2024"

[dependencies]
serde = { version = "1.0.200", features = ["derive"] }
acme-sibling = { path = "../sibling", version = "1.2.3" }
`;

const WORKSPACE_CRATE_MANIFEST = `[workspace]
members = ["crates/*"]

[workspace.package]
version = "0.0.1"
edition = "2024"

[workspace.dependencies]
serde = { version = "1.0.200" }
`;

/** A bun lockfile, trailing commas included: bun writes JSONC, not strict JSON. */
const LOCKFILE = `{
  "lockfileVersion": 1,
  "workspaces": {
    "": { "name": "@acme/codebase", "devDependencies": { "typescript": "^5.9.0", }, },
    "packages/app": { "name": "@acme/app", "version": "1.2.3", "dependencies": { "biome": "^2.5.11", }, },
  },
  "packages": {
    "typescript": ["typescript@5.9.2", "", {}, "sha512-abc"],
  },
}
`;

describe('versionless manifest hashing', () => {
  it('ignores a version bump and nothing else', async () => {
    await withProject(async (project) => {
      const baseline = await hashVersionlessManifests(project.root, project.workspaceRoot);

      await project.write('package.json', JSON.stringify({ ...PACKAGE_MANIFEST, version: '2.0.0' }));
      await project.write('crates/core/Cargo.toml', CRATE_MANIFEST.replace('version = "1.2.3"', 'version = "2.0.0"'));
      expect(await hashVersionlessManifests(project.root, project.workspaceRoot)).toBe(baseline);

      // Same release, one real change alongside it: the digest must move again.
      await project.write(
        'package.json',
        JSON.stringify({
          ...PACKAGE_MANIFEST,
          version: '2.0.0',
          dependencies: { ...PACKAGE_MANIFEST.dependencies, biome: '^2.6.0' },
        }),
      );
      expect(await hashVersionlessManifests(project.root, project.workspaceRoot)).not.toBe(baseline);
    });
  });

  it('keeps every crate version that names a different crate', () => {
    const stripped = parseToml(stripCargoTomlVersion(CRATE_MANIFEST));
    const workspace = parseToml(stripCargoTomlVersion(WORKSPACE_CRATE_MANIFEST));

    expect(stripped).toEqual({
      package: { name: 'acme-core', edition: '2024' },
      dependencies: {
        serde: { version: '1.0.200', features: ['derive'] },
        // A sibling pinned by path AND version keeps that version: the digest
        // describes a dependency that really did change on release.
        'acme-sibling': { path: '../sibling', version: '1.2.3' },
      },
    });
    expect(workspace).toEqual({
      workspace: {
        members: ['crates/*'],
        package: { edition: '2024' },
        dependencies: { serde: { version: '1.0.200' } },
      },
    });
  });

  it('reports a crate dependency bump that leaves the crate version alone', async () => {
    await withProject(async (project) => {
      const baseline = await hashVersionlessManifests(project.root, project.workspaceRoot);
      await project.write('crates/core/Cargo.toml', CRATE_MANIFEST.replace('1.0.200', '1.0.201'));

      expect(await hashVersionlessManifests(project.root, project.workspaceRoot)).not.toBe(baseline);
    });
  });

  it('reports a moved crate manifest', async () => {
    await withProject(async (project) => {
      const baseline = await hashVersionlessManifests(project.root, project.workspaceRoot);
      await project.move('crates/core/Cargo.toml', 'crates/renamed/Cargo.toml');

      expect(await hashVersionlessManifests(project.root, project.workspaceRoot)).not.toBe(baseline);
    });
  });

  it('hashes a Rust-only project that has no package manifest', async () => {
    await withProject(async (project) => {
      await project.remove('package.json');
      const rustOnly = await hashVersionlessManifests(project.root, project.workspaceRoot);
      await project.write('crates/core/Cargo.toml', CRATE_MANIFEST.replace('1.0.200', '1.0.201'));

      expect(rustOnly).toMatch(/^[0-9a-f]{64}$/);
      expect(await hashVersionlessManifests(project.root, project.workspaceRoot)).not.toBe(rustOnly);
    });
  });

  it('hashes a manifest it cannot parse instead of dropping it', async () => {
    await withProject(async (project) => {
      await project.write('package.json', '{ this is not json');
      const broken = await hashVersionlessManifests(project.root, project.workspaceRoot);
      expect(await hashVersionlessManifests(project.root, project.workspaceRoot)).toBe(broken);

      // An unreadable manifest still contributes its bytes, so a change inside
      // it invalidates. Dropping it would leave a stale cache hit instead.
      await project.write('package.json', '{ this is not json either');
      expect(await hashVersionlessManifests(project.root, project.workspaceRoot)).not.toBe(broken);
    });
  });

  it('ignores the order a tool wrote the package manifest in', () => {
    const reordered = JSON.stringify({
      dependencies: PACKAGE_MANIFEST.dependencies,
      exports: PACKAGE_MANIFEST.exports,
      name: PACKAGE_MANIFEST.name,
      version: '9.9.9',
    });

    expect(stripPackageJsonVersion(reordered)).toBe(stripPackageJsonVersion(JSON.stringify(PACKAGE_MANIFEST)));
    expect(stripPackageJsonVersion(JSON.stringify(PACKAGE_MANIFEST))).toContain('workspace:*');
  });

  it('ignores lockfile member versions and nothing else in the lockfile', async () => {
    await withProject(async (project) => {
      const baseline = await hashVersionlessWorkspaceManifests(project.workspaceRoot);

      await project.writeWorkspace('bun.lock', LOCKFILE.replace('"version": "1.2.3"', '"version": "2.0.0"'));
      expect(await hashVersionlessWorkspaceManifests(project.workspaceRoot)).toBe(baseline);

      await project.writeWorkspace('bun.lock', LOCKFILE.replace('typescript@5.9.2', 'typescript@5.9.3'));
      expect(await hashVersionlessWorkspaceManifests(project.workspaceRoot)).not.toBe(baseline);

      // The resolved dependencies are why the lockfile is an input at all.
      expect(stripLockfileWorkspaceVersions(LOCKFILE)).toContain('typescript@5.9.2');
      expect(stripLockfileWorkspaceVersions(LOCKFILE)).toContain('^2.5.11');
    });
  });

  it('hashes the same tree the same way twice', async () => {
    await withProject(async (project) => {
      expect(await hashVersionlessManifests(project.root, project.workspaceRoot)).toBe(
        await hashVersionlessManifests(project.root, project.workspaceRoot),
      );
      expect(await hashVersionlessWorkspaceManifests(project.workspaceRoot)).toBe(
        await hashVersionlessWorkspaceManifests(project.workspaceRoot),
      );
    });
  });
});

interface ProjectFixture {
  workspaceRoot: string;
  root: string;
  write(path: string, contents: string): Promise<void>;
  writeWorkspace(path: string, contents: string): Promise<void>;
  move(from: string, to: string): Promise<void>;
  remove(path: string): Promise<void>;
}

async function withProject(body: (project: ProjectFixture) => Promise<void>): Promise<void> {
  const workspaceRoot = await mkdtemp(join(tmpdir(), 'smoothbricks-manifest-hash-'));
  const root = 'packages/app';
  const write = async (path: string, contents: string): Promise<void> => {
    const absolute = join(workspaceRoot, path);
    await mkdir(dirname(absolute), { recursive: true });
    await writeFile(absolute, contents);
  };
  try {
    await write(join(root, 'package.json'), JSON.stringify(PACKAGE_MANIFEST));
    await write(join(root, 'crates/core/Cargo.toml'), CRATE_MANIFEST);
    await write(join(root, 'src/index.ts'), 'export const answer = 42;\n');
    await write('package.json', JSON.stringify({ name: '@acme/codebase', version: '0.0.0', private: true }));
    await write('bun.lock', LOCKFILE);
    await body({
      workspaceRoot,
      root,
      write: async (path, contents) => write(join(root, path), contents),
      writeWorkspace: write,
      move: async (from, to) => {
        const target = join(workspaceRoot, root, to);
        await mkdir(dirname(target), { recursive: true });
        await rename(join(workspaceRoot, root, from), target);
      },
      remove: async (path) => rm(join(workspaceRoot, root, path)),
    });
  } finally {
    await rm(workspaceRoot, { recursive: true, force: true });
  }
}
