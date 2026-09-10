import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, rename, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { parse as parseToml } from 'smol-toml';

import { hashVersionlessCrateManifests, stripCargoTomlVersion } from './manifest-hash.js';

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

describe('versionless crate manifest hashing', () => {
  it('ignores the version a crate declares for itself and nothing else', async () => {
    await withProject(async (project) => {
      const baseline = await hashVersionlessCrateManifests(project.root, project.workspaceRoot);

      await project.write('crates/core/Cargo.toml', CRATE_MANIFEST.replace('version = "1.2.3"', 'version = "2.0.0"'));
      expect(await hashVersionlessCrateManifests(project.root, project.workspaceRoot)).toBe(baseline);

      // Same release, one real change alongside it: the digest must move again.
      await project.write('crates/core/Cargo.toml', CRATE_MANIFEST.replace('1.0.200', '1.0.201'));
      expect(await hashVersionlessCrateManifests(project.root, project.workspaceRoot)).not.toBe(baseline);
    });
  });

  it('keeps every version that names a different crate', () => {
    expect(parseToml(stripCargoTomlVersion(CRATE_MANIFEST))).toEqual({
      package: { name: 'acme-core', edition: '2024' },
      dependencies: {
        serde: { version: '1.0.200', features: ['derive'] },
        // A sibling pinned by path AND version keeps that version: the digest
        // describes a dependency that really did change on release.
        'acme-sibling': { path: '../sibling', version: '1.2.3' },
      },
    });
    expect(parseToml(stripCargoTomlVersion(WORKSPACE_CRATE_MANIFEST))).toEqual({
      workspace: {
        members: ['crates/*'],
        package: { edition: '2024' },
        dependencies: { serde: { version: '1.0.200' } },
      },
    });
  });

  it('reports a moved crate manifest', async () => {
    await withProject(async (project) => {
      const baseline = await hashVersionlessCrateManifests(project.root, project.workspaceRoot);
      await project.move('crates/core/Cargo.toml', 'crates/renamed/Cargo.toml');

      expect(await hashVersionlessCrateManifests(project.root, project.workspaceRoot)).not.toBe(baseline);
    });
  });

  it('hashes a manifest it cannot parse instead of dropping it', async () => {
    await withProject(async (project) => {
      await project.write('crates/core/Cargo.toml', 'this is not toml [[[');
      const broken = await hashVersionlessCrateManifests(project.root, project.workspaceRoot);
      expect(await hashVersionlessCrateManifests(project.root, project.workspaceRoot)).toBe(broken);

      // An unreadable manifest still contributes its bytes, so a change inside
      // it invalidates. Dropping it would leave a stale cache hit instead.
      await project.write('crates/core/Cargo.toml', 'this is not toml either ]]]');
      expect(await hashVersionlessCrateManifests(project.root, project.workspaceRoot)).not.toBe(broken);
    });
  });

  it('hashes the same tree the same way twice', async () => {
    await withProject(async (project) => {
      expect(await hashVersionlessCrateManifests(project.root, project.workspaceRoot)).toMatch(/^[0-9a-f]{64}$/);
      expect(await hashVersionlessCrateManifests(project.root, project.workspaceRoot)).toBe(
        await hashVersionlessCrateManifests(project.root, project.workspaceRoot),
      );
    });
  });
});

interface ProjectFixture {
  workspaceRoot: string;
  root: string;
  write(path: string, contents: string): Promise<void>;
  move(from: string, to: string): Promise<void>;
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
    await write(join(root, 'package.json'), JSON.stringify({ name: '@acme/app', version: '1.2.3' }));
    await write(join(root, 'crates/core/Cargo.toml'), CRATE_MANIFEST);
    await write(join(root, 'src/index.ts'), 'export const answer = 42;\n');
    await body({
      workspaceRoot,
      root,
      write: async (path, contents) => write(join(root, path), contents),
      move: async (from, to) => {
        const target = join(workspaceRoot, root, to);
        await mkdir(dirname(target), { recursive: true });
        await rename(join(workspaceRoot, root, from), target);
      },
    });
  } finally {
    await rm(workspaceRoot, { recursive: true, force: true });
  }
}
