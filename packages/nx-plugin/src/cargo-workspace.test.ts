import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import {
  attributeCargoWorkspacePackages,
  cargoPackageTestInputs,
  listCargoWorkspacePackages,
} from './cargo-workspace.js';

describe('Cargo workspace layouts', () => {
  it('keeps package-rooted workspace discovery and inputs project-relative', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-plugin-cargo-package-root-'));
    try {
      await write(root, 'Cargo.toml', '[workspace]\nmembers = ["crates/ferris-core"]\n');
      await write(root, 'Cargo.lock', 'version = 4\n');
      await write(root, 'crates/ferris-core/Cargo.toml', '[package]\nname = "ferris-core"\n');

      expect(listCargoWorkspacePackages(root)).toEqual([
        { name: 'ferris-core', dir: 'crates/ferris-core', testShards: 1 },
      ]);
      expect(
        await cargoPackageTestInputs({
          workspaceRoot: root,
          absoluteProjectRoot: root,
          memberDir: 'crates/ferris-core',
        }),
      ).toEqual([
        '{projectRoot}/Cargo.toml',
        '{projectRoot}/Cargo.lock',
        '{projectRoot}/crates/ferris-core/**/*.rs',
        '{projectRoot}/crates/ferris-core/Cargo.toml',
        '{projectRoot}/**/.cargo/config.toml',
        '{projectRoot}/scripts/*.sh',
        '!{projectRoot}/**/target/**',
      ]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('expands repo-root member globs and attributes every crate to its containing Nx project', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-plugin-cargo-repo-root-'));
    try {
      await write(
        root,
        'Cargo.toml',
        [
          '[workspace]',
          'members = ["packages/containium", "packages/*/crates/*", "vendor/*/crates/*"]',
          'exclude = ["packages/*/crates/excluded-*", "vendor/ignored"]',
          '',
          '[workspace.dependencies]',
          'runtime-core = { path = "packages/runtime/crates/runtime-core" }',
          '',
        ].join('\n'),
      );
      await write(root, 'Cargo.lock', 'version = 4\n');
      await write(root, 'packages/containium/Cargo.toml', '[package]\nname = "containium-cli"\n');
      await write(root, 'packages/runtime/crates/runtime-core/Cargo.toml', '[package]\nname = "runtime-core"\n');
      await write(
        root,
        'packages/host/crates/host-runtime/Cargo.toml',
        '[package]\nname = "host-runtime"\n\n[dependencies]\nruntime-core = { workspace = true }\n',
      );
      await write(root, 'packages/host/crates/excluded-fixture/Cargo.toml', '[package]\nname = "excluded-fixture"\n');
      await write(
        root,
        'vendor/ignored/crates/excluded-by-parent/Cargo.toml',
        '[package]\nname = "excluded-by-parent"\n',
      );

      const packages = listCargoWorkspacePackages(root);
      expect(packages).toEqual([
        { name: 'containium-cli', dir: 'packages/containium', testShards: 1 },
        { name: 'host-runtime', dir: 'packages/host/crates/host-runtime', testShards: 1 },
        { name: 'runtime-core', dir: 'packages/runtime/crates/runtime-core', testShards: 1 },
      ]);
      expect(
        attributeCargoWorkspacePackages(packages, [
          { name: '@axe.sc/codebase', root: '.' },
          { name: 'runtime', root: 'packages/runtime' },
          { name: 'host', root: 'packages/host' },
          { name: 'containium', root: 'packages/containium' },
        ]),
      ).toEqual([
        {
          name: 'containium-cli',
          dir: 'packages/containium',
          testShards: 1,
          projectName: 'containium',
          projectRoot: 'packages/containium',
        },
        {
          name: 'host-runtime',
          dir: 'packages/host/crates/host-runtime',
          testShards: 1,
          projectName: 'host',
          projectRoot: 'packages/host',
        },
        {
          name: 'runtime-core',
          dir: 'packages/runtime/crates/runtime-core',
          testShards: 1,
          projectName: 'runtime',
          projectRoot: 'packages/runtime',
        },
      ]);
      expect(
        await cargoPackageTestInputs({
          workspaceRoot: root,
          absoluteProjectRoot: root,
          memberDir: 'packages/host/crates/host-runtime',
          inputRoot: '{workspaceRoot}',
        }),
      ).toEqual([
        '{workspaceRoot}/Cargo.toml',
        '{workspaceRoot}/Cargo.lock',
        '{workspaceRoot}/packages/host/crates/host-runtime/**/*.rs',
        '{workspaceRoot}/packages/host/crates/host-runtime/Cargo.toml',
        '{workspaceRoot}/packages/runtime/crates/runtime-core/**/*.rs',
        '{workspaceRoot}/packages/runtime/crates/runtime-core/Cargo.toml',
        '{workspaceRoot}/**/.cargo/config.toml',
        '{workspaceRoot}/scripts/*.sh',
        '!{workspaceRoot}/**/target/**',
      ]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('routes path dependencies outside the workspace through the externalRustCrates named input', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-plugin-cargo-external-deps-'));
    try {
      await write(
        root,
        'Cargo.toml',
        [
          '[workspace]',
          'members = ["crates/app"]',
          '',
          '[workspace.dependencies]',
          'sibling-core = { path = "../sibling/crates/sibling-core" }',
          'pinned-core = { path = "/opt/pinned/crates/pinned-core" }',
          'local-core = { path = "crates/local-core" }',
          '',
        ].join('\n'),
      );
      await write(root, 'Cargo.lock', 'version = 4\n');
      await write(
        root,
        'crates/app/Cargo.toml',
        [
          '[package]',
          'name = "app"',
          '',
          '[dependencies]',
          'sibling-core = { workspace = true }',
          'pinned-core = { workspace = true }',
          'local-core = { workspace = true }',
          'escaping-core = { path = "../../../escaping/crates/escaping-core" }',
          '',
        ].join('\n'),
      );
      await write(root, 'crates/local-core/Cargo.toml', '[package]\nname = "local-core"\n');

      await write(root, 'nx.json', JSON.stringify({ namedInputs: { default: ['{projectRoot}/**/*'] } }));
      await expect(
        cargoPackageTestInputs({ workspaceRoot: root, absoluteProjectRoot: root, memberDir: 'crates/app' }),
      ).rejects.toThrow(
        'crates/app/Cargo.toml depends on crates outside the Nx workspace ' +
          '(escaping-core -> ../escaping/crates/escaping-core, pinned-core -> /opt/pinned/crates/pinned-core, ' +
          'sibling-core -> ../sibling/crates/sibling-core) that no fileset can hash; ' +
          `declare namedInputs.externalRustCrates in ${join(root, 'nx.json')}`,
      );

      await write(
        root,
        'nx.json',
        JSON.stringify({ namedInputs: { externalRustCrates: [{ runtime: 'echo hashed' }] } }),
      );
      const inputs = await cargoPackageTestInputs({
        workspaceRoot: root,
        absoluteProjectRoot: root,
        memberDir: 'crates/app',
        inputRoot: '{workspaceRoot}',
      });
      expect(inputs).toEqual([
        '{workspaceRoot}/Cargo.toml',
        '{workspaceRoot}/Cargo.lock',
        '{workspaceRoot}/crates/app/**/*.rs',
        '{workspaceRoot}/crates/app/Cargo.toml',
        '{workspaceRoot}/crates/local-core/**/*.rs',
        '{workspaceRoot}/crates/local-core/Cargo.toml',
        '{workspaceRoot}/**/.cargo/config.toml',
        '{workspaceRoot}/scripts/*.sh',
        '!{workspaceRoot}/**/target/**',
        'externalRustCrates',
      ]);
      expect(inputs.some((input) => input.includes('..') || input.includes('{workspaceRoot}//'))).toBe(false);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('reports member patterns it cannot expand instead of silently omitting crates', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-plugin-cargo-invalid-glob-'));
    try {
      await write(root, 'Cargo.toml', '[workspace]\nmembers = ["packages/**/crates/*"]\n');
      expect(() => listCargoWorkspacePackages(root)).toThrow(
        'Cargo workspace member uses an unsupported glob pattern: packages/**/crates/*',
      );

      await write(root, 'Cargo.toml', '[workspace]\nmembers = ["packages/*/crates/*"]\n');
      expect(() => listCargoWorkspacePackages(root)).toThrow(
        'Cargo workspace member glob matched no directories: packages/*/crates/*',
      );
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

async function write(root: string, path: string, contents: string): Promise<void> {
  const absolutePath = join(root, path);
  await mkdir(dirname(absolutePath), { recursive: true });
  await writeFile(absolutePath, contents);
}
