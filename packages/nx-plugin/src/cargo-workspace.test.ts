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
      // A path dependency that is not a workspace member still gets linked into
      // every test binary above it, so it must reach the inputs transitively.
      await write(
        root,
        'packages/runtime/crates/runtime-core/Cargo.toml',
        '[package]\nname = "runtime-core"\n\n[dependencies]\nruntime-base = { path = "../../base" }\n',
      );
      await write(root, 'packages/runtime/base/Cargo.toml', '[package]\nname = "runtime-base"\n');
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
          { name: '@fixture/codebase', root: '.' },
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
        '{workspaceRoot}/packages/runtime/base/**/*.rs',
        '{workspaceRoot}/packages/runtime/base/Cargo.toml',
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
      ).rejects.toThrow();

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

  it('hashes git and registry dependencies through Cargo.lock instead of demanding external source inputs', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-plugin-cargo-git-deps-'));
    try {
      await write(
        root,
        'Cargo.toml',
        [
          '[workspace]',
          'members = ["crates/app"]',
          '',
          '[workspace.dependencies]',
          'engine-core = { git = "https://git.example.net/org/engine.git", rev = "abc123" }',
          '',
        ].join('\n'),
      );
      await write(root, 'Cargo.lock', 'version = 4\n');
      await write(
        root,
        'crates/app/Cargo.toml',
        ['[package]', 'name = "app"', '', '[dependencies]', 'engine-core = { workspace = true }', ''].join('\n'),
      );
      await write(root, 'nx.json', JSON.stringify({ namedInputs: { default: ['{projectRoot}/**/*'] } }));

      // Cargo owns git resolution; the locked rev in Cargo.lock is the input.
      // No externalRustCrates named input is required, and nothing hashes a
      // workstation tree.
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
        '{workspaceRoot}/**/.cargo/config.toml',
        '{workspaceRoot}/scripts/*.sh',
        '!{workspaceRoot}/**/target/**',
      ]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('includes target-specific dependencies and preserves absolute path roots', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-plugin-cargo-target-deps-'));
    try {
      await write(root, 'Cargo.toml', '[workspace]\nmembers = ["crates/app"]\n');
      await write(root, 'Cargo.lock', 'version = 4\n');
      await write(
        root,
        'crates/app/Cargo.toml',
        '[package]\nname = "app"\n\n[target.\'cfg(unix)\'.dependencies]\n' +
          'external-core = { path = "/opt/pinned/external-core" }\n' +
          'target-core = { path = "../target-core" }\n',
      );
      await write(
        root,
        'crates/target-core/Cargo.toml',
        '[package]\nname = "target-core"\n\n[build-dependencies]\nbase = { path = "../base" }\n',
      );
      await write(root, 'crates/base/Cargo.toml', '[package]\nname = "base"\n');
      await write(root, 'nx.json', JSON.stringify({ namedInputs: {} }));
      const request = { workspaceRoot: root, absoluteProjectRoot: root, memberDir: 'crates/app' };
      await expect(cargoPackageTestInputs(request)).rejects.toThrow();
      await write(
        root,
        'nx.json',
        JSON.stringify({ namedInputs: { externalRustCrates: [{ runtime: 'echo external' }] } }),
      );
      expect(await cargoPackageTestInputs(request)).toEqual([
        '{projectRoot}/Cargo.toml',
        '{projectRoot}/Cargo.lock',
        '{projectRoot}/crates/app/**/*.rs',
        '{projectRoot}/crates/app/Cargo.toml',
        '{projectRoot}/crates/base/**/*.rs',
        '{projectRoot}/crates/base/Cargo.toml',
        '{projectRoot}/crates/target-core/**/*.rs',
        '{projectRoot}/crates/target-core/Cargo.toml',
        '{projectRoot}/**/.cargo/config.toml',
        '{projectRoot}/scripts/*.sh',
        '!{projectRoot}/**/target/**',
        'externalRustCrates',
      ]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('keeps absolute in-workspace dependencies in the ordinary source closure', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-plugin-cargo-absolute-local-'));
    try {
      await write(root, 'Cargo.toml', '[workspace]\nmembers=["crates/app"]\n');
      await write(root, 'crates/base/Cargo.toml', '[package]\nname="base"\n');
      await write(
        root,
        'crates/app/Cargo.toml',
        `[package]\nname="app"\n[dependencies]\nbase={path=${JSON.stringify(join(root, 'crates/base'))}}\n`,
      );
      const inputs = await cargoPackageTestInputs({
        workspaceRoot: root,
        absoluteProjectRoot: root,
        memberDir: 'crates/app',
      });
      expect(inputs).toContain('{projectRoot}/crates/base/**/*.rs');
      expect(inputs).not.toContain('externalRustCrates');
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
