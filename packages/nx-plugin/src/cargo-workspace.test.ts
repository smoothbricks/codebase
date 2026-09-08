import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { Glob } from 'bun';

import {
  attributeCargoWorkspacePackages,
  cargoPackageTestInputs,
  listCargoWorkspacePackages,
} from './cargo-workspace.js';

describe('Cargo workspace layouts', () => {
  it('discovers root, globbed and implicit members, excludes packages and assigns the deepest owner', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-cargo-members-'));
    try {
      await write(
        root,
        'Cargo.toml',
        '[package]\nname="root-app"\n[workspace]\nmembers=[".","packages/*/crates/*"]\nexclude=["packages/*/crates/ignored-*", "vendor/ignored"]\n',
      );
      await write(
        root,
        'packages/service/crates/api/Cargo.toml',
        '[package]\nname="api"\n[dependencies]\nbase={path="../../../../vendor/base"}\nignored={path="../../../../vendor/ignored"}\n',
      );
      await write(root, 'packages/service/crates/ignored-fixture/Cargo.toml', '[package]\nname="ignored-fixture"\n');
      await write(
        root,
        'vendor/base/Cargo.toml',
        '[package]\nname="base"\n[dev-dependencies]\napi={path="../../packages/service/crates/api"}\n',
      );
      await write(root, 'vendor/ignored/Cargo.toml', '[package]\nname="ignored"\n');
      const packages = listCargoWorkspacePackages(root);
      expect(packages).toEqual([
        { name: 'api', dir: 'packages/service/crates/api', testShards: 1 },
        { name: 'base', dir: 'vendor/base', testShards: 1 },
        { name: 'root-app', dir: '.', testShards: 1 },
      ]);
      expect(
        attributeCargoWorkspacePackages(packages, [
          { name: 'root', root: '.' },
          { name: 'service', root: 'packages/service' },
          { name: 'api-project', root: 'packages/service/crates/api' },
        ]).map(({ name, projectName }) => [name, projectName]),
      ).toEqual([
        ['api', 'api-project'],
        ['base', 'root'],
        ['root-app', 'root'],
      ]);
      expect(listCargoWorkspacePackages(root)).toEqual(packages);
      await write(root, 'Cargo.toml', '[package]\nname="standalone"\n[workspace]\n');
      expect(listCargoWorkspacePackages(root)).toEqual([{ name: 'standalone', dir: '.', testShards: 1 }]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('hashes assets, build scripts and transitive cfg/build/dev dependencies without unrelated package sources', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-cargo-inputs-'));
    try {
      await write(
        root,
        'Cargo.toml',
        '[workspace]\nmembers=["crates/app"]\nexclude=["crates/excluded"]\n[workspace.dependencies]\nrenamed={package="base",path="crates/base"}\n',
      );
      await write(
        root,
        'crates/app/Cargo.toml',
        '[package]\nname="app"\nbuild="build/build.rs"\n[dependencies]\nrenamed={workspace=true}\n[target.\'cfg(unix)\'.build-dependencies]\nbuilder={path="../builder"}\n[target.\'cfg(windows)\'.dev-dependencies]\nfixture={path="../excluded"}\n',
      );
      await write(
        root,
        'crates/base/Cargo.toml',
        '[package]\nname="base"\n[build-dependencies]\nleaf={path="../leaf"}\n',
      );
      await write(root, 'crates/leaf/Cargo.toml', '[package]\nname="leaf"\n[dev-dependencies]\napp={path="../app"}\n');
      await write(root, 'crates/builder/Cargo.toml', '[package]\nname="builder"\n');
      await write(root, 'crates/excluded/Cargo.toml', '[package]\nname="excluded"\n');
      await write(root, 'crates/app/nested/Cargo.toml', '[package]\nname="unrelated"\n');
      const request = { workspaceRoot: root, absoluteProjectRoot: root, memberDir: 'crates/app' };
      const inputs = await cargoPackageTestInputs(request);
      for (const path of [
        'Cargo.toml',
        'Cargo.lock',
        '.cargo/config',
        '.cargo/config.toml',
        '.config/nextest.toml',
        'rust-toolchain',
        'rust-toolchain.toml',
        'rustfmt.toml',
        '.rustfmt.toml',
        'clippy.toml',
        'crates/app/src/lib.rs',
        'crates/app/src/template.html',
        'crates/app/build/build.rs',
        'crates/app/native/header.h',
        'crates/app/schema/input.proto',
        'crates/app/tests/fixture.bin',
        'crates/base/src/lib.rs',
        'crates/builder/build.rs',
        'crates/leaf/src/table.csv',
        'crates/excluded/src/lib.rs',
      ])
        expect(matches(inputs, path)).toBe(true);
      for (const path of [
        'crates/other/src/lib.rs',
        'crates/app/nested/src/lib.rs',
        'target/debug/output',
        'crates/app/target/generated.rs',
        'crates/other/.cargo/config.toml',
        'scripts/unrelated.sh',
      ])
        expect(matches(inputs, path)).toBe(false);
      await write(root, 'crates/app/target/debug/generated.rs', 'generated');
      await write(root, 'crates/app/src/template.html', 'changed asset');
      expect(matches(await cargoPackageTestInputs(request), 'crates/app/target/debug/generated.rs')).toBe(false);
      expect(await cargoPackageTestInputs(request)).toEqual(await cargoPackageTestInputs(request));
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('keeps dependencies outside Cargo but inside Nx as ordinary files and includes ancestor config', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-cargo-nested-'));
    try {
      await write(root, 'packages/app/Cargo.toml', '[workspace]\nmembers=["crates/app"]\n');
      await write(
        root,
        'packages/app/crates/app/Cargo.toml',
        '[package]\nname="app"\n[dependencies]\nbase={path="../../../base"}\n',
      );
      await write(root, 'packages/base/Cargo.toml', '[package]\nname="base"\n');
      const inputs = await cargoPackageTestInputs({
        workspaceRoot: root,
        absoluteProjectRoot: join(root, 'packages/app'),
        memberDir: 'crates/app',
      });
      expect(matches(inputs, 'packages/base/src/lib.rs', 'packages/app')).toBe(true);
      expect(matches(inputs, '.cargo/config.toml', 'packages/app')).toBe(true);
      expect(matches(inputs, 'packages/rust-toolchain.toml', 'packages/app')).toBe(true);
      expect(matches(inputs, 'packages/base/target/debug/output', 'packages/app')).toBe(false);
      expect(inputs).not.toContain('externalRustCrates');
      expect(inputs.some((input) => input.includes('../'))).toBe(false);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('resolves inherited dependencies against their own workspace and hashes local patches', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-cargo-inheritance-'));
    try {
      await write(
        root,
        'Cargo.toml',
        '[workspace]\nmembers=["app"]\nexclude=["vendor"]\n[patch.crates-io]\npatched={path="patches/base"}\n',
      );
      await write(root, 'app/Cargo.toml', '[package]\nname="app"\n[dependencies]\nbase={path="../vendor/base"}\n');
      await write(
        root,
        'vendor/Cargo.toml',
        '[workspace]\nmembers=["base","leaf"]\n[workspace.dependencies]\nleaf={path="leaf"}\n',
      );
      await write(
        root,
        'vendor/base/Cargo.toml',
        '[package]\nname="base"\nworkspace=".."\n[dependencies]\nleaf={workspace=true}\n',
      );
      await write(root, 'vendor/leaf/Cargo.toml', '[package]\nname="leaf"\n');
      await write(root, 'patches/base/Cargo.toml', '[package]\nname="patched"\n');
      const inputs = await cargoPackageTestInputs({
        workspaceRoot: root,
        absoluteProjectRoot: root,
        memberDir: 'app',
      });
      for (const path of ['vendor/Cargo.toml', 'vendor/leaf/src/lib.rs', 'patches/base/src/lib.rs']) {
        expect(matches(inputs, path)).toBe(true);
      }
      expect(matches(inputs, 'leaf/src/lib.rs')).toBe(false);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('excludes configured Cargo output directories before or after outputs exist', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-cargo-output-'));
    try {
      await write(root, 'Cargo.toml', '[package]\nname="app"\n[workspace]\n');
      await write(root, '.cargo/config.toml', '[build]\ntarget-dir="cache/cargo"\n');
      const request = { workspaceRoot: root, absoluteProjectRoot: root, memberDir: '.' };
      const before = await cargoPackageTestInputs(request);
      expect(matches(before, 'cache/cargo/debug/generated.rs')).toBe(false);
      expect(matches(before, 'src/data.bin')).toBe(true);
      await write(root, 'cache/cargo/debug/generated.rs', 'generated');
      expect(await cargoPackageTestInputs(request)).toEqual(before);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('requires externalRustCrates only for paths outside Nx, including absolute and declared build paths', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-cargo-external-'));
    try {
      await write(
        root,
        'Cargo.toml',
        '[workspace]\nmembers=["crates/app"]\n[workspace.dependencies]\nexternal={path="../external"}\n',
      );
      await write(
        root,
        'crates/app/Cargo.toml',
        `[package]\nname="app"\nbuild="../../../build.rs"\n[dependencies]\nexternal={workspace=true}\nlocal={path=${JSON.stringify(join(root, 'crates/base'))}}\n[target.'cfg(unix)'.dependencies]\nabsolute={path=${JSON.stringify(join(dirname(root), 'absolute'))}}\n`,
      );
      await write(root, 'crates/base/Cargo.toml', '[package]\nname="base"\n');
      await write(root, 'nx.json', JSON.stringify({ namedInputs: {} }));
      const request = { workspaceRoot: root, absoluteProjectRoot: root, memberDir: 'crates/app' };
      await expect(cargoPackageTestInputs(request)).rejects.toThrow();
      await write(
        root,
        'nx.json',
        JSON.stringify({ namedInputs: { externalRustCrates: [{ runtime: 'echo source-digest' }] } }),
      );
      const inputs = await cargoPackageTestInputs(request);
      expect(inputs).toContain('externalRustCrates');
      expect(matches(inputs, 'crates/base/src/lib.rs')).toBe(true);
      expect(inputs.some((input) => input.includes('../'))).toBe(false);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('uses Cargo.lock for registry and git dependencies, and declared local target paths for source', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-cargo-declared-'));
    try {
      await write(
        root,
        'Cargo.toml',
        '[workspace]\nmembers=["crates/app"]\n[workspace.dependencies]\nremote={git="https://example.org/source.git",rev="abc123"}\n',
      );
      await write(
        root,
        'crates/app/Cargo.toml',
        '[package]\nname="app"\nbuild="../../shared/build.rs"\n[lib]\npath="../../shared/lib.rs"\n[dependencies]\nremote={workspace=true}\nregistry="1"\n',
      );
      const inputs = await cargoPackageTestInputs({
        workspaceRoot: root,
        absoluteProjectRoot: root,
        memberDir: 'crates/app',
      });
      expect(matches(inputs, 'Cargo.lock')).toBe(true);
      expect(matches(inputs, 'shared/build.rs')).toBe(true);
      expect(matches(inputs, 'shared/lib.rs')).toBe(true);
      expect(inputs).not.toContain('externalRustCrates');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('rejects unsupported and empty member glob expansions rather than omitting packages', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-cargo-globs-'));
    try {
      await write(root, 'Cargo.toml', '[workspace]\nmembers=["packages/**/crates/*"]\n');
      expect(() => listCargoWorkspacePackages(root)).toThrow();
      await write(root, 'Cargo.toml', '[workspace]\nmembers=["packages/*/crates/*"]\n');
      expect(() => listCargoWorkspacePackages(root)).toThrow();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

function matches(inputs: readonly string[], path: string, projectRoot = ''): boolean {
  const filesets = inputs.filter((input) => input.includes('{projectRoot}') || input.includes('{workspaceRoot}'));
  const match = (input: string): boolean =>
    new Glob(
      input.replace('{projectRoot}/', projectRoot ? `${projectRoot}/` : '').replace('{workspaceRoot}/', ''),
    ).match(path);
  return (
    filesets.some((input) => !input.startsWith('!') && match(input)) &&
    !filesets.some((input) => input.startsWith('!') && match(input.slice(1)))
  );
}

async function write(root: string, path: string, contents: string): Promise<void> {
  const absolutePath = join(root, path);
  await mkdir(dirname(absolutePath), { recursive: true });
  await writeFile(absolutePath, contents);
}
