import { describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { useFixtureCargoHome } from '@smoothbricks/cargo-retain/testing/fixture-cargo-home';
import { Glob } from 'bun';
import { prepareCargoFixture } from './__tests__/cargo-fixture.js';
import { readCargoResolve } from './cargo-source-hash.js';

import {
  attributeCargoWorkspacePackages,
  cargoPackageTestInputs,
  cargoTestGroupTargetName,
  cargoTestPackageTargetName,
  filterScope,
  listCargoWorkspacePackages,
  packageNameFromCargoTestTarget,
  SERIAL_TEST_GROUP_PLATFORM,
  serialTestGroups,
} from './cargo-workspace.js';

useFixtureCargoHome();
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

  it('hashes assets, build scripts and cfg/build/dev dependencies, the crates they link, and no unrelated package source', async () => {
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
        '[package]\nname="app"\nbuild="build/build.rs"\n[dependencies]\nrenamed={workspace=true}\n[target.\'cfg(unix)\'.build-dependencies]\nbuilder={path="../builder"}\n[target.\'cfg(windows)\'.dev-dependencies]\nfixture={package="excluded",path="../excluded"}\n',
      );
      await write(root, 'crates/app/build/build.rs', 'fn main() {}\n');
      await write(
        root,
        'crates/base/Cargo.toml',
        '[package]\nname="base"\n[build-dependencies]\nleaf={path="../leaf"}\n',
      );
      await write(root, 'crates/leaf/Cargo.toml', '[package]\nname="leaf"\n[dev-dependencies]\napp={path="../app"}\n');
      await write(root, 'crates/builder/Cargo.toml', '[package]\nname="builder"\n');
      await write(root, 'crates/excluded/Cargo.toml', '[package]\nname="excluded"\n');
      await write(root, 'crates/app/nested/Cargo.toml', '[package]\nname="unrelated"\n');
      const inputs = await testInputs(root, 'crates/app');
      for (const path of [
        'Cargo.toml',
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
      // The lock reaches runners through the crate's cargo-lock node, not this fileset.
      expect(matches(inputs, 'Cargo.lock')).toBe(false);
      await write(root, 'crates/app/target/debug/generated.rs', 'generated');
      await write(root, 'crates/app/src/template.html', 'changed asset');
      expect(matches(await testInputs(root, 'crates/app'), 'crates/app/target/debug/generated.rs')).toBe(false);
      expect(await testInputs(root, 'crates/app')).toEqual(await testInputs(root, 'crates/app'));
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  // Cargo builds a dependency for its library and build script alone. A dependency's own dev-dependencies never
  // compile into the test binaries of the crate above it, so an edit to one must leave that crate's key alone, while
  // every crate it links still keys it.
  it("hashes a dependency's dev-dependencies for that dependency's tests only, and what it links for both", async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-cargo-dev-closure-'));
    try {
      await write(root, 'Cargo.toml', '[workspace]\nmembers=["crates/*"]\n');
      await write(root, 'crates/top/Cargo.toml', '[package]\nname="top"\n[dependencies]\nmid={path="../mid"}\n');
      await write(
        root,
        'crates/mid/Cargo.toml',
        '[package]\nname="mid"\n[dependencies]\nleaf={path="../leaf"}\n[dev-dependencies]\nhelper={path="../helper"}\n',
      );
      await write(root, 'crates/leaf/Cargo.toml', '[package]\nname="leaf"\n');
      await write(
        root,
        'crates/helper/Cargo.toml',
        '[package]\nname="helper"\n[dependencies]\nhelper-leaf={path="../helper-leaf"}\n',
      );
      await write(root, 'crates/helper-leaf/Cargo.toml', '[package]\nname="helper-leaf"\n');
      const top = await testInputs(root, 'crates/top');
      const mid = await testInputs(root, 'crates/mid');
      const hashedBy = (path: string) => ({ top: matches(top, path), mid: matches(mid, path) });
      expect(hashedBy('crates/top/src/lib.rs')).toEqual({ top: true, mid: false });
      expect(hashedBy('crates/mid/src/lib.rs')).toEqual({ top: true, mid: true });
      expect(hashedBy('crates/leaf/src/lib.rs')).toEqual({ top: true, mid: true });
      expect(hashedBy('crates/helper/src/lib.rs')).toEqual({ top: false, mid: true });
      expect(hashedBy('crates/helper-leaf/src/lib.rs')).toEqual({ top: false, mid: true });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  // rustc compiles an `include_str!`/`include_bytes!` file into the unit and Cargo rebuilds when it changes, but the
  // package fileset stops at the package directory: a shipped skill or corpus beside the crates changed nothing in
  // the archive's key, and the runner replayed a verdict about the old bytes.
  it("hashes the files a package's sources compile in from outside its directory", async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-cargo-compiled-in-'));
    try {
      await write(root, 'Cargo.toml', '[workspace]\nmembers=["crates/app"]\n');
      await write(root, 'crates/app/Cargo.toml', '[package]\nname="app"\n');
      await write(
        root,
        'crates/app/src/lib.rs',
        [
          'pub const SKILL: &str = include_str!("../../../skills/SKILL.md");',
          '#[cfg(test)] const CORPUS: &[u8] = include_bytes! ( "../../../corpus.json" );',
          'const INSIDE: &str = include_str!("data/inside.txt");',
          '// Not compiled: include_str!("../../../missing.txt")',
          'const COMPUTED: &str = include_str!(env!("DATA"));',
        ].join('\n'),
      );
      await write(root, 'crates/app/src/data/inside.txt', 'inside');
      await write(root, 'skills/SKILL.md', 'skill');
      await write(root, 'skills/OTHER.md', 'unrelated');
      await write(root, 'corpus.json', '[]');
      await write(root, 'crates/app/nested/Cargo.toml', '[package]\nname="nested"\n');
      await write(root, 'crates/app/nested/src/lib.rs', 'const N: &str = include_str!("../../../../nested.txt");');
      await write(root, 'nested.txt', 'another package compiles this');

      const inputs = await testInputs(root, 'crates/app');
      for (const path of ['skills/SKILL.md', 'corpus.json', 'crates/app/src/data/inside.txt']) {
        expect(matches(inputs, path)).toBe(true);
      }
      for (const path of ['skills/OTHER.md', 'missing.txt', 'nested.txt']) {
        expect(matches(inputs, path)).toBe(false);
      }
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
      const inputs = await testInputs(root, 'crates/app', join(root, 'packages/app'));
      expect(matches(inputs, 'packages/app/crates/app/src/lib.rs')).toBe(true);
      expect(matches(inputs, 'packages/base/src/lib.rs')).toBe(true);
      expect(matches(inputs, '.cargo/config.toml')).toBe(true);
      expect(matches(inputs, 'packages/rust-toolchain.toml')).toBe(true);
      expect(matches(inputs, 'packages/base/target/debug/output')).toBe(false);
      expect(inputs).not.toContain('externalRustCrates');
      expect(inputs.some((input) => input.includes('../'))).toBe(false);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('hashes an inherited dependency with its own workspace manifest, and the local patch Cargo resolves to', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-cargo-inheritance-'));
    try {
      await write(
        root,
        'Cargo.toml',
        '[workspace]\nmembers=["app"]\nexclude=["vendor","patches"]\n[patch.crates-io]\npatched={path="patches/base"}\n',
      );
      await write(
        root,
        'app/Cargo.toml',
        '[package]\nname="app"\n[dependencies]\nbase={path="../vendor/base"}\npatched="0.1"\n',
      );
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
      await write(root, 'patches/base/Cargo.toml', '[package]\nname="patched"\nversion="0.1.0"\n');
      const inputs = await testInputs(root, 'app');
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
      const before = await testInputs(root, '.');
      expect(matches(before, 'cache/cargo/debug/generated.rs')).toBe(false);
      expect(matches(before, 'src/data.bin')).toBe(true);
      await write(root, 'cache/cargo/debug/generated.rs', 'generated');
      expect(await testInputs(root, '.')).toEqual(before);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('requires externalRustCrates only for paths outside Nx, including absolute and declared build paths', async () => {
    // The Nx workspace sits one level down, so the packages and build script outside it are this test's own.
    const outside = await mkdtemp(join(tmpdir(), 'nx-cargo-external-'));
    const root = join(outside, 'nx');
    try {
      await write(
        root,
        'Cargo.toml',
        '[workspace]\nmembers=["crates/app"]\n[workspace.dependencies]\nexternal={path="../external"}\n',
      );
      await write(
        root,
        'crates/app/Cargo.toml',
        '[package]\nname="app"\nbuild="../../../build.rs"\n[dependencies]\nexternal={workspace=true}\nlocal={package="base",path=' +
          JSON.stringify(join(root, 'crates/base')) +
          "}\n[target.'cfg(unix)'.dependencies]\nabsolute={path=" +
          JSON.stringify(join(outside, 'absolute')) +
          '}\n',
      );
      await write(root, 'crates/base/Cargo.toml', '[package]\nname="base"\n');
      await write(outside, 'build.rs', 'fn main() {}\n');
      for (const name of ['external', 'absolute']) {
        await write(outside, `${name}/Cargo.toml`, `[package]\nname="${name}"\n`);
        await write(outside, `${name}/src/lib.rs`, '');
      }
      await write(root, 'nx.json', JSON.stringify({ namedInputs: {} }));
      await expect(testInputs(root, 'crates/app')).rejects.toThrow();
      await write(
        root,
        'nx.json',
        JSON.stringify({ namedInputs: { externalRustCrates: [{ runtime: 'echo source-digest' }] } }),
      );
      const inputs = await testInputs(root, 'crates/app');
      expect(inputs).toContain('externalRustCrates');
      expect(matches(inputs, 'crates/base/src/lib.rs')).toBe(true);
      expect(inputs.some((input) => input.includes('../'))).toBe(false);
    } finally {
      await rm(outside, { recursive: true, force: true });
    }
  });

  it('hashes declared target paths outside the package as source, and never Cargo.lock', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-cargo-declared-'));
    try {
      await write(root, 'Cargo.toml', '[workspace]\nmembers=["crates/app"]\n');
      await write(
        root,
        'crates/app/Cargo.toml',
        '[package]\nname="app"\nbuild="../../shared/build.rs"\n[lib]\npath="../../shared/lib.rs"\n',
      );
      await write(root, 'shared/build.rs', 'fn main() {}\n');
      await write(root, 'shared/lib.rs', '');
      const inputs = await testInputs(root, 'crates/app');
      // The registry and git entries of Cargo.lock reach runners through the crate's lock-scope node.
      expect(matches(inputs, 'Cargo.lock')).toBe(false);
      expect(matches(inputs, 'shared/build.rs')).toBe(true);
      expect(matches(inputs, 'shared/lib.rs')).toBe(true);
      expect(matches(inputs, 'shared/unrelated.rs')).toBe(false);
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

/**
 * Whether Nx would hash `path` (workspace-relative) under `inputs`.
 *
 * Only `{workspaceRoot}` filesets resolve here. Nx resolves a `{projectRoot}`
 * fileset against the files its project owns, which a directory prefix cannot
 * model: resolving it as one would pass the very inputs that hash nothing once
 * a nested project owns the crate, so this harness refuses them instead.
 */
function matches(inputs: readonly string[], path: string): boolean {
  expect(inputs.filter((input) => input.includes('{projectRoot}'))).toEqual([]);
  const filesets = inputs.filter((input) => input.includes('{workspaceRoot}'));
  const match = (input: string): boolean => new Glob(input.replace('{workspaceRoot}/', '')).match(path);
  return (
    filesets.some((input) => !input.startsWith('!') && match(input)) &&
    !filesets.some((input) => input.startsWith('!') && match(input.slice(1)))
  );
}

/**
 * The inputs graph inference derives for the member at `memberDir` of the Cargo workspace at `cargoRoot`: from
 * Cargo's own locked resolve of the fixture, its empty libraries and lockfile supplied first as inference fixtures
 * get them.
 */
async function testInputs(root: string, memberDir: string, cargoRoot = root): Promise<string[]> {
  await prepareCargoFixture(root);
  const indexed = new Set(
    [...new Glob('**/Cargo.toml').scanSync({ cwd: root })].filter(
      (path) => !path.split('/').some((part) => ['node_modules', 'target', '.nx'].includes(part)),
    ),
  );
  const cargo = await readCargoResolve(join(cargoRoot, 'Cargo.toml'), root, indexed);
  return cargoPackageTestInputs({ workspaceRoot: root, absoluteProjectRoot: cargoRoot, memberDir, cargo });
}

async function write(root: string, path: string, contents: string): Promise<void> {
  const absolutePath = join(root, path);
  await mkdir(dirname(absolutePath), { recursive: true });
  await writeFile(absolutePath, contents);
}

describe('nextest serial test groups', () => {
  it('derives one serial filter from the default-profile overrides that name a group capped at one thread', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-serial-groups-'));
    try {
      const config = join(root, 'nextest.toml');
      await write(
        root,
        'nextest.toml',
        [
          '[test-groups.real-apfs]',
          'max-threads = 1',
          '',
          '[test-groups.wide]',
          'max-threads = 4',
          '',
          '[[profile.default.overrides]]',
          "filter = 'test(/real_apfs_/)'",
          `platform = '${SERIAL_TEST_GROUP_PLATFORM}'`,
          "test-group = 'real-apfs'",
          '',
          '[[profile.default.overrides]]',
          "filter = 'binary(apfs_native_macos)'",
          `platform = '${SERIAL_TEST_GROUP_PLATFORM}'`,
          "test-group = 'real-apfs'",
          '',
          '[[profile.default.overrides]]',
          "filter = 'test(/wide_/)'",
          "test-group = 'wide'",
          '',
          '[[profile.ci.overrides]]',
          "filter = 'test(/ci_only/)'",
          `platform = '${SERIAL_TEST_GROUP_PLATFORM}'`,
          "test-group = 'real-apfs'",
          '',
        ].join('\n'),
      );
      expect(serialTestGroups(config)).toEqual([
        {
          group: 'real-apfs',
          filter: '(test(/real_apfs_/)) or (binary(apfs_native_macos))',
          scope: { kind: 'workspace' },
        },
      ]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('scopes each group to the packages its overrides name, and to the workspace once any override names none', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-serial-scope-'));
    const override = (filter: string, group: string): string[] => [
      '[[profile.default.overrides]]',
      `filter = '${filter}'`,
      `platform = '${SERIAL_TEST_GROUP_PLATFORM}'`,
      `test-group = '${group}'`,
      '',
    ];
    try {
      await write(
        root,
        'nextest.toml',
        [
          '[test-groups.device]',
          'max-threads = 1',
          '',
          '[test-groups.host]',
          'max-threads = 1',
          '',
          '[test-groups.loose]',
          'max-threads = 1',
          '',
          ...override('package(apfs) and test(/real_/)', 'device'),
          ...override('package(core) and (test(/real_/) or binary(native))', 'host'),
          ...override('package(cli) and test(/real_/)', 'host'),
          ...override('package(apfs) and test(/loose_/)', 'loose'),
          ...override('test(/loose_/)', 'loose'),
        ].join('\n'),
      );
      expect(serialTestGroups(join(root, 'nextest.toml'))).toEqual([
        {
          group: 'device',
          filter: '(package(apfs) and test(/real_/))',
          scope: { kind: 'packages', packages: ['apfs'] },
        },
        {
          // Several overrides of one group are one union, scoped to the packages of every clause.
          group: 'host',
          filter: '(package(core) and (test(/real_/) or binary(native))) or (package(cli) and test(/real_/))',
          scope: { kind: 'packages', packages: ['cli', 'core'] },
        },
        {
          // One override scoped to no package leaves the group's runner hashing every member.
          group: 'loose',
          filter: '(package(apfs) and test(/loose_/)) or (test(/loose_/))',
          scope: { kind: 'workspace' },
        },
      ]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('declares no serial group when the config is absent or names no group capped at one thread', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-serial-none-'));
    try {
      expect(serialTestGroups(join(root, 'missing.toml'))).toEqual([]);
      await write(root, 'nextest.toml', '[test-groups.wide]\nmax-threads = 4\n');
      expect(serialTestGroups(join(root, 'nextest.toml'))).toEqual([]);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('refuses a serial override that declares no macOS platform rather than guessing where the group applies', async () => {
    const root = await mkdtemp(join(tmpdir(), 'nx-serial-platform-'));
    try {
      await write(
        root,
        'nextest.toml',
        "[test-groups.real-apfs]\nmax-threads = 1\n\n[[profile.default.overrides]]\nfilter = 'test(/real_apfs_/)'\ntest-group = 'real-apfs'\n",
      );
      expect(() => serialTestGroups(join(root, 'nextest.toml'))).toThrow(/must declare platform/);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('never reads a serial group runner as a crate, and refuses a crate that would take its name', () => {
    expect(packageNameFromCargoTestTarget(cargoTestGroupTargetName('real-apfs'))).toBeNull();
    expect(packageNameFromCargoTestTarget(cargoTestPackageTargetName('big', 'shard1'))).toBe('big');
    expect(() => cargoTestPackageTargetName('group-real-apfs')).toThrow(/reserved/);
  });
});

describe('nextest filter scopes', () => {
  it('bounds a filter by the packages it is scoped to, however its operators and blanks are spelled', () => {
    const scoped: ReadonlyArray<readonly [string, readonly string[]]> = [
      // A plain name, with or without nextest's equality prefix, bare or parenthesised.
      ['package(cowshed-apfs)', ['cowshed-apfs']],
      ['package(=cowshed_apfs)', ['cowshed_apfs']],
      ['(package(a))', ['a']],
      ['((package(a)))', ['a']],
      // `and`, `&` and `-` keep the package of the side that names one, with or without blanks around them.
      ['package(a) and test(/x/)', ['a']],
      ['package(a) & test(/x/)', ['a']],
      ['package(a)&test(/x/)', ['a']],
      ['package(a) - test(/x/)', ['a']],
      ['package(a)-test(/x/)', ['a']],
      ['package(a) and not test(/x/)', ['a']],
      ['package(a) and !test(/x/)', ['a']],
      ['package(a) and not(test(/x/))', ['a']],
      ['test(/x/) and package(a)', ['a']],
      ['binary(b) & (package(a) or package(c))', ['a', 'c']],
      // An intersection takes the narrower side.
      ['(package(a) or package(b)) and package(a)', ['a']],
      ['package(a) and (package(a) or package(b))', ['a']],
      // A union is bounded by every clause, once each, in any spelling and order.
      ['package(b) or package(a)', ['a', 'b']],
      ['package(b) | package(a)', ['a', 'b']],
      ['package(b)+package(a)', ['a', 'b']],
      ['package(a) or package(a)', ['a']],
      // `and` binds tighter than `or`, so each clause keeps its own package.
      ['package(a) and test(/x/) or package(b) and test(/y/)', ['a', 'b']],
      ['package(a) or package(b) and test(/y/)', ['a', 'b']],
      ['(package(a) and test(/x/)) or (package(b) and (test(/y/) or binary(z)))', ['a', 'b']],
      // A difference is bounded by what it subtracts from, not by what it subtracts.
      ['(package(a) or package(b)) - package(a)', ['a', 'b']],
      ['package(a) - package(b)', ['a']],
      // Operators and parentheses inside a matcher are its text, not the filter's syntax.
      ['package(a) and test(/x or y | z + w - v & u (/)', ['a']],
      ['package(a) and test(/(x|y)z/)', ['a']],
      ['package(a) and test(/[)] | package(b) | [x]/)', ['a']],
      ['package(a) and test(~one and two or three)', ['a']],
      ['package(a) and test(=m::t::f_or_g)', ['a']],
      // nextest reads the line endings of a multi-line TOML string as blanks.
      ['package(a)\n  and test(/x/)\n  or package(b)', ['a', 'b']],
      ['package(a)\r\n  and test(/x/)', ['a']],
    ];
    for (const [filter, packages] of scoped) {
      expect(filterScope(filter), JSON.stringify(filter)).toEqual({ kind: 'packages', packages });
    }
  });

  it('is the whole workspace for whatever can select from every package, or cannot be read with certainty', () => {
    const workspace: readonly string[] = [
      // Scoped to no package at all.
      'test(/real_apfs_/)',
      'binary(apfs_native_macos)',
      'all()',
      'default()',
      'none()',
      // One unscoped clause leaves a union unscoped, whatever spells the union.
      'package(a) or test(/x/)',
      'package(a) | test(/x/)',
      'package(a)+test(/x/)',
      'test(/x/) or package(a)',
      '(package(a) and test(/x/)) or binary(b)',
      // `and`, `&` and `-` bind tighter than `or`, `|` and `+`, with or without blanks: the union is not under the package.
      'package(a) and test(/x/) | test(/y/)',
      'package(a) and test(/x/)|test(/y/)',
      'package(a) & test(/x/) + test(/y/)',
      'package(a)&test(/x/)+test(/y/)',
      'package(a) and test(/x/) or test(/y/)',
      '(package(a) and test(/x/))|test(/y/)',
      // Negation selects from every other package.
      'not package(a)',
      '!package(a)',
      'not (package(a))',
      '!(package(a) or package(b))',
      'package(a) or not package(b)',
      'not not package(a)',
      // A name that a glob, a substring or a regex can widen.
      'package(cowshed-*)',
      'package(cowshed-ap?s)',
      'package(~apfs)',
      'package(/apfs/)',
      'package(#cowshed-apfs)',
      'package(=a*)',
      'package(a|b)',
      'package(a b)',
      'package( a)',
      'package(a )',
      'package()',
      'package(=)',
      // Predicates that reach past the package they name.
      'deps(a)',
      'rdeps(a)',
      'binary_id(a)',
      'kind(lib)',
      'platform(host)',
      'group(g)',
      // An argument whose end depends on nextest's escapes, or a comma, which nextest refuses.
      'package(a) and test(=x\\)) | test(y)',
      'package(a) and test(/x\\/y/) | test(z)',
      'package(a) and test(=x,y)',
      'package(a,b)',
      // A union that only an exact reading of the regexes finds.
      'package(a) and test(/(/) | test(/)/)',
      // Not a filterset nextest accepts.
      '',
      '   ',
      'package(a',
      'package(a))',
      '(package(a)',
      'package(a) and',
      'and package(a)',
      'package(a) or',
      '| package(a)',
      'package(a) && test(/x/)',
      'package(a) || test(/x/)',
      'package(a) AND test(/x/)',
      'package(a) OR package(b)',
      'NOT package(a)',
      'package(a) test(/x/)',
      'package(a) and and test(/x/)',
      'Package(a)',
      'packages(a)',
      'package a',
      'package(a)\tand test(/x/)',
      'test(/x/',
      'test(/x/ y)',
      'test(/a/b/)',
      // Packages that exclude each other leave nothing to name.
      'package(a) and package(b)',
    ];
    for (const filter of workspace) {
      expect(filterScope(filter), JSON.stringify(filter)).toEqual({ kind: 'workspace' });
    }
  });
});
