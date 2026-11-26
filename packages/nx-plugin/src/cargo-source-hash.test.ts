import { expect, it } from 'bun:test';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { statSync } from 'node:fs';
import {
  chmod,
  mkdir,
  mkdtemp,
  readFile,
  realpath,
  rename,
  rm,
  stat,
  symlink,
  utimes,
  writeFile,
} from 'node:fs/promises';
import { homedir } from 'node:os';
import { dirname, join, relative } from 'node:path';
import { parseCargoMetadata } from '@smoothbricks/build-inputs/cargo-closure';
import { CARGO_ANCESTOR_INPUTS } from '@smoothbricks/cargo-retain/cargo-ancestor-inputs';
import { useFixtureCargoHome } from '@smoothbricks/cargo-retain/testing/fixture-cargo-home';
import { outsideTreesTmpdir } from '@smoothbricks/cargo-retain/testing/outside-trees';
import { assertRealKeyMatchesCacheKeyLaws } from './__tests__/cargo-key-law-binding.js';
import { loadRecordedCargoResolve, writeWorkspaceCargoFixtureFiles } from './__tests__/recorded-cargo-metadata.js';
import { runtimeInputValue } from './__tests__/runtime-input.js';
import { indexedCargoSources } from './cargo-closure-input.js';
import {
  type CargoResolve,
  cargoFromMetadata,
  compiledInFiles,
  hashCargoInputs,
  hashCargoPathInputs,
  resolutionKey,
} from './cargo-source-hash.js';

useFixtureCargoHome();

/** Builds a validated {@link CargoResolve} from in-memory metadata without spawning Cargo. */
function resolveFrom(document: unknown): CargoResolve {
  return cargoFromMetadata(parseCargoMetadata(JSON.stringify(document)));
}

async function put(root: string, path: string, text: string): Promise<void> {
  const file = join(root, path);
  await mkdir(dirname(file), { recursive: true });
  await writeFile(file, text);
}

/** Writes the standard fixture workspace and locks it with real Cargo (for real-process boundary tests only). */
async function lockedWorkspaceCargoFixture(root: string) {
  const fixture = await writeWorkspaceCargoFixtureFiles(root);
  execFileSync('cargo', ['generate-lockfile', '--offline', '--manifest-path', fixture.manifest], { stdio: 'pipe' });
  return fixture;
}

it('hashCargoInputs satisfies key_is_pure (cache_key.bend:790) and key_moves_with_input (cache_key.bend:828) differentially against cache-key-model', async () => {
  // Two sibling checkouts of the recorded workspace share identical ancestors; model items listed in `xs` map to `.rs`
  // inputs of the `external` package, and items absent from `xs` map to non-input `.txt` files beside them.
  const root = await realpath(await mkdtemp(join(outsideTreesTmpdir(), 'cargo-hash-laws-')));
  try {
    const first = await writeWorkspaceCargoFixtureFiles(join(root, 'c1'));
    const second = await writeWorkspaceCargoFixtureFiles(join(root, 'c2'));
    const cargo1 = loadRecordedCargoResolve(first.root);
    const cargo2 = loadRecordedCargoResolve(second.root);
    const drawn1 = join(first.root, 'external/src/drawn');
    const drawn2 = join(second.root, 'external/src/drawn');

    await assertRealKeyMatchesCacheKeyLaws({
      name: 'hashCargoInputs',
      drawsPerSeed: 15,
      evaluate: async ({ entries }) => {
        await Promise.all([rm(drawn1, { recursive: true, force: true }), rm(drawn2, { recursive: true, force: true })]);
        await Promise.all([mkdir(drawn1, { recursive: true }), mkdir(drawn2, { recursive: true })]);
        await Promise.all(
          entries.flatMap(({ slug, inKey, bytes1, bytes2 }) => {
            const filename = inKey ? `${slug}.rs` : `${slug}.txt`;
            return [writeFile(join(drawn1, filename), bytes1), writeFile(join(drawn2, filename), bytes2)];
          }),
        );
        return Promise.all([hashCargoInputs(cargo1, first.workspace), hashCargoInputs(cargo2, second.workspace)]);
      },
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it.each(['app', '.'])('invalidates transitive and inherited Cargo inputs with Nx rooted at %s', async (nxDirectory) => {
  const root = await realpath(await mkdtemp(join(outsideTreesTmpdir(), 'cargo-hash-')));
  const app = join(root, 'app');
  const nxRoot = join(root, nxDirectory);
  try {
    await put(
      root,
      'app/Cargo.toml',
      '[package]\nname="app"\nversion="0.1.0"\nedition="2021"\n[dependencies]\nbridge={path="../external/bridge"}\n[workspace]\n',
    );
    await put(root, 'app/src/lib.rs', 'pub fn run() {}\n');
    await put(
      root,
      'external/Cargo.toml',
      '[workspace]\nmembers=["bridge","leaf"]\nresolver="2"\n[workspace.package]\nedition="2021"\n',
    );
    await put(
      root,
      'external/bridge/Cargo.toml',
      '[package]\nname="bridge"\nversion="0.1.0"\nedition.workspace=true\n[dependencies]\nleaf={path="../leaf"}\n',
    );
    await put(root, 'external/bridge/src/lib.rs', 'pub fn bridge() {}\n');
    await put(root, 'external/leaf/Cargo.toml', '[package]\nname="leaf"\nversion="0.1.0"\nedition.workspace=true\n');
    await put(root, 'external/leaf/src/lib.rs', 'pub fn leaf() -> u8 { 1 }\n');
    const cargo = resolveFrom({
      packages: [
        {
          id: 'app',
          name: 'app',
          version: '0.1.0',
          source: null,
          manifest_path: join(app, 'Cargo.toml'),
          targets: [{ src_path: join(app, 'src/lib.rs') }],
          governing_manifest_path: join(app, 'Cargo.toml'),
        },
        {
          id: 'bridge',
          name: 'bridge',
          version: '0.1.0',
          source: null,
          manifest_path: join(root, 'external/bridge/Cargo.toml'),
          targets: [{ src_path: join(root, 'external/bridge/src/lib.rs') }],
          governing_manifest_path: join(root, 'external/Cargo.toml'),
        },
        {
          id: 'leaf',
          name: 'leaf',
          version: '0.1.0',
          source: null,
          manifest_path: join(root, 'external/leaf/Cargo.toml'),
          targets: [{ src_path: join(root, 'external/leaf/src/lib.rs') }],
          governing_manifest_path: join(root, 'external/Cargo.toml'),
        },
      ],
      resolve: {
        nodes: [
          { id: 'app', deps: [{ pkg: 'bridge', dep_kinds: [{ kind: null }] }] },
          { id: 'bridge', deps: [{ pkg: 'leaf', dep_kinds: [{ kind: null }] }] },
          { id: 'leaf', deps: [] },
        ],
      },
      workspace_members: ['app'],
      workspace_root: app,
    });

    const initial = await hashCargoInputs(cargo, nxRoot);
    await put(root, 'external/leaf/src/lib.rs', 'pub fn leaf() -> u8 { 2 }\n');
    const sourceChanged = await hashCargoInputs(cargo, nxRoot);
    expect(sourceChanged).not.toBe(initial);
    await put(
      root,
      'external/Cargo.toml',
      '[workspace]\nmembers=["bridge","leaf"]\nresolver="2"\n[workspace.package]\nedition="2024"\n',
    );
    const inheritedChanged = await hashCargoInputs(cargo, nxRoot);
    expect(inheritedChanged).not.toBe(sourceChanged);
    await put(root, 'external/leaf/target/noise.rs', 'uncompiled build output');
    expect(await hashCargoInputs(cargo, nxRoot)).toBe(inheritedChanged);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it('identical external inputs hash equally at different checkout depths, with bytes and file names still significant', async () => {
  const root = await realpath(await mkdtemp(join(outsideTreesTmpdir(), 'cargo-hash-portable-')));
  const shared = join(root, 'shared');
  const roots: readonly [string, string] = [join(root, 'main'), join(root, 'sheds/deep/unit')];
  try {
    await mkdir(join(shared, 'src'), { recursive: true });
    await writeFile(
      join(shared, 'Cargo.toml'),
      '[package]\nname="shared"\nversion="0.1.0"\nedition="2021"\n[workspace]\n',
    );
    const include = `const DATA: &str = include_str!(${JSON.stringify(join(root, 'data.txt'))});\n`;
    await writeFile(join(shared, 'src/lib.rs'), `pub fn value() -> u8 { 7 }\n${include}`);
    await writeFile(join(shared, 'src/extra.rs'), 'pub const EXTRA: u8 = 9;\n');
    await writeFile(join(root, 'data.txt'), 'shared-data-one');
    const manifest =
      '[package]\nname="consumer"\nversion="0.1.0"\nedition="2021"\n[dependencies]\nshared={path=' +
      JSON.stringify(shared) +
      '}\n[workspace]\n';
    const pairs = await Promise.all(
      roots.map(async (workspace): Promise<readonly [string, CargoResolve]> => {
        await mkdir(join(workspace, 'src'), { recursive: true });
        await writeFile(join(workspace, 'Cargo.toml'), manifest);
        await writeFile(join(workspace, 'src/lib.rs'), `pub fn value() -> u8 { shared::value() }\n${include}`);
        await symlink(join(shared, 'src'), join(workspace, 'linked-src'), 'dir');
        return [
          workspace,
          resolveFrom({
            packages: [
              {
                id: 'consumer',
                name: 'consumer',
                version: '0.1.0',
                source: null,
                manifest_path: join(workspace, 'Cargo.toml'),
                targets: [{ src_path: join(workspace, 'src/lib.rs') }],
                governing_manifest_path: join(workspace, 'Cargo.toml'),
              },
              {
                id: 'shared',
                name: 'shared',
                version: '0.1.0',
                source: null,
                manifest_path: join(shared, 'Cargo.toml'),
                targets: [{ src_path: join(shared, 'src/lib.rs') }],
                governing_manifest_path: join(shared, 'Cargo.toml'),
              },
            ],
            resolve: {
              nodes: [
                { id: 'consumer', deps: [{ pkg: 'shared', dep_kinds: [{ kind: null }] }] },
                { id: 'shared', deps: [] },
              ],
            },
            workspace_members: ['consumer'],
            workspace_root: workspace,
          }),
        ];
      }),
    );
    const hashBoth = (options?: { includeWorkspace?: boolean }) =>
      Promise.all(pairs.map(([workspace, cargo]) => hashCargoInputs(cargo, workspace, options)));

    const before: string[] = [];
    const after: string[] = [];
    for (const includeWorkspace of [false, true]) {
      const hashes = await hashBoth({ includeWorkspace });
      expect(hashes[0]).toBeDefined();
      expect(hashes[1]).toBe(hashes[0]);
      before.push(hashes.join(','));
    }
    await writeFile(join(root, 'data.txt'), 'shared-data-two');
    for (const [index, includeWorkspace] of [false, true].entries()) {
      const hashes = await hashBoth({ includeWorkspace });
      expect(hashes[1]).toBe(hashes[0]);
      expect(hashes.join(',')).not.toBe(before[index]);
      after.push(hashes.join(','));
    }
    await rename(join(shared, 'src/extra.rs'), join(shared, 'src/renamed.rs'));
    const renamed = await hashBoth();
    expect(renamed[1]).toBe(renamed[0]);
    expect(renamed.join(',')).not.toBe(after[0]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it('the runtime input is byte-identical under cargo lock contention: nothing on stderr', async () => {
  // Nx hashes a runtime input's stdout and stderr together, and hashes once per
  // distinct task env, so the bin runs many times at once at the start of a
  // run. Cargo reports package-cache lock contention on stderr; a private
  // CARGO_HOME makes that contention real here.
  const root = await mkdtemp(join(outsideTreesTmpdir(), 'cargo-hash-'));
  const manifest = join(root, 'Cargo.toml');
  try {
    await mkdir(join(root, 'src'), { recursive: true });
    await writeFile(manifest, '[package]\nname="app"\nversion="0.1.0"\nedition="2021"\n[workspace]\n');
    await writeFile(join(root, 'src', 'lib.rs'), 'pub fn run() {}\n');
    // A runtime input inherits the Nx process's environment untouched. Node
    // warns with its own pid when FORCE_COLOR and NO_COLOR are both set.
    // The declared wrapper excludes successful diagnostics from the value;
    // all eight genuinely contended runs must still succeed and hash equally.
    const env = { ...process.env, CARGO_HOME: join(root, '.cargo-home'), FORCE_COLOR: 'true', NO_COLOR: '1' };
    execFileSync('cargo', ['generate-lockfile', '--offline', '--manifest-path', manifest], { stdio: 'pipe', env });
    // Invoke the declared runtime-input owner, not the raw bin: successful stderr is not a hash value.
    const bin = join(import.meta.dir, '..', 'dist', 'bin', 'smoo-nx-cargo-hash.js');
    const runtimeInput = join(import.meta.dir, '..', 'runtime-input.sh');
    const runs = await Promise.all(
      Array.from({ length: 8 }, async () => {
        const proc = Bun.spawn(['sh', runtimeInput, 'node', bin, manifest], {
          cwd: root,
          env,
          stdout: 'pipe',
          stderr: 'pipe',
        });
        const [stdout, stderr, exitCode] = await Promise.all([
          new Response(proc.stdout).text(),
          new Response(proc.stderr).text(),
          proc.exited,
        ]);
        return { stdout, stderr, exitCode };
      }),
    );
    for (const run of runs) {
      expect(run.exitCode).toBe(0);
      expect(run.stderr).toBe('');
    }
    expect(new Set(runs.map((run) => run.stdout)).size).toBe(1);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it('failed runtime inputs have the same key at different checkout paths and still name the cause', async () => {
  const root = await mkdtemp(join(outsideTreesTmpdir(), 'cargo-hash-refusal-'));
  try {
    const bin = join(import.meta.dir, '../dist/bin/smoo-nx-cargo-hash.js');
    for (const directory of ['first', 'second']) {
      const workspace = join(root, directory);
      await mkdir(workspace);
      await writeFile(join(workspace, 'Cargo.toml'), '[package\n');
      const child = Bun.spawn(['node', bin, 'Cargo.toml'], {
        cwd: workspace,
        env: process.env,
        stdout: 'pipe',
        stderr: 'pipe',
      });
      const [exitCode, stdout, stderr] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ]);
      expect({ exitCode, stdout }).toEqual({ exitCode: 1, stdout: 'cargo-input-unavailable\n' });
      // The verdict is the key; stderr must retain Cargo's cause without the
      // checkout path, but may also contain an unrelated native-loader warning.
      expect(stderr).toContain('unclosed table');
      expect(stderr).not.toContain(workspace);
      expect(stderr).not.toContain(await realpath(workspace));
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it('include-workspace hashes in-workspace transitive source edits without changing the external-only contract', async () => {
  const root = await mkdtemp(join(outsideTreesTmpdir(), 'cargo-hash-workspace-'));
  try {
    const { root: canonical, workspace, leafSource } = await writeWorkspaceCargoFixtureFiles(root);
    const cargo = loadRecordedCargoResolve(canonical);
    const external = await hashCargoInputs(cargo, workspace);
    const included = await hashCargoInputs(cargo, workspace, { includeWorkspace: true });
    expect(await hashCargoInputs(cargo, workspace, { includeWorkspace: true })).toBe(included);

    await writeFile(leafSource, 'pub fn answer() -> u8 { 2 }\n');
    expect(await hashCargoInputs(cargo, workspace)).toBe(external);
    const changed = await hashCargoInputs(cargo, workspace, { includeWorkspace: true });
    expect(changed).not.toBe(included);
    expect(await hashCargoInputs(cargo, workspace, { includeWorkspace: true })).toBe(changed);

    await writeFile(join(canonical, 'external/src/lib.rs'), 'pub fn answer() -> u8 { 20 }\n');
    expect(await hashCargoInputs(cargo, workspace)).not.toBe(external);
    expect(await hashCargoInputs(cargo, workspace, { includeWorkspace: true })).not.toBe(changed);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it.each(['external', 'workspace/node_modules/external'])(
  'follows same-size same-mtime dependency edits in %s and hashes the newly reached source bytes',
  async (externalDirectory) => {
    const root = await realpath(await mkdtemp(join(outsideTreesTmpdir(), 'cargo-hash-boundary-')));
    const workspace = join(root, 'workspace');
    const manifest = join(workspace, 'Cargo.toml');
    const external = join(root, externalDirectory);
    const externalManifest = join(external, 'Cargo.toml');
    const dependencyManifest = (name: string) =>
      '[package]\nname="external"\nversion="0.1.0"\nedition="2021"\n[workspace]\n' +
      `[dependencies]\nside={path=${JSON.stringify(relative(external, join(root, name)))}}\n`;
    const sideManifest = '[package]\nname="side"\nversion="0.1.0"\nedition="2021"\n[workspace]\n';
    const timestamp = new Date(1_700_000_000_000);
    const resolveSide = (side: 'left' | 'rght'): CargoResolve =>
      resolveFrom({
        packages: [
          {
            id: 'app',
            name: 'app',
            version: '0.1.0',
            source: null,
            manifest_path: manifest,
            targets: [{ src_path: join(workspace, 'src/lib.rs') }],
            governing_manifest_path: manifest,
          },
          {
            id: 'external',
            name: 'external',
            version: '0.1.0',
            source: null,
            manifest_path: externalManifest,
            targets: [{ src_path: join(external, 'src/lib.rs') }],
            governing_manifest_path: externalManifest,
          },
          {
            id: 'side',
            name: 'side',
            version: '0.1.0',
            source: null,
            manifest_path: join(root, side, 'Cargo.toml'),
            targets: [{ src_path: join(root, side, 'src/lib.rs') }],
            governing_manifest_path: join(root, side, 'Cargo.toml'),
          },
        ],
        resolve: {
          nodes: [
            { id: 'app', deps: [{ pkg: 'external', dep_kinds: [{ kind: null }] }] },
            { id: 'external', deps: [{ pkg: 'side', dep_kinds: [{ kind: null }] }] },
            { id: 'side', deps: [] },
          ],
        },
        workspace_members: ['app'],
        workspace_root: workspace,
      });
    try {
      await put(
        root,
        'workspace/Cargo.toml',
        '[package]\nname="app"\nversion="0.1.0"\nedition="2021"\n[workspace]\nexclude=["node_modules/external"]\n' +
          `[dependencies]\nexternal={path=${JSON.stringify(relative(workspace, external))}}\n`,
      );
      await put(root, 'workspace/src/lib.rs', 'pub fn app() {}\n');
      await put(root, `${externalDirectory}/Cargo.toml`, dependencyManifest('left'));
      await put(root, `${externalDirectory}/src/lib.rs`, 'pub fn external() {}\n');
      for (const side of ['left', 'rght']) {
        await put(root, `${side}/Cargo.toml`, sideManifest);
        await put(root, `${side}/src/lib.rs`, 'pub fn side() -> u8 { 1 }\n');
      }
      execFileSync('git', ['init', '--quiet', workspace], { stdio: 'pipe' });
      await put(root, 'workspace/.gitignore', 'node_modules/\n.nx/\n');
      await utimes(externalManifest, timestamp, timestamp);
      const before = await stat(externalManifest);
      const leftResolve = resolveSide('left');
      const indexed = new Set(['Cargo.toml']);
      const keyBefore = resolutionKey(workspace, manifest, leftResolve, indexed);
      const initial = await hashCargoInputs(leftResolve, workspace);

      // Both packages have the same name/version, so only the dependency manifest's bytes identify its new location.
      await writeFile(externalManifest, dependencyManifest('rght'));
      await utimes(externalManifest, timestamp, timestamp);
      const after = await stat(externalManifest);
      expect(after.size).toBe(before.size);
      expect(after.mtimeMs).toBe(before.mtimeMs);
      expect(resolutionKey(workspace, manifest, leftResolve, indexed)).not.toBe(keyBefore);

      const rightResolve = resolveSide('rght');
      const changed = await hashCargoInputs(rightResolve, workspace);
      expect(changed).not.toBe(initial);

      await writeFile(join(root, 'left/src/lib.rs'), 'pub fn side() -> u8 { 2 }\n');
      expect(await hashCargoInputs(rightResolve, workspace)).toBe(changed);

      const source = join(root, 'rght/src/lib.rs');
      const sourceBefore = await stat(source);
      await writeFile(source, 'pub fn side() -> u8 { 2 }\n');
      await utimes(source, sourceBefore.atime, sourceBefore.mtime);
      const sourceChanged = await hashCargoInputs(rightResolve, workspace);
      expect(sourceChanged).not.toBe(changed);

      await rm(join(root, 'rght/Cargo.toml'));
      await expect(hashCargoInputs(rightResolve, workspace)).rejects.toThrow('ENOENT');
      await put(root, 'rght/Cargo.toml', sideManifest);
      expect(await hashCargoInputs(rightResolve, workspace)).toBe(sourceChanged);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  },
);

it('include-workspace CLI accepts an optional manifest and refuses unsupported arguments', async () => {
  const root = await mkdtemp(join(outsideTreesTmpdir(), 'cargo-hash-cli-'));
  try {
    const { workspace, leafSource } = await lockedWorkspaceCargoFixture(root);
    const bin = join(import.meta.dir, '../dist/bin/smoo-nx-cargo-hash.js');
    const run = async (args: string[]) => {
      // Bun.spawn's default environment is the one this process started with,
      // not process.env, so pass it: the file's own CARGO_HOME must reach the CLI.
      const child = Bun.spawn(['bun', bin, ...args], {
        cwd: workspace,
        env: process.env,
        stdout: 'pipe',
        stderr: 'pipe',
      });
      const [exitCode, stdout, stderr] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ]);
      return { exitCode, stdout, stderr };
    };
    const external = await run(['Cargo.toml']);
    expect(external.exitCode, external.stderr).toBe(0);
    const included = await run(['--include-workspace']);
    expect(included.exitCode, included.stderr).toBe(0);
    expect(included.stderr).toBe('');
    expect(await run(['--include-workspace', 'Cargo.toml'])).toEqual(included);

    await writeFile(leafSource, 'pub fn answer() -> u8 { 2 }\n');
    expect(await run([])).toEqual(external);
    const changed = await run(['--include-workspace', 'Cargo.toml']);
    expect(changed.exitCode, changed.stderr).toBe(0);
    expect(changed.stderr).toBe('');
    expect(changed.stdout).not.toBe(included.stdout);
    expect(await run(['--include-workspace'])).toEqual(changed);

    const closure = await run(['--include-workspace', '--closure', 'crates/leaf', 'Cargo.toml']);
    expect(closure.exitCode, closure.stderr).toBe(0);
    expect(closure.stderr).toBe('');
    expect(closure.stdout).not.toBe(changed.stdout);
    expect(await run(['--closure', 'crates/leaf', '--include-workspace'])).toEqual(closure);

    const invalidArgs: string[][] = [
      ['--unknown'],
      ['--include-workspace', '--unknown'],
      ['Cargo.toml', '--include-workspace'],
      ['--include-workspace', 'Cargo.toml', 'extra'],
      ['--include-workspace', '--include-workspace'],
      ['--closure'],
      ['--closure', '--include-workspace'],
      ['--closure', 'crates/leaf', '--closure', 'packages/app'],
    ];
    const refusals = await Promise.all(invalidArgs.map((args) => run(args)));
    for (const refused of refusals) {
      expect(refused.exitCode, refused.stderr).toBe(2);
      expect(refused.stdout).toBe('cargo-input-invalid-arguments\n');
      expect(refused.stderr).toBe('');
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it('closure hashes the dependency closure of one directory and ignores unrelated workspace members', async () => {
  const root = await mkdtemp(join(outsideTreesTmpdir(), 'cargo-hash-closure-'));
  try {
    const { root: canonical, workspace, leafSource } = await writeWorkspaceCargoFixtureFiles(root);
    const cargo = loadRecordedCargoResolve(canonical);
    const app = { includeWorkspace: true, closure: join(workspace, 'packages/app') };
    const leaf = { includeWorkspace: true, closure: join(workspace, 'crates/leaf') };
    const initial = await hashCargoInputs(cargo, workspace, app);
    const leafOnly = await hashCargoInputs(cargo, workspace, leaf);
    const whole = await hashCargoInputs(cargo, workspace, { includeWorkspace: true });

    await writeFile(join(workspace, 'crates/unrelated/src/lib.rs'), 'pub fn unrelated() -> u8 { 4 }\n');
    expect(await hashCargoInputs(cargo, workspace, app)).toBe(initial);
    expect(await hashCargoInputs(cargo, workspace, { includeWorkspace: true })).not.toBe(whole);

    await writeFile(join(workspace, 'crates/bridge/src/lib.rs'), 'pub fn answer() -> u8 { leaf::answer() }\n');
    const bridgeChanged = await hashCargoInputs(cargo, workspace, app);
    expect(bridgeChanged).not.toBe(initial);
    expect(await hashCargoInputs(cargo, workspace, leaf)).toBe(leafOnly);

    await writeFile(leafSource, 'pub fn answer() -> u8 { 2 }\n');
    expect(await hashCargoInputs(cargo, workspace, app)).not.toBe(bridgeChanged);
    expect(await hashCargoInputs(cargo, workspace, leaf)).not.toBe(leafOnly);

    // The external package is reached through bridge, so the app closure sees it and the leaf closure does not.
    const appBefore = await hashCargoInputs(cargo, workspace, app);
    const leafBefore = await hashCargoInputs(cargo, workspace, leaf);
    await writeFile(join(canonical, 'external/src/lib.rs'), 'pub fn answer() -> u8 { 20 }\n');
    expect(await hashCargoInputs(cargo, workspace, app)).not.toBe(appBefore);
    expect(await hashCargoInputs(cargo, workspace, leaf)).toBe(leafBefore);

    await mkdir(join(workspace, 'docs'), { recursive: true });
    await expect(
      hashCargoInputs(cargo, workspace, { includeWorkspace: true, closure: join(workspace, 'docs') }),
    ).rejects.toThrow('no local Cargo package lies under');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it('leaves gitignored sources beneath a package out of the digest but keeps an ignored package root', async () => {
  const root = await mkdtemp(join(outsideTreesTmpdir(), 'cargo-hash-ignored-'));
  try {
    const { root: canonical, workspace } = await writeWorkspaceCargoFixtureFiles(root);
    const cargo = loadRecordedCargoResolve(canonical);
    execFileSync('git', ['init', '--quiet', workspace], { stdio: 'pipe' });
    await writeFile(join(workspace, '.gitignore'), 'target/\ngenerated.rs\ncrates/leaf/\n');
    const options = { includeWorkspace: true };
    const initial = await hashCargoInputs(cargo, workspace, options);

    // A source a producer generates later: a fresh tree and a built one hash alike.
    await writeFile(join(workspace, 'crates/bridge/src/generated.rs'), 'pub const GENERATED: u8 = 1;\n');
    expect(await hashCargoInputs(cargo, workspace, options)).toBe(initial);

    // An untracked source nobody ignores is an edit like any other.
    await writeFile(join(workspace, 'crates/bridge/src/extra.rs'), 'pub const EXTRA: u8 = 1;\n');
    const untracked = await hashCargoInputs(cargo, workspace, options);
    expect(untracked).not.toBe(initial);

    // A package whose own directory is ignored still hashes its sources.
    await writeFile(join(workspace, 'crates/leaf/src/lib.rs'), 'pub fn answer() -> u8 { 3 }\n');
    expect(await hashCargoInputs(cargo, workspace, options)).not.toBe(untracked);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it('hashes a Cargo workspace that lives inside a directory its enclosing repository ignores', async () => {
  // A sandbox's TMPDIR, or a checkout under another repository's ignored scratch directory: every
  // package sits beneath one ignored ancestor, where `git ls-files` refuses the pathspec outright.
  const repository = await mkdtemp(join(outsideTreesTmpdir(), 'cargo-hash-enclosed-'));
  try {
    execFileSync('git', ['init', '--quiet', repository], { stdio: 'pipe' });
    await writeFile(join(repository, '.gitignore'), 'scratch/\n');
    const {
      root: canonical,
      workspace,
      leafSource,
    } = await writeWorkspaceCargoFixtureFiles(join(repository, 'scratch/deep'));
    const cargo = loadRecordedCargoResolve(canonical);
    const options = { includeWorkspace: true };
    const initial = await hashCargoInputs(cargo, workspace, options);
    await writeFile(leafSource, 'pub fn answer() -> u8 { 2 }\n');
    expect(await hashCargoInputs(cargo, workspace, options)).not.toBe(initial);
  } finally {
    await rm(repository, { recursive: true, force: true });
  }
});

it('the runtime input reads no git configuration from the operator home', async () => {
  // A host client's runtime input runs in a workspace's sandboxed daemon with the host's HOME
  // (cowshed 04_sandbox.md). Git must not read that home: the sandbox denies its global
  // configuration, and git dies on any error but a missing file; its global ignore file would
  // decide which sources the key covers.
  const root = await mkdtemp(join(outsideTreesTmpdir(), 'cargo-hash-operator-git-'));
  try {
    const { workspace } = await lockedWorkspaceCargoFixture(root);
    execFileSync('git', ['init', '--quiet', workspace], { stdio: 'pipe' });
    const bin = join(import.meta.dir, '../dist/bin/smoo-nx-cargo-hash.js');
    const run = async (home: string) => {
      // The operator's environment, as a host client sends it: its HOME, and none of the
      // git isolation a sandboxed job would carry.
      const env: Record<string, string | undefined> = { ...process.env, HOME: home };
      for (const name of [
        'GIT_CONFIG_GLOBAL',
        'GIT_CONFIG_NOSYSTEM',
        'GIT_CONFIG_SYSTEM',
        'GIT_CONFIG_COUNT',
        'XDG_CONFIG_HOME',
      ]) {
        delete env[name];
      }
      env.RUSTUP_HOME = process.env.RUSTUP_HOME ?? join(homedir(), '.rustup');
      const child = Bun.spawn(['node', bin, '--include-workspace'], {
        cwd: workspace,
        env,
        stdout: 'pipe',
        stderr: 'pipe',
      });
      const [exitCode, stdout, stderr] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ]);
      return { exitCode, stdout, stderr };
    };
    const clean = join(root, 'clean-home');
    await mkdir(clean);
    const baseline = await run(clean);
    expect(baseline.exitCode, baseline.stderr).toBe(0);

    // A global configuration git cannot access. A self-referential link fails access() with
    // ELOOP where the sandbox fails it with EPERM; git dies on either.
    const denied = join(root, 'denied-home');
    await mkdir(join(denied, '.config/git'), { recursive: true });
    await symlink('config', join(denied, '.config/git/config'));
    expect(await run(denied)).toEqual(baseline);

    // A global ignore file that would hide every source the key covers.
    const ignoring = join(root, 'ignoring-home');
    await mkdir(join(ignoring, '.config/git'), { recursive: true });
    await writeFile(join(ignoring, '.config/git/ignore'), '*.rs\n');
    expect(await run(ignoring)).toEqual(baseline);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it('real-cargo smoke: hashCargoPathInputs matches recorded metadata, drives helper and CLI runtime inputs, and refuses a missing manifest', async () => {
  const root = await mkdtemp(join(outsideTreesTmpdir(), 'cargo-hash-smoke-'));
  const dist = join(import.meta.dir, '../dist');
  try {
    const { root: canonical, workspace, manifest, leafSource } = await lockedWorkspaceCargoFixture(root);
    const recorded = loadRecordedCargoResolve(canonical);

    // Real cargo resolution produces the exact same digests as the recorded CargoMetadata fixture.
    const appClosure = { includeWorkspace: true, closure: join(workspace, 'packages/app') };
    expect(await hashCargoPathInputs(manifest, workspace)).toBe(await hashCargoInputs(recorded, workspace));
    expect(await hashCargoPathInputs(manifest, workspace, { includeWorkspace: true })).toBe(
      await hashCargoInputs(recorded, workspace, { includeWorkspace: true }),
    );
    expect(await hashCargoPathInputs(manifest, workspace, appClosure)).toBe(
      await hashCargoInputs(recorded, workspace, appClosure),
    );

    // Both runtime-input spellings (helper import and CLI binary) match hashCargoPathInputs before and after edits.
    const hashProgram =
      `import { hashCargoPathInputs } from ${JSON.stringify(join(dist, 'cargo-source-hash.js'))}; ` +
      'console.log(await hashCargoPathInputs("Cargo.toml", process.cwd(), { includeWorkspace: true }));';
    const helperRuntime = `bun -e '${hashProgram}'`;
    const cliRuntime = `bun ${JSON.stringify(join(dist, 'bin/smoo-nx-cargo-hash.js'))} --include-workspace`;

    const initialExpected = `${await hashCargoPathInputs(manifest, workspace, { includeWorkspace: true })}\n`;
    const [initialHelper, initialCli] = await Promise.all([
      runtimeInputValue(helperRuntime, workspace),
      runtimeInputValue(cliRuntime, workspace),
    ]);
    expect(initialHelper).toBe(initialExpected);
    expect(initialCli).toBe(initialExpected);

    await writeFile(leafSource, 'pub fn answer() -> u8 { 2 }\n');
    const editedExpected = `${await hashCargoPathInputs(manifest, workspace, { includeWorkspace: true })}\n`;
    expect(editedExpected).not.toBe(initialExpected);
    const [editedHelper, editedCli] = await Promise.all([
      runtimeInputValue(helperRuntime, workspace),
      runtimeInputValue(cliRuntime, workspace),
    ]);
    expect(editedHelper).toBe(editedExpected);
    expect(editedCli).toBe(editedExpected);

    // Missing path dependency manifest refuses resolution cleanly and recovers once restored.
    const externalManifest = join(canonical, 'external/Cargo.toml');
    const savedExternal = await readFile(externalManifest, 'utf8');
    await rm(externalManifest);
    await expect(hashCargoPathInputs(manifest, workspace)).rejects.toThrow('Locked offline Cargo resolution failed');
    await writeFile(externalManifest, savedExternal);
    expect(await hashCargoPathInputs(manifest, workspace, { includeWorkspace: true })).toBe(editedExpected.trimEnd());
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it('keeps the v2 transcript across forward source includes and an ignored source included through an alias', async () => {
  const root = await realpath(await mkdtemp(join(outsideTreesTmpdir(), 'cargo-hash-fused-')));
  const manifest = join(root, 'Cargo.toml');
  const contents = new Map([
    ['Cargo.toml', '[workspace]\nmembers=["a","z"]\nresolver="2"\n'],
    ['a/Cargo.toml', '[package]\nname="a"\nversion="0.1.0"\nedition="2021"\n'],
    ['z/Cargo.toml', '[package]\nname="z"\nversion="0.1.0"\nedition="2021"\n'],
    ['a/src/lib.rs', 'const Z: &str = include_str!("../../z/src/lib.rs");\n'],
    ['z/src/lib.rs', 'const H: &str = include_str!("../hidden.txt");\nconst P: &str = include_str!("payload.bin");\n'],
    ['z/src/hidden.rs', 'const S: &str = include_str!("secret.bin");\n'],
    ['z/src/payload.bin', 'payload'],
    ['z/src/secret.bin', 'secret'],
    ['.gitignore', 'z/src/hidden.rs\n'],
  ]);
  try {
    for (const [path, text] of contents) {
      await mkdir(dirname(join(root, path)), { recursive: true });
      await writeFile(join(root, path), text);
    }
    await symlink('src/hidden.rs', join(root, 'z/hidden.txt'));
    execFileSync('git', ['init', '--quiet', root], { stdio: 'pipe' });
    const cargo = resolveFrom({
      packages: [
        {
          id: 'a',
          name: 'a',
          version: '0.1.0',
          source: null,
          manifest_path: join(root, 'a/Cargo.toml'),
          targets: [{ src_path: join(root, 'a/src/lib.rs') }],
          governing_manifest_path: manifest,
        },
        {
          id: 'z',
          name: 'z',
          version: '0.1.0',
          source: null,
          manifest_path: join(root, 'z/Cargo.toml'),
          targets: [{ src_path: join(root, 'z/src/lib.rs') }],
          governing_manifest_path: manifest,
        },
      ],
      resolve: {
        nodes: [
          { id: 'a', deps: [] },
          { id: 'z', deps: [] },
        ],
      },
      workspace_members: ['a', 'z'],
      workspace_root: root,
    });
    // Independent explicit v2 transcript. In particular, z's source is named by a,
    // while hidden.rs is discovery-only until the unignored alias names its bytes.
    const inputs: { file: string; key: string }[] = [
      { file: manifest, key: JSON.stringify(['a', '0.1.0', 'ancestor', '1/Cargo.toml']) },
      { file: join(root, 'a/Cargo.toml'), key: JSON.stringify(['a', '0.1.0', 'file', 'Cargo.toml']) },
      { file: join(root, 'a/src/lib.rs'), key: JSON.stringify(['a', '0.1.0', 'file', 'src/lib.rs']) },
      {
        file: join(root, 'z/src/lib.rs'),
        key: JSON.stringify(['a', '0.1.0', 'include', 'src/lib.rs\0../../z/src/lib.rs']),
      },
      { file: join(root, 'z/Cargo.toml'), key: JSON.stringify(['z', '0.1.0', 'file', 'Cargo.toml']) },
      {
        file: join(root, 'z/src/secret.bin'),
        key: JSON.stringify(['z', '0.1.0', 'include', 'src/hidden.rs\0secret.bin']),
      },
      {
        file: join(root, 'z/src/hidden.rs'),
        key: JSON.stringify(['z', '0.1.0', 'include', 'src/lib.rs\0../hidden.txt']),
      },
      {
        file: join(root, 'z/src/payload.bin'),
        key: JSON.stringify(['z', '0.1.0', 'include', 'src/lib.rs\0payload.bin']),
      },
    ];
    for (let ancestor = dirname(root), depth = 2; ; ancestor = dirname(ancestor), depth += 1) {
      for (const name of CARGO_ANCESTOR_INPUTS) {
        const file = join(ancestor, name);
        if (statSync(file, { throwIfNoEntry: false })?.isFile()) {
          inputs.push({ file, key: JSON.stringify(['a', '0.1.0', 'ancestor', `${depth}/${name}`]) });
        }
      }
      if (dirname(ancestor) === ancestor) break;
    }
    const expected = createHash('sha256');
    expected.update('cargo-path-inputs-v2\0');
    expected.update(JSON.stringify(['z', '0.1.0', 'link', 'hidden.txt']));
    expected.update('\0symlink\0src/hidden.rs\0');
    for (const { file, key } of inputs.sort((left, right) =>
      left.key < right.key ? -1 : left.key > right.key ? 1 : 0,
    )) {
      const bytes = await readFile(file);
      expected.update(key);
      expected.update(`\0${bytes.length}\0`);
      expected.update(bytes);
    }
    const before = await hashCargoInputs(cargo, root, { includeWorkspace: true });
    expect(before).toBe(expected.digest('hex'));
    await writeFile(join(root, 'z/src/secret.bin'), 'changed');
    expect(await hashCargoInputs(cargo, root, { includeWorkspace: true })).not.toBe(before);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it('keeps literal discovery exact and refuses unreadable sources', async () => {
  const root = await realpath(await mkdtemp(join(outsideTreesTmpdir(), 'cargo-hash-literals-')));
  const manifest = join(root, 'Cargo.toml');
  const source = join(root, 'src/lib.rs');
  const unicode = 'spelling-e\u0301-\u{10000}-payload';
  try {
    await mkdir(dirname(source), { recursive: true });
    await writeFile(manifest, '[package]\nname="literals"\nversion="0.1.0"\nedition="2021"\n[workspace]\n');
    await writeFile(join(root, 'src/data'), 'data');
    await writeFile(join(root, 'src', unicode), 'unicode spelling');
    await writeFile(
      source,
      [
        'const A: &str = include_str!(r#"data"#,);',
        'const B: &[u8] = include_bytes!["data"];',
        'const C: &str = include_str!{"data"};',
        `const U: &str = include_str!(${JSON.stringify(unicode)});`,
        '// include_str!("missing")',
        '/* outer /* include_bytes!("missing") */ comment */',
        'const D: &str = r###"include_str!("missing")"###;',
        'include!("missing");',
        'include_str!("");',
        'include_str!("da\\\\ta");',
        'include_str!(concat!("da", "ta"));',
        'include_str!("missing");',
        'include_str!("data"];',
        'include_str!("unterminated',
      ].join('\n'),
    );
    expect(compiledInFiles(root, await indexedCargoSources(root), new Map())).toEqual([
      { file: join(root, 'src/data'), source, literal: 'data' },
      { file: join(root, 'src', unicode), source, literal: unicode },
    ]);
    const cargo = resolveFrom({
      packages: [
        {
          id: 'literals',
          name: 'literals',
          version: '0.1.0',
          source: null,
          manifest_path: manifest,
          targets: [{ src_path: source }],
          governing_manifest_path: manifest,
        },
      ],
      resolve: { nodes: [{ id: 'literals', deps: [] }] },
      workspace_members: ['literals'],
      workspace_root: root,
    });
    // A source text no scan has seen is read, so a read the operating system refuses is a refusal, never a skip.
    // Nx indexes only a file it can read: the new text is hashed first, then the scan's own read is denied.
    // Dated past the old mtime, as Nx reuses a file's hash within the second it was last hashed.
    const { mtimeMs } = await stat(source);
    await writeFile(source, `${await readFile(source, 'utf8')}\n// unread\n`);
    const edited = new Date(mtimeMs + 2000);
    await utimes(source, edited, edited);
    const unread = await indexedCargoSources(root);
    await chmod(source, 0);
    expect(() => compiledInFiles(root, unread, new Map())).toThrow('EACCES');
    // The digest walk must also preserve the operational refusal rather than skip the unreadable source.
    await expect(hashCargoInputs(cargo, root, { includeWorkspace: true })).rejects.toThrow('EACCES');
  } finally {
    await chmod(source, 0o600);
    await rm(root, { recursive: true, force: true });
  }
});
