import { expect, it } from 'bun:test';
import { execFileSync } from 'node:child_process';
import { mkdir, mkdtemp, readFile, realpath, rename, rm, symlink, writeFile } from 'node:fs/promises';
import { homedir, tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { useFixtureCargoHome } from './__tests__/fixture-cargo-home.js';
import { runtimeInputValue } from './__tests__/runtime-input.js';
import { hashCargoPathInputs } from './cargo-source-hash.js';

useFixtureCargoHome();

it.each(['app', '.'])('invalidates transitive and inherited Cargo inputs with Nx rooted at %s', async (nxDirectory) => {
  const root = await mkdtemp(join(tmpdir(), 'cargo-hash-'));
  const app = join(root, 'app');
  const manifest = join(app, 'Cargo.toml');
  const nxRoot = join(root, nxDirectory);
  async function put(path: string, text: string): Promise<void> {
    const file = join(root, path);
    await mkdir(dirname(file), { recursive: true });
    await writeFile(file, text);
  }
  try {
    await put(
      'app/Cargo.toml',
      '[package]\nname="app"\nversion="0.1.0"\nedition="2021"\n[dependencies]\nbridge={path="../external/bridge"}\n[workspace]\n',
    );
    await put('app/src/lib.rs', 'pub fn run() {}\n');
    await put(
      'external/Cargo.toml',
      '[workspace]\nmembers=["bridge","leaf"]\nresolver="2"\n[workspace.package]\nedition="2021"\n',
    );
    await put(
      'external/bridge/Cargo.toml',
      '[package]\nname="bridge"\nversion="0.1.0"\nedition.workspace=true\n[dependencies]\nleaf={path="../leaf"}\n',
    );
    await put('external/bridge/src/lib.rs', 'pub fn bridge() {}\n');
    await put('external/leaf/Cargo.toml', '[package]\nname="leaf"\nversion="0.1.0"\nedition.workspace=true\n');
    await put('external/leaf/src/lib.rs', 'pub fn leaf() -> u8 { 1 }\n');
    execFileSync('cargo', ['generate-lockfile', '--offline', '--manifest-path', manifest], { stdio: 'pipe' });
    const initial = await hashCargoPathInputs(manifest, nxRoot);
    await put('external/leaf/src/lib.rs', 'pub fn leaf() -> u8 { 2 }\n');
    const sourceChanged = await hashCargoPathInputs(manifest, nxRoot);
    expect(sourceChanged).not.toBe(initial);
    await put(
      'external/Cargo.toml',
      '[workspace]\nmembers=["bridge","leaf"]\nresolver="2"\n[workspace.package]\nedition="2024"\n',
    );
    const inheritedChanged = await hashCargoPathInputs(manifest, nxRoot);
    expect(inheritedChanged).not.toBe(sourceChanged);
    await put('external/leaf/target/noise.rs', 'uncompiled build output');
    expect(await hashCargoPathInputs(manifest, nxRoot)).toBe(inheritedChanged);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it('identical external inputs hash equally at different checkout depths, with bytes and file names still significant', async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'cargo-hash-portable-')));
  const shared = join(root, 'shared');
  const roots: readonly [string, string] = [join(root, 'main'), join(root, 'sheds/deep/unit')];
  try {
    await mkdir(join(shared, 'src'), { recursive: true });
    await writeFile(
      join(shared, 'Cargo.toml'),
      '[package]\nname="shared"\nversion="0.1.0"\nedition="2021"\n[workspace]\n',
    );
    const include = 'const DATA: &str = include_str!(' + JSON.stringify(join(root, 'data.txt')) + ');\n';
    await writeFile(join(shared, 'src/lib.rs'), 'pub fn value() -> u8 { 7 }\n' + include);
    await writeFile(join(shared, 'src/extra.rs'), 'pub const EXTRA: u8 = 9;\n');
    await writeFile(join(root, 'data.txt'), 'shared-data-one');
    const manifest =
      '[package]\nname="consumer"\nversion="0.1.0"\nedition="2021"\n[dependencies]\nshared={path=' +
      JSON.stringify(shared) +
      '}\n[workspace]\n';
    for (const workspace of roots) {
      await mkdir(join(workspace, 'src'), { recursive: true });
      await writeFile(join(workspace, 'Cargo.toml'), manifest);
      await writeFile(join(workspace, 'src/lib.rs'), 'pub fn value() -> u8 { shared::value() }\n' + include);
      await symlink(join(shared, 'src'), join(workspace, 'linked-src'), 'dir');
      execFileSync('cargo', ['generate-lockfile', '--offline', '--manifest-path', join(workspace, 'Cargo.toml')], {
        stdio: 'pipe',
      });
    }
    expect(await readFile(join(roots[0], 'Cargo.lock'), 'utf8')).toBe(
      await readFile(join(roots[1], 'Cargo.lock'), 'utf8'),
    );
    const before: string[] = [];
    const after: string[] = [];
    for (const includeWorkspace of [false, true]) {
      const hashes = await Promise.all(
        roots.map((workspace) => hashCargoPathInputs(join(workspace, 'Cargo.toml'), workspace, { includeWorkspace })),
      );
      expect(hashes[0]).toBeDefined();
      expect(hashes[1]).toBe(hashes[0]);
      before.push(hashes.join(','));
    }
    await writeFile(join(root, 'data.txt'), 'shared-data-two');
    for (const [index, includeWorkspace] of [false, true].entries()) {
      const hashes = await Promise.all(
        roots.map((workspace) => hashCargoPathInputs(join(workspace, 'Cargo.toml'), workspace, { includeWorkspace })),
      );
      expect(hashes[1]).toBe(hashes[0]);
      expect(hashes.join(',')).not.toBe(before[index]);
      after.push(hashes.join(','));
    }
    await rename(join(shared, 'src/extra.rs'), join(shared, 'src/renamed.rs'));
    const renamed = await Promise.all(
      roots.map((workspace) => hashCargoPathInputs(join(workspace, 'Cargo.toml'), workspace)),
    );
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
  const root = await mkdtemp(join(tmpdir(), 'cargo-hash-'));
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
  const root = await mkdtemp(join(tmpdir(), 'cargo-hash-refusal-'));
  try {
    const bin = join(import.meta.dir, '../dist/bin/smoo-nx-cargo-hash.js');
    const causes: string[] = [];
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
      // Cargo's own refusal reaches the reader of the failed input, without
      // the checkout path Nx would otherwise hash into the key.
      expect(stderr).toContain('unclosed table');
      expect(stderr).not.toContain(workspace);
      expect(stderr).not.toContain(await realpath(workspace));
      causes.push(stderr);
      // The cause is not the verdict: the real producer still refuses the
      // manifest with Cargo's operational cause.
      const producer = Bun.spawn(['cargo', 'build', '--locked', '--offline'], {
        cwd: workspace,
        env: process.env,
        stdout: 'pipe',
        stderr: 'pipe',
      });
      const [producerCode, producerError] = await Promise.all([
        producer.exited,
        new Response(producer.stderr).text(),
        new Response(producer.stdout).text(),
      ]);
      expect(producerCode).not.toBe(0);
      expect(producerError).toContain('Cargo.toml');
    }
    expect(causes[1]).toBe(causes[0]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

async function workspaceCargoFixture(root: string) {
  const workspace = join(root, 'workspace');
  const manifest = join(workspace, 'Cargo.toml');
  const sources = {
    'workspace/Cargo.toml':
      '[workspace]\nmembers=["packages/app","crates/bridge","crates/leaf","crates/unrelated"]\nresolver="2"\n',
    'workspace/packages/app/Cargo.toml':
      '[package]\nname="app"\nversion="0.1.0"\nedition="2021"\n[dependencies]\nbridge={path="../../crates/bridge"}\n',
    'workspace/packages/app/src/main.rs': 'fn main() { println!("{}", bridge::answer()); }\n',
    'workspace/crates/bridge/Cargo.toml':
      '[package]\nname="bridge"\nversion="0.1.0"\nedition="2021"\n[dependencies]\nleaf={path="../leaf"}\nexternal={path="../../../external"}\n',
    'workspace/crates/bridge/src/lib.rs': 'pub fn answer() -> u8 { leaf::answer() + external::answer() }\n',
    'workspace/crates/leaf/Cargo.toml': '[package]\nname="leaf"\nversion="0.1.0"\nedition="2021"\n',
    'workspace/crates/leaf/src/lib.rs': 'pub fn answer() -> u8 { 1 }\n',
    'workspace/crates/unrelated/Cargo.toml': '[package]\nname="unrelated"\nversion="0.1.0"\nedition="2021"\n',
    'workspace/crates/unrelated/src/lib.rs': 'pub fn unrelated() -> u8 { 3 }\n',
    'external/Cargo.toml': '[package]\nname="external"\nversion="0.1.0"\nedition="2021"\n[workspace]\n',
    'external/src/lib.rs': 'pub fn answer() -> u8 { 10 }\n',
  };
  for (const [path, text] of Object.entries(sources)) {
    const file = join(root, path);
    await mkdir(dirname(file), { recursive: true });
    await writeFile(file, text);
  }
  execFileSync('cargo', ['generate-lockfile', '--offline', '--manifest-path', manifest], { stdio: 'pipe' });
  return { workspace, manifest, leafSource: join(workspace, 'crates/leaf/src/lib.rs') };
}

it('include-workspace hashes in-workspace transitive source edits without changing the external-only contract', async () => {
  const root = await mkdtemp(join(tmpdir(), 'cargo-hash-workspace-'));
  try {
    const { workspace, manifest, leafSource } = await workspaceCargoFixture(root);
    const external = await hashCargoPathInputs(manifest, workspace);
    const included = await hashCargoPathInputs(manifest, workspace, { includeWorkspace: true });
    expect(await hashCargoPathInputs(manifest, workspace, { includeWorkspace: true })).toBe(included);

    await writeFile(leafSource, 'pub fn answer() -> u8 { 2 }\n');
    expect(await hashCargoPathInputs(manifest, workspace)).toBe(external);
    const changed = await hashCargoPathInputs(manifest, workspace, { includeWorkspace: true });
    expect(changed).not.toBe(included);
    expect(await hashCargoPathInputs(manifest, workspace, { includeWorkspace: true })).toBe(changed);

    await writeFile(join(root, 'external/src/lib.rs'), 'pub fn answer() -> u8 { 20 }\n');
    expect(await hashCargoPathInputs(manifest, workspace)).not.toBe(external);
    expect(await hashCargoPathInputs(manifest, workspace, { includeWorkspace: true })).not.toBe(changed);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it('include-workspace CLI accepts an optional manifest and refuses unsupported arguments', async () => {
  const root = await mkdtemp(join(tmpdir(), 'cargo-hash-cli-'));
  try {
    const { workspace, leafSource } = await workspaceCargoFixture(root);
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

    for (const args of [
      ['--unknown'],
      ['--include-workspace', '--unknown'],
      ['Cargo.toml', '--include-workspace'],
      ['--include-workspace', 'Cargo.toml', 'extra'],
      ['--include-workspace', '--include-workspace'],
      ['--closure'],
      ['--closure', '--include-workspace'],
      ['--closure', 'crates/leaf', '--closure', 'packages/app'],
    ]) {
      const refused = await run(args);
      expect(refused.exitCode, refused.stderr).toBe(2);
      expect(refused.stdout).toBe('cargo-input-invalid-arguments\n');
      expect(refused.stderr).toBe('');
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it('closure hashes the dependency closure of one directory and ignores unrelated workspace members', async () => {
  const root = await mkdtemp(join(tmpdir(), 'cargo-hash-closure-'));
  try {
    const { workspace, manifest, leafSource } = await workspaceCargoFixture(root);
    const app = { includeWorkspace: true, closure: join(workspace, 'packages/app') };
    const leaf = { includeWorkspace: true, closure: join(workspace, 'crates/leaf') };
    const initial = await hashCargoPathInputs(manifest, workspace, app);
    const leafOnly = await hashCargoPathInputs(manifest, workspace, leaf);
    const whole = await hashCargoPathInputs(manifest, workspace, { includeWorkspace: true });

    await writeFile(join(workspace, 'crates/unrelated/src/lib.rs'), 'pub fn unrelated() -> u8 { 4 }\n');
    expect(await hashCargoPathInputs(manifest, workspace, app)).toBe(initial);
    expect(await hashCargoPathInputs(manifest, workspace, { includeWorkspace: true })).not.toBe(whole);

    await writeFile(join(workspace, 'crates/bridge/src/lib.rs'), 'pub fn answer() -> u8 { leaf::answer() }\n');
    const bridgeChanged = await hashCargoPathInputs(manifest, workspace, app);
    expect(bridgeChanged).not.toBe(initial);
    expect(await hashCargoPathInputs(manifest, workspace, leaf)).toBe(leafOnly);

    await writeFile(leafSource, 'pub fn answer() -> u8 { 2 }\n');
    expect(await hashCargoPathInputs(manifest, workspace, app)).not.toBe(bridgeChanged);
    expect(await hashCargoPathInputs(manifest, workspace, leaf)).not.toBe(leafOnly);

    // The external package is reached through bridge, so the app closure sees it and the leaf closure does not.
    const appBefore = await hashCargoPathInputs(manifest, workspace, app);
    const leafBefore = await hashCargoPathInputs(manifest, workspace, leaf);
    await writeFile(join(root, 'external/src/lib.rs'), 'pub fn answer() -> u8 { 20 }\n');
    expect(await hashCargoPathInputs(manifest, workspace, app)).not.toBe(appBefore);
    expect(await hashCargoPathInputs(manifest, workspace, leaf)).toBe(leafBefore);

    await mkdir(join(workspace, 'docs'), { recursive: true });
    await expect(
      hashCargoPathInputs(manifest, workspace, { includeWorkspace: true, closure: join(workspace, 'docs') }),
    ).rejects.toThrow('no local Cargo package lies under');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it('leaves gitignored sources beneath a package out of the digest but keeps an ignored package root', async () => {
  const root = await mkdtemp(join(tmpdir(), 'cargo-hash-ignored-'));
  try {
    const { workspace, manifest } = await workspaceCargoFixture(root);
    execFileSync('git', ['init', '--quiet', workspace], { stdio: 'pipe' });
    await writeFile(join(workspace, '.gitignore'), 'target/\ngenerated.rs\ncrates/leaf/\n');
    const options = { includeWorkspace: true };
    const initial = await hashCargoPathInputs(manifest, workspace, options);

    // A source a producer generates later: a fresh tree and a built one hash alike.
    await writeFile(join(workspace, 'crates/bridge/src/generated.rs'), 'pub const GENERATED: u8 = 1;\n');
    expect(await hashCargoPathInputs(manifest, workspace, options)).toBe(initial);

    // An untracked source nobody ignores is an edit like any other.
    await writeFile(join(workspace, 'crates/bridge/src/extra.rs'), 'pub const EXTRA: u8 = 1;\n');
    const untracked = await hashCargoPathInputs(manifest, workspace, options);
    expect(untracked).not.toBe(initial);

    // A package whose own directory is ignored still hashes its sources.
    await writeFile(join(workspace, 'crates/leaf/src/lib.rs'), 'pub fn answer() -> u8 { 3 }\n');
    expect(await hashCargoPathInputs(manifest, workspace, options)).not.toBe(untracked);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it('hashes a Cargo workspace that lives inside a directory its enclosing repository ignores', async () => {
  // A sandbox's TMPDIR, or a checkout under another repository's ignored scratch directory: every
  // package sits beneath one ignored ancestor, where `git ls-files` refuses the pathspec outright.
  const repository = await mkdtemp(join(tmpdir(), 'cargo-hash-enclosed-'));
  try {
    execFileSync('git', ['init', '--quiet', repository], { stdio: 'pipe' });
    await writeFile(join(repository, '.gitignore'), 'scratch/\n');
    const { workspace, manifest, leafSource } = await workspaceCargoFixture(join(repository, 'scratch/deep'));
    const options = { includeWorkspace: true };
    const initial = await hashCargoPathInputs(manifest, workspace, options);
    await writeFile(leafSource, 'pub fn answer() -> u8 { 2 }\n');
    expect(await hashCargoPathInputs(manifest, workspace, options)).not.toBe(initial);
  } finally {
    await rm(repository, { recursive: true, force: true });
  }
});

it('the runtime input reads no git configuration from the operator home', async () => {
  // A host client's runtime input runs in a workspace's sandboxed daemon with the host's HOME
  // (cowshed 04_sandbox.md). Git must not read that home: the sandbox denies its global
  // configuration, and git dies on any error but a missing file; its global ignore file would
  // decide which sources the key covers.
  const root = await mkdtemp(join(tmpdir(), 'cargo-hash-operator-git-'));
  try {
    const { workspace } = await workspaceCargoFixture(root);
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

it.each(['helper', 'cli'])(
  'include-workspace gives a custom Nx cached Cargo consumer a runtime input that follows transitive edits via %s',
  async (mode) => {
    const root = await mkdtemp(join(tmpdir(), 'cargo-hash-consumer-'));
    const dist = join(import.meta.dir, '../dist');
    try {
      const { workspace, manifest, leafSource } = await workspaceCargoFixture(root);
      const hashProgram =
        `import { hashCargoPathInputs } from ${JSON.stringify(join(dist, 'cargo-source-hash.js'))}; ` +
        'console.log(await hashCargoPathInputs("Cargo.toml", process.cwd(), { includeWorkspace: true }));';
      // What a consumer's nx.json names as a `runtime` input; Nx keys the task on its stdout.
      const runtime =
        mode === 'helper'
          ? `bun -e '${hashProgram}'`
          : `bun ${JSON.stringify(join(dist, 'bin/smoo-nx-cargo-hash.js'))} --include-workspace`;

      const initial = await runtimeInputValue(runtime, workspace);
      expect(initial).toBe(`${await hashCargoPathInputs(manifest, workspace, { includeWorkspace: true })}\n`);
      expect(await runtimeInputValue(runtime, workspace)).toBe(initial);

      // Reached from the consumer only through bridge.
      await writeFile(leafSource, 'pub fn answer() -> u8 { 2 }\n');
      const edited = await runtimeInputValue(runtime, workspace);
      expect(edited).not.toBe(initial);
      expect(edited).toBe(`${await hashCargoPathInputs(manifest, workspace, { includeWorkspace: true })}\n`);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  },
);
