import { expect, it } from 'bun:test';
import { execFileSync } from 'node:child_process';
import { mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { useFixtureCargoHome } from './__tests__/fixture-cargo-home.js';
import { fixtureNxEnv, stopFixtureNxDaemon } from './__tests__/fixture-nx-env.js';
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
    // A runtime input inherits the Nx process's environment untouched. An Nx
    // running inside another Nx task carries that task's FORCE_COLOR, and a
    // shell may export NO_COLOR; with both set, Node warns on stderr under its
    // own pid the moment anything imports `node:util`, so each run would
    // hash differently.
    const env = { ...process.env, CARGO_HOME: join(root, '.cargo-home'), FORCE_COLOR: 'true', NO_COLOR: '1' };
    execFileSync('cargo', ['generate-lockfile', '--offline', '--manifest-path', manifest], { stdio: 'pipe', env });
    // The built bin under node, exactly as nx.json's runtime input invokes it (test depends on build).
    const bin = join(import.meta.dir, '..', 'dist', 'bin', 'smoo-nx-cargo-hash.js');
    const runs = await Promise.all(
      Array.from({ length: 8 }, async () => {
        const proc = Bun.spawn(['node', bin, manifest], {
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
      expect(refused.stdout).toBe('');
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

it.each(['helper', 'cli'])(
  'include-workspace keeps a custom Nx cached Cargo consumer fresh via %s',
  async (mode) => {
    const root = await realpath(await mkdtemp(join(tmpdir(), 'cargo-hash-nx-')));
    const repositoryRoot = join(import.meta.dir, '../../..');
    const dist = join(import.meta.dir, '../dist');
    try {
      const { workspace, leafSource } = await workspaceCargoFixture(root);
      execFileSync('git', ['init', '--quiet', workspace], { stdio: 'pipe' });
      await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
      await writeFile(join(workspace, '.gitignore'), 'node_modules/\ntarget/\ndist/\n.nx/\n');
      await writeFile(join(workspace, 'package.json'), '{"name":"cargo-consumer-fixture","private":true}\n');
      const hashProgram =
        `import { hashCargoPathInputs } from ${JSON.stringify(join(dist, 'cargo-source-hash.js'))}; ` +
        'console.log(await hashCargoPathInputs("Cargo.toml", process.cwd(), { includeWorkspace: true }));';
      await writeFile(
        join(workspace, 'nx.json'),
        JSON.stringify({
          namedInputs: {
            cargoSources: [
              '{workspaceRoot}/Cargo.lock',
              {
                runtime:
                  mode === 'helper'
                    ? `bun -e '${hashProgram}'`
                    : `bun ${JSON.stringify(join(dist, 'bin/smoo-nx-cargo-hash.js'))} --include-workspace`,
              },
            ],
          },
        }),
      );
      await writeFile(
        join(workspace, 'packages/app/project.json'),
        JSON.stringify({
          name: 'app',
          targets: {
            build: {
              executor: 'nx:run-commands',
              cache: true,
              inputs: ['{projectRoot}/**/*', '!{projectRoot}/dist/**/*', 'cargoSources'],
              outputs: ['{projectRoot}/dist'],
              options: { command: 'bun packages/app/build.ts' },
            },
          },
        }),
      );
      const executions = join(root, 'executions.log');
      const outputDirectory = join(workspace, 'packages/app/dist');
      const output = join(outputDirectory, 'result.txt');
      await writeFile(
        join(workspace, 'packages/app/build.ts'),
        `import { execFileSync } from 'node:child_process';
import { appendFile, mkdir, writeFile } from 'node:fs/promises';
const result = execFileSync('cargo', ['run', '--quiet', '--locked', '--offline', '--manifest-path', 'packages/app/Cargo.toml'], { encoding: 'utf8' });
await mkdir(${JSON.stringify(outputDirectory)}, { recursive: true });
await writeFile(${JSON.stringify(output)}, result);
await appendFile(${JSON.stringify(executions)}, result);
`,
      );
      const run = async () => {
        const child = Bun.spawn(['bun', join(repositoryRoot, 'node_modules/.bin/nx'), 'run', 'app:build'], {
          cwd: workspace,
          env: fixtureNxEnv(workspace),
          stdout: 'pipe',
          stderr: 'pipe',
        });
        const [exitCode, stdout, stderr] = await Promise.all([
          child.exited,
          new Response(child.stdout).text(),
          new Response(child.stderr).text(),
        ]);
        expect(exitCode, stdout + stderr).toBe(0);
      };

      await run();
      expect(await readFile(output, 'utf8')).toBe('11\n');
      expect(await readFile(executions, 'utf8')).toBe('11\n');
      await rm(outputDirectory, { recursive: true });
      await run();
      expect(await readFile(output, 'utf8')).toBe('11\n');
      expect(await readFile(executions, 'utf8')).toBe('11\n');

      await writeFile(leafSource, 'pub fn answer() -> u8 { 2 }\n');
      await run();
      expect(await readFile(output, 'utf8')).toBe('12\n');
      expect(await readFile(executions, 'utf8')).toBe('11\n12\n');
      await rm(outputDirectory, { recursive: true });
      await run();
      expect(await readFile(output, 'utf8')).toBe('12\n');
      expect(await readFile(executions, 'utf8')).toBe('11\n12\n');
    } finally {
      try {
        await stopFixtureNxDaemon(join(root, 'workspace'));
      } finally {
        await rm(root, { recursive: true, force: true });
      }
    }
  },
  120_000,
);
