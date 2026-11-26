import { expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { useFixtureCargoHome } from '@smoothbricks/cargo-retain/testing/fixture-cargo-home';
import type { TargetConfiguration } from 'nx/src/devkit-exports.js';
import { WorkspaceContext } from 'nx/src/native/index.js';
import { resetWorkspaceContext } from 'nx/src/utils/workspace-context.js';
import { prepareCargoFixture } from './__tests__/cargo-fixture.js';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { createNodesV2 } from './index.js';

useFixtureCargoHome();

const probes = ['alpha', 'beta'] as const;

function testSource(name: string, body: string): string {
  return `#[test]
fn ${name}_case() {
    std::fs::write(std::path::Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("ran.log"), "${name}").unwrap();
    std::fs::write(std::path::Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("published-report.txt"), "${name}").unwrap();
    ${body}
}
`;
}

function filesetGlobs(target: TargetConfiguration | undefined): {
  readonly positive: string[];
  readonly negative: string[];
} {
  const strings = (target?.inputs ?? []).filter(
    (input): input is string => typeof input === 'string' && input.includes('{workspaceRoot}/'),
  );
  return {
    positive: strings.filter((input) => !input.startsWith('!')).map((input) => input.slice('{workspaceRoot}/'.length)),
    negative: strings.filter((input) => input.startsWith('!')).map((input) => input.slice('!{workspaceRoot}/'.length)),
  };
}

//#region smoo/cowshed!n/native-test-verdicts
it('replays native NX results without binaries and restores declared outputs on a cache hit', async () => {
  await withNxFixture('native-result-cache', async ({ workspace, socketDirectory }) => {
    await symlink(
      fileURLToPath(new URL('../../../node_modules', import.meta.url)),
      join(workspace, 'node_modules'),
      'dir',
    );
    const plugin: unknown = await Bun.file(join(workspace, 'node_modules/@smoothbricks/nx-plugin/package.json')).json();
    if (typeof plugin !== 'object' || plugin === null || !('version' in plugin) || typeof plugin.version !== 'string') {
      throw new Error('the fixture Nx plugin has no installed version');
    }
    const { version } = plugin;
    await writeFile(
      join(workspace, 'package-lock.json'),
      JSON.stringify({
        name: 'native-probe',
        lockfileVersion: 3,
        packages: {
          '': { name: 'native-probe', devDependencies: { '@smoothbricks/nx-plugin': version } },
          'node_modules/@smoothbricks/nx-plugin': { version, dev: true },
        },
      }),
    );
    await writeFile(join(workspace, 'nx.json'), JSON.stringify({ plugins: ['@smoothbricks/nx-plugin'] }));
    await writeFile(
      join(workspace, 'package.json'),
      JSON.stringify({
        name: 'native-probe',
        private: true,
        devDependencies: { '@smoothbricks/nx-plugin': version },
        nx: {
          targets: {
            'cargo-test-alpha': { outputs: ['{workspaceRoot}/alpha/published-report.txt'] },
          },
        },
      }),
    );
    await writeFile(join(workspace, '.gitignore'), 'node_modules/\ntarget/\n.nx/\n*.log\npublished-report.txt\n');
    await writeFile(
      join(workspace, 'Cargo.toml'),
      '[workspace]\nmembers=["alpha","beta","unselected"]\nresolver="2"\n',
    );
    for (const name of probes) {
      await mkdir(join(workspace, name, 'src'), { recursive: true });
      await writeFile(
        join(workspace, name, 'Cargo.toml'),
        `[package]\nname="${name}"\nversion="0.1.0"\nedition="2021"\n`,
      );
      await writeFile(join(workspace, name, 'src/lib.rs'), testSource(name, ''));
      await writeFile(join(workspace, name, 'src/main.rs'), 'fn main() {}\n');
    }
    await mkdir(join(workspace, 'unselected', 'src'), { recursive: true });
    await writeFile(
      join(workspace, 'unselected/Cargo.toml'),
      '[package]\nname="unselected"\nversion="0.1.0"\nedition="2021"\n',
    );
    await writeFile(join(workspace, 'unselected/src/lib.rs'), 'compile_error!("this crate is not selected");\n');
    await mkdir(socketDirectory, { recursive: true });
    const env = { ...fixtureNxEnv(workspace, socketDirectory), TMPDIR: socketDirectory };
    const run = async (args: string[]) => {
      const child = Bun.spawn(args, { cwd: workspace, env, stdout: 'pipe', stderr: 'pipe' });
      const [code, stdout, stderr] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ]);
      return { code, log: stdout + stderr };
    };
    for (const command of [
      ['git', 'init', '--quiet'],
      ['cargo', 'generate-lockfile', '--offline'],
    ]) {
      const result = await run(command);
      expect(result.code, result.log).toBe(0);
    }
    const nx = () =>
      run([
        'bun',
        'nx',
        'run-many',
        '-p',
        'native-probe',
        '-t',
        ...probes.map((name) => `cargo-test-${name}`),
        '--outputStyle=static',
        '--skipRemoteCache',
      ]);
    const mark = (name: string) => join(workspace, name, 'ran.log');
    const absent = (name: string) => expect(readFile(mark(name), 'utf8')).rejects.toMatchObject({ code: 'ENOENT' });
    const cleanMarks = () => Promise.all(probes.map((name) => rm(mark(name), { force: true })));
    const report = join(workspace, 'alpha/published-report.txt');

    const cold = await nx();
    expect(cold.code, cold.log).toBe(0);
    expect(cold.log).not.toContain('cargo-test-build');
    for (const name of probes) expect(await readFile(mark(name), 'utf8')).toBe(name);
    expect(await readFile(report, 'utf8')).toBe('alpha');

    await cleanMarks();
    await rm(join(workspace, 'target'), { force: true, recursive: true });
    await rm(report);
    const hit = await nx();
    expect(hit.code, hit.log).toBe(0);
    expect(hit.log).toMatch(/read the output from the cache instead of running the command for (?:2|3) out of 3 tasks/);
    for (const name of probes) await absent(name);
    await expect(readFile(join(workspace, 'target/.rustc_info.json'))).rejects.toMatchObject({ code: 'ENOENT' });
    expect(await readFile(report, 'utf8')).toBe('alpha');
  });
});

// Laws: specs/cowshed/laws/cache_key.bend `key_has_only_reads` (:599) and `changed_input_changes_key` (:838).
// Each per-crate runner declares only its own crate's sources (plus workspace manifests), never a sibling crate's, so
// an edit to one member re-keys only that member's runner.
it('keys each per-crate native test runner on its own crate sources and not sibling or unselected crates', async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'smoothbricks-native-test-inputs-')));
  try {
    await writeFile(join(root, 'package.json'), JSON.stringify({ name: 'native-probe', private: true }));
    await writeFile(join(root, 'Cargo.toml'), '[workspace]\nmembers=["alpha","beta","unselected"]\nresolver="2"\n');
    for (const name of [...probes, 'unselected'] as const) {
      await mkdir(join(root, name, 'src'), { recursive: true });
      await writeFile(join(root, name, 'Cargo.toml'), `[package]\nname="${name}"\nversion="0.1.0"\nedition="2021"\n`);
      await writeFile(join(root, name, 'src/lib.rs'), `pub fn ${name}() {}\n`);
    }
    await prepareCargoFixture(root);
    resetWorkspaceContext();
    const [, infer] = createNodesV2;
    const results = await infer(['package.json'], undefined, { workspaceRoot: root, nxJsonConfiguration: {} });
    const targets = results[0]?.[1].projects?.['.']?.targets ?? {};
    const alphaTarget = targets['cargo-test-alpha'];
    const betaTarget = targets['cargo-test-beta'];

    expect(alphaTarget?.inputs).toContain('{workspaceRoot}/alpha/**/*');
    expect(alphaTarget?.inputs).not.toContain('{workspaceRoot}/beta/**/*');
    expect(alphaTarget?.inputs).not.toContain('{workspaceRoot}/unselected/**/*');

    expect(betaTarget?.inputs).toContain('{workspaceRoot}/beta/**/*');
    expect(betaTarget?.inputs).not.toContain('{workspaceRoot}/alpha/**/*');
    expect(betaTarget?.inputs).not.toContain('{workspaceRoot}/unselected/**/*');

    const context = new WorkspaceContext(root, join(root, '.nx/input-hash-context'));
    const alphaGlobs = filesetGlobs(alphaTarget);
    const betaGlobs = filesetGlobs(betaTarget);
    const alphaBefore = context.hashFilesMatchingGlob(alphaGlobs.positive, alphaGlobs.negative);
    const betaBefore = context.hashFilesMatchingGlob(betaGlobs.positive, betaGlobs.negative);

    await writeFile(join(root, 'alpha/src/lib.rs'), 'pub fn alpha() {} // invalidate only alpha\n');
    context.incrementalUpdate(['alpha/src/lib.rs'], []);
    const alphaAfterAlphaEdit = context.hashFilesMatchingGlob(alphaGlobs.positive, alphaGlobs.negative);
    expect(alphaAfterAlphaEdit).not.toBe(alphaBefore);
    expect(context.hashFilesMatchingGlob(betaGlobs.positive, betaGlobs.negative)).toBe(betaBefore);

    await writeFile(join(root, 'beta/src/lib.rs'), 'pub fn beta() {} // invalidate only beta\n');
    context.incrementalUpdate(['beta/src/lib.rs'], []);
    const betaAfterBetaEdit = context.hashFilesMatchingGlob(betaGlobs.positive, betaGlobs.negative);
    expect(betaAfterBetaEdit).not.toBe(betaBefore);
    expect(context.hashFilesMatchingGlob(alphaGlobs.positive, alphaGlobs.negative)).toBe(alphaAfterAlphaEdit);

    await writeFile(join(root, 'unselected/src/lib.rs'), 'compile_error!("changed unselected");\n');
    context.incrementalUpdate(['unselected/src/lib.rs'], []);
    expect(context.hashFilesMatchingGlob(alphaGlobs.positive, alphaGlobs.negative)).toBe(alphaAfterAlphaEdit);
    expect(context.hashFilesMatchingGlob(betaGlobs.positive, betaGlobs.negative)).toBe(betaAfterBetaEdit);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
//#endregion smoo/cowshed!n/native-test-verdicts
