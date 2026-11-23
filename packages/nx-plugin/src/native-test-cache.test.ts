import { expect, it } from 'bun:test';
import { mkdir, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { useFixtureCargoHome } from '@smoothbricks/cargo-retain/testing/fixture-cargo-home';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { readRecord, taskDirectory } from './executors/bounded-exec/verdict.js';

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

//#region smoo/cowshed!n/native-test-verdicts
it('replays native NX results without binaries and builds only the missed selections', async () => {
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
    const alpha = join(workspace, 'alpha/src/lib.rs');
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

    await writeFile(alpha, testSource('alpha', '// invalidate only alpha'));
    const miss = await nx();
    expect(miss.code, miss.log).toBe(0);
    expect(await readFile(mark('alpha'), 'utf8')).toBe('alpha');
    await absent('beta');
    expect(miss.log).toMatch(/nx run native-probe:cargo-test-beta +\[(?:local cache|existing outputs match the cache)/);

    await cleanMarks();
    const record = taskDirectory(join(workspace, '.nx/workspace-data'), 'native-probe:cargo-test-alpha');
    await writeFile(alpha, 'compile_error!("native fixture did not build");\n');
    const buildFailure = await nx();
    expect(buildFailure.code, buildFailure.log).not.toBe(0);
    expect(buildFailure.log).toContain('native fixture did not build');
    expect(buildFailure.log).toContain('Fixture command exited');
    expect(buildFailure.log).toMatch(/could not compile.*alpha/);
    expect(await readRecord(record)).toMatchObject({ verdict: { outcome: 'failed', tests: [] } });
    await absent('alpha');
    await absent('beta');

    await writeFile(alpha, testSource('alpha', 'panic!("native test failed");'));
    const testFailure = await nx();
    expect(testFailure.code, testFailure.log).not.toBe(0);
    expect(testFailure.log).toContain('native test failed');
    expect(testFailure.log).not.toContain('Fixture command exited');
    expect(await readRecord(record)).toMatchObject({
      verdict: { outcome: 'failed', tests: [expect.stringContaining('alpha_case')] },
    });
    expect(await readFile(mark('alpha'), 'utf8')).toBe('alpha');
    await absent('beta');
  });
});

it('prepares one real integration binary for five independent native selectors', async () => {
  await withNxFixture('native-shared-prep', async ({ workspace, socketDirectory }) => {
    const selectors = ['agent', 'generator', 'replay', 'provider', 'callback'];
    const pluginRoot = fileURLToPath(new URL('../../../node_modules', import.meta.url));
    await symlink(pluginRoot, join(workspace, 'node_modules'), 'dir');
    await writeFile(join(workspace, 'nx.json'), JSON.stringify({ plugins: ['@smoothbricks/nx-plugin'] }));
    const targets = Object.fromEntries(
      selectors.map((name) => [
        `test-${name}`,
        {
          executor: '@smoothbricks/nx-plugin:bounded-exec',
          cache: true,
          inputs: ['{projectRoot}/Cargo.toml', '{projectRoot}/Cargo.lock', '{projectRoot}/tests/**/*.rs'],
          options: {
            nativeTest: {
              cargoArgs: ['-p', 'native-probe', '--test', 'integration'],
              selectionArgs: ['-E', `test(=${name}_case)`],
            },
          },
        },
      ]),
    );
    await writeFile(
      join(workspace, 'package.json'),
      JSON.stringify({ name: 'native-probe', private: true, nx: { targets } }),
    );
    await writeFile(
      join(workspace, 'Cargo.toml'),
      '[package]\nname="native-probe"\nversion="0.1.0"\nedition="2021"\n',
    );
    await writeFile(join(workspace, '.gitignore'), 'node_modules/\ntarget/\n.nx/\n*.log\n.bin/\n');
    await mkdir(join(workspace, 'tests'));
    await mkdir(join(workspace, '.bin'));
    await mkdir(socketDirectory, { recursive: true });
    const source = join(workspace, 'tests/integration.rs');
    const fixtureSource = (version: string) =>
      selectors
        .map(
          (name) => `#[test] fn ${name}_case() {
            let path = std::env::var_os("CARGO_MANIFEST_DIR").unwrap();
            let root = std::path::Path::new(&path);
            std::fs::write(root.join("${name}.log"), "${version}").unwrap();
          }\n`,
        )
        .join('');
    await writeFile(source, fixtureSource('initial'));
    const toolPath = (process.env.PATH ?? '')
      .split(':')
      .filter((directory) => directory !== process.env.COWSHED_CARGO_ROUTE)
      .join(':');
    const cargo = Bun.which('cargo', { PATH: toolPath });
    if (cargo === null) throw new Error('the fixture requires real Cargo');
    const trace = join(workspace, 'prep.log');
    await writeFile(
      join(workspace, '.bin/cargo'),
      `#!/bin/sh
case " $* " in
  *' nextest list '*' --list-type binaries-only '*) printf '%s\\n' "$*" >>"$NATIVE_PREP_TRACE" ;;
esac
exec "$NATIVE_REAL_CARGO" "$@"
`,
      { mode: 0o755 },
    );
    const env = {
      ...fixtureNxEnv(workspace, socketDirectory),
      TMPDIR: socketDirectory,
      PATH: `${join(workspace, '.bin')}:${toolPath}`,
      NATIVE_PREP_TRACE: trace,
      NATIVE_REAL_CARGO: cargo,
    };
    const run = async (args: string[]) => {
      const child = Bun.spawn(args, { cwd: workspace, env, stdout: 'pipe', stderr: 'pipe' });
      const [code, stdout, stderr] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ]);
      expect(code, stdout + stderr).toBe(0);
    };
    await run(['git', 'init', '--quiet']);
    await run(['cargo', 'generate-lockfile', '--offline']);
    const nx = () =>
      run(['bun', 'nx', 'run-many', '-p', 'native-probe', '-t', ...Object.keys(targets), '--outputStyle=static']);
    const observed = () => readFile(trace, 'utf8').then((log) => log.trim().split('\n'));
    await nx();
    for (const name of selectors) {
      expect(await readFile(join(workspace, `${name}.log`), 'utf8')).toBe('initial');
      expect(
        await readRecord(taskDirectory(join(workspace, '.nx/workspace-data'), `native-probe:test-${name}`)),
      ).toMatchObject({ verdict: { outcome: 'passed' } });
    }
    expect(await observed()).toHaveLength(1);

    await nx();
    expect(await observed()).toHaveLength(1);
    await writeFile(source, fixtureSource('revised build input'));
    await nx();
    expect(await observed()).toHaveLength(2);
    for (const name of selectors) {
      expect(await readFile(join(workspace, `${name}.log`), 'utf8')).toBe('revised build input');
    }
  });
});
//#endregion smoo/cowshed!n/native-test-verdicts
