import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { artifacts, cargoEnv, cargoOk } from '@smoothbricks/cargo-retain/testing/cargo-fixture';
import { useFixtureCargoHome } from '@smoothbricks/cargo-retain/testing/fixture-cargo-home';
import { outsideTreesTmpdir } from '@smoothbricks/cargo-retain/testing/outside-trees';
import { buildScriptEnvironmentDrift, cargoConfigFiles, cargoSees, describeDrift } from './cargo-build-script-env.js';

useFixtureCargoHome();

// One package whose build script reads a plain variable, a variable its `.cargo/config.toml` forces, and one in
// Cargo's own namespace, each through `rerun-if-env-changed`, as cc-rs does for CC_<target>.
const BUILD_RS = `fn main() {
    for name in ["SMOO_DRIFT_PROBE", "SMOO_FORCED_PROBE", "CARGO_SMOO_PROBE"] {
        println!("cargo:rerun-if-env-changed={name}");
    }
}
`;

let root = '';
let configText = '';

beforeAll(async () => {
  root = await realpath(await mkdtemp(join(outsideTreesTmpdir(), 'cargo-build-script-env-')));
  await mkdir(join(root, 'src'));
  await mkdir(join(root, '.cargo'));
  await writeFile(
    join(root, 'Cargo.toml'),
    '[package]\nname = "probe"\nversion = "0.1.0"\nedition = "2021"\n\n[workspace]\n',
  );
  await writeFile(join(root, 'build.rs'), BUILD_RS);
  await writeFile(join(root, 'src/lib.rs'), '');
  await writeFile(join(root, '.cargo/config.toml'), '[env]\nSMOO_FORCED_PROBE = { value = "pinned", force = true }\n');
  configText = await readFile(join(root, '.cargo/config.toml'), 'utf8');
});

afterAll(async () => {
  await rm(root, { recursive: true, force: true });
});

/** Build the probe with `SMOO_DRIFT_PROBE` as given; whether Cargo rebuilt its library. */
async function libraryRebuilt(probe: string | undefined): Promise<boolean> {
  const env = { ...cargoEnv(), CARGO_TARGET_DIR: join(root, 'target'), SMOO_FORCED_PROBE: 'ambient' };
  delete env.SMOO_DRIFT_PROBE;
  delete env.CARGO_SMOO_PROBE;
  if (probe !== undefined) env.SMOO_DRIFT_PROBE = probe;
  const run = await cargoOk(root, ['build', '--offline', '--message-format=json'], env);
  const library = artifacts(run).find((artifact) => artifact.name === 'probe');
  if (library === undefined) throw new Error(`cargo built no probe library\n${run.stdout}${run.stderr}`);
  return !library.fresh;
}

function driftUnder(environment: Record<string, string>) {
  const sees = cargoSees([{ path: join(root, '.cargo/config.toml'), root, text: configText }], environment);
  if (!sees.ok) throw new Error(sees.error);
  const drift = buildScriptEnvironmentDrift(join(root, 'target', 'debug'), sees.value);
  if (!drift.ok) throw new Error(drift.error);
  return drift.value;
}

describe('build-script environment drift', () => {
  it('names exactly the variable a real Cargo build reruns its build script for, and only while it differs', async () => {
    expect(await libraryRebuilt('seed')).toBe(true);
    // The forced entry is Cargo's whatever the shell says, and Cargo's own namespace is no shell's.
    expect(driftUnder({ SMOO_DRIFT_PROBE: 'seed', SMOO_FORCED_PROBE: 'other', CARGO_SMOO_PROBE: 'nested' })).toEqual(
      [],
    );
    expect(await libraryRebuilt('seed')).toBe(false);

    const drift = driftUnder({ SMOO_DRIFT_PROBE: 'shell' });
    expect(drift).toEqual([
      { variable: 'SMOO_DRIFT_PROBE', recorded: 'seed', current: 'shell', packages: ['probe'], runs: 1 },
    ]);
    const [only] = drift;
    if (only === undefined) throw new Error('no drift');
    expect(describeDrift(only)).toBe(
      'SMOO_DRIFT_PROBE: 1 build-script run(s) of probe recorded "seed"; Cargo now sees "shell", so it reruns them and rebuilds what depends on them',
    );
    expect(driftUnder({})).toEqual([
      { variable: 'SMOO_DRIFT_PROBE', recorded: 'seed', current: null, packages: ['probe'], runs: 1 },
    ]);
    // Cargo agrees: the drifted value reruns the script and rebuilds the library, and then the records match.
    expect(await libraryRebuilt('shell')).toBe(true);
    expect(driftUnder({ SMOO_DRIFT_PROBE: 'shell' })).toEqual([]);
    expect(await libraryRebuilt('shell')).toBe(false);
  });

  it('has nothing to compare in a target directory Cargo never built', () => {
    const sees = cargoSees([], {});
    if (!sees.ok) throw new Error(sees.error);
    expect(buildScriptEnvironmentDrift(join(root, 'never-built'), sees.value)).toEqual({ ok: true, value: [] });
  });

  it('refuses a record it cannot read, naming the file', async () => {
    const fingerprint = join(root, 'broken', 'build', 'probe', '0123456789abcdef', 'fingerprint');
    await mkdir(fingerprint, { recursive: true });
    await writeFile(join(fingerprint, 'run-build-script-build-script-build.json'), '{"deps": 1.5}');
    // A stray file where Cargo keeps a package directory holds no runs; it does not blank the report.
    await writeFile(join(root, 'broken', 'build', 'stray'), '');
    const sees = cargoSees([], {});
    if (!sees.ok) throw new Error(sees.error);
    const drift = buildScriptEnvironmentDrift(join(root, 'broken'), sees.value);
    expect(drift.ok).toBe(false);
    if (!drift.ok) expect(drift.error).toContain(join(fingerprint, 'run-build-script-build-script-build.json'));
  });

  it('refuses an [env] entry Cargo would not accept, naming its file', () => {
    const sees = cargoSees(
      [{ path: '/x/.cargo/config.toml', root: '/x', text: '[env]\nSMOO = { force = true }\n' }],
      {},
    );
    expect(sees.ok).toBe(false);
    if (!sees.ok) expect(sees.error).toContain('/x/.cargo/config.toml');
  });

  it("reads Cargo's configuration chain: the deepest entry decides, relative to its own file, then CARGO_HOME's", async () => {
    const inner = join(root, 'chain', 'inner');
    const home = join(root, 'chain-home');
    await mkdir(join(inner, '.cargo'), { recursive: true });
    await mkdir(join(root, 'chain', '.cargo'), { recursive: true });
    await mkdir(home, { recursive: true });
    await writeFile(join(root, 'chain', '.cargo', 'config.toml'), '[env]\nSMOO_DEEP = "outer"\nSMOO_OUTER = "outer"\n');
    await writeFile(join(inner, '.cargo', 'config.toml'), '[env]\nSMOO_DEEP = { value = "tools", relative = true }\n');
    await writeFile(join(home, 'config.toml'), '[env]\nSMOO_HOME = { value = "home", force = true }\n');
    const files = cargoConfigFiles(inner, home);
    if (!files.ok) throw new Error(files.error);
    // The fixture's own root holds the probe's configuration; the walk above it finds nothing (outsideTreesTmpdir).
    expect(files.value.map((file) => file.path)).toEqual([
      join(inner, '.cargo', 'config.toml'),
      join(root, 'chain', '.cargo', 'config.toml'),
      join(root, '.cargo', 'config.toml'),
      join(home, 'config.toml'),
    ]);
    const sees = cargoSees(files.value, { SMOO_HOME: 'shell', SMOO_OUTER: 'shell' });
    if (!sees.ok) throw new Error(sees.error);
    expect(sees.value('SMOO_DEEP')).toBe(join(inner, 'tools'));
    expect(sees.value('SMOO_OUTER')).toBe('shell');
    expect(sees.value('SMOO_HOME')).toBe('home');
    expect(sees.value('SMOO_FORCED_PROBE')).toBe('pinned');
    expect(sees.value('SMOO_ABSENT')).toBeNull();
  });
});
