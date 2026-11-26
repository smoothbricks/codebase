import { expect, it } from 'bun:test';
import { execFileSync, spawn } from 'node:child_process';
import { appendFile, mkdir, mkdtemp, readdir, readFile, realpath, rm, utimes, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { delimiter, dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { useFixtureCargoHome } from '@smoothbricks/cargo-retain/testing/fixture-cargo-home';
import { type CountedCargo, countedCargo, guardEvent } from '../../__tests__/counted-cargo.js';
import { FIXTURE_STORE_ENV } from './native-test.js';

useFixtureCargoHome();

const LAYOUT_SCRIPT = fileURLToPath(new URL('./native-fixture-layout.ts', import.meta.url));
const REAL_PATH = process.env.PATH ?? '';

interface LayoutOutcome {
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
  readonly stdout: string;
  readonly stderr: string;
  readonly fixtureDir: string;
}

interface SpawnedLayout {
  readonly pid: number;
  readonly fixtureDir: string;
  readonly done: Promise<LayoutOutcome>;
  killGroup(): void;
}

interface FixtureWorkspace {
  readonly root: string;
  readonly workspace: string;
  readonly store: string;
  readonly cargo: CountedCargo;
  readonly storeAnswer: string;
  spawnLayout(cargoArgs: readonly string[], extraEnv?: Readonly<Record<string, string>>): Promise<SpawnedLayout>;
  runLayout(cargoArgs: readonly string[], extraEnv?: Readonly<Record<string, string>>): Promise<LayoutOutcome>;
}

/** Backdate `paths` so their mtimes precede a subsequent flight start even on coarse-tick filesystems. */
async function backdate(...paths: readonly string[]): Promise<void> {
  const past = new Date(Date.now() - 5_000);
  for (const path of paths) await utimes(path, past, past);
}

async function put(root: string, relativePath: string, text: string): Promise<string> {
  const file = join(root, relativePath);
  const parent = dirname(file);
  await mkdir(parent, { recursive: true });
  await writeFile(file, text);
  await backdate(file, parent);
  return file;
}

function cleanEnvironment(pathValue: string): Record<string, string> {
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (
      value === undefined ||
      key === 'CARGO_MAKEFLAGS' ||
      key === 'CARGO_BUILD_JOBS' ||
      key === 'CARGO_TARGET_DIR' ||
      key === 'COWSHED_CARGO_ROUTE' ||
      key.startsWith('MBX_')
    ) {
      continue;
    }
    env[key] = value;
  }
  env.PATH = pathValue;
  return env;
}

function baseEnvironment(cargo: CountedCargo, store: string): Record<string, string> {
  return {
    ...cleanEnvironment(`${cargo.bin}${delimiter}${REAL_PATH}`),
    [FIXTURE_STORE_ENV]: store,
  };
}

function realCargoMetadata(workspace: string, extraArgs: readonly string[] = []): string {
  return execFileSync('cargo', ['--frozen', 'metadata', '--format-version', '1', '--all-features', ...extraArgs], {
    cwd: workspace,
    encoding: 'utf8',
    env: cleanEnvironment(REAL_PATH),
    maxBuffer: 256 * 1024 * 1024,
  });
}

function sortJsonKeys(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortJsonKeys);
  if (typeof value !== 'object' || value === null) return value;
  const sorted: Record<string, unknown> = {};
  for (const key of Object.keys(value).sort()) {
    sorted[key] = sortJsonKeys(Reflect.get(value, key));
  }
  return sorted;
}

async function withWorkspace(body: (fixture: FixtureWorkspace) => Promise<void>): Promise<void> {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'smoo-fixture-metadata-')));
  const workspace = join(root, 'workspace');
  const store = join(root, 'store');
  const open = new Set<SpawnedLayout>();
  let cargo: CountedCargo | null = null;
  try {
    await put(workspace, 'Cargo.toml', '[workspace]\nresolver = "2"\nmembers = ["alpha", "beta"]\n');
    await put(workspace, 'alpha/Cargo.toml', '[package]\nname = "alpha"\nversion = "0.1.0"\nedition = "2021"\n');
    await put(workspace, 'alpha/src/lib.rs', 'pub fn alpha() -> u8 { 1 }\n');
    await put(workspace, 'alpha/tests/smoke.rs', '#[test] fn smoke() { assert_eq!(alpha::alpha(), 1); }\n');
    await put(
      workspace,
      'beta/Cargo.toml',
      '[package]\nname = "beta"\nversion = "0.1.0"\nedition = "2021"\n\n[dependencies]\nalpha = { path = "../alpha" }\n',
    );
    await put(workspace, 'beta/src/lib.rs', 'pub fn beta() -> u8 { alpha::alpha() + 1 }\n');
    await put(workspace, 'beta/src/main.rs', 'fn main() { println!("{}", beta::beta()); }\n');
    await put(workspace, 'beta/src/bin/tool.rs', 'fn main() { println!("tool"); }\n');
    execFileSync('cargo', ['generate-lockfile', '--offline'], {
      cwd: workspace,
      env: cleanEnvironment(REAL_PATH),
      stdio: 'pipe',
    });
    await backdate(
      join(workspace, 'Cargo.lock'),
      join(workspace, 'Cargo.toml'),
      join(workspace, 'alpha'),
      join(workspace, 'alpha/Cargo.toml'),
      join(workspace, 'alpha/src'),
      join(workspace, 'alpha/tests'),
      join(workspace, 'beta'),
      join(workspace, 'beta/Cargo.toml'),
      join(workspace, 'beta/src'),
      join(workspace, 'beta/src/bin'),
    );

    const counted = await countedCargo(root);
    cargo = counted;
    const storeAnswer = join(store, encodeURIComponent(workspace), 'answer');

    const spawnLayout = async (
      cargoArgs: readonly string[],
      extraEnv: Readonly<Record<string, string>> = {},
    ): Promise<SpawnedLayout> => {
      const fixtureDir = await mkdtemp(join(root, 'fixture-'));
      const child = spawn(process.execPath, [LAYOUT_SCRIPT, fixtureDir, ...cargoArgs], {
        cwd: workspace,
        env: { ...baseEnvironment(counted, store), ...extraEnv },
        detached: process.platform !== 'win32',
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      const pid = child.pid;
      if (pid === undefined) throw new Error('layout script did not spawn');
      const stdoutChunks: Buffer[] = [];
      const stderrChunks: Buffer[] = [];
      child.stdout.on('data', (chunk: Buffer) => stdoutChunks.push(chunk));
      child.stderr.on('data', (chunk: Buffer) => stderrChunks.push(chunk));
      const exited = new Promise<LayoutOutcome>((resolve, reject) => {
        child.once('error', reject);
        child.once('close', (code, signal) => {
          resolve({
            code,
            signal,
            stdout: Buffer.concat(stdoutChunks).toString('utf8'),
            stderr: Buffer.concat(stderrChunks).toString('utf8'),
            fixtureDir,
          });
        });
      });
      const done = guardEvent(
        exited,
        `layout script (${cargoArgs.join(' ')}) to exit`,
        () =>
          `pid ${pid}, stdout=${JSON.stringify(Buffer.concat(stdoutChunks).toString('utf8'))}, stderr=${JSON.stringify(Buffer.concat(stderrChunks).toString('utf8'))}`,
      );
      const handle: SpawnedLayout = {
        pid,
        fixtureDir,
        done,
        killGroup: () => {
          try {
            process.kill(process.platform === 'win32' ? pid : -pid, 'SIGKILL');
          } catch (error) {
            if (!(error instanceof Error && 'code' in error && error.code === 'ESRCH')) throw error;
          }
        },
      };
      open.add(handle);
      void done.finally(() => open.delete(handle));
      return handle;
    };

    const runLayout = async (
      cargoArgs: readonly string[],
      extraEnv: Readonly<Record<string, string>> = {},
    ): Promise<LayoutOutcome> => (await spawnLayout(cargoArgs, extraEnv)).done;

    await body({ root, workspace, store, cargo: counted, storeAnswer, spawnLayout, runLayout });
  } finally {
    for (const child of open) child.killGroup();
    if (cargo !== null) await cargo.release();
    await Promise.allSettled([...open].map((child) => child.done));
    await rm(root, { recursive: true, force: true });
  }
}

//#region smoo/cowshed!n/native-test-verdicts
it('reuses stored Cargo metadata across unchanged repeats, sibling packages and jobserver env changes, and re-runs once per real input change', async () => {
  await withWorkspace(async ({ root, workspace, cargo, runLayout }) => {
    const first = await runLayout(['-p', 'alpha']);
    expect(first.code, first.stderr).toBe(0);
    expect(first.stdout).toBe('shared');
    expect((await cargo.runs()).spawned).toBe(1);
    const firstMetadata = await readFile(join(first.fixtureDir, 'cargo-metadata.json'), 'utf8');
    expect(firstMetadata).toBe(realCargoMetadata(workspace));

    const repeat = await runLayout(['-p', 'alpha']);
    expect(repeat.code, repeat.stderr).toBe(0);
    expect(repeat.stdout).toBe('shared');
    expect((await cargo.runs()).spawned).toBe(1);
    expect(await readFile(join(repeat.fixtureDir, 'cargo-metadata.json'), 'utf8')).toBe(firstMetadata);

    const sibling = await runLayout(['-p', 'beta']);
    expect(sibling.code, sibling.stderr).toBe(0);
    expect(sibling.stdout).toBe('private');
    expect((await cargo.runs()).spawned).toBe(1);

    const jobA = await runLayout(['-p', 'alpha'], {
      CARGO_MAKEFLAGS: '-j --jobserver-auth=fifo:/tmp/job-a.fifo',
      CARGO_BUILD_JOBS: '1',
    });
    const jobB = await runLayout(['-p', 'alpha'], {
      CARGO_MAKEFLAGS: '-j --jobserver-auth=fifo:/tmp/job-b.fifo',
      CARGO_BUILD_JOBS: '8',
    });
    expect(jobA.code, jobA.stderr).toBe(0);
    expect(jobB.code, jobB.stderr).toBe(0);
    expect((await cargo.runs()).spawned).toBe(1);

    const mutations: readonly {
      readonly label: string;
      readonly apply: () => Promise<void>;
      readonly extraEnv?: Readonly<Record<string, string>>;
    }[] = [
      {
        label: 'path-source manifest edit',
        apply: async () => {
          const manifest = join(workspace, 'alpha/Cargo.toml');
          await appendFile(manifest, '# manifest changed\n');
          await backdate(manifest);
        },
      },
      {
        label: 'Cargo.lock edit',
        apply: async () => {
          const lockfile = join(workspace, 'Cargo.lock');
          await appendFile(lockfile, '# lockfile changed\n');
          await backdate(lockfile);
        },
      },
      {
        label: 'new file in tests/',
        apply: async () => {
          await put(workspace, 'alpha/tests/extra.rs', '#[test] fn extra() {}\n');
        },
      },
      {
        label: 'new file in src/bin/',
        apply: async () => {
          await put(workspace, 'beta/src/bin/extra.rs', 'fn main() {}\n');
        },
      },
      {
        label: 'removed target source',
        apply: async () => {
          await rm(join(workspace, 'beta/src/main.rs'));
          await backdate(join(workspace, 'beta/src'));
        },
      },
      {
        label: '.cargo/config.toml edit',
        apply: async () => {
          await put(workspace, '.cargo/config.toml', '# config changed\n');
        },
      },
      {
        label: 'changed CARGO_TARGET_DIR',
        apply: async () => {},
        extraEnv: { CARGO_TARGET_DIR: join(root, 'alt-target') },
      },
    ];

    let expectedSpawns = 1;
    for (const mutation of mutations) {
      await mutation.apply();
      expectedSpawns += 1;
      const changed = await runLayout(['-p', 'alpha'], mutation.extraEnv);
      expect(changed.code, `${mutation.label} (first): ${changed.stderr}`).toBe(0);
      expect((await cargo.runs()).spawned, `${mutation.label} (first)`).toBe(expectedSpawns);
      const cached = await runLayout(['-p', 'alpha'], mutation.extraEnv);
      expect(cached.code, `${mutation.label} (repeat): ${cached.stderr}`).toBe(0);
      expect((await cargo.runs()).spawned, `${mutation.label} (repeat)`).toBe(expectedSpawns);
    }
  });
});

it('shares a single cold Cargo metadata flight across concurrent invocations', async () => {
  await withWorkspace(async ({ cargo, spawnLayout }) => {
    await cargo.beforeAnswer.close();
    const launched = await Promise.all(Array.from({ length: 4 }, () => spawnLayout(['-p', 'alpha'])));
    await cargo.waitForRuns((runs) => runs.read === 1, 'the single cold metadata child to read');
    await cargo.beforeAnswer.open();
    const outcomes = await Promise.all(launched.map((item) => item.done));
    const payloads = await Promise.all(
      outcomes.map(async (outcome) => {
        expect(outcome.code, outcome.stderr).toBe(0);
        expect(outcome.stdout).toBe('shared');
        return readFile(join(outcome.fixtureDir, 'cargo-metadata.json'), 'utf8');
      }),
    );
    for (const payload of payloads) expect(payload).toBe(payloads[0]);
    const runs = await cargo.runs();
    expect(runs.spawned).toBe(1);
    expect(runs.peak).toBe(1);
  });
});

it('lets a waiting invocation run Cargo immediately after a killed flight holder releases the lock', async () => {
  await withWorkspace(async ({ workspace, cargo, spawnLayout, runLayout }) => {
    await cargo.beforeAnswer.close();
    const holder = await spawnLayout(['-p', 'alpha']);
    const first = await cargo.waitForRuns(
      (runs) => runs.read === 1,
      'the holder metadata child to hold the flight lock',
    );
    const waiter = await spawnLayout(['-p', 'alpha']);
    holder.killGroup();
    await holder.done;
    await cargo.waitForDisconnects(first.pids);

    await cargo.waitForRuns((runs) => runs.spawned === 2, 'the waiter to start its own metadata child');
    await cargo.beforeAnswer.open();
    const answered = await waiter.done;
    expect(answered.code, answered.stderr).toBe(0);
    expect(answered.stdout).toBe('shared');
    const expected = realCargoMetadata(workspace);
    expect(await readFile(join(answered.fixtureDir, 'cargo-metadata.json'), 'utf8')).toBe(expected);

    const repeat = await runLayout(['-p', 'alpha']);
    expect(repeat.code, repeat.stderr).toBe(0);
    expect((await cargo.runs()).spawned).toBe(2);
  });
});

it('exits nonzero without storing when Cargo fails and runs Cargo again once repaired', async () => {
  await withWorkspace(async ({ workspace, storeAnswer, cargo, runLayout }) => {
    const manifest = join(workspace, 'alpha/Cargo.toml');
    const original = await readFile(manifest, 'utf8');
    await writeFile(manifest, 'broken = [\n');
    await backdate(manifest);

    const failed = await runLayout(['-p', 'alpha']);
    expect(failed.code).not.toBe(0);
    expect((await cargo.runs()).spawned).toBe(1);
    await expect(readFile(storeAnswer, 'utf8')).rejects.toMatchObject({ code: 'ENOENT' });
    const entries = await readdir(dirname(storeAnswer));
    expect(entries.some((name) => name.endsWith('.tmp'))).toBe(false);

    await writeFile(manifest, original);
    await backdate(manifest);
    const recovered = await runLayout(['-p', 'alpha']);
    expect(recovered.code, recovered.stderr).toBe(0);
    expect((await cargo.runs()).spawned).toBe(2);
    expect(await readFile(join(recovered.fixtureDir, 'cargo-metadata.json'), 'utf8')).toBe(
      realCargoMetadata(workspace),
    );

    const repeat = await runLayout(['-p', 'alpha']);
    expect(repeat.code, repeat.stderr).toBe(0);
    expect((await cargo.runs()).spawned).toBe(2);
  });
});

it('answers without storing when an input changes while Cargo is in flight', async () => {
  await withWorkspace(async ({ workspace, storeAnswer, cargo, spawnLayout, runLayout }) => {
    await cargo.beforeAnswer.close();
    const inflight = await spawnLayout(['-p', 'alpha']);
    await cargo.waitForRuns((runs) => runs.read === 1, 'the in-flight metadata child to read');
    const manifest = join(workspace, 'alpha/Cargo.toml');
    await appendFile(manifest, '# edited during flight\n');
    await cargo.beforeAnswer.open();

    const first = await inflight.done;
    expect(first.code, first.stderr).toBe(0);
    expect(first.stdout).toBe('shared');
    expect((await readFile(join(first.fixtureDir, 'cargo-metadata.json'), 'utf8')).length).toBeGreaterThan(0);
    await expect(readFile(storeAnswer, 'utf8')).rejects.toMatchObject({ code: 'ENOENT' });

    await backdate(manifest);
    const second = await runLayout(['-p', 'alpha']);
    expect(second.code, second.stderr).toBe(0);
    expect((await cargo.runs()).spawned).toBe(2);

    const third = await runLayout(['-p', 'alpha']);
    expect(third.code, third.stderr).toBe(0);
    expect((await cargo.runs()).spawned).toBe(2);
  });
});

it('answers without storing when a target source is created while Cargo is in flight', async () => {
  await withWorkspace(async ({ workspace, storeAnswer, cargo, spawnLayout, runLayout }) => {
    await cargo.beforeAnswer.close();
    const inflight = await spawnLayout(['-p', 'alpha']);
    await cargo.waitForRuns((runs) => runs.read === 1, 'the in-flight metadata child to read');
    const main = join(workspace, 'alpha/src/main.rs');
    await writeFile(main, 'fn main() {}\n');
    await cargo.beforeAnswer.open();

    const first = await inflight.done;
    expect(first.code, first.stderr).toBe(0);
    expect(first.stdout).toBe('shared');
    expect((await readFile(join(first.fixtureDir, 'cargo-metadata.json'), 'utf8')).length).toBeGreaterThan(0);
    await expect(readFile(storeAnswer, 'utf8')).rejects.toMatchObject({ code: 'ENOENT' });

    await backdate(main, dirname(main));
    const second = await runLayout(['-p', 'alpha']);
    expect(second.code, second.stderr).toBe(0);
    expect(second.stdout).toBe('private');
    expect((await cargo.runs()).spawned).toBe(2);
    const written: unknown = JSON.parse(await readFile(join(second.fixtureDir, 'cargo-metadata.json'), 'utf8'));
    const real: unknown = JSON.parse(
      realCargoMetadata(workspace, ['--config', join(second.fixtureDir, 'layout.toml')]),
    );
    expect(sortJsonKeys(written)).toEqual(sortJsonKeys(real));

    const third = await runLayout(['-p', 'alpha']);
    expect(third.code, third.stderr).toBe(0);
    expect(third.stdout).toBe('private');
    expect((await cargo.runs()).spawned).toBe(2);
  });
});

it('writes private-mode cargo-metadata.json matching real cargo metadata with --config layout.toml', async () => {
  await withWorkspace(async ({ workspace, runLayout }) => {
    const result = await runLayout(['-p', 'beta']);
    expect(result.code, result.stderr).toBe(0);
    expect(result.stdout).toBe('private');
    const written: unknown = JSON.parse(await readFile(join(result.fixtureDir, 'cargo-metadata.json'), 'utf8'));
    const real: unknown = JSON.parse(
      realCargoMetadata(workspace, ['--config', join(result.fixtureDir, 'layout.toml')]),
    );
    expect(sortJsonKeys(written)).toEqual(sortJsonKeys(real));
  });
});
//#endregion smoo/cowshed!n/native-test-verdicts
