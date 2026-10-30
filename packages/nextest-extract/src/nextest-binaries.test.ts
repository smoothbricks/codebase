import { describe, expect, it } from 'bun:test';
import { spawn, spawnSync } from 'node:child_process';
import { cp, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { cargoEnv } from '@smoothbricks/cargo-retain/testing/cargo-fixture';
import {
  cargoTargetDirectory,
  ensureNextestBinaries,
  type Materialization,
  NEXTEST_BINARIES_METADATA,
  NEXTEST_BUILD_KEY,
  NEXTEST_CARGO_METADATA,
  readBinariesMetadata,
  readMaterialization,
  recordBuildKey,
} from './nextest-binaries.js';

/** The shape `cargo nextest list --list-type binaries-only --message-format json` prints, cut to what is read. */
function metadata(
  targetDirectory: string,
  tests: Record<string, string>,
  nonTest: Record<string, string> = {},
): string {
  return JSON.stringify({
    'rust-build-meta': {
      'target-directory': targetDirectory,
      'non-test-binaries': {
        'path+file:///w#probe@0.1.0': Object.entries(nonTest).map(([name, path]) => ({ name, kind: 'bin-exe', path })),
      },
    },
    'rust-binaries': Object.fromEntries(
      Object.entries(tests).map(([id, path]) => [id, { 'binary-id': id, 'binary-path': path, kind: 'lib' }]),
    ),
  });
}

const NO_BUILD: Materialization = { elapsedMs: 0, mbx: null, misses: 0, crates: [] };

describe('readBinariesMetadata', () => {
  it('names every test and non-test binary relative to the target directory the build ran in', () => {
    const read = readBinariesMetadata(
      metadata('/a/target', { probe: '/a/target/debug/deps/probe-1f' }, { tool: 'debug/tool' }),
    );
    expect(read.targetDirectory).toBe('/a/target');
    expect(read.binaries).toEqual([
      { id: 'probe', relativePath: 'debug/deps/probe-1f' },
      { id: 'tool (bin-exe)', relativePath: 'debug/tool' },
    ]);
  });

  it('refuses a binary outside the target directory, which no remap could find', () => {
    expect(() => readBinariesMetadata(metadata('/a/target', { probe: '/elsewhere/probe-1f' }))).toThrow(
      'outside its target directory /a/target',
    );
  });
});

describe('readMaterialization', () => {
  it("reads mbx's summary, its misses and the crates cargo named, through colour codes", () => {
    const stderr =
      '\u001b[1m\u001b[32m   Compiling\u001b[0m probe v0.1.0 (/w)\n' +
      '   Compiling helper v0.2.0\n' +
      'mbx[cache]: 7 hits, 2 misses, 0 not looked up\n';
    expect(readMaterialization(stderr, 1500)).toEqual({
      elapsedMs: 1500,
      mbx: 'mbx[cache]: 7 hits, 2 misses, 0 not looked up',
      misses: 2,
      crates: ['probe', 'helper'],
    });
  });

  it('reports no misses when the build ran without mbx', () => {
    expect(readMaterialization('   Compiling probe v0.1.0\n', 10)).toMatchObject({ mbx: null, misses: 0 });
  });
});

describe('ensureNextestBinaries', () => {
  async function checkout(): Promise<{ root: string; file: string; key: string; target: string }> {
    const root = await realpath(await mkdtemp(join(tmpdir(), 'nextest-binaries-')));
    const target = join(root, 'target');
    const file = join(root, 'binaries-metadata.json');
    const key = join(root, 'build.key');
    // Built in another checkout: every path is that checkout's, and the run remaps them onto this one's.
    await writeFile(
      file,
      metadata('/built/target', { probe: '/built/target/debug/deps/probe-1f' }, { tool: 'debug/tool' }),
    );
    await writeFile(key, 'task-2\n');
    await mkdir(target);
    return { root, file, key, target };
  }

  async function place(target: string): Promise<void> {
    await mkdir(join(target, 'debug/deps'), { recursive: true });
    await writeFile(join(target, 'debug/deps/probe-1f'), '');
    await writeFile(join(target, 'debug/tool'), '');
  }

  const built = (target: string) => async (): Promise<Materialization> => {
    await place(target);
    return { elapsedMs: 42, mbx: 'mbx[cache]: 2 hits, 0 misses', misses: 0, crates: ['probe'] };
  };

  it('builds nothing when the target holds this build with every binary', async () => {
    const { root, file, key, target } = await checkout();
    try {
      await place(target);
      await recordBuildKey(target, 'task-2');
      let builds = 0;
      const readiness = await ensureNextestBinaries(
        file,
        key,
        root,
        async () => {
          builds += 1;
          return NO_BUILD;
        },
        async () => target,
      );
      expect(builds).toBe(0);
      expect(readiness).toEqual({ kind: 'present', key: 'task-2', count: 2, targetDirectory: target });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('rebuilds a target that holds every path of another build: a path is no proof of its bytes', async () => {
    const { root, file, key, target } = await checkout();
    try {
      await place(target);
      await recordBuildKey(target, 'task-1');
      let builds = 0;
      const readiness = await ensureNextestBinaries(
        file,
        key,
        root,
        async () => {
          builds += 1;
          return built(target)();
        },
        async () => target,
      );
      expect(builds).toBe(1);
      expect(readiness).toMatchObject({ kind: 'materialized', key: 'task-2', held: 'task-1', absent: 0 });
      expect(await readFile(join(target, 'nextest-build.key'), 'utf8')).toBe('task-2\n');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('builds the binaries a checkout lacks once for every fixture that asks at the same time', async () => {
    const { root, file, key, target } = await checkout();
    try {
      let builds = 0;
      const slow = async (): Promise<Materialization> => {
        builds += 1;
        await new Promise((resolve) => setTimeout(resolve, 300));
        return built(target)();
      };
      const readiness = await Promise.all(
        [1, 2, 3].map(() => ensureNextestBinaries(file, key, root, slow, async () => target)),
      );
      expect(builds).toBe(1);
      expect(readiness.map((ready) => ready.kind).sort()).toEqual(['materialized', 'present', 'present']);
      expect(readiness.find((ready) => ready.kind === 'materialized')).toMatchObject({ held: null, absent: 2 });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('takes over the lock of a fixture that died holding it', async () => {
    const { root, file, key, target } = await checkout();
    try {
      const dead = spawnSync('true').pid;
      await writeFile(join(target, 'nextest-build.lock'), String(dead));
      const readiness = await ensureNextestBinaries(file, key, root, built(target), async () => target);
      expect(readiness.kind).toBe('materialized');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('refuses a build that leaves a named binary absent instead of running without it', async () => {
    const { root, file, key, target } = await checkout();
    try {
      await expect(
        ensureNextestBinaries(
          file,
          key,
          root,
          async () => NO_BUILD,
          async () => target,
        ),
      ).rejects.toThrow('2 test binaries are still absent');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('refuses metadata that arrived without the key of the build that wrote it', async () => {
    const { root, file, target } = await checkout();
    try {
      await expect(
        ensureNextestBinaries(file, join(root, 'absent.key'), root, built(target), async () => target),
      ).rejects.toThrow('names no build');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe('in place, from another checkout', () => {
  async function run(
    command: string,
    args: string[],
    cwd: string,
  ): Promise<{ status: number | null; stdout: string; stderr: string }> {
    const child = spawn(command, args, { cwd, env: cargoEnv(), stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk: Buffer) => {
      stdout += chunk.toString('utf8');
    });
    child.stderr.on('data', (chunk: Buffer) => {
      stderr += chunk.toString('utf8');
    });
    const [status] = await new Promise<[number | null]>((resolve, reject) => {
      child.once('error', reject);
      child.once('close', (code) => resolve([code]));
    });
    return { status, stdout, stderr };
  }

  it("runs a second checkout's tests from the first checkout's metadata, building what that checkout lacks", async () => {
    const parent = await realpath(await mkdtemp(join(tmpdir(), 'nextest-binaries-checkouts-')));
    const built = join(parent, 'built');
    const other = join(parent, 'other');
    const list = ['--frozen', 'nextest', 'list', '--list-type', 'binaries-only', '--message-format', 'json'];
    const bin = join(import.meta.dir, 'bin/smoo-nx-nextest-binaries.ts');
    try {
      await mkdir(join(built, 'src'), { recursive: true });
      await writeFile(
        join(built, 'Cargo.toml'),
        '[package]\nname = "reuse-probe"\nversion = "0.1.0"\nedition = "2021"\n',
      );
      // The test reads its manifest directory at run time: the remap must hand it this checkout's, not the builder's.
      await writeFile(
        join(built, 'src/lib.rs'),
        '#[test]\nfn reads_its_own_checkout() {\n' +
          '    let dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();\n' +
          '    assert!(std::path::Path::new(&dir).join("checkout.txt").exists(), "{dir}");\n}\n',
      );
      expect(spawnSync('cargo', ['generate-lockfile', '--offline'], { cwd: built, env: cargoEnv() }).status).toBe(0);
      await cp(built, other, { recursive: true });
      await writeFile(join(other, 'checkout.txt'), 'the running checkout\n');

      // The cargo-test-build target, in the first checkout: the metadata, the task's key, and its record in the target.
      const listed = await run('cargo', list, built);
      expect(listed.status).toBe(0);
      await mkdir(join(built, '.cache/nextest'), { recursive: true });
      await writeFile(join(built, NEXTEST_BINARIES_METADATA), listed.stdout);
      const cargoMetadata = await run(
        'cargo',
        ['--frozen', 'metadata', '--format-version', '1', '--all-features'],
        built,
      );
      expect(cargoMetadata.status).toBe(0);
      await writeFile(join(built, NEXTEST_CARGO_METADATA), cargoMetadata.stdout);
      await writeFile(join(built, NEXTEST_BUILD_KEY), 'task-1\n');
      expect((await run(process.execPath, [bin, 'record', NEXTEST_BUILD_KEY], built)).status).toBe(0);
      expect(await readFile(join(built, 'target/nextest-build.key'), 'utf8')).toBe('task-1\n');

      // Its outputs arrive in the second checkout, as an Nx cache hit restores them.
      await mkdir(join(other, '.cache/nextest'), { recursive: true });
      for (const file of [NEXTEST_BINARIES_METADATA, NEXTEST_CARGO_METADATA, NEXTEST_BUILD_KEY]) {
        await writeFile(join(other, file), await readFile(join(built, file)));
      }
      const target = await cargoTargetDirectory(other);
      expect(target).toBe(join(other, 'target'));

      // The fixture: the second checkout holds no build, so it builds this one, then finds it on the next call.
      const fixture = ['ensure', NEXTEST_BINARIES_METADATA, NEXTEST_BUILD_KEY, '--', 'cargo', ...list];
      const first = await run(process.execPath, [bin, ...fixture], other);
      expect({ status: first.status, stderr: first.stderr }).toMatchObject({ status: 0 });
      expect(first.stderr).toContain('build task-1: the target held no recorded build in');
      const second = await run(process.execPath, [bin, ...fixture], other);
      expect({ status: second.status, stderr: second.stderr }).toMatchObject({ status: 0 });
      expect(second.stderr).toContain('build task-1: 1 test binaries present in');

      // The run: in place, no archive, the builder's paths remapped onto this checkout.
      const ranArgs = [
        '--frozen',
        'nextest',
        'run',
        '--binaries-metadata',
        NEXTEST_BINARIES_METADATA,
        '--cargo-metadata',
        NEXTEST_CARGO_METADATA,
        '--workspace-remap',
        '.',
        '--target-dir-remap',
        target,
        '--user-config-file',
        'none',
      ];
      const ran = await run('cargo', ranArgs, other);
      expect(ran.stderr).toContain('1 test run: 1 passed');
      expect(ran.stderr).not.toContain('Extract');
      expect(ran.status).toBe(0);

      // The source changes and a build of it arrives from the cache: the same metadata, the same binary path, another
      // key. The fixture must rebuild what the second checkout holds; a run of the old binary would pass.
      await writeFile(
        join(other, 'src/lib.rs'),
        '#[test]\nfn reads_its_own_checkout() {\n    panic!("the edited source runs");\n}\n',
      );
      await writeFile(join(other, NEXTEST_BUILD_KEY), 'task-2\n');
      const third = await run(process.execPath, [bin, ...fixture], other);
      expect({ status: third.status, stderr: third.stderr }).toMatchObject({ status: 0 });
      expect(third.stderr).toContain('build task-2: the target held build task-1 in');
      const stale = await run('cargo', ranArgs, other);
      expect(stale.stderr).toContain('the edited source runs');
      expect(stale.status).not.toBe(0);
    } finally {
      await rm(parent, { recursive: true, force: true });
    }
  });
});
