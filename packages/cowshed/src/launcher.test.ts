/// <reference types="bun" />
/// <reference types="node" />

import { afterEach, describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { chmod, copyFile, mkdir, mkdtemp, rm, stat, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { launcherPath, packageRootFromModule, runLauncher } from './launcher.js';
import { NATIVE_TARGETS, platformDirectory } from './platform.js';

const LAUNCHER = launcherPath(packageRootFromModule(import.meta.url));
const HOST_DIRECTORY = platformDirectory(process.platform, process.arch);

/** What `uname -sm` prints on the host each table row ships for. */
const UNAME: Record<(typeof NATIVE_TARGETS)[number]['directory'], string> = {
  'darwin-arm64': 'Darwin arm64',
  'darwin-x64': 'Darwin x86_64',
  'linux-arm64-gnu': 'Linux aarch64',
  'linux-x64-gnu': 'Linux x86_64',
};

const fixtureRoots: string[] = [];

afterEach(async () => {
  await Promise.all(fixtureRoots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

/**
 * A checkout-shaped fixture: the launcher at `packages/cowshed/bin/cowshed`, a workspace
 * `target/release`, and a home for the host-stable install. Every binary is a script that says
 * which one it is and echoes its argv, then exits 17.
 */
async function fixture() {
  const root = await mkdtemp(join(tmpdir(), 'cowshed-launcher-'));
  fixtureRoots.push(root);
  const packageRoot = join(root, 'packages', 'cowshed');
  const launcher = launcherPath(packageRoot);
  await mkdir(dirname(launcher), { recursive: true });
  await copyFile(LAUNCHER, launcher);
  const home = join(root, 'home');
  return {
    root,
    packageRoot,
    launcher,
    home,
    packaged: (directory: string) => join(packageRoot, 'dist', 'bin', directory, 'cowshed'),
    workspace: join(root, 'target', 'release', 'cowshed'),
    stable: join(home, 'Library', 'Application Support', 'dev.cowshed', 'bin', 'cowshed'),
    /** A `uname` on PATH that reports `answer` for `-sm`. */
    async uname(answer: string): Promise<string> {
      const bin = join(root, 'fake-bin');
      await binary(join(bin, 'uname'), `echo '${answer}'`);
      return bin;
    },
  };
}

async function binary(path: string, body: string, mode = 0o755): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `#!/bin/sh\n${body}\n`);
  await chmod(path, mode);
}

/** A fixture binary that names itself. */
async function named(path: string, name: string, mode = 0o755): Promise<void> {
  await binary(path, `printf '%s' '${name}'; for argument in "$@"; do printf ' [%s]' "$argument"; done; exit 17`, mode);
}

/**
 * Run a fixture launcher with only HOME and PATH set. PATH is this test's own, behind `path` when
 * given: the launcher reaches `uname`, `readlink` and `chmod` through it, and a host keeps those
 * where it keeps them. NixOS keeps nothing but `sh` and `env` in /bin and /usr/bin.
 */
function launch(launcher: string, argv: readonly string[], env: { home: string; path?: string }) {
  const path = [env.path, process.env.PATH].filter((entry) => entry !== undefined).join(':');
  const result = spawnSync(launcher, [...argv], { encoding: 'utf8', env: { HOME: env.home, PATH: path } });
  return { status: result.status, stdout: result.stdout, stderr: result.stderr };
}

const host = HOST_DIRECTORY ?? 'darwin-arm64';
const hostOnly = HOST_DIRECTORY === null ? it.skip : it;

describe('cowshed launcher', () => {
  hostOnly('runs the binary packaged for this host, passing argv and exit status through', async () => {
    const f = await fixture();
    await named(f.packaged(host), 'packaged');
    await named(f.workspace, 'workspace');

    const run = launch(f.launcher, ['exec', 'raven', '--', 'printf', 'two words'], f);

    expect(run.status).toBe(17);
    expect(run.stdout).toBe('packaged [exec] [raven] [--] [printf] [two words]');
  });

  it('picks the directory for the host uname reports, for every host the package ships', async () => {
    for (const target of NATIVE_TARGETS) {
      const f = await fixture();
      for (const other of NATIVE_TARGETS) {
        await named(f.packaged(other.directory), other.directory);
      }

      const run = launch(f.launcher, ['ls'], { home: f.home, path: await f.uname(UNAME[target.directory]) });

      expect(run.stdout).toBe(`${target.directory} [ls]`);
    }
  });

  hostOnly('uses target/release of the workspace a linked checkout sits in when nothing is packaged', async () => {
    const f = await fixture();
    await named(f.workspace, 'workspace');

    const run = launch(f.launcher, ['doctor'], f);

    expect(run.stdout).toBe('workspace [doctor]');
  });

  hostOnly('follows the chain of links a package manager puts in front of it', async () => {
    const f = await fixture();
    await named(f.packaged(host), 'packaged');
    const relative = join(f.root, 'bin', 'cowshed');
    await mkdir(dirname(relative), { recursive: true });
    await symlink(join('..', 'packages', 'cowshed', 'bin', 'cowshed'), relative);
    const absolute = join(f.root, 'global-bin', 'cowshed');
    await mkdir(dirname(absolute), { recursive: true });
    await symlink(relative, absolute);

    expect(launch(absolute, ['--version'], f).stdout).toBe('packaged [--version]');
  });

  hostOnly('says every path it looked in, and exits environment-missing, when no binary exists', async () => {
    const f = await fixture();

    const run = launch(f.launcher, ['path', 'main'], f);

    expect(run.status).toBe(5);
    expect(run.stdout).toBe('');
    expect(run.stderr).toContain(`dist/bin/${host}/cowshed`);
    expect(run.stderr).toContain('target/release/cowshed');
    expect(run.stderr).toContain('next: build this platform with `nx build cowshed -c production`');
  });

  it('names the host it ships no binary for without looking in dist', async () => {
    const f = await fixture();
    await named(f.packaged(host), 'packaged');

    const run = launch(f.launcher, ['ls'], { home: f.home, path: await f.uname('FreeBSD amd64') });

    expect(run.status).toBe(5);
    expect(run.stderr).toContain('ships no CLI binary for FreeBSD amd64');
    expect(run.stderr).not.toContain('dist/bin');
  });

  hostOnly('restores the execute bit a publish dropped from a packaged binary', async () => {
    const f = await fixture();
    await named(f.packaged(host), 'packaged', 0o644);

    const run = launch(f.launcher, ['ls'], f);

    expect(run.stdout).toBe('packaged [ls]');
    expect((await stat(f.packaged(host))).mode & 0o111).toBe(0o111);
  });

  hostOnly('leaves a healthy packaged binary exactly as it is', async () => {
    const f = await fixture();
    await named(f.packaged(host), 'packaged', 0o750);

    launch(f.launcher, ['ls'], f);

    expect((await stat(f.packaged(host))).mode & 0o777).toBe(0o750);
  });

  hostOnly('runs sccache, even after leading flags, from the installed copy launchd runs', async () => {
    const f = await fixture();
    await named(f.packaged(host), 'packaged');
    await named(f.stable, 'stable');

    expect(launch(f.launcher, ['--json', 'sccache', 'start'], f).stdout).toBe('stable [--json] [sccache] [start]');
  });

  hostOnly('runs sccache from the packaged binary until something is installed', async () => {
    const f = await fixture();
    await named(f.packaged(host), 'packaged');

    expect(launch(f.launcher, ['sccache', 'start'], f).stdout).toBe('packaged [sccache] [start]');
  });

  hostOnly('runs gateway, setup, skill and every other verb from the invoking build', async () => {
    // `setup` writes the stable install and `gateway start`/`status` compare the daemon's
    // executable with the one asking: run from the installed copy, a stale gateway compared
    // with itself, looked current, and was never replaced.
    const f = await fixture();
    await named(f.packaged(host), 'packaged');
    await named(f.stable, 'stable');

    for (const argv of [['gateway', 'start'], ['gateway', 'status'], ['setup'], ['skill', 'install'], ['ls']]) {
      expect(launch(f.launcher, argv, f).stdout).toStartWith('packaged ');
    }
  });

  hostOnly('the library runs the CLI through the launcher and resolves with its exit status', async () => {
    const f = await fixture();
    await binary(f.packaged(host), 'exit 23');

    expect(await runLauncher(f.packageRoot, ['ls'])).toBe(23);
  });
});
