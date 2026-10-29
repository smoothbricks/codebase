/// <reference types="bun" />
/// <reference types="node" />

import { afterEach, describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { chmod, copyFile, mkdir, mkdtemp, rm, stat, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { launcherPath, packageRootFromModule, runLauncher } from './launcher.js';
import { cliDirectory, NATIVE_TARGETS } from './platform.js';

const LAUNCHER = launcherPath(packageRootFromModule(import.meta.url));
const HOST_DIRECTORY = cliDirectory(process.platform, process.arch);

/** What `uname -sm` prints on the host each table row ships for. */
const UNAME: Record<(typeof NATIVE_TARGETS)[number]['cli']['directory'], string> = {
  'darwin-arm64': 'Darwin arm64',
  'darwin-x64': 'Darwin x86_64',
  'linux-arm64': 'Linux aarch64',
  'linux-x64': 'Linux x86_64',
};

const fixtureRoots: string[] = [];

afterEach(async () => {
  await Promise.all(fixtureRoots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

/**
 * A published-package-shaped fixture: the launcher at `packages/cowshed/bin/cowshed`, the package's
 * development (`.cache/bin`) and packaged (`dist/bin`) trees, a cargo `target/release` no profile
 * falls back to, and a home for the host-stable install. `checkout()` adds the tracked CLI crate
 * entry, `crates/cowshed-cli/src/main.rs`, that makes it a source checkout. Every binary is a script
 * that says which one it is and echoes its argv, then exits 17.
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
    development: (directory: string) => join(packageRoot, '.cache', 'bin', directory, 'cowshed'),
    packaged: (directory: string) => join(packageRoot, 'dist', 'bin', directory, 'cowshed'),
    /** The tracked CLI crate entry a checkout holds and no published package ships. */
    async checkout(): Promise<void> {
      const entry = join(packageRoot, 'crates', 'cowshed-cli', 'src');
      await mkdir(entry, { recursive: true });
      await writeFile(join(entry, 'main.rs'), '');
    },
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
  hostOnly('runs the build its source authority picks, passing argv and exit status through', async () => {
    const f = await fixture();
    await named(f.workspace, 'workspace');
    const argv = ['exec', 'raven', '--', 'printf', 'two words'];

    const unbuilt = launch(f.launcher, argv, f);

    expect(unbuilt.status).toBe(5);
    expect(unbuilt.stdout).toBe('');
    expect(unbuilt.stderr).toContain(`dist/bin/${host}/cowshed`);
    expect(unbuilt.stderr).toContain('next: install a published @smoothbricks/cowshed');

    await named(f.packaged(host), 'packaged');

    const published = launch(f.launcher, argv, f);

    expect(published.status).toBe(17);
    expect(published.stdout).toBe('packaged [exec] [raven] [--] [printf] [two words]');

    // A tracked CLI crate entry makes it a checkout: it runs its development build, and neither the packaged
    // tree beside it nor a cargo release is a substitute for one it has not built.
    await f.checkout();

    const unbuiltCheckout = launch(f.launcher, argv, f);

    expect(unbuiltCheckout.status).toBe(5);
    expect(unbuiltCheckout.stdout).toBe('');
    expect(unbuiltCheckout.stderr).toContain(`.cache/bin/${host}/cowshed`);
    expect(unbuiltCheckout.stderr).not.toContain('dist/bin');
    expect(unbuiltCheckout.stderr).toContain('next: build it with `nx run cowshed:cli-native`');

    await named(f.development(host), 'development');

    const developed = launch(f.launcher, argv, f);

    expect(developed.status).toBe(17);
    expect(developed.stdout).toBe('development [exec] [raven] [--] [printf] [two words]');
  });

  it('picks the directory for the host uname reports, for every host the package ships', async () => {
    for (const target of NATIVE_TARGETS) {
      const f = await fixture();
      for (const other of NATIVE_TARGETS) {
        await named(f.packaged(other.cli.directory), other.cli.directory);
      }

      const run = launch(f.launcher, ['ls'], { home: f.home, path: await f.uname(UNAME[target.cli.directory]) });

      expect(run.stdout).toBe(`${target.cli.directory} [ls]`);
    }
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

  it('names the host it ships no binary for without looking in dist or .cache', async () => {
    const f = await fixture();
    await named(f.packaged(host), 'packaged');
    await named(f.development(host), 'development');

    const run = launch(f.launcher, ['ls'], { home: f.home, path: await f.uname('FreeBSD amd64') });

    expect(run.status).toBe(5);
    expect(run.stderr).toContain('ships no CLI binary for FreeBSD amd64');
    expect(run.stderr).not.toContain('dist/bin');
    expect(run.stderr).not.toContain('.cache/bin');
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

  hostOnly('runs published-package consumer verbs from the installed copy', async () => {
    const f = await fixture();
    await named(f.packaged(host), 'packaged');
    await named(f.stable, 'stable');

    for (const argv of [['build-state', '--json'], ['--json', 'build-state'], ['skill', 'install'], ['ls']]) {
      const run = launch(f.launcher, argv, f);
      expect(run.status).toBe(17);
      expect(run.stdout).toBe(`stable${argv.map((argument) => ` [${argument}]`).join('')}`);
      expect(run.stderr).toBe('');
    }
  });

  hostOnly('runs setup and gateway from the packaged binary even with an installed copy', async () => {
    // Setup installs the invoking build; gateway compares the daemon with that build.
    const f = await fixture();
    await named(f.packaged(host), 'packaged');
    await named(f.stable, 'stable');

    for (const argv of [['setup'], ['--json', 'setup'], ['gateway', 'start'], ['gateway', 'status']]) {
      const run = launch(f.launcher, argv, f);
      expect(run.status).toBe(17);
      expect(run.stdout).toBe(`packaged${argv.map((argument) => ` [${argument}]`).join('')}`);
      expect(run.stderr).toBe('');
    }
  });

  hostOnly('runs published-package consumer verbs from the packaged binary with no install', async () => {
    const f = await fixture();
    await named(f.packaged(host), 'packaged');

    const run = launch(f.launcher, ['build-state', '--json'], f);

    expect(run.status).toBe(17);
    expect(run.stdout).toBe('packaged [build-state] [--json]');
    expect(run.stderr).toBe('');
  });

  hostOnly(
    "runs a consumer's every verb but setup and gateway from the installed copy, through a link from outside the checkout",
    async () => {
      // Another repository links this checkout's package (a developer link), so its `cowshed` is this
      // launcher reached through its own node_modules/.bin. The checkout's development build cannot
      // talk to the daemon the install runs, so the consumer gets the installed copy.
      const f = await fixture();
      await f.checkout();
      await named(f.development(host), 'development');
      const consumerRoot = `${f.root}-consumer`;
      fixtureRoots.push(consumerRoot);
      const consumer = join(consumerRoot, 'node_modules', '.bin', 'cowshed');
      await mkdir(dirname(consumer), { recursive: true });
      await symlink(f.launcher, consumer);

      expect(launch(consumer, ['fork', 'raven'], f).stdout).toBe('development [fork] [raven]');

      await named(f.stable, 'stable');

      expect(launch(consumer, ['--json', 'fork', 'raven'], f).stdout).toBe('stable [--json] [fork] [raven]');
      for (const argv of [['setup'], ['gateway', 'status']]) {
        expect(launch(consumer, argv, f).stdout).toStartWith('development ');
      }
      // The checkout's own launcher still runs its development build.
      expect(launch(f.launcher, ['fork', 'raven'], f).stdout).toBe('development [fork] [raven]');
    },
  );

  hostOnly('the library runs the CLI through the launcher and resolves with its exit status', async () => {
    const f = await fixture();
    await binary(f.packaged(host), 'exit 23');

    expect(await runLauncher(f.packageRoot, ['ls'])).toBe(23);
  });
});
