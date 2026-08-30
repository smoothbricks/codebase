/// <reference types="bun" />
/// <reference types="node" />

import { afterEach, describe, expect, it } from 'bun:test';
import { chmod, mkdir, mkdtemp, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { packageRootFromModule, runCli } from './cli-trampoline.js';

const fixtureRoots: string[] = [];

afterEach(async () => {
  await Promise.all(fixtureRoots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe('cowshed CLI trampoline', () => {
  it('prefers the packaged host binary over the workspace release binary', async () => {
    const root = await fixtureRoot();
    const packaged = join(root, 'dist', 'bin', 'darwin-arm64', 'cowshed');
    const workspace = join(root, 'target', 'release', 'cowshed');
    await Promise.all([fixtureFile(packaged), fixtureFile(workspace)]);
    const spawns: Array<{ executable: string; argv: readonly string[] }> = [];

    const exitCode = await runCli(['ls', '--all'], {
      packageRoot: root,
      platform: 'darwin',
      arch: 'arm64',
      async spawnBinary(executable, argv) {
        spawns.push({ executable, argv });
        return 17;
      },
      async runNapi() {
        throw new Error('Node-API fallback must not run');
      },
    });

    expect(exitCode).toBe(17);
    expect(spawns).toEqual([{ executable: packaged, argv: ['ls', '--all'] }]);
  });

  it('uses target/release for a bun-linked workspace when no packaged binary exists', async () => {
    const root = await fixtureRoot();
    const workspace = join(root, 'target', 'release', 'cowshed');
    await fixtureFile(workspace);
    const spawns: string[] = [];

    const exitCode = await runCli(['doctor'], {
      packageRoot: root,
      platform: 'linux',
      arch: 'x64',
      async spawnBinary(executable) {
        spawns.push(executable);
        return 0;
      },
      async runNapi() {
        throw new Error('Node-API fallback must not run');
      },
    });

    expect(exitCode).toBe(0);
    expect(spawns).toEqual([workspace]);
  });

  it('uses the Node-API runner only when neither native binary exists', async () => {
    const root = await fixtureRoot();
    const napiCalls: string[][] = [];

    const exitCode = await runCli(['path', 'main'], {
      packageRoot: root,
      platform: 'darwin',
      arch: 'x64',
      async spawnBinary() {
        throw new Error('native spawn must not run');
      },
      async runNapi(argv) {
        napiCalls.push([...argv]);
        return 9;
      },
    });

    expect(exitCode).toBe(9);
    expect(napiCalls).toEqual([['path', 'main']]);
  });

  it('finds the package root from source and compiled module locations', async () => {
    const root = await fixtureRoot();

    expect(packageRootFromModule(pathToFileURL(join(root, 'src', 'cli.ts')).href)).toBe(root);
    expect(packageRootFromModule(pathToFileURL(join(root, 'dist', 'ts', 'cli.js')).href)).toBe(root);
  });

  it('re-chmods a packaged binary denied by permissions and retries the spawn once', async () => {
    const root = await fixtureRoot();
    const packaged = join(root, 'dist', 'bin', 'darwin-arm64', 'cowshed');
    await mkdir(dirname(packaged), { recursive: true });
    // The healed retry fails with ENOENT (missing shebang interpreter), so a
    // surfaced EACCES proves the original error won over the retry's.
    await writeFile(packaged, '#!/nonexistent-cowshed-interpreter\n');
    await chmod(packaged, 0o644);

    await expect(
      runCli(['ls', '--all'], { packageRoot: root, platform: 'darwin', arch: 'arm64' }),
    ).rejects.toMatchObject({
      code: 'EACCES',
    });
    expect((await stat(packaged)).mode & 0o111).toBe(0o111);
  });

  it('spawns healthy packaged binaries without touching their permissions', async () => {
    const root = await fixtureRoot();
    const packaged = join(root, 'dist', 'bin', 'darwin-arm64', 'cowshed');
    await mkdir(dirname(packaged), { recursive: true });
    await writeFile(packaged, '#!/bin/sh\nexit 17\n');
    await chmod(packaged, 0o755);

    const exitCode = await runCli(['ls', '--all'], { packageRoot: root, platform: 'darwin', arch: 'arm64' });

    expect(exitCode).toBe(17);
    expect((await stat(packaged)).mode & 0o777).toBe(0o755);
  });

  it('surfaces non-permission spawn failures without healing', async () => {
    const root = await fixtureRoot();

    // exists() selects the packaged native backend without materializing its file.
    await expect(
      runCli(['ls', '--all'], {
        packageRoot: root,
        platform: 'darwin',
        arch: 'arm64',
        exists: (path) => path === join(root, 'dist', 'bin', 'darwin-arm64', 'cowshed'),
      }),
    ).rejects.toMatchObject({ code: 'ENOENT' });
  });

  it('routes daemon verbs through the host-stable install ahead of packaged binaries', async () => {
    const root = await fixtureRoot();
    const packaged = join(root, 'dist', 'bin', 'darwin-arm64', 'cowshed');
    const stable = join(root, 'home', 'Library', 'Application Support', 'dev.cowshed', 'bin', 'cowshed');
    await Promise.all([fixtureFile(packaged), fixtureFile(stable)]);
    const spawns: Array<{ executable: string; argv: readonly string[] }> = [];

    const exitCode = await runCli(['gateway', 'status'], {
      packageRoot: root,
      platform: 'darwin',
      arch: 'arm64',
      home: join(root, 'home'),
      async spawnBinary(executable, argv) {
        spawns.push({ executable, argv });
        return 0;
      },
    });

    expect(exitCode).toBe(0);
    expect(spawns).toEqual([{ executable: stable, argv: ['gateway', 'status'] }]);
  });

  it('detects daemon verbs after leading flags', async () => {
    const root = await fixtureRoot();
    const packaged = join(root, 'dist', 'bin', 'darwin-arm64', 'cowshed');
    const stable = join(root, 'home', 'Library', 'Application Support', 'dev.cowshed', 'bin', 'cowshed');
    await Promise.all([fixtureFile(packaged), fixtureFile(stable)]);
    const spawns: string[] = [];

    await runCli(['--json', 'sccache', 'start'], {
      packageRoot: root,
      platform: 'darwin',
      arch: 'arm64',
      home: join(root, 'home'),
      async spawnBinary(executable) {
        spawns.push(executable);
        return 0;
      },
    });

    expect(spawns).toEqual([stable]);
  });

  it('keeps non-daemon verbs on the packaged binary even when the stable install exists', async () => {
    const root = await fixtureRoot();
    const packaged = join(root, 'dist', 'bin', 'darwin-arm64', 'cowshed');
    const stable = join(root, 'home', 'Library', 'Application Support', 'dev.cowshed', 'bin', 'cowshed');
    await Promise.all([fixtureFile(packaged), fixtureFile(stable)]);
    const spawns: string[] = [];

    await runCli(['ls', '--all'], {
      packageRoot: root,
      platform: 'darwin',
      arch: 'arm64',
      home: join(root, 'home'),
      async spawnBinary(executable) {
        spawns.push(executable);
        return 0;
      },
    });

    expect(spawns).toEqual([packaged]);
  });

  it('keeps setup and skill on the packaged binary: setup writes the stable install and must never run from it', async () => {
    const root = await fixtureRoot();
    const packaged = join(root, 'dist', 'bin', 'darwin-arm64', 'cowshed');
    const stable = join(root, 'home', 'Library', 'Application Support', 'dev.cowshed', 'bin', 'cowshed');
    await Promise.all([fixtureFile(packaged), fixtureFile(stable)]);
    const spawns: string[] = [];

    for (const argv of [['setup'], ['skill', 'install']]) {
      await runCli(argv, {
        packageRoot: root,
        platform: 'darwin',
        arch: 'arm64',
        home: join(root, 'home'),
        async spawnBinary(executable) {
          spawns.push(executable);
          return 0;
        },
      });
    }

    expect(spawns).toEqual([packaged, packaged]);
  });

  it('falls through to the packaged binary for daemon verbs when no stable install exists', async () => {
    const root = await fixtureRoot();
    const packaged = join(root, 'dist', 'bin', 'darwin-arm64', 'cowshed');
    await fixtureFile(packaged);
    const spawns: string[] = [];

    await runCli(['gateway', 'start'], {
      packageRoot: root,
      platform: 'darwin',
      arch: 'arm64',
      home: join(root, 'home'),
      async spawnBinary(executable) {
        spawns.push(executable);
        return 0;
      },
    });

    expect(spawns).toEqual([packaged]);
  });
});

async function fixtureRoot(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'cowshed-cli-trampoline-'));
  fixtureRoots.push(root);
  return root;
}

async function fixtureFile(path: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, 'fixture');
}
