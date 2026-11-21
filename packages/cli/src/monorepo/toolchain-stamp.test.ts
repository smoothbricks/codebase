import { expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { managedAssetsRoot } from '@smoothbricks/nx-plugin/managed-assets';

it('leaves caches and tool probes untouched during capture, then maintains only Cargo-owned caches at final entry', async () => {
  const root = await mkdtemp(join(tmpdir(), 'toolchain-stamp-'));
  try {
    const bin = join(root, 'bin');
    const target = join(root, 'target');
    const devenv = join(root, 'tooling', 'direnv');
    await Promise.all([bin, target, devenv].map((path) => mkdir(path, { recursive: true })));
    const probes = join(root, 'probes');
    for (const name of ['rustc', 'cc']) {
      await writeFile(
        join(bin, name),
        `#!/bin/sh\nprintf '%s\\n' '${name}' >> "$PROBE_LOG"\nprintf '%s\\n' '${name} fixture'\n`,
        { mode: 0o755 },
      );
    }
    const stamp = join(target, '.toolchain-stamp');
    const rustcInfo = join(target, '.rustc_info.json');
    await writeFile(stamp, 'previous toolchain');
    await writeFile(rustcInfo, 'previous compiler');

    const ordinary = join(target, 'lane', 'aarch64-apple-darwin', 'debug', 'build', 'fixture-42');
    const staging = join(target, 'lane', '.cargo-staging', 'debug', 'build', 'fixture-42');
    const cmake = join('out', 'build', 'CMakeCache.txt');
    for (const dir of [ordinary, staging]) {
      await mkdir(join(dir, 'out', 'build'), { recursive: true });
      await writeFile(join(dir, cmake), 'previous CMake toolchain');
    }

    const enter = async (skipTasks: string) => {
      const child = Bun.spawn([process.execPath, join(managedAssetsRoot, 'raw/tooling/direnv/toolchain-stamp.ts')], {
        env: {
          ...process.env,
          PATH: `${bin}:${process.env.PATH ?? ''}`,
          DEVENV_ROOT: devenv,
          DEVENV_SKIP_TASKS: skipTasks,
          SDKROOT: '',
          DEVELOPER_DIR: '',
          PROBE_LOG: probes,
        },
        stdout: 'pipe',
        stderr: 'pipe',
      });
      const [exitCode, stdout, stderr] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ]);
      expect({ exitCode, stdout }).toEqual({ exitCode: 0, stdout: '' });
      return stderr;
    };

    expect(await enter('1')).toBe('');
    expect(await Bun.file(probes).exists()).toBe(false);
    expect(await readFile(stamp, 'utf8')).toBe('previous toolchain');
    expect(await readFile(rustcInfo, 'utf8')).toBe('previous compiler');
    expect(await readFile(join(ordinary, cmake), 'utf8')).toBe('previous CMake toolchain');
    expect(await readFile(join(staging, cmake), 'utf8')).toBe('previous CMake toolchain');

    const maintenance = await enter('');
    expect(await readFile(probes, 'utf8')).toBe('rustc\ncc\n');
    expect(await readFile(stamp, 'utf8')).toBe('rustc fixture\nSDKROOT=\nDEVELOPER_DIR=\ncc fixture');
    expect(await Bun.file(rustcInfo).exists()).toBe(false);
    expect(await Bun.file(join(ordinary, cmake)).exists()).toBe(false);
    expect(await readFile(join(staging, cmake), 'utf8')).toBe('previous CMake toolchain');
    expect(maintenance).toContain('removed 1 cmake build-script cache(s)');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
