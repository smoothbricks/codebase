import { expect, it } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { managedAssetsRoot } from '@smoothbricks/nx-plugin/managed-assets';

it('leaves caches and tool probes untouched during capture, then maintains them at final entry', async () => {
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
      expect({ exitCode, stdout, stderr }).toEqual({ exitCode: 0, stdout: '', stderr: '' });
    };

    await enter('1');
    expect(await Bun.file(probes).exists()).toBe(false);
    expect(await readFile(stamp, 'utf8')).toBe('previous toolchain');
    expect(await readFile(rustcInfo, 'utf8')).toBe('previous compiler');

    await enter('');
    expect(await readFile(probes, 'utf8')).toBe('rustc\ncc\n');
    expect(await readFile(stamp, 'utf8')).toBe('rustc fixture\nSDKROOT=\nDEVELOPER_DIR=\ncc fixture');
    expect(await Bun.file(rustcInfo).exists()).toBe(false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
