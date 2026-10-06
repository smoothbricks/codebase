import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import {
  chmodSync,
  existsSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readlinkSync,
  realpathSync,
  rmSync,
  symlinkSync,
  utimesSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const enterShell = join(import.meta.dir, 'enter-shell.ts');

/**
 * A stale Nx plugin build clears Nx's workspace data on shell entry. In a cowshed
 * checkout `.nx/workspace-data` is a link onto the checkout's build volume: the
 * entry empties the directory it names and keeps the link, so the next Nx run
 * writes the volume instead of making a real directory in the link's place.
 */
it('a stale plugin build clears the workspace data through its link and keeps the link', () => {
  const root = realpathSync(mkdtempSync(join(tmpdir(), 'smoo-enter-shell-')));
  try {
    const plugin = join(root, 'packages/nx-plugin');
    mkdirSync(join(plugin, 'src'), { recursive: true });
    mkdirSync(join(plugin, 'dist'), { recursive: true });
    writeFileSync(join(plugin, 'tsconfig.lib.json'), '{}');
    writeFileSync(join(plugin, 'src/index.ts'), 'export {};\n');
    const marker = join(plugin, 'dist/tsconfig.lib.tsbuildinfo');
    writeFileSync(marker, '');
    utimesSync(marker, new Date(0), new Date(0));

    const volume = join(root, 'volume/nx/workspace-data');
    mkdirSync(join(volume, 'd'), { recursive: true });
    writeFileSync(join(volume, 'project-graph.json'), '{}');
    writeFileSync(join(volume, 'd/server-process.json'), '{}');
    mkdirSync(join(root, '.nx'));
    const link = join(root, '.nx/workspace-data');
    symlinkSync('../volume/nx/workspace-data', link);

    // The rebuild itself is not under test: a `ttsc` that succeeds emitting nothing.
    const bin = join(root, 'bin');
    mkdirSync(bin);
    writeFileSync(join(bin, 'ttsc'), '#!/bin/sh\nexit 0\n');
    chmodSync(join(bin, 'ttsc'), 0o755);
    mkdirSync(join(root, 'tooling/direnv'), { recursive: true });

    const entered = spawnSync('bun', [enterShell], {
      cwd: root,
      encoding: 'utf8',
      env: {
        ...process.env,
        DEVENV_ROOT: join(root, 'tooling/direnv'),
        PATH: `${bin}:${process.env.PATH}`,
        // A CI runner never syncs runtime pins, so the entry does nothing else.
        GITHUB_ACTIONS: 'true',
      },
    });
    expect(entered.status, `${entered.stdout}${entered.stderr}`).toBe(0);
    expect(lstatSync(link).isSymbolicLink()).toBe(true);
    expect(readlinkSync(link)).toBe('../volume/nx/workspace-data');
    expect(readdirSync(volume)).toEqual([]);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

/**
 * devenv captures the shell's environment, with DEVENV_SKIP_TASKS set, before it runs the tasks that install
 * node_modules. A checkout with none (or with another lockfile's) has no working `ttsc` then, and a hook that fails
 * aborts the capture before the install could run: the hosted-runner jobs died there. The capture pass does nothing;
 * the entry proper, without the variable, still stops on a plugin it cannot rebuild.
 */
it('the environment capture pass rebuilds nothing, the entry proper still does', () => {
  const root = realpathSync(mkdtempSync(join(tmpdir(), 'smoo-enter-shell-')));
  try {
    const plugin = join(root, 'packages/nx-plugin');
    mkdirSync(join(plugin, 'src'), { recursive: true });
    writeFileSync(join(plugin, 'tsconfig.lib.json'), '{}');
    writeFileSync(join(plugin, 'src/index.ts'), 'export {};\n');
    mkdirSync(join(root, 'tooling/direnv'), { recursive: true });

    // No build marker: the plugin is stale. This `ttsc` is the one a checkout without node_modules has.
    const bin = join(root, 'bin');
    mkdirSync(bin);
    const attempts = join(root, 'ttsc-attempts');
    writeFileSync(join(bin, 'ttsc'), `#!/bin/sh\necho attempt >> '${attempts}'\nexit 2\n`);
    chmodSync(join(bin, 'ttsc'), 0o755);

    const enter = (extra: Record<string, string>) =>
      spawnSync('bun', [enterShell], {
        cwd: root,
        encoding: 'utf8',
        env: {
          ...process.env,
          DEVENV_ROOT: join(root, 'tooling/direnv'),
          PATH: `${bin}:${process.env.PATH}`,
          GITHUB_ACTIONS: 'true',
          ...extra,
        },
      });

    const capture = enter({ DEVENV_SKIP_TASKS: '1' });
    expect(capture.status, `${capture.stdout}${capture.stderr}`).toBe(0);
    expect(existsSync(attempts)).toBe(false);

    const entry = enter({ DEVENV_SKIP_TASKS: '' });
    expect(entry.status, `${entry.stdout}${entry.stderr}`).not.toBe(0);
    expect(existsSync(attempts)).toBe(true);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
