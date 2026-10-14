import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import {
  chmodSync,
  existsSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  readlinkSync,
  realpathSync,
  renameSync,
  rmSync,
  symlinkSync,
  utimesSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { FileLock } from 'nx/src/native/index.js';

const enterShell = join(import.meta.dir, 'enter-shell.ts');

/**
 * A checkout whose plugin build is stale, with `.nx/workspace-data` linked onto a build volume as cowshed links it,
 * and a `ttsc` that succeeds emitting nothing: the rebuild itself is not under test. `enter` runs the shell entry
 * proper over it; `root` is removed by `dispose`.
 */
function staleCheckout() {
  const root = realpathSync(mkdtempSync(join(tmpdir(), 'smoo-enter-shell-')));
  const plugin = join(root, 'packages/nx-plugin');
  mkdirSync(join(plugin, 'src'), { recursive: true });
  mkdirSync(join(plugin, 'dist'), { recursive: true });
  writeFileSync(join(plugin, 'tsconfig.lib.json'), '{}');
  writeFileSync(join(plugin, 'src/index.ts'), 'export {};\n');
  const marker = join(plugin, 'dist/tsconfig.lib.tsbuildinfo');
  writeFileSync(marker, '');
  utimesSync(marker, new Date(0), new Date(0));

  const volume = join(root, 'volume/nx/workspace-data');
  mkdirSync(volume, { recursive: true });
  mkdirSync(join(root, '.nx'));
  const link = join(root, '.nx/workspace-data');
  symlinkSync('../volume/nx/workspace-data', link);

  const bin = join(root, 'bin');
  mkdirSync(bin);
  writeFileSync(join(bin, 'ttsc'), '#!/bin/sh\nexit 0\n');
  chmodSync(join(bin, 'ttsc'), 0o755);
  mkdirSync(join(root, 'tooling/direnv'), { recursive: true });

  const enter = () =>
    spawnSync('bun', [enterShell], {
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
  return { volume, link, enter, dispose: () => rmSync(root, { recursive: true, force: true }) };
}

/**
 * A stale Nx plugin build clears what the rebuilt plugin invalidates on shell entry: the daemon's directory, whose
 * record names the daemon that loaded the old plugin, and the project graph that plugin computed. In a cowshed
 * checkout `.nx/workspace-data` is a link onto the checkout's build volume: the entry clears through it and keeps the
 * link, so the next Nx run writes the volume instead of making a real directory in the link's place.
 *
 * Everything else stays. The task database indexes `.nx/cache`, with its SQLite sidecars and Nx's open lock: a main
 * whose land changed the plugin once lost the adopted volume's whole index on its first shell entry, and every rebase
 * onto it then carried nothing. Nx's locks and its lockfile and file-hash caches are not the plugin's output. The
 * plugin's own state is content-keyed, and a concurrent Nx process writes it under its own lock (below).
 */
it('a stale plugin build clears the daemon record and project graph through its link and keeps every other entry', () => {
  const { volume, link, enter, dispose } = staleCheckout();
  try {
    mkdirSync(join(volume, 'd'));
    writeFileSync(join(volume, 'd/server-process.json'), '{}');
    for (const graph of ['project-graph.json', 'file-map.json', 'source-maps.json']) {
      writeFileSync(join(volume, graph), '{}');
    }
    const kept = [
      'A-v3.db',
      'A-v3.db-wal',
      'A-v3.db-shm',
      'A-v3.lock',
      'project-graph.lock',
      'nx_files.nxt',
      'lockfile-nodes.hash',
      'parsed-lock-file.nodes.json',
      'producer-receipt-0123.json',
    ];
    for (const entry of kept) writeFileSync(join(volume, entry), 'kept');
    mkdirSync(join(volume, 'cargo-resolve/0123'), { recursive: true });
    writeFileSync(join(volume, 'cargo-resolve/0123/entry.cache'), 'kept');
    mkdirSync(join(volume, 'bounded-exec/task'), { recursive: true });
    writeFileSync(join(volume, 'bounded-exec/task/verdict.json'), 'kept');

    const entered = enter();
    expect(entered.status, `${entered.stdout}${entered.stderr}`).toBe(0);
    expect(lstatSync(link).isSymbolicLink()).toBe(true);
    expect(readlinkSync(link)).toBe('../volume/nx/workspace-data');
    expect(readdirSync(volume).sort()).toEqual([...kept, 'bounded-exec', 'cargo-resolve'].sort());
    expect(readFileSync(join(volume, 'cargo-resolve/0123/entry.cache'), 'utf8')).toBe('kept');
    expect(readFileSync(join(volume, 'bounded-exec/task/verdict.json'), 'utf8')).toBe('kept');
  } finally {
    dispose();
  }
});

/**
 * A Cargo resolution holds `cargo-resolve/<id>/resolve.lock` while locked, offline Cargo runs, then publishes its
 * entry by a temporary file and a rename in that directory. A shell entry that emptied `.nx/workspace-data` removed
 * the directory from under its holder, and the publication failed `ENOENT` opening the temporary file: a rebase's
 * task-hash probe died so while the Nx daemon job started beside it entered a fresh shell over the just-rebased plugin.
 */
it('a stale plugin build leaves a Cargo resolution a concurrent Nx process holds able to publish', () => {
  const { volume, enter, dispose } = staleCheckout();
  const directory = join(volume, 'cargo-resolve/0123');
  mkdirSync(directory, { recursive: true });
  const lock = new FileLock(join(directory, 'resolve.lock'));
  lock.lock();
  try {
    const entered = enter();
    expect(entered.status, `${entered.stdout}${entered.stderr}`).toBe(0);
    const temporary = join(directory, `entry.cache.${process.pid}.tmp`);
    writeFileSync(temporary, 'key\n{}\n');
    renameSync(temporary, join(directory, 'entry.cache'));
    expect(readFileSync(join(directory, 'entry.cache'), 'utf8')).toBe('key\n{}\n');
  } finally {
    lock.unlock();
    dispose();
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

it('direct devenv entry sources the existing workspace policy before project hooks', () => {
  const module = readFileSync(join(import.meta.dir, 'devenv.smoo.nix'), 'utf8');
  const prologue = module.match(/\(lib\.mkBefore ''\n([\s\S]*?) {6}export PATH=/)?.[1];
  expect(prologue).toBeDefined();
  const root = realpathSync(mkdtempSync(join(tmpdir(), 'smoo-direct-entry-')));
  try {
    mkdirSync(join(root, 'tooling/direnv'), { recursive: true });
    mkdirSync(join(root, '.cowshed'));
    writeFileSync(
      join(root, '.cowshed/env'),
      'if [ "${GITHUB_ACTIONS:-}" != true ]; then unset CI; fi\nexport RUSTC_WRAPPER=/store/pinned-sccache\n',
    );
    for (const marker of ['false', 'true']) {
      const entered = spawnSync('/bin/sh', ['-c', `${prologue}` + 'printf \'%s\\n\' "${CI-unset}" "$RUSTC_WRAPPER"'], {
        encoding: 'utf8',
        env: { DEVENV_ROOT: join(root, 'tooling/direnv'), CI: 'true', GITHUB_ACTIONS: marker },
      });
      expect(entered.status, entered.stderr).toBe(0);
      expect(entered.stdout).toBe(`${marker === 'true' ? 'true' : 'unset'}\n/store/pinned-sccache\n`);
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
