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
  statSync,
  symlinkSync,
  utimesSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { FileLock } from 'nx/src/native/index.js';

const enterShell = join(import.meta.dir, 'enter-shell.ts');

/** Where the shell entry records that a package's `dist` was verified current: outside `dist`, which tsc-js owns. */
const buildMarker = 'node_modules/.cache/smoo/dist-verified';

/**
 * A checkout whose plugin build is stale, with `.nx/workspace-data` linked onto a build volume as cowshed links it,
 * and a `ttsc` that succeeds emitting nothing: the rebuild itself is not under test. `enter` runs the shell entry
 * proper over it; `root` is removed by `dispose`.
 */
function staleCheckout() {
  const root = realpathSync(mkdtempSync(join(tmpdir(), 'smoo-enter-shell-')));
  const plugin = join(root, 'packages/nx-plugin');
  for (const directory of [plugin, join(root, 'packages/cli')]) {
    mkdirSync(join(directory, 'src'), { recursive: true });
    mkdirSync(join(directory, 'dist'), { recursive: true });
    writeFileSync(join(directory, 'package.json'), '{}');
    writeFileSync(join(directory, 'tsconfig.lib.json'), '{}');
    writeFileSync(join(directory, 'src/index.ts'), 'export {};\n');
    const marker = join(directory, buildMarker);
    mkdirSync(dirname(marker), { recursive: true });
    writeFileSync(marker, '');
    utimesSync(marker, new Date(0), new Date(0));
  }

  const volume = join(root, 'volume/nx/workspace-data');
  mkdirSync(volume, { recursive: true });
  mkdirSync(join(root, '.nx'));
  const link = join(root, '.nx/workspace-data');
  symlinkSync('../volume/nx/workspace-data', link);

  const bin = join(root, 'bin');
  mkdirSync(bin);
  // Each call's arguments, one line per call, beside `bin`.
  writeFileSync(join(bin, 'ttsc'), '#!/bin/sh\nprintf "%s\\n" "$*" >> "$(dirname "$0")/../ttsc-calls"\nexit 0\n');
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
  return { root, plugin, volume, link, enter, dispose: () => rmSync(root, { recursive: true, force: true }) };
}

/**
 * A stale Nx plugin build clears what the rebuilt plugin invalidates on shell entry: the daemon's directory, whose
 * record names the daemon that loaded the old plugin, and the project graph that plugin computed. In a cowshed
 * checkout `.nx/workspace-data` is a link onto the checkout's build volume: the entry clears through it and keeps the
 * link, so the next Nx run writes the volume instead of making a real directory in the link's place.
 *
 * Everything else stays. The task database indexes `.nx/cache`, with its SQLite sidecars and Nx's open lock: deleting
 * it would leave every entry unreachable and never evicted. Nx's locks and its lockfile and file-hash caches are not
 * the plugin's output. The plugin's own state is content-keyed, and a concurrent Nx process writes it under its own
 * lock (below).
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
 * The entry's rebuild emits exactly as nx-plugin's typescript-emit executor does (its EMIT_OVERRIDES): one
 * non-composite, non-incremental program. An incremental emit wrote `dist/tsconfig.lib.tsbuildinfo`, which no Nx output
 * produces, and the 2026-10-28 Publish refused the CLI's verified dist at its merge boundary for that file.
 */
it('the rebuild emits as the Nx emit does, writing no build info into dist', () => {
  const { root, enter, dispose } = staleCheckout();
  try {
    const entered = enter();
    expect(entered.status, `${entered.stdout}${entered.stderr}`).toBe(0);
    const calls = readFileSync(join(root, 'ttsc-calls'), 'utf8').trim().split('\n');
    expect(calls.length).toBeGreaterThan(0);
    for (const call of calls) {
      expect(call).toBe(
        '-p tsconfig.lib.json --emit --composite false --declaration true --declarationMap true --emitDeclarationOnly false --incremental false',
      );
    }
  } finally {
    dispose();
  }
});
it('bootstrap emits the native-free lineage entry before its consumer and repairs a missing declaration in a warm dist', () => {
  const { root, plugin, enter, dispose } = staleCheckout();
  try {
    const owner = join(root, 'packages/renamed-runtime');
    mkdirSync(join(owner, 'src'), { recursive: true });
    mkdirSync(join(owner, 'dist/ts'), { recursive: true });
    writeFileSync(join(owner, 'package.json'), JSON.stringify({ name: '@smoothbricks/cowshed' }));
    writeFileSync(join(owner, 'tsconfig.lib.json'), '{}');
    writeFileSync(join(owner, 'tsconfig.lineage.json'), '{}');
    writeFileSync(join(owner, 'src/process-lineage.ts'), 'export {};\n');
    writeFileSync(join(owner, 'dist/ts/index.d.ts'), 'export {};\n');
    for (const file of ['package.json', 'tsconfig.lib.json', 'tsconfig.lineage.json', 'src/process-lineage.ts']) {
      utimesSync(join(owner, file), new Date(0), new Date(0));
    }
    const marker = join(owner, buildMarker);
    mkdirSync(dirname(marker), { recursive: true });
    writeFileSync(marker, '');
    utimesSync(marker, new Date(1000), new Date(1000));
    mkdirSync(join(plugin, 'node_modules/@smoothbricks'), { recursive: true });
    symlinkSync(owner, join(plugin, 'node_modules/@smoothbricks/cowshed'));
    writeFileSync(
      join(plugin, 'package.json'),
      JSON.stringify({ dependencies: { '@smoothbricks/cowshed': 'workspace:*' } }),
    );

    const bin = join(root, 'bin');
    writeFileSync(
      join(bin, 'tsc'),
      `#!/bin/sh
set -eu
root=$(dirname "$(dirname "$0")")
[ "$PWD" = "$root/packages/renamed-runtime" ]
[ "$*" = '--build tsconfig.lineage.json --force' ]
printf 'lineage\n' >> "$root/compiler-order"
mkdir -p dist/lineage
printf 'export {};\n' > dist/lineage/process-lineage.d.ts
printf 'export {};\n' > dist/lineage/process-lineage.js
`,
    );
    writeFileSync(
      join(bin, 'ttsc'),
      `#!/bin/sh
set -eu
root=$(dirname "$(dirname "$0")")
if [ ! -f "$root/packages/renamed-runtime/dist/lineage/process-lineage.d.ts" ]; then
  printf 'Cannot find module @smoothbricks/cowshed/process-lineage\n' >&2
  exit 79
fi
[ -f "$root/packages/renamed-runtime/dist/lineage/process-lineage.js" ]
[ "$PWD" != "$root/packages/renamed-runtime" ]
mkdir -p dist
printf 'export {};' > dist/index.js
basename "$PWD" >> "$root/compiler-order"
`,
    );
    chmodSync(join(bin, 'tsc'), 0o755);
    chmodSync(join(bin, 'ttsc'), 0o755);

    const entered = enter();
    expect(entered.status, `${entered.stdout}${entered.stderr}`).toBe(0);
    const order = join(root, 'compiler-order');
    expect(readFileSync(order, 'utf8').trim().split('\n')).toEqual(['lineage', 'nx-plugin', 'cli']);
    const warm = enter();
    expect(warm.status, `${warm.stdout}${warm.stderr}`).toBe(0);
    expect(readFileSync(order, 'utf8').trim().split('\n')).toEqual(['lineage', 'nx-plugin', 'cli']);

    rmSync(join(owner, 'dist/lineage/process-lineage.d.ts'));
    const repaired = enter();
    expect(repaired.status, `${repaired.stdout}${repaired.stderr}`).toBe(0);
    expect(readFileSync(order, 'utf8').trim().split('\n')).toEqual(['lineage', 'nx-plugin', 'cli', 'lineage']);

    const configuration = join(owner, 'tsconfig.lineage.json');
    writeFileSync(configuration, '{"compilerOptions":{"strict":true}}');
    const changedAt = new Date(statSync(marker).mtimeMs + 1000);
    utimesSync(configuration, changedAt, changedAt);
    const reconfigured = enter();
    expect(reconfigured.status, `${reconfigured.stdout}${reconfigured.stderr}`).toBe(0);
    expect(readFileSync(order, 'utf8').trim().split('\n')).toEqual([
      'lineage',
      'nx-plugin',
      'cli',
      'lineage',
      'lineage',
    ]);
  } finally {
    dispose();
  }
});
/**
 * A Cargo resolution holds `cargo-resolve/<id>/resolve.lock` while locked, offline Cargo runs, then publishes its
 * entry by a temporary file and a rename in that directory. A shell entry that emptied `.nx/workspace-data` removed
 * the directory from under its holder, and the publication failed `ENOENT` opening the temporary file.
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
    for (const name of ['nx-plugin', 'cli']) {
      const directory = join(root, 'packages', name);
      mkdirSync(join(directory, 'src'), { recursive: true });
      writeFileSync(join(directory, 'package.json'), '{}');
      writeFileSync(join(directory, 'tsconfig.lib.json'), '{}');
      writeFileSync(join(directory, 'src/index.ts'), 'export {};\n');
    }
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

/**
 * The plugin and CLI load their workspace runtime dependencies from dist. Bootstrap visits their shared closure once,
 * dependency first, rebuilds missing or empty output even with an existing marker, and skips current packages. Only
 * the plugin's closure invalidates Nx state; a CLI-only rebuild leaves the daemon and graph alone.
 */
it('a stale, unbuilt or emptied workspace dependency is rebuilt before the plugin and CLI, a current one is not', () => {
  const root = realpathSync(mkdtempSync(join(tmpdir(), 'smoo-enter-shell-')));
  try {
    // `verified` is when the package's marker says its `dist` was last current; `dist` lists the files that holds.
    const packageAt = (directory: string, manifest: object, verified: Date, dist: string[] | null = ['index.js']) => {
      mkdirSync(join(directory, 'src'), { recursive: true });
      if (dist !== null) mkdirSync(join(directory, 'dist'), { recursive: true });
      for (const file of dist ?? []) writeFileSync(join(directory, 'dist', file), 'export {};\n');
      writeFileSync(join(directory, 'package.json'), JSON.stringify(manifest));
      writeFileSync(join(directory, 'tsconfig.lib.json'), '{}');
      writeFileSync(join(directory, 'src/index.ts'), 'export {};\n');
      const marker = join(directory, buildMarker);
      mkdirSync(dirname(marker), { recursive: true });
      writeFileSync(marker, '');
      utimesSync(marker, verified, verified);
    };
    const never = new Date(0);
    const later = new Date(Date.now() + 60_000);
    const plugin = join(root, 'packages/nx-plugin');
    const tool = join(root, 'packages/tool');
    const kept = join(root, 'packages/kept');
    const unbuilt = join(root, 'packages/unbuilt');
    const wiped = join(root, 'packages/wiped');
    const cli = join(root, 'packages/cli');
    const cliOnly = join(root, 'packages/cli-only');
    packageAt(cliOnly, {}, never);
    packageAt(
      cli,
      {
        dependencies: {
          '@fixture/tool': 'workspace:*',
          '@fixture/cli-only': 'workspace:*',
          '@fixture/plugin': 'workspace:*',
        },
      },
      later,
      null,
    );
    mkdirSync(join(cli, 'node_modules/@fixture'), { recursive: true });
    symlinkSync('../../../tool', join(cli, 'node_modules/@fixture/tool'));
    symlinkSync('../../../cli-only', join(cli, 'node_modules/@fixture/cli-only'));
    symlinkSync('../../../nx-plugin', join(cli, 'node_modules/@fixture/plugin'));
    packageAt(tool, {}, never);
    packageAt(kept, {}, later);
    // The marker outlives a deleted or emptied `dist`; it does not make one current.
    packageAt(unbuilt, {}, later, null);
    packageAt(wiped, {}, later, []);
    packageAt(
      plugin,
      {
        dependencies: {
          '@fixture/tool': 'workspace:*',
          '@fixture/kept': 'workspace:*',
          '@fixture/unbuilt': 'workspace:*',
          '@fixture/wiped': 'workspace:*',
          typia: '14.0.6',
        },
      },
      never,
    );
    mkdirSync(join(plugin, 'node_modules/@fixture'), { recursive: true });
    for (const name of ['tool', 'kept', 'unbuilt', 'wiped']) {
      symlinkSync(`../../../${name}`, join(plugin, `node_modules/@fixture/${name}`));
    }
    mkdirSync(join(root, 'tooling/direnv'), { recursive: true });

    const bin = join(root, 'bin');
    mkdirSync(bin);
    const builds = join(root, 'ttsc-builds');
    // A build makes its `dist` and an output, and leaves no TypeScript build info in it.
    writeFileSync(
      join(bin, 'ttsc'),
      `#!/bin/sh\npwd >> '${builds}'\nmkdir -p dist\necho 'export {};' > dist/index.js\n`,
    );
    chmodSync(join(bin, 'ttsc'), 0o755);

    const enter = () =>
      spawnSync('bun', [enterShell], {
        cwd: root,
        encoding: 'utf8',
        env: {
          ...process.env,
          DEVENV_ROOT: join(root, 'tooling/direnv'),
          PATH: `${bin}:${process.env.PATH}`,
          GITHUB_ACTIONS: 'true',
        },
      });
    for (const entry of ['first', 'second']) {
      const entered = enter();
      expect(entered.status, `${entry} entry: ${entered.stdout}${entered.stderr}`).toBe(0);
      // The second entry rebuilds nothing: the markers the first recorded hold, with no build info in any `dist`.
      expect(readFileSync(builds, 'utf8').trim().split('\n')).toEqual([tool, unbuilt, wiped, plugin, cliOnly, cli]);
    }
    for (const directory of [tool, unbuilt, wiped, plugin, cliOnly, cli]) {
      expect(readdirSync(join(directory, 'dist'))).toEqual(['index.js']);
    }
    const daemon = join(root, '.nx/workspace-data/d/server-process.json');
    mkdirSync(dirname(daemon), { recursive: true });
    writeFileSync(daemon, 'kept');
    const graph = join(root, '.nx/workspace-data/project-graph.json');
    writeFileSync(graph, 'kept');
    // Stale CLI source and an emptied CLI-only dependency must not clear the plugin's daemon or graph.
    utimesSync(join(cli, buildMarker), never, never);
    rmSync(join(cliOnly, 'dist/index.js'));
    const entered = enter();
    expect(entered.status, entered.stderr).toBe(0);
    expect(readFileSync(builds, 'utf8').trim().split('\n')).toEqual([
      tool,
      unbuilt,
      wiped,
      plugin,
      cliOnly,
      cli,
      cliOnly,
      cli,
    ]);
    expect(readFileSync(daemon, 'utf8')).toBe('kept');
    expect(readFileSync(graph, 'utf8')).toBe('kept');
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

/**
 * A marker only spares the next entry a rebuild, so one that cannot be recorded (here `node_modules` is a file) is
 * said on stderr and the shell still enters.
 */
it('a build marker that cannot be recorded warns and still lets the shell enter', () => {
  const { plugin, enter, dispose } = staleCheckout();
  try {
    rmSync(join(plugin, 'node_modules'), { recursive: true });
    writeFileSync(join(plugin, 'node_modules'), '');
    const entered = enter();
    expect(entered.status, `${entered.stdout}${entered.stderr}`).toBe(0);
    expect(entered.stderr).toContain(`Could not record the build marker ${join(plugin, buildMarker)}`);
  } finally {
    dispose();
  }
});

it('direct devenv entry sources the existing workspace policy before project hooks', () => {
  const module = readFileSync(join(import.meta.dir, 'devenv.smoo.nix'), 'utf8');
  // The prologue's entry, then its sourcing of the workspace file, which follows the managed PATH writer: that writer
  // serves a workspace cowshed had not served yet, so the shell enters on the served file.
  const prologue = module.match(
    /\(lib\.mkBefore ''\n( {6}cd "\$DEVENV_ROOT\/\.\.\/\.\."\n)[\s\S]*? {6}PATH="\$\("\$PWD\/tooling\/direnv\/repo-path"\)" \|\| exit \$\?\n(?: {6}#.*\n)*( {6}if \[ -f \.cowshed\/env \]; then\n[\s\S]*? {6}fi\n)/,
  );
  if (prologue === null) {
    throw new Error(
      'devenv.smoo.nix: no prologue entry, managed PATH writer and workspace file sourcing, in that order',
    );
  }
  const policy = `${prologue[1]}${prologue[2]}`;
  const root = realpathSync(mkdtempSync(join(tmpdir(), 'smoo-direct-entry-')));
  try {
    mkdirSync(join(root, 'tooling/direnv'), { recursive: true });
    mkdirSync(join(root, '.cowshed'));
    writeFileSync(
      join(root, '.cowshed/env'),
      'if [ "${GITHUB_ACTIONS:-}" != true ]; then unset CI; fi\nexport RUSTC_WRAPPER=/store/pinned-sccache\n',
    );
    for (const marker of ['false', 'true']) {
      const entered = spawnSync('/bin/sh', ['-c', `${policy}printf \'%s\\n\' "\${CI-unset}" "$RUSTC_WRAPPER"`], {
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
