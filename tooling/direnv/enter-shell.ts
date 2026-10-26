#!/usr/bin/env bun
import { mkdir, readdir, readFile, realpath, rm, stat, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { $ } from 'bun';

const devenvRoot = process.env.DEVENV_ROOT;
if (!devenvRoot) {
  throw new Error('DEVENV_ROOT must be set before running enter-shell.ts');
}

const projectRoot = path.resolve(devenvRoot, '../..');
process.chdir(projectRoot);

/**
 * What a rebuilt plugin invalidates in `.nx/workspace-data`, by pinned Nx 23.2.1's own names: the daemon's directory
 * (`daemon/tmp-dir.js`), whose record names the daemon that loaded the old plugin, which exits once the record is gone;
 * and the project graph that plugin computed (`project-graph/nx-deps-cache.js`).
 */
const pluginDerivedNxState = ['d', 'project-graph.json', 'file-map.json', 'source-maps.json'] as const;

/**
 * Where a package's shell entry records that its `dist` was last verified current: in the package's own tool state
 * (`node_modules/.cache`, a build-volume path in a cowshed checkout), never under `dist`. The tsc-js target owns `dist`
 * whole: its emit is not incremental, so it writes no TypeScript build info, and it removes from `dist` whatever it did
 * not write, the build info this entry's own emit leaves there included. Nx restoring the target's cache artifact
 * replaces `dist` the same way. A marker under `dist` would be gone after every build, and the next shell entry would
 * rebuild a package that is current.
 */
const buildMarkerFile = 'node_modules/.cache/smoo/dist-verified';

// devenv runs this hook twice per shell entry. First it captures the shell's environment: a pass that runs the shell
// hook to read what the prologue exports, marked DEVENV_SKIP_TASKS (`capture_shell_environment` in cachix/devenv sets
// it on that subprocess alone), and that runs BEFORE the tasks ordered ahead of devenv:enterShell (smoo:install,
// smoo:cargo-fetch). Then it runs those tasks and starts the shell, whose hook runs again with everything installed. A
// fresh checkout, or a runner that restored node_modules cached for another lockfile, has no working `ttsc` in the
// first pass, and a failing hook aborts that capture ("Shell environment capture failed") before smoo:install could
// make one. So only the entry proper rebuilds the plugin and syncs the runtime pins.
if ((process.env.DEVENV_SKIP_TASKS ?? '') === '') {
  await rebuildNxPluginIfStale();

  await syncRuntimePinsIfDrifted();
}

/** Shell entry tracks runtime pins: cheap local `--version` probes, sync on drift, best-effort (offline failure warns, validate is the gate, a CI runner never rewrites). */
async function syncRuntimePinsIfDrifted(): Promise<void> {
  // A CI runner never rewrites pins. GITHUB_ACTIONS, not CI: agent harnesses set
  // CI=true on every command, and keying on it left the pins drifting locally.
  if (process.env.GITHUB_ACTIONS === 'true') {
    return;
  }
  const manifest = await Bun.file(path.join(projectRoot, 'package.json'))
    .json()
    .catch(() => null);
  const pinnedBun = typeof manifest?.packageManager === 'string' ? manifest.packageManager : null;
  const pinnedNode = typeof manifest?.engines?.node === 'string' ? manifest.engines.node : null;
  const [bun, node] = await Promise.all([
    $`bun --version`
      .cwd(projectRoot)
      .quiet(true)
      .nothrow()
      .then((r) => (r.exitCode === 0 ? r.text().trim() : null)),
    $`node --version`
      .cwd(projectRoot)
      .quiet(true)
      .nothrow()
      .then((r) => (r.exitCode === 0 ? r.text().trim().replace(/^v/, '') : null)),
  ]);
  const nodeMajor = node?.split('.', 1)[0];
  if (
    (pinnedBun === null || pinnedBun === `bun@${bun}`) &&
    (pinnedNode === null || nodeMajor === undefined || pinnedNode === `>=${nodeMajor}.0.0`)
  ) {
    return;
  }
  try {
    await runQuietly('smoo', ['monorepo', 'init', '--runtime-only'], projectRoot);
  } catch (error) {
    console.error(
      `! Runtime pin sync failed (${error instanceof Error ? error.message : String(error)}); shell continues with pinned versions. Run \`smoo monorepo init --runtime-only\` once online.`,
    );
  }
}

/**
 * The plugin and, first, every workspace package it depends on at run time (its `workspace:*` `dependencies`, and
 * theirs): loading the plugin imports their `dist`, so a checkout without them cannot compute the graph that would
 * build them. Each comes before whatever depends on it.
 */
async function bootstrapPackages(): Promise<string[]> {
  const ordered: string[] = [];
  const visit = async (directory: string): Promise<void> => {
    if (ordered.includes(directory)) return;
    const manifest: unknown = JSON.parse(await readFile(path.join(directory, 'package.json'), 'utf8'));
    const dependencies =
      typeof manifest === 'object' && manifest !== null && 'dependencies' in manifest
        ? manifest.dependencies
        : undefined;
    if (typeof dependencies === 'object' && dependencies !== null) {
      for (const [name, range] of Object.entries(dependencies)) {
        if (range !== 'workspace:*') continue;
        await visit(await realpath(path.join(directory, 'node_modules', name)));
      }
    }
    ordered.push(directory);
  };
  await visit(path.join(projectRoot, 'packages/nx-plugin'));
  return ordered;
}

async function rebuildNxPluginIfStale(): Promise<void> {
  let rebuilt = false;
  for (const directory of await bootstrapPackages()) {
    const buildMarker = path.join(directory, buildMarkerFile);
    const markerStat = await stat(buildMarker).catch(() => null);
    // The marker stands outside `dist`, so wiping `dist` leaves it behind: only a `dist` holding files can be current.
    const built = (await readdir(path.join(directory, 'dist')).catch(() => [])).length > 0;
    // Directory mtimes miss edits to existing nested files, so compare source file mtimes against the build marker.
    if (markerStat && built && !(await hasFileNewerThanMarker(directory, markerStat.mtimeMs))) continue;
    // Bootstrap must not call `nx`: the project graph loads this plugin and
    // `require('typescript').readConfigFile`. A cold macOS shell (publish) has
    // no prior graph cache, so `nx run nx-plugin:tsc-js` deadlocks on the plugin
    // it is trying to build. `ttsc` only needs the package tsconfig.
    await runQuietly('ttsc', ['-p', 'tsconfig.lib.json', '--emit'], directory);
    // The marker records when dist was last VERIFIED current, not when tsc last
    // chose to write bytes. ttsc is content-incremental, so an mtime-only change —
    // a checkout, a revert, a formatter rewriting a file identically — makes it
    // exit 0 emitting nothing, so the entry stamps the marker itself after every
    // successful emit. A marker left behind the sources would never clear the
    // condition above again, so every later shell entry and every direnv reload
    // would rebuild and call clearNxDaemonState, permanently destroying Nx's task
    // cache for the whole repository.
    await recordBuild(buildMarker);
    rebuilt = true;
  }
  if (rebuilt) await clearNxDaemonState();
}

/** Stamps `marker` with now. Failing only costs the next shell entry a rebuild, so it warns and lets the shell enter. */
async function recordBuild(marker: string): Promise<void> {
  try {
    await mkdir(path.dirname(marker), { recursive: true });
    await writeFile(marker, '');
  } catch (error) {
    console.error(
      `! Could not record the build marker ${marker} (${error instanceof Error ? error.message : String(error)}); the next shell entry rebuilds that package.`,
    );
  }
}

async function clearNxDaemonState(): Promise<void> {
  // Only what a rebuilt plugin invalidates (`pluginDerivedNxState`). NOT
  // `.nx/cache`, and NOT anything else in `.nx/workspace-data`.
  //
  // `.nx/cache` holds task results and the `terminalOutputs/<hash>` files the task
  // orchestrator writes when a task exits. Deleting it here raced every concurrent
  // Nx invocation: a shell entry or direnv reload (both run this file, and direnv
  // watches the devenv config) removed the directory while another terminal's Nx
  // was mid-run, and that run then died writing its own terminal output —
  // `ENOENT: open '.nx/cache/terminalOutputs/<hash>'` strictly after its tasks had
  // succeeded. `--skip-nx-cache` does not avoid it, because the orchestrator writes
  // that file regardless of whether results are cached.
  //
  // Keeping the task cache is safe, not merely cheaper: a target's hash covers its
  // resolved configuration, so entries produced by the previous plugin build are
  // unreachable rather than wrong once inference changes. Clearing the graph files
  // is what makes the new plugin take effect; clearing results was only collateral.
  //
  // The task database (`<machine>-v<schema>.db`, its SQLite `-wal`/`-shm`/`-journal`
  // and the `.lock` Nx opens it under) is the cache's index: Nx answers a hit only
  // for a `cache_outputs` row and evicts only by row, so deleting it leaves every
  // entry in `.nx/cache` unreachable and never evicted. It went with the graph
  // files once, on the first shell entry of a main whose land changed this plugin:
  // the volume main had just adopted kept its gate's entries and lost their index,
  // so every rebase onto main carried nothing (specs/cowshed/16_build_volumes.md,
  // "Every checkout has exactly one Nx state").
  //
  // Nothing else there is the old plugin's output either. Nx's locks and its
  // lockfile and file-hash caches do not depend on it, and the plugin's own state
  // (`cargo-resolve/`, `bounded-exec/`, producer receipts) is content-keyed, so a
  // rebuilt plugin finds it unreachable rather than wrong. A concurrent Nx process
  // writes that state under its own lock: emptying the directory raced it as
  // deleting `.nx/cache` did. A rebase's task-hash probe held its Cargo resolution
  // lock in `cargo-resolve/<id>/` when the Nx daemon job started beside it entered
  // a fresh shell over the just-rebased plugin and deleted the directory, and the
  // probe died `ENOENT` opening its cache entry's temporary file.
  //
  // Deliberately the workspace-local path, NOT NX_WORKSPACE_DATA_DIRECTORY. Host
  // CI runners redirect that to a shared per-lane tree which also holds the cache
  // provenance DB, and a fresh checkout has no build marker, so this runs on every
  // job: following the variable would clear that shared tree under every other job
  // of the lane. Nx's own graph cache accounts for changed plugin files, so the
  // redirected case needs no help from here.
  //
  // Through it, never the path itself: in a cowshed checkout the path is a link
  // onto the checkout's build volume, and removing it (as `rm -rf` of the path
  // did) left the next Nx run to make a real directory in its place, off the
  // volume, until cowshed's next refresh discarded it.
  const data = path.join(projectRoot, '.nx/workspace-data');
  await Promise.all(pluginDerivedNxState.map((entry) => rm(path.join(data, entry), { recursive: true, force: true })));
}

async function runQuietly(command: string, args: readonly string[], cwd: string): Promise<void> {
  const result = await $`${command} ${args}`.cwd(cwd).quiet(true).nothrow();
  if (result.exitCode === 0) {
    return;
  }
  if (result.stdout.length > 0) {
    process.stdout.write(result.stdout);
  }
  if (result.stderr.length > 0) {
    process.stderr.write(result.stderr);
  }
  throw new Error(`${command} ${args.join(' ')} failed with exit code ${result.exitCode}`);
}

async function hasFileNewerThanMarker(directory: string, markerMtimeMs: number): Promise<boolean> {
  const sourcePaths = [path.join(directory, 'src'), path.join(directory, 'tsconfig.lib.json')];

  for (const sourcePath of sourcePaths) {
    if (await pathHasFileNewerThanMarker(sourcePath, markerMtimeMs)) {
      return true;
    }
  }
  return false;
}

// `tsconfig.lib.json` excludes `src/**/*.test.ts` and `src/**/__tests__/**`, so those
// files cannot change dist. Counting them made editing any plugin test mark the
// plugin stale, which rebuilt it and cleared Nx's daemon state on every shell entry.

async function pathHasFileNewerThanMarker(sourcePath: string, markerMtimeMs: number): Promise<boolean> {
  const sourceStat = await stat(sourcePath);
  if (sourceStat.isFile()) {
    return sourceStat.mtimeMs > markerMtimeMs;
  }
  if (!sourceStat.isDirectory()) {
    return false;
  }

  for (const entry of await readdir(sourcePath, { withFileTypes: true })) {
    const childPath = path.join(sourcePath, entry.name);
    if (entry.isDirectory()) {
      if (entry.name === '__tests__') {
        continue;
      }
      if (await pathHasFileNewerThanMarker(childPath, markerMtimeMs)) {
        return true;
      }
      continue;
    }
    if (entry.name.endsWith('.test.ts')) {
      continue;
    }
    if (entry.isFile() && (await stat(childPath)).mtimeMs > markerMtimeMs) {
      return true;
    }
  }
  return false;
}
