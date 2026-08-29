#!/usr/bin/env bun
import { readdir, stat, utimes } from 'node:fs/promises';
import path from 'node:path';
import { $ } from 'bun';

const devenvRoot = process.env.DEVENV_ROOT;
if (!devenvRoot) {
  throw new Error('DEVENV_ROOT must be set before running enter-shell.ts');
}

const projectRoot = path.resolve(devenvRoot, '../..');
process.chdir(projectRoot);

await import('./setup-environment.ts');
await rebuildNxPluginIfStale();

async function rebuildNxPluginIfStale(): Promise<void> {
  const buildMarker = path.join(projectRoot, 'packages/nx-plugin/dist/tsconfig.lib.tsbuildinfo');
  const markerStat = await stat(buildMarker).catch(() => null);
  // Directory mtimes miss edits to existing nested files, so compare source file mtimes against the TS build marker.
  if (!markerStat || (await hasFileNewerThanMarker(markerStat.mtimeMs))) {
    // Bootstrap must not call `nx`: the project graph loads this plugin and
    // `require('typescript').readConfigFile`. A cold macOS shell (publish) has
    // no prior graph cache, so `nx run nx-plugin:tsc-js` deadlocks on the plugin
    // it is trying to build. `ttsc` only needs the package tsconfig.
    await runQuietly('ttsc', ['-p', 'tsconfig.lib.json', '--emit'], path.join(projectRoot, 'packages/nx-plugin'));
    // The marker records when dist was last VERIFIED current, not when tsc last
    // chose to write bytes. ttsc is content-incremental, so an mtime-only change —
    // a checkout, a revert, a formatter rewriting a file identically — makes it
    // exit 0 emitting nothing and leaves the marker behind the sources forever.
    // Without this touch the condition above never clears again, so every later
    // shell entry and every direnv reload rebuilds and calls clearNxDaemonState,
    // permanently destroying Nx's task cache for the whole repository.
    await utimes(buildMarker, new Date(), new Date()).catch(() => {});
    await clearNxDaemonState();
  }
}

async function clearNxDaemonState(): Promise<void> {
  // Only `.nx/workspace-data` — the daemon socket and project-graph DB, which is
  // the state a rebuilt plugin actually invalidates. NOT `.nx/cache`.
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
  // unreachable rather than wrong once inference changes. Clearing the graph DB is
  // what makes the new plugin take effect; clearing results was only collateral.
  //
  // Deliberately the workspace-local path, NOT NX_WORKSPACE_DATA_DIRECTORY. Host
  // CI runners redirect that to a shared per-lane tree which also holds the cache
  // provenance DB, and a fresh checkout has no build marker, so this runs on every
  // job: following the variable would delete that DB every time and the shared
  // cache could never be warm. Nx's own graph cache accounts for changed plugin
  // files, so the redirected case needs no help from here.
  await runQuietly('rm', ['-rf', path.join(projectRoot, '.nx/workspace-data')], projectRoot);
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

async function hasFileNewerThanMarker(markerMtimeMs: number): Promise<boolean> {
  const sourcePaths = [
    path.join(projectRoot, 'packages/nx-plugin/src'),
    path.join(projectRoot, 'packages/nx-plugin/tsconfig.lib.json'),
  ];

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
