#!/usr/bin/env bun
import { readdir, stat } from 'node:fs/promises';
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
    await clearNxDaemonState();
  }
}

async function clearNxDaemonState(): Promise<void> {
  // Equivalent to `nx reset` without constructing the project graph.
  await $`rm -rf ${path.join(projectRoot, '.nx/cache')} ${path.join(projectRoot, '.nx/workspace-data')}`
    .quiet(true)
    .nothrow();
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
      if (await pathHasFileNewerThanMarker(childPath, markerMtimeMs)) {
        return true;
      }
      continue;
    }
    if (entry.isFile() && (await stat(childPath)).mtimeMs > markerMtimeMs) {
      return true;
    }
  }
  return false;
}
