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
process.env.PATH = `${path.join(projectRoot, 'tooling')}:${path.join(projectRoot, 'node_modules/.bin')}:${process.env.PATH ?? ''}`;

await import('./setup-environment.ts');
await rebuildNxPluginIfStale();

async function rebuildNxPluginIfStale(): Promise<void> {
  const buildMarker = path.join(projectRoot, 'packages/nx-plugin/dist/tsconfig.lib.tsbuildinfo');
  const markerStat = await stat(buildMarker).catch(() => null);
  // Directory mtimes miss edits to existing nested files, so compare source file mtimes against the TS build marker.
  if (!markerStat || (await hasFileNewerThanMarker(markerStat.mtimeMs))) {
    await $`nx run nx-plugin:tsc-js`;
    await $`nx reset`;
  }
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
