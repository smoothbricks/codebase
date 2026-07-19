import { execSync } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { escapeRegex, getWorkspacePackages } from '../lib/workspace.js';

export interface SyncBunLockfileVersionsOptions {
  log?: boolean;
  /** `git add` bun.lock when versions were resynced. */
  stage?: boolean;
  /**
   * `install` (default for validation): lockfile must match package.json.
   * `publish`: rewrite unpublished prerelease package.json versions to the last
   * stable git tag so `bun pm pack` embeds installable dependency versions.
   */
  mode?: 'install' | 'publish';
}

// Temporary Bun workaround. Delete once supported Bun versions stop leaving
// workspace package versions stale in bun.lock after manifest bumps, and stop
// resolving `workspace:*` from the lockfile during `bun pm pack`:
// - https://github.com/oven-sh/bun/issues/18906
// - https://github.com/oven-sh/bun/issues/20477
// - https://github.com/oven-sh/bun/issues/20829
//
// Two modes, because one lockfile serves two jobs:
//
// 1) install / frozen CI  → lockfile ≡ package.json (including -next)
// 2) pre-publish pack    → for package.json prereleases that were never published
//                          (post-release "prepare next"), lockfile entry := last
//                          stable tag so a releasing package does not embed an
//                          unpublished -next dependency version.
//
// Do not run publish-mode rewrite in day-to-day validate or pre-commit: bun install
// rewrites lockfile back to package.json and frozen CI then fails.
export function syncBunLockfileVersions(root: string, options: SyncBunLockfileVersionsOptions = {}): number {
  const log = options.log ?? true;
  const mode = options.mode ?? 'publish';
  const lockfilePath = join(root, 'bun.lock');
  if (!existsSync(lockfilePath)) {
    throw new Error('bun.lock not found');
  }
  const packages = getWorkspacePackages(root);
  let lockfile = readFileSync(lockfilePath, 'utf8');
  let updated = 0;
  for (const pkg of packages) {
    const targetVersion = lockfileTargetVersion(root, pkg.projectName, pkg.version, mode);
    const relativePath = pkg.path.replaceAll('\\', '/');
    const escaped = escapeRegex(relativePath);
    const pattern = new RegExp(`("${escaped}":\\s*\\{[^}]*"version":\\s*")([^"]+)(")`);
    const match = lockfile.match(pattern);
    if (!match) {
      if (log) {
        console.log(`skip: ${relativePath} (not found in lockfile)`);
      }
      continue;
    }
    const lockVersion = match[2];
    if (lockVersion === targetVersion) {
      if (log) {
        console.log(`ok:   ${relativePath} = ${targetVersion}`);
      }
      continue;
    }
    lockfile = lockfile.replace(pattern, `$1${targetVersion}$3`);
    if (log) {
      const suffix =
        mode === 'publish' && targetVersion !== pkg.version
          ? ` (latest stable tag; package.json has ${pkg.version})`
          : '';
      console.log(`fix:  ${relativePath}: ${lockVersion} -> ${targetVersion}${suffix}`);
    }
    updated++;
  }
  if (updated > 0) {
    writeFileSync(lockfilePath, lockfile);
    if (options.stage) {
      execSync('git add bun.lock', { cwd: root, stdio: ['pipe', 'pipe', 'pipe'] });
    }
  }
  if (log || updated > 0) {
    console.log(
      updated > 0
        ? `Updated ${updated} workspace version(s) in bun.lock${options.stage ? ' (staged)' : ''}`
        : 'All workspace versions already in sync.',
    );
  }
  return updated;
}

/** Install/CI invariant: bun.lock workspace versions match package.json. */
export function validateBunLockfileVersions(root: string): number {
  const lockfilePath = join(root, 'bun.lock');
  if (!existsSync(lockfilePath)) {
    console.error('bun.lock not found');
    return 1;
  }
  const packages = getWorkspacePackages(root);
  const lockfile = readFileSync(lockfilePath, 'utf8');
  let failures = 0;
  for (const pkg of packages) {
    const targetVersion = pkg.version;
    const relativePath = pkg.path.replaceAll('\\', '/');
    const escaped = escapeRegex(relativePath);
    const pattern = new RegExp(`("${escaped}":\\s*\\{[^}]*"version":\\s*")([^"]+)(")`);
    const match = lockfile.match(pattern);
    if (!match) {
      console.error(`bun.lock missing workspace entry for ${relativePath}`);
      failures++;
      continue;
    }
    const lockVersion = match[2];
    if (lockVersion !== targetVersion) {
      console.error(`${relativePath}: bun.lock workspace version must be ${targetVersion}, got ${lockVersion}`);
      failures++;
    }
  }
  if (failures === 0) {
    console.log('bun.lock workspace versions are valid.');
  }
  return failures;
}

/**
 * Version Bun should embed for a workspace dependency when packing for publish.
 * Unpublished package.json prereleases (prepare-next) map to the last stable tag.
 */
export function publishPackDependencyVersion(root: string, projectName: string, packageVersion: string): string {
  return lockfileTargetVersion(root, projectName, packageVersion, 'publish');
}

function lockfileTargetVersion(
  root: string,
  projectName: string,
  packageVersion: string,
  mode: 'install' | 'publish',
): string {
  if (mode === 'install' || !packageVersion.includes('-')) {
    return packageVersion;
  }
  return latestStableTagVersion(root, projectName) ?? packageVersion;
}

function latestStableTagVersion(root: string, projectName: string): string | null {
  try {
    const output = execSync(`git tag --list '${projectName}@*' --sort=-v:refname`, {
      cwd: root,
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    const prefix = `${projectName}@`;
    for (const line of output.split('\n')) {
      const tag = line.trim();
      if (!tag.startsWith(prefix)) continue;
      const version = tag.slice(prefix.length);
      if (version && !version.includes('-')) {
        return version;
      }
    }
    return null;
  } catch {
    return null;
  }
}
