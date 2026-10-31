import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { escapeRegex, getWorkspacePackages } from '../lib/workspace.js';

export interface SyncBunLockfileVersionsOptions {
  log?: boolean;
  /** `git add` bun.lock when versions were resynced. */
  stage?: boolean;
}

// Install / frozen CI invariant: each bun.lock workspace version equals its package.json version, `-next`
// prereleases included. A release pack never rewrites the lockfile: it pins its workspace dependencies in the packed
// manifest instead (withPublishManifest).
export function syncBunLockfileVersions(root: string, options: SyncBunLockfileVersionsOptions = {}): number {
  const log = options.log ?? true;
  const lockfilePath = join(root, 'bun.lock');
  if (!existsSync(lockfilePath)) {
    throw new Error('bun.lock not found');
  }
  const packages = getWorkspacePackages(root);
  let lockfile = readFileSync(lockfilePath, 'utf8');
  let updated = 0;
  for (const pkg of packages) {
    const targetVersion = pkg.version;
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
      console.log(`fix:  ${relativePath}: ${lockVersion} -> ${targetVersion}`);
    }
    updated++;
  }
  if (updated > 0) {
    writeFileSync(lockfilePath, lockfile);
    if (options.stage) {
      // eslint-disable-next-line deadline/no-unbounded-wait -- WHY unbounded: a local git index update: it asks no remote, and a held index.lock fails it at once rather than waiting, so only the operator's interrupt ends a hung one
      const result = spawnSync('git', ['add', 'bun.lock'], { cwd: root, stdio: 'inherit' });
      if (result.error) {
        throw new Error(`git add bun.lock failed to start: ${result.error.message}`);
      }
      if (result.status !== 0) {
        throw new Error(`git add bun.lock failed with exit code ${result.status ?? 1}`);
      }
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
