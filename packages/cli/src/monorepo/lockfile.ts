import { execSync } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { escapeRegex, getWorkspacePackages } from '../lib/workspace.js';

export interface SyncBunLockfileVersionsOptions {
  log?: boolean;
  /** `git add` bun.lock when versions were resynced. */
  stage?: boolean;
}

// Temporary Bun workaround. Delete this sync function, validateBunLockfileVersions,
// the `smoo monorepo sync-bun-lockfile-versions` command, and the matching Nx
// versionActions hook once supported Bun versions stop leaving workspace package
// versions stale in bun.lock after manifest bumps. Until then, `bun pm pack`
// rewrites `workspace:*` dependencies using those stale lockfile versions instead
// of the current package.json versions. Track removal against:
// - https://github.com/oven-sh/bun/issues/18906
// - https://github.com/oven-sh/bun/issues/20477
// - https://github.com/oven-sh/bun/issues/20829
//
// Target is always the current package.json version. Do not rewrite prereleases
// to older stable tags — that fights `bun install` and churns bun.lock forever.
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
