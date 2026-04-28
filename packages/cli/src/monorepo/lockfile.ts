import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { escapeRegex, getWorkspacePackages } from '../lib/workspace.js';

export interface SyncBunLockfileVersionsOptions {
  log?: boolean;
}

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
    if (lockVersion === pkg.version) {
      if (log) {
        console.log(`ok:   ${relativePath} = ${pkg.version}`);
      }
      continue;
    }
    lockfile = lockfile.replace(pattern, `$1${pkg.version}$3`);
    if (log) {
      console.log(`fix:  ${relativePath}: ${lockVersion} -> ${pkg.version}`);
    }
    updated++;
  }
  if (updated > 0) {
    writeFileSync(lockfilePath, lockfile);
  }
  if (log) {
    console.log(
      updated > 0 ? `Updated ${updated} workspace version(s) in bun.lock` : 'All workspace versions already in sync.',
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
    if (lockVersion !== pkg.version) {
      console.error(`${relativePath}: bun.lock workspace version must be ${pkg.version}, got ${lockVersion}`);
      failures++;
    }
  }
  if (failures === 0) {
    console.log('bun.lock workspace versions are valid.');
  }
  return failures;
}
