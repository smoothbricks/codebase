import { execSync } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { escapeRegex, getWorkspacePackages } from '../lib/workspace.js';

export interface SyncBunLockfileVersionsOptions {
  log?: boolean;
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
    // If the package.json version is a prerelease (e.g. 0.2.2-next.0) it may
    // never have been published.  Use the latest stable git tag version so
    // that `bun pm pack` writes an installable dependency range for consumers.
    const targetVersion = pkg.version.includes('-')
      ? (latestStableTagVersion(root, pkg.projectName) ?? pkg.version)
      : pkg.version;

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
      const suffix = targetVersion !== pkg.version ? ` (latest stable tag; package.json has ${pkg.version})` : '';
      console.log(`fix:  ${relativePath}: ${lockVersion} -> ${targetVersion}${suffix}`);
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
    const targetVersion = pkg.version.includes('-')
      ? (latestStableTagVersion(root, pkg.projectName) ?? pkg.version)
      : pkg.version;

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
