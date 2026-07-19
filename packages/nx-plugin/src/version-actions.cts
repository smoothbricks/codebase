import { execSync } from 'node:child_process';
import { existsSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import type { AfterAllProjectsVersioned, VersionActions } from 'nx/release';

type VersionActionsModule = typeof VersionActions & {
  default?: typeof VersionActions & { afterAllProjectsVersioned: AfterAllProjectsVersioned };
  afterAllProjectsVersioned: AfterAllProjectsVersioned;
};

const nxJsVersionActions = require('@nx/js/src/release/version-actions') as VersionActionsModule;
const baseVersionActions = nxJsVersionActions.default ?? nxJsVersionActions;

const afterAllProjectsVersioned: AfterAllProjectsVersioned = async (cwd, options) => {
  const result = await nxJsVersionActions.afterAllProjectsVersioned(cwd, options);

  // Temporary Bun workaround. Delete this hook together with the CLI lockfile
  // sync/validation code once supported Bun versions fix all three issues:
  // - https://github.com/oven-sh/bun/issues/18906
  // - https://github.com/oven-sh/bun/issues/20477
  // - https://github.com/oven-sh/bun/issues/20829
  //
  // After version bumps, rewrite bun.lock for pack/publish:
  // - stable package.json versions stay as-is
  // - unpublished package.json prereleases (prepare-next) map to last stable tag
  //   so a releasing package does not embed unpublishable -next deps via workspace:*
  const updated = syncBunLockfileVersionsForPublish(cwd);
  if (updated === 0) {
    return result;
  }

  return {
    changedFiles: Array.from(new Set([...result.changedFiles, 'bun.lock'])),
    deletedFiles: result.deletedFiles,
  };
};

baseVersionActions.afterAllProjectsVersioned = afterAllProjectsVersioned;

export = baseVersionActions;

function syncBunLockfileVersionsForPublish(root: string): number {
  const lockfilePath = join(root, 'bun.lock');
  if (!existsSync(lockfilePath)) {
    throw new Error('bun.lock not found');
  }
  const packages = workspacePackages(root);
  let lockfile = readFileSync(lockfilePath, 'utf8');
  let updated = 0;
  for (const pkg of packages) {
    const targetVersion = publishLockfileVersion(root, pkg.projectName, pkg.version);
    const relativePath = pkg.path.replaceAll('\\', '/');
    const escaped = escapeRegex(relativePath);
    const pattern = new RegExp(`("${escaped}":\\s*\\{[^}]*"version":\\s*")([^"]+)(")`);
    const match = lockfile.match(pattern);
    if (!match) {
      console.log(`skip: ${relativePath} (not found in lockfile)`);
      continue;
    }
    const lockVersion = match[2];
    if (lockVersion === targetVersion) {
      console.log(`ok:   ${relativePath} = ${targetVersion}`);
      continue;
    }
    lockfile = lockfile.replace(pattern, `$1${targetVersion}$3`);
    const suffix = targetVersion !== pkg.version ? ` (latest stable tag; package.json has ${pkg.version})` : '';
    console.log(`fix:  ${relativePath}: ${lockVersion} -> ${targetVersion}${suffix}`);
    updated++;
  }
  if (updated > 0) {
    writeFileSync(lockfilePath, lockfile);
  }
  console.log(
    updated > 0 ? `Updated ${updated} workspace version(s) in bun.lock` : 'All workspace versions already in sync.'
  );
  return updated;
}

function publishLockfileVersion(root: string, projectName: string, packageVersion: string): string {
  if (!packageVersion.includes('-')) {
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

interface WorkspacePackage {
  path: string;
  name: string;
  projectName: string;
  version: string;
}

function workspacePackages(root: string): WorkspacePackage[] {
  const packagesRoot = join(root, 'packages');
  return readdirSync(packagesRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => {
      const path = `packages/${entry.name}`;
      const packageJsonPath = join(root, path, 'package.json');
      if (!existsSync(packageJsonPath)) {
        return null;
      }
      const parsed: unknown = JSON.parse(readFileSync(packageJsonPath, 'utf8'));
      if (!isRecord(parsed) || typeof parsed.name !== 'string' || typeof parsed.version !== 'string') {
        return null;
      }
      const json = parsed;
      const nx = isRecord(json.nx) ? json.nx : null;
      const projectName = (nx ? (typeof nx.name === 'string' ? nx.name : null) : null) ?? json.name;
      return { path, name: json.name, projectName, version: json.version };
    })
    .filter((pkg): pkg is WorkspacePackage => pkg !== null);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
