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
  // Nx runs `bun install --lockfile-only`, but Bun currently leaves workspace
  // versions stale in bun.lock. `bun pm pack` then rewrites `workspace:*` using
  // those stale lockfile versions instead of the current package.json versions.
  const updated = syncBunLockfileVersions(cwd);
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

function syncBunLockfileVersions(root: string): number {
  const lockfilePath = join(root, 'bun.lock');
  if (!existsSync(lockfilePath)) {
    throw new Error('bun.lock not found');
  }
  const packages = workspacePackages(root);
  let lockfile = readFileSync(lockfilePath, 'utf8');
  let updated = 0;
  for (const pkg of packages) {
    // Always mirror package.json. Bun can leave stale workspace versions in
    // bun.lock after version bumps; do not rewrite prereleases to older tags.
    const targetVersion = pkg.version;

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
    console.log(`fix:  ${relativePath}: ${lockVersion} -> ${targetVersion}`);
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
