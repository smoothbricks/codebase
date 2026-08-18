/**
 * Locate the consumer workspace's `packages/` directory for test-trace
 * auto-discovery.
 *
 * WHY anchor on cwd, not import.meta: this module ships inside an installed
 * dependency (Bun isolated store: `node_modules/.bun/<pkg>@<v>/node_modules/…`),
 * so its own file location says nothing about the consuming repo's layout. The
 * test process cwd is always inside the consumer workspace, and the nearest
 * ancestor holding a Bun lockfile is that workspace's install root.
 *
 * WHY lockfile-only markers: every Bun test run happens in an installed
 * workspace, which by definition has `bun.lock`/`bun.lockb` at its root.
 * Probing `package.json` contents would add a JSON parse for no extra
 * coverage. Nonstandard layouts (no `packages/` convention) are served by the
 * `LMAO_PACKAGES_DIR` override instead of marker heuristics.
 */

import { existsSync } from 'node:fs';
import { dirname, isAbsolute, join, resolve } from 'node:path';

export type WorkspacePackagesDirResult =
  | { readonly ok: true; readonly packagesDir: string; readonly source: 'env' | 'workspace-root' }
  | { readonly ok: false; readonly searched: readonly string[] };

const WORKSPACE_LOCKFILES = ['bun.lock', 'bun.lockb'] as const;

function isWorkspaceRoot(directory: string): boolean {
  return WORKSPACE_LOCKFILES.some((lockfile) => existsSync(join(directory, lockfile)));
}

/**
 * Walk up from `startDir` to the nearest workspace root that has a `packages/`
 * directory. A root whose `packages/` is missing (e.g. a nested example app
 * with its own lockfile) does not stop the walk — the search continues upward
 * so a shadowing lockfile cannot hide the real monorepo root.
 *
 * `LMAO_PACKAGES_DIR` (absolute, or relative to `startDir`) short-circuits the
 * walk entirely for layouts that do not follow the `packages/` convention.
 */
export function findWorkspacePackagesDir(
  startDir: string,
  env: Record<string, string | undefined> = process.env,
): WorkspacePackagesDirResult {
  const override = env['LMAO_PACKAGES_DIR'];
  if (override !== undefined && override !== '') {
    const packagesDir = isAbsolute(override) ? override : resolve(startDir, override);
    if (existsSync(packagesDir)) {
      return { ok: true, packagesDir, source: 'env' };
    }
    return { ok: false, searched: [packagesDir] };
  }

  const searched: string[] = [];
  let directory = resolve(startDir);
  while (true) {
    if (isWorkspaceRoot(directory)) {
      const packagesDir = join(directory, 'packages');
      searched.push(packagesDir);
      if (existsSync(packagesDir)) {
        return { ok: true, packagesDir, source: 'workspace-root' };
      }
    }
    const parent = dirname(directory);
    if (parent === directory) {
      return { ok: false, searched };
    }
    directory = parent;
  }
}
