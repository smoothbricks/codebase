import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { realpath, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { nxFixtureRoot } from './__tests__/fixture-nx-env.js';

/**
 * Guards cowshed's rule "Every checkout has exactly one Nx state" (specs/cowshed/16_build_volumes.md) for host shells:
 * a host shell's Nx keeps its cache and task database in the checkout's own `.nx`, which a cowshed checkout links into
 * its build volume. Cowshed's jobs name those directories through `NX_CACHE_DIRECTORY` and
 * `NX_WORKSPACE_DATA_DIRECTORY`; a host shell names neither. Without a configured location the pinned Nx 23.2.1
 * (`utils/cache-directory.js`, `computeSharedDataLocation`) moves both to `~/.nx/<hash of the repository key>`, shared
 * by every checkout whose git remote keys alike and read by no build volume: land gates then ran cold, and a rebase
 * carried nothing because main's volume indexed nothing. The repository's `nx.json` `cacheDirectory`, which smoo's
 * workspace policy writes (`CHECKOUT_NX_CACHE_DIRECTORY`, `workspace-config-policy.ts`), keeps both local.
 *
 * Resolved by the installed Nx itself, in a process with no `NX_*` variable, as a host shell runs it. HOME is a scratch
 * directory, so a regression resolves under it rather than writing into the user's `~/.nx`.
 */

const repositoryRoot = join(import.meta.dir, '../../..');

/** What Nx resolves at import: its workspace root, `cacheDir`, and the directory `getDbConnection` opens. */
const PROBE = `
const { createRequire } = require('node:module');
const nx = createRequire(require('node:path').join(process.cwd(), 'package.json'));
const { workspaceRoot } = nx('nx/src/utils/workspace-root');
const { cacheDir, sharedDataDirectory } = nx('nx/src/utils/cache-directory');
process.stdout.write(JSON.stringify({
  workspaceRoot,
  cache: cacheDir,
  taskDatabase: sharedDataDirectory(workspaceRoot, 'workspace-data'),
}));
`;

interface NxStateLocation {
  readonly workspaceRoot: string;
  readonly cache: string;
  readonly taskDatabase: string;
}

it("keeps a host shell's Nx cache and task database in the checkout's own .nx", async () => {
  const home = await nxFixtureRoot('nx-state-location-');
  try {
    const env: Record<string, string> = {};
    for (const [key, value] of Object.entries(process.env)) {
      if (value !== undefined && !key.startsWith('NX_') && key !== 'CI') env[key] = value;
    }
    env.HOME = home;
    const probe = spawnSync(process.execPath, ['-e', PROBE], { cwd: repositoryRoot, env, encoding: 'utf8' });
    expect(probe.error).toBeUndefined();
    expect({ status: probe.status, stderr: probe.stderr }).toEqual({ status: 0, stderr: '' });
    const location: NxStateLocation = JSON.parse(probe.stdout);
    expect(await realpath(location.workspaceRoot)).toBe(await realpath(repositoryRoot));
    expect(location).toEqual({
      workspaceRoot: location.workspaceRoot,
      cache: join(location.workspaceRoot, '.nx', 'cache'),
      taskDatabase: join(location.workspaceRoot, '.nx', 'workspace-data'),
    });
  } finally {
    await rm(home, { recursive: true, force: true });
  }
});
