import { spawnSync } from 'node:child_process';
import { rm } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { publishFixtureSocketDirectory, retireFixtureSocketDirectory } from '../testing.js';
import { nxFixtureRoot } from './fixture-nx-env.js';

/** The repository this package is built in: its own nextest config, nx.json and manifests are what these tests judge. */
export const REPOSITORY = fileURLToPath(new URL('../../../..', import.meta.url));

// The child computes outside this package's bunfig; it takes the same transform preload as the test runner.
const VALIDATION_PRELOAD = fileURLToPath(import.meta.resolve('@smoothbricks/validation/bun/preload'));

/**
 * What `script` (an ES module body, run by `bun -e` at the repository root) writes to stdout, run in this repository's own
 * Nx workspace as Nx builds it with this plugin: daemonless, in a child that exits when the script ends. Installed
 * plugins run in-process; source plugins use owned restartable workers. Workspace data and sockets belong to the fixture,
 * so it neither starts a daemon nor writes the checkout's graph cache.
 * Each call costs a child process.
 */
export async function runInRepositoryNx(script: string): Promise<string> {
  const data = await nxFixtureRoot('repository-nx-');
  try {
    const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith('NX_')));
    const run = spawnSync(process.execPath, ['--preload', VALIDATION_PRELOAD, '-e', script], {
      cwd: REPOSITORY,
      encoding: 'utf8',
      maxBuffer: 256 * 1024 * 1024,
      env: {
        ...env,
        NX_DAEMON: 'false',
        NX_ISOLATE_PLUGINS: 'false',
        NX_CACHE_PROJECT_GRAPH: 'false',
        NX_WORKSPACE_DATA_DIRECTORY: data,
        NX_SOCKET_DIR: await publishFixtureSocketDirectory(data),
      },
    });
    if (run.status !== 0) throw new Error(`the repository Nx child exited ${run.status}: ${run.stderr}`);
    return run.stdout;
  } finally {
    await retireFixtureSocketDirectory(data);
    await rm(data, { recursive: true, force: true });
  }
}
