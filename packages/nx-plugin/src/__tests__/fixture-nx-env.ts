import { existsSync } from 'node:fs';
import { join } from 'node:path';

const repositoryRoot = join(import.meta.dir, '../../../..');
const DAEMON_STOP_TIMEOUT_MS = 10_000;

/**
 * The environment of an `nx` a test runs against a fixture workspace.
 *
 * Every inherited `NX_*` variable is dropped first. Run as
 * `nx run nx-plugin:test`, the test process is itself an Nx task, and what the
 * outer Nx exported to it (NX_SKIP_NX_CACHE under `--skip-nx-cache`, the
 * NX_TASK_* identity, output capture) would reconfigure the fixture's Nx: a
 * skipped cache turns every second run into a miss. NX_DAEMON goes with them,
 * so an inherited `false` never reaches the fixture: its Nx runs under the
 * daemon Nx would pick on its own, owned by the fixture root and released by
 * `stopFixtureNxDaemon` before the root is deleted.
 *
 * FORCE_COLOR, which an outer Nx also exports, is kept set, next to the
 * NO_COLOR a shell may export. The fixture's runtime inputs inherit both, and
 * must hash the same under them.
 */
export function fixtureNxEnv(workspace: string): Record<string, string> {
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (value !== undefined && !key.startsWith('NX_')) env[key] = value;
  }
  return {
    ...env,
    FORCE_COLOR: 'true',
    NO_COLOR: '1',
    NX_WORKSPACE_ROOT_PATH: workspace,
    NX_ISOLATE_PLUGINS: 'false',
    NX_WORKSPACE_DATA_DIRECTORY: join(workspace, '.nx/workspace-data'),
    NX_CACHE_DIRECTORY: join(workspace, '.nx/cache'),
  };
}

/**
 * Stop the Nx daemon a fixture's own `nx` runs started, through Nx's supported
 * `nx daemon --stop`, before the fixture root is deleted. The daemon is told
 * the same root and data directory that started it (`fixtureNxEnv`), so it
 * can only be this fixture's: no other workspace's daemon is reached, and
 * nothing is reset. Nx records a started daemon in `d/server-process.json`
 * under the workspace-data directory, so a root without that record never had
 * one; the record goes when the daemon has shut down, and one that outlives
 * the stop is reported, not buried by the delete that follows.
 */
export async function stopFixtureNxDaemon(workspace: string): Promise<void> {
  const record = join(workspace, '.nx/workspace-data/d/server-process.json');
  if (!existsSync(record)) return;
  const child = Bun.spawn(['bun', join(repositoryRoot, 'node_modules/.bin/nx'), 'daemon', '--stop'], {
    cwd: workspace,
    env: fixtureNxEnv(workspace),
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const [exitCode, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  if (exitCode !== 0) {
    throw new Error(`nx daemon --stop failed for fixture ${workspace} with exit code ${exitCode}\n${stdout}${stderr}`);
  }
  const deadline = Date.now() + DAEMON_STOP_TIMEOUT_MS;
  while (existsSync(record)) {
    if (Date.now() > deadline) {
      throw new Error(`Nx daemon of ${workspace} still recorded ${DAEMON_STOP_TIMEOUT_MS}ms after nx daemon --stop`);
    }
    await Bun.sleep(50);
  }
}
