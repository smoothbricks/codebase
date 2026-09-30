import { join } from 'node:path';

/**
 * The environment of an `nx` a test runs against a fixture workspace.
 *
 * Every inherited `NX_*` variable is dropped first. Run as
 * `nx run nx-plugin:test`, the test process is itself an Nx task, and what the
 * outer Nx exported to it (NX_SKIP_NX_CACHE under `--skip-nx-cache`, the
 * NX_TASK_* identity, output capture) would reconfigure the fixture's Nx: a
 * skipped cache turns every second run into a miss.
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
    NX_DAEMON: 'false',
    NX_ISOLATE_PLUGINS: 'false',
    NX_WORKSPACE_DATA_DIRECTORY: join(workspace, '.nx/workspace-data'),
    NX_CACHE_DIRECTORY: join(workspace, '.nx/cache'),
  };
}
