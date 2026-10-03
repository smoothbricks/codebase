import { spawn } from 'node:child_process';
import { existsSync, watch } from 'node:fs';
import { dirname, join } from 'node:path';
import { guardEvent } from './counted-cargo.js';

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
  // Subscribe before sending the stop request: an unlink is an event, not a delay to guess.
  const subscription = watch(dirname(record));
  const retirement = new Promise<Error | null>((resolve) => {
    subscription.on('change', () => {
      if (!existsSync(record)) resolve(null);
    });
    subscription.once('error', resolve);
    if (!existsSync(record)) resolve(null);
  });
  let stdout = '';
  let stderr = '';
  try {
    const child = spawn('bun', [join(repositoryRoot, 'node_modules/.bin/nx'), 'daemon', '--stop'], {
      cwd: workspace,
      env: fixtureNxEnv(workspace),
      detached: process.platform !== 'win32',
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    child.stdout.setEncoding('utf8').on('data', (text: string) => {
      stdout += text;
    });
    child.stderr.setEncoding('utf8').on('data', (text: string) => {
      stderr += text;
    });
    let spawnFailure: Error | null = null;
    child.once('error', (error) => {
      spawnFailure = error;
    });
    const ended = new Promise<number | null>((resolve) => {
      child.once('close', (code) => resolve(code));
    });
    const describe = () =>
      `fixture ${workspace}; stop pid ${child.pid ?? 'not started'}, exit ${child.exitCode ?? 'pending'}, ` +
      `signal ${child.signalCode ?? 'none'}; daemon record ${existsSync(record) ? 'present' : 'removed'}\n${stdout}${stderr}`;
    let exitCode: number | null;
    try {
      exitCode = await guardEvent(ended, 'nx daemon --stop to exit', describe);
    } catch (error) {
      if (child.pid !== undefined) {
        try {
          process.kill(process.platform === 'win32' ? child.pid : -child.pid, 'SIGKILL');
        } catch (killError) {
          if (!(killError instanceof Error && 'code' in killError && killError.code === 'ESRCH')) {
            throw new AggregateError(
              [error, killError],
              `could not stop the fixture's failed Nx command\n${describe()}`,
            );
          }
        }
      }
      await ended;
      throw error;
    }
    if (spawnFailure !== null || exitCode !== 0) {
      throw new Error(`nx daemon --stop failed with exit code ${exitCode}\n${describe()}`, { cause: spawnFailure });
    }
    const error = await guardEvent(
      retirement,
      'Nx daemon record to retire',
      describe,
      undefined,
      DAEMON_STOP_TIMEOUT_MS,
    );
    if (error !== null) throw error;
  } finally {
    subscription.close();
  }
}
