import { onTestFinished } from 'bun:test';
import { spawn } from 'node:child_process';
import { rm } from 'node:fs/promises';
import { join } from 'node:path';
import { ownedFixtureRoot, stopNxDaemon } from '../testing.js';
import { guardEvent } from './counted-cargo.js';

const repositoryRoot = join(import.meta.dir, '../../../..');

/**
 * The environment of an `nx` a test runs against a fixture workspace.
 *
 * Every inherited `NX_*` variable is dropped first. Run as
 * `nx run nx-plugin:test`, the test process is itself an Nx task, and what the
 * outer Nx exported to it (NX_SKIP_NX_CACHE under `--skip-nx-cache`, the
 * NX_TASK_* identity, output capture) would reconfigure the fixture's Nx: a
 * skipped cache turns every second run into a miss.
 *
 * NX_DAEMON is then `false`: a daemonless Nx leaves nothing running once it
 * exits, and only a test whose subject is the daemon overrides it with
 * `true`, inside {@link withNxFixture}, which stops that daemon. NX_USE_LOCAL
 * is `true`, as in this repository's shell: otherwise a fixture daemon asked
 * for Nx Console's status pulls `nx@latest` from the registry in an `npm`
 * child working in the fixture root.
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
    NX_DAEMON: 'false',
    NX_USE_LOCAL: 'true',
    NX_WORKSPACE_ROOT_PATH: workspace,
    NX_ISOLATE_PLUGINS: 'false',
    NX_WORKSPACE_DATA_DIRECTORY: join(workspace, '.nx/workspace-data'),
    NX_CACHE_DIRECTORY: join(workspace, '.nx/cache'),
  };
}

/**
 * A fixture root of this package's suite, owned by this test process: the
 * next run's first fixture reclaims it, and whatever still works in it, if
 * this process is killed before the root is retired.
 */
export function nxFixtureRoot(prefix: string): Promise<string> {
  return ownedFixtureRoot('nx-plugin', prefix);
}

/** A fixture root and the Nx workspace in it whose daemon the fixture owns. */
export interface NxFixture {
  readonly root: string;
  readonly workspace: string;
}

/**
 * Run `body` in a fresh fixture root whose Nx workspace is `workspace`
 * (relative to the root), then retire it: stop the workspace's daemon and
 * delete the root, however `body` ends. Bun ends a timed-out test without
 * cancelling its body, so the fixture also retires when the test finishes.
 */
export async function withNxFixture<T>(
  prefix: string,
  body: (fixture: NxFixture) => Promise<T>,
  workspace = '.',
): Promise<T> {
  const root = await nxFixtureRoot(prefix);
  const fixture = { root, workspace: join(root, workspace) };
  let retirement: Promise<void> | undefined;
  const retire = () => {
    retirement ??= retireNxFixture(fixture);
    return retirement;
  };
  onTestFinished(retire);
  let result: T;
  try {
    result = await body(fixture);
  } catch (error) {
    try {
      await retire();
    } catch (retireError) {
      throw new AggregateError([error, retireError], `fixture ${root} failed, and so did its retirement`);
    }
    throw error;
  }
  await retire();
  return result;
}

/**
 * Stop the fixture workspace's daemon, then delete the root. A daemon that
 * outlives its stop fails the retirement and keeps the root, so its working
 * directory still names a fixture the next run reclaims.
 */
async function retireNxFixture({ root, workspace }: NxFixture): Promise<void> {
  await stopFixtureNxDaemon(workspace);
  await rm(root, { recursive: true, force: true });
}

/**
 * Stop the Nx daemon a fixture's own `nx` runs started, through Nx's supported
 * `nx daemon --stop`, and wait until it and every process it ran have exited.
 * The daemon is told the same root and data directory that started it
 * (`fixtureNxEnv`), so it can only be this fixture's: no other workspace's
 * daemon is reached, and nothing is reset.
 */
export function stopFixtureNxDaemon(workspace: string): Promise<void> {
  return stopNxDaemon(workspace, async () => {
    let stdout = '';
    let stderr = '';
    const child = spawn('bun', [join(repositoryRoot, 'node_modules/.bin/nx'), 'daemon', '--stop'], {
      cwd: workspace,
      env: { ...fixtureNxEnv(workspace), NX_DAEMON: 'true' },
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
    const { promise: ended, resolve } = Promise.withResolvers<number | null>();
    child.once('close', resolve);
    const describe = () =>
      `fixture ${workspace}; stop pid ${child.pid ?? 'not started'}, exit ${child.exitCode ?? 'pending'}, ` +
      `signal ${child.signalCode ?? 'none'}\n${stdout}${stderr}`;
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
  });
}
