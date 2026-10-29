import { onTestFinished } from 'bun:test';
import { spawn } from 'node:child_process';
import { mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  fixtureSocketDirectory,
  ownedFixtureRoot,
  publishFixtureSocketDirectory,
  retireFixtureSocketDirectory,
  stopNxDaemon,
} from '../testing.js';
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
 * Every inherited `MBX_*` variable is dropped too. A gate job of an mbx project
 * carries cowshed's canonical mbx environment (specs/cowshed/11_shell.md,
 * "Canonical mbx environment"), and a fixture is no such project: its Nx would
 * join the outer job's pool (each bounded-exec leaf inside `mbx reserve`), and
 * its Cargo retention hook would leave the fixture's targets to mbx.
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
 *
 * NX_SOCKET_DIR is `socketDirectory`, the fixture's own published
 * {@link fixtureSocketDirectory} ({@link withNxFixture} passes it), never the
 * inherited one, which a Cowshed job shares with every other Nx of its
 * checkout. Nx's default socket root is the literal `/tmp/.nx`, which a
 * sandbox does not admit, and it hashes the daemon's pid into the name, so two
 * daemons of one workspace bind two paths. With the directory named, they bind
 * one: a test whose subject is two daemons on two socket paths names a second
 * directory for the second. Building the environment publishes nothing.
 *
 * NX_NATIVE_FILE_CACHE_DIRECTORY is the fixture's own too. Nx loads its native
 * binding from a copy in that cache, by default `/tmp/.nx/<uid>/native-cache`,
 * which every Nx on the host shares and any `nx reset` deletes. A load that loses
 * that race falls back to Nx's WASI binding without a word, and fails later on
 * what WASI lacks (`FileLock is not a constructor`, from a task output lease).
 */
export function fixtureNxEnv(workspace: string, socketDirectory: string): Record<string, string> {
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (value !== undefined && !key.startsWith('NX_') && !key.startsWith('MBX_')) env[key] = value;
  }
  return {
    ...env,
    FORCE_COLOR: 'true',
    NO_COLOR: '1',
    NX_DAEMON: 'false',
    NX_USE_LOCAL: 'true',
    NX_WORKSPACE_ROOT_PATH: workspace,
    NX_ISOLATE_PLUGINS: 'false',
    NX_SOCKET_DIR: socketDirectory,
    NX_WORKSPACE_DATA_DIRECTORY: join(workspace, '.nx/workspace-data'),
    NX_CACHE_DIRECTORY: join(workspace, '.nx/cache'),
    NX_NATIVE_FILE_CACHE_DIRECTORY: join(workspace, '.nx/native-file-cache'),
  };
}

/**
 * A fixture root of this package's suite in the temp directory, owned by this
 * test process: the next run's first fixture reclaims it, and whatever still
 * works in it, if this process is killed before the root is retired.
 */
export function nxFixtureRoot(prefix: string): Promise<string> {
  return ownedFixtureRoot(tmpdir(), 'nx-plugin', prefix);
}

/** A fixture root, the Nx workspace in it whose daemon the fixture owns, and that workspace's published socket directory. */
export interface NxFixture {
  readonly root: string;
  readonly workspace: string;
  readonly socketDirectory: string;
}

/**
 * Run `body` in a fresh fixture root whose Nx workspace is `workspace`
 * (relative to the root, made if it is not the root), with the workspace's
 * socket alias published, then retire it: stop the workspace's daemon, unlink
 * the alias and delete the root, however `body` ends. Bun ends a timed-out test
 * without cancelling its body, so the fixture also retires when the test
 * finishes.
 */
export async function withNxFixture<T>(
  prefix: string,
  body: (fixture: NxFixture) => Promise<T>,
  workspace = '.',
): Promise<T> {
  const root = await nxFixtureRoot(prefix);
  const owned = { root, workspace: join(root, workspace) };
  let retirement: Promise<void> | undefined;
  const retire = () => {
    retirement ??= retireNxFixture(owned);
    return retirement;
  };
  onTestFinished(retire);
  let result: T;
  try {
    await mkdir(owned.workspace, { recursive: true });
    result = await body({ ...owned, socketDirectory: await publishFixtureSocketDirectory(owned.workspace) });
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
 * Stop the fixture workspace's daemon, unlink its owned short socket alias,
 * then delete the root. A daemon that outlives its stop fails the retirement
 * and keeps all three, so its working directory still names a fixture the next
 * run reclaims, alias included. Nothing is published here: the stop names the
 * directory publication returned, by the same pure naming.
 */
async function retireNxFixture({ root, workspace }: Omit<NxFixture, 'socketDirectory'>): Promise<void> {
  await stopFixtureNxDaemon(workspace, fixtureNxEnv(workspace, fixtureSocketDirectory(workspace)));
  await retireFixtureSocketDirectory(workspace);
  await rm(root, { recursive: true, force: true });
}

/**
 * Stop the Nx daemon a fixture's own `nx` runs started, through Nx's supported
 * `nx daemon --stop`, and wait until it and every process it ran have exited.
 * The daemon is told the same root, data and socket directory that started it
 * (`env`, the one a test started it under), so it can only be this fixture's:
 * no other workspace's daemon is reached, and nothing is reset.
 */
export function stopFixtureNxDaemon(workspace: string, env: Record<string, string>): Promise<void> {
  return stopNxDaemon(workspace, async () => {
    let stdout = '';
    let stderr = '';
    const child = spawn('bun', [join(repositoryRoot, 'node_modules/.bin/nx'), 'daemon', '--stop'], {
      cwd: workspace,
      env: { ...env, NX_DAEMON: 'true' },
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
