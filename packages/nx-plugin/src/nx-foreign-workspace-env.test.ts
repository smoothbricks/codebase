import { expect, it } from 'bun:test';
import { spawn } from 'node:child_process';
import { mkdir, mkdtemp, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { guardEvent } from './__tests__/counted-cargo.js';
import { fixtureNxEnv, stopFixtureNxDaemon, withNxFixture } from './__tests__/fixture-nx-env.js';
import { nxDaemonProcesses } from './testing.js';

/**
 * Guards the foreign-environment hunk of `patches/nx@23.2.1.patch`. A shell set up for one workspace keeps its
 * `NX_WORKSPACE_ROOT_PATH`, `NX_WORKSPACE_DATA_DIRECTORY`, `NX_CACHE_DIRECTORY` and `NX_SOCKET_DIR` when it moves into
 * another checkout. Nx takes `NX_WORKSPACE_ROOT_PATH` verbatim as its root, so `nx daemon --stop` run there stopped the
 * first workspace's daemon and deleted its socket directory.
 *
 * The CLI now refuses before it reaches any daemon, socket or file, and names the variables and both workspaces. Drop
 * the hunk, and keep the first test, once the Nx version in use refuses here.
 *
 * A workspace is the same workspace through every path to its directory, not only through symlinks: CI binds the
 * checkout at `/work/...` for devenv's eval cache and runs its steps in the checkout itself, so the environment names
 * the bind and the current directory names the checkout. The alias tests below call the hunk's own
 * `foreignWorkspaceEnvironment` under a real second path to one directory (see `AliasedScratch`), never a symlink and
 * never a stand-in for the comparison, and go with the hunk.
 */

const repositoryRoot = join(import.meta.dir, '../../..');
const nxEntry = join(repositoryRoot, 'node_modules', '.bin', 'nx');

interface NxRun {
  readonly code: number | null;
  readonly stdout: string;
  readonly stderr: string;
}

async function runNx(cwd: string, env: Record<string, string>, args: readonly string[]): Promise<NxRun> {
  return run(cwd, env, ['bun', nxEntry, ...args], `nx ${args.join(' ')}`);
}

async function run(
  cwd: string,
  env: Record<string, string>,
  [program, ...argv]: readonly string[],
  what: string,
): Promise<NxRun> {
  if (program === undefined) throw new Error(`no command to run for ${what}`);
  const child = spawn(program, argv, {
    cwd,
    env,
    detached: process.platform !== 'win32',
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '';
  let stderr = '';
  child.stdout.setEncoding('utf8').on('data', (text: string) => {
    stdout += text;
  });
  child.stderr.setEncoding('utf8').on('data', (text: string) => {
    stderr += text;
  });
  const { promise: ended, resolve, reject } = Promise.withResolvers<number | null>();
  child.once('error', reject);
  child.once('close', resolve);
  try {
    const code = await guardEvent(
      ended,
      `${what} in ${cwd} to close`,
      () => `pid ${child.pid}, exit ${child.exitCode}, signal ${child.signalCode}\n${stdout}${stderr}`,
    );
    return { code, stdout, stderr };
  } catch (error) {
    if (child.pid !== undefined && child.exitCode === null && child.signalCode === null) {
      try {
        process.kill(process.platform === 'win32' ? child.pid : -child.pid, 'SIGKILL');
      } catch (killError) {
        if (!(killError instanceof Error && 'code' in killError && killError.code === 'ESRCH')) {
          throw new AggregateError([error, killError], `could not retire the failed ${what}`);
        }
      }
    }
    await ended.catch(() => undefined);
    throw error;
  }
}

async function writeWorkspace(root: string, name: string): Promise<void> {
  await mkdir(join(root, 'p'), { recursive: true });
  await symlink(join(repositoryRoot, 'node_modules'), join(root, 'node_modules'), 'dir');
  await writeFile(join(root, 'nx.json'), '{}\n');
  await writeFile(join(root, 'package.json'), JSON.stringify({ name, private: true }));
  await writeFile(join(root, 'p', 'project.json'), JSON.stringify({ name: `${name}-p` }));
}

it("refuses nx run in one workspace under another workspace's environment, and leaves that daemon running", async () => {
  // A socket path has a ~104 byte budget and the fixture root spends most of it, so A's socket directory, which
  // lies in A, is reached through a short link under /tmp, as a managed shell's runtime link reaches its checkout's.
  const link = await mkdtemp(join('/tmp', 'nx-fe-'));
  try {
    await withNxFixture(
      'nx-foreign-env-',
      async ({ root, workspace: a }) => {
        const b = join(root, 'b');
        await writeWorkspace(a, 'a');
        await writeWorkspace(b, 'b');
        await mkdir(join(a, '.nx', 'run'), { recursive: true });
        await symlink(join(a, '.nx', 'run'), join(link, 'a'), 'dir');
        const state = {
          NX_WORKSPACE_ROOT_PATH: a,
          NX_WORKSPACE_DATA_DIRECTORY: join(a, '.nx', 'workspace-data'),
          NX_CACHE_DIRECTORY: join(a, '.nx', 'cache'),
          NX_SOCKET_DIR: join(link, 'a', 'nx'),
        };
        const aEnv = { ...fixtureNxEnv(a), ...state, NX_DAEMON: 'true' };

        const started = await runNx(a, aEnv, ['daemon', '--start']);
        expect(started.code, started.stdout + started.stderr).toBe(0);
        const listed = await runNx(a, aEnv, ['show', 'projects']);
        expect(listed.code, listed.stdout + listed.stderr).toBe(0);
        expect(listed.stdout.split('\n')).toContain('a-p');
        const [daemon] = await nxDaemonProcesses(a);
        if (daemon === undefined)
          throw new Error(`A's daemon left no running record\n${listed.stdout}${listed.stderr}`);

        const stop = await runNx(b, aEnv, ['daemon', '--stop']);
        expect(stop.code, stop.stdout + stop.stderr).not.toBe(0);
        for (const [name, value] of Object.entries(state)) {
          expect(stop.stderr).toContain(`${name}=${value}`);
        }
        expect(stop.stderr).toContain(`direnv exec ${b} nx`);

        expect((await nxDaemonProcesses(a))[0]?.pid).toBe(daemon.pid);
        const answered = await runNx(a, aEnv, ['show', 'projects']);
        expect(answered.code, answered.stdout + answered.stderr).toBe(0);
        expect(answered.stdout.split('\n')).toContain('a-p');
        expect((await nxDaemonProcesses(a))[0]?.pid).toBe(daemon.pid);

        await stopFixtureNxDaemon(a, aEnv);
        expect(await nxDaemonProcesses(a)).toEqual([]);
      },
      'a',
    );
  } finally {
    await rm(link, { recursive: true, force: true });
  }
});

/** This process's environment without Nx's variables, which a module of Nx would read when it loads. */
const hostEnv = Object.fromEntries(
  Object.entries(process.env).filter(
    (entry): entry is [string, string] => entry[1] !== undefined && !entry[0].startsWith('NX_'),
  ),
);

/**
 * The Nx module that decides, and the node that runs it. The alias tests call the decision directly under `node`, as
 * `nx` itself runs by its shebang: a bind namespace's sudo variant runs as root, which must not leave a root-owned
 * `.nx` in the scratch workspace, and Bun's `realpath` folds macOS's second path into the first where node's keeps it.
 */
const workspaceRootModule = join(repositoryRoot, 'node_modules', 'nx', 'dist', 'src', 'utils', 'workspace-root.js');

function nodeProgram(): string {
  const node = Bun.which('node');
  if (node === null) throw new Error('the alias tests run the Nx check under node, which is not on PATH');
  return node;
}

/**
 * A scratch directory and a second path to it: the same directory by device and inode, whose canonical path is its own
 * and that no symlink resolution joins to `directory`.
 *
 * - macOS names every directory of its data volume a second time under `/System/Volumes/Data` (a firmlink), with no
 *   privilege. `/tmp` resolves to `/private/tmp`, which is on the data volume; the temp directory may be another
 *   volume's mount, where the second name resolves back to the first, so the scratch lies under `/tmp`.
 * - Linux has none until something binds one. The program then runs under `launch`, in a mount namespace of its own
 *   where `mount --bind` puts `alias` beside `directory`: nothing outlives it and no mount of the host changes (see
 *   `bindingLaunch`).
 * Another platform has no second path to make, and fails.
 */
interface AliasedScratch {
  readonly directory: string;
  readonly alias: string;
  /** The command words that give the program run after them `alias` beside `directory`. */
  readonly launch: readonly string[];
}

async function withAliasedScratch<T>(body: (scratch: AliasedScratch) => Promise<T>): Promise<T> {
  const directory = await realpath(await mkdtemp(join('/tmp', 'nx-fe-alias-')));
  try {
    switch (process.platform) {
      case 'darwin':
        return await body({ directory, alias: join('/System/Volumes/Data', directory), launch: [] });
      case 'linux': {
        const alias = `${directory}-alias`;
        await mkdir(alias);
        try {
          return await body({ directory, alias, launch: await bindingLaunch(directory, alias) });
        } finally {
          await rm(alias, { recursive: true, force: true });
        }
      }
      default:
        throw new Error(`${process.platform} has no second path to a directory to make`);
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

/**
 * The words that bind `alias` to `directory` for the program after them, on this Linux host. The bind lives in a
 * mount namespace made for that program (`unshare --mount` makes every mount private to it), so it ends with the
 * program and the test never removes anything through a live bind. Unprivileged user namespaces need no sudo, and a
 * developer's host has them; Ubuntu 24.04 refuses them by default
 * (`kernel.apparmor_restrict_unprivileged_userns`) and its hosted runners have passwordless sudo instead. The first way
 * that runs a program is used; a host that offers neither fails with both refusals.
 */
async function bindingLaunch(directory: string, alias: string): Promise<readonly string[]> {
  const bind = ['--', 'sh', '-c', 'mount --bind "$1" "$2" && shift 2 && exec "$@"', 'sh', directory, alias];
  const refusals: string[] = [];
  for (const launch of [
    ['unshare', '--user', '--map-root-user', '--mount', ...bind],
    ['sudo', '-n', 'unshare', '--mount', ...bind],
  ]) {
    const way = launch.slice(0, launch.indexOf('--')).join(' ');
    try {
      const tried = await run(directory, hostEnv, [...launch, nodeProgram(), '-e', '0'], way);
      if (tried.code === 0) return launch;
      refusals.push(`${way}: exit ${tried.code}\n${tried.stdout}${tried.stderr}`);
    } catch (error) {
      refusals.push(`${way}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
  throw new Error(`no way to bind a second path to ${directory} on this host:\n${refusals.join('\n')}`);
}

/** The JSON `script` prints under `scratch`'s launch, in `scratch.directory`. */
async function evaluate(scratch: AliasedScratch, script: string, what: string): Promise<unknown> {
  const ran = await run(scratch.directory, hostEnv, [...scratch.launch, nodeProgram(), '-e', script], what);
  expect(ran.code, `${what}\n${ran.stdout}${ran.stderr}`).toBe(0);
  return JSON.parse(ran.stdout);
}

/**
 * Fail unless `alias` is, under the scratch's launch, a second canonical path to `directory`: each resolves to itself
 * and both have one device and inode. Without it a passing alias test would prove nothing about aliases.
 */
async function expectSecondPath(scratch: AliasedScratch): Promise<void> {
  const { directory, alias } = scratch;
  const script = `
    const { realpathSync, statSync } = require('node:fs');
    const identity = (path) => { const { dev, ino } = statSync(path, { bigint: true }); return dev + ':' + ino; };
    console.log(JSON.stringify({
      directory: realpathSync.native(${JSON.stringify(directory)}),
      alias: realpathSync.native(${JSON.stringify(alias)}),
      same: identity(${JSON.stringify(directory)}) === identity(${JSON.stringify(alias)}),
    }));
  `;
  expect(await evaluate(scratch, script, 'the second-path probe')).toEqual({ directory, alias, same: true });
}

/** What the patched Nx check reports for `env` in `cwd`, under the scratch's launch: `null`, or the workspaces named. */
function foreignWorkspaceEnvironment(
  scratch: AliasedScratch,
  cwd: string,
  env: Record<string, string>,
): Promise<unknown> {
  const script = `
    const { foreignWorkspaceEnvironment } = require(${JSON.stringify(workspaceRootModule)});
    console.log(JSON.stringify(foreignWorkspaceEnvironment(${JSON.stringify(cwd)}, ${JSON.stringify(env)})));
  `;
  return evaluate(scratch, script, 'the foreign-environment check');
}

it('accepts an environment that names the current workspace by a second path to its directory', async () => {
  await withAliasedScratch(async (scratch) => {
    const a = join(scratch.directory, 'a');
    await writeWorkspace(a, 'a');
    const aliasOfA = join(scratch.alias, 'a');
    const named = {
      NX_WORKSPACE_ROOT_PATH: aliasOfA,
      NX_WORKSPACE_DATA_DIRECTORY: join(aliasOfA, '.nx', 'workspace-data'),
      NX_CACHE_DIRECTORY: join(aliasOfA, '.nx', 'cache'),
      NX_SOCKET_DIR: join(aliasOfA, '.nx', 'run'),
    };
    await expectSecondPath(scratch);
    // One variable at a time, so a failure names the attribution that does not follow the alias: the root itself, and
    // the data, cache and socket directories, which belong to the workspace whose root contains them.
    for (const [name, value] of Object.entries(named)) {
      expect(await foreignWorkspaceEnvironment(scratch, a, { [name]: value }), `${name}=${value}`).toBeNull();
    }
    expect(await foreignWorkspaceEnvironment(scratch, a, named)).toBeNull();
  });
});

it("refuses an environment that names another workspace through a second path, and not the current one's", async () => {
  await withAliasedScratch(async (scratch) => {
    const a = join(scratch.directory, 'a');
    const aliasOfA = join(scratch.alias, 'a');
    const aliasOfB = join(scratch.alias, 'b');
    await writeWorkspace(a, 'a');
    await writeWorkspace(join(scratch.directory, 'b'), 'b');
    const foreignData = join(aliasOfB, '.nx', 'workspace-data');
    await expectSecondPath(scratch);
    expect(
      await foreignWorkspaceEnvironment(scratch, a, {
        NX_WORKSPACE_ROOT_PATH: aliasOfB,
        NX_WORKSPACE_DATA_DIRECTORY: foreignData,
        // The current workspace's own, through its second path, in the same environment: not reported.
        NX_CACHE_DIRECTORY: join(aliasOfA, '.nx', 'cache'),
      }),
    ).toEqual({
      workspace: a,
      variables: [
        { name: 'NX_WORKSPACE_ROOT_PATH', value: aliasOfB, workspace: aliasOfB },
        { name: 'NX_WORKSPACE_DATA_DIRECTORY', value: foreignData, workspace: aliasOfB },
      ],
    });
  });
});
