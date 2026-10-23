import { onTestFinished } from 'bun:test';
import { existsSync } from 'node:fs';
import { mkdir, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  awaitExit,
  fixtureSocketDirectory,
  ownedFixtureRoot,
  processTable,
  publishFixtureSocketDirectory,
  retireFixtureSocketDirectory,
  stopNxDaemon,
  terminate,
  withDescendants,
} from '@smoothbricks/nx-plugin/testing';
import { decode, printCommandOutput } from '../../../lib/run.js';
import type { GitReleaseTagInfo } from '../../core.js';

const GIT_TIMEOUT_MS = 10_000;

/**
 * Every fixture Nx process reports its own phases. A fixture Nx run has stalled
 * for the whole 30s test deadline while its measured tasks took 294ms, and no
 * evidence of the stall survived. With these set, the client writes its daemon
 * wait and request lines, with timestamps, into the fixture daemon log. The
 * daemon logs the time each graph, plugin and hashing phase took, and the
 * client prints its own phase durations to stderr. Both are logging only and
 * change no Nx behaviour.
 */
export const FIXTURE_NX_DIAGNOSTICS: Readonly<Record<string, string>> = {
  NX_PERF_LOGGING: 'true',
  NX_DAEMON_VERBOSE_LOGGING: 'true',
};

// Isolate fixture git from the runner environment and skip fsync. These are
// throwaway temp repos (deleted when the fixture retires) so durability is
// irrelevant — fsync is pure cost. On GitHub Actions the ~20 sequential git
// spawns in a push test otherwise sum past the 30s cap: fsync on the runner's
// disk is the dominant cost, and inheriting the runner's global/system config
// (credential helpers, url.*.insteadOf rewrites, fsmonitor) slows every spawn
// and risks a credential-prompt hang. Unknown core.* keys are ignored by older
// git, so this is a harmless speedup locally and the real fix on CI.
const GIT_FIXTURE_ENV: Record<string, string> = {
  GIT_CONFIG_GLOBAL: '/dev/null',
  GIT_CONFIG_SYSTEM: '/dev/null',
  GIT_TERMINAL_PROMPT: '0',
  GIT_OPTIONAL_LOCKS: '0',
  GIT_CONFIG_COUNT: '1',
  GIT_CONFIG_KEY_0: 'core.fsync',
  GIT_CONFIG_VALUE_0: 'none',
};

/**
 * A fixture root while its test owns it. `nxRoots` names every Nx workspace
 * under the root that an `nx` client ran in, the root itself included, and
 * `clients` the pid of every `nx` client `runFixtureNx` still has running.
 * Retirement reads both, so it needs nothing from a body that never returns.
 */
interface Fixture {
  readonly root: string;
  readonly nxRoots: Set<string>;
  readonly clients: Set<number>;
  retirement: Promise<void> | null;
}

/** Fixtures whose retirement has not begun. */
const liveFixtures = new Set<Fixture>();

/**
 * A temp git repository for one test, owned by this test process (a killed
 * run's roots, and any Nx daemon still working in one, are reclaimed by the
 * next run's first fixture). Bun moves on when a test times out but does not
 * cancel its body, and the run can exit before that body reaches its
 * `finally`. The Nx daemon such a fixture started then outlives the run, with
 * its working directory in a temp root nothing will clean up. So the fixture
 * retires when the test finishes, whichever of the body or the test ends
 * first: it snapshots the Nx daemon log, stops every Nx process it started and
 * deletes the root. A body failure reports the same log first.
 */
export async function withFixtureRepo(fn: (root: string) => Promise<void>): Promise<void> {
  let fixture: Fixture | null = null;
  onTestFinished(async () => {
    if (fixture !== null && fixture.retirement === null) {
      await reportFixtureNxDaemon(fixture.root, 'the test finished while the fixture still existed');
      await retireFixture(fixture);
    }
  });
  const root = await ownedFixtureRoot(tmpdir(), 'cli', 'smoo-release-test-');
  const owned: Fixture = { root, nxRoots: new Set([root]), clients: new Set(), retirement: null };
  fixture = owned;
  liveFixtures.add(owned);
  try {
    await git(root, ['init', '-b', 'main']);
    await git(root, ['config', 'user.name', 'Test User']);
    await git(root, ['config', 'user.email', 'test@example.com']);
    await fn(root);
  } catch (error) {
    await reportFixtureNxDaemon(root, 'the fixture body failed');
    throw error;
  } finally {
    await retireFixture(owned);
  }
}

/**
 * Stop every Nx process a fixture started, wait until each has exited, then
 * unlink the socket alias of each workspace it ran in, then delete its root. The
 * body's `finally` and the test's finish share one retirement. The `nx`
 * clients go first, with everything they spawned: a client killed while it
 * starts a daemon would otherwise leave that daemon running before it has
 * recorded itself. A process that outlives its stop fails the retirement and
 * keeps the root, so its working directory still names the fixture it came
 * from. Nx deletes its socket directory itself when the daemon stops. Retiring a
 * workspace unlinks only the alias that names that directory, never another alias.
 */
function retireFixture(fixture: Fixture): Promise<void> {
  fixture.retirement ??= (async () => {
    liveFixtures.delete(fixture);
    const clients = withDescendants(await processTable(), [...fixture.clients]);
    for (const client of clients) {
      terminate(client.pid);
    }
    await awaitExit(clients, `nx clients of fixture ${fixture.root}`);
    for (const nxRoot of fixture.nxRoots) {
      await stopFixtureNxDaemon(nxRoot);
      await retireFixtureSocketDirectory(nxRoot);
    }
    await rm(fixture.root, { recursive: true, force: true });
  })();
  return fixture.retirement;
}

/**
 * Write a fixture's Nx daemon log to stderr before the root that holds it is
 * deleted. Bun's test output is the artifact the gate keeps. A root without the
 * log never started a daemon, so it has nothing to report.
 */
async function reportFixtureNxDaemon(root: string, reason: string): Promise<void> {
  const log = join(root, '.nx', 'workspace-data', 'd', 'daemon.log');
  if (!existsSync(log)) {
    return;
  }
  const text = await readFile(log, 'utf8').catch((error: unknown) => `daemon log unreadable: ${String(error)}`);
  process.stderr.write(`Nx daemon log of fixture ${root} (${reason}):\n${text}\n`);
}

export async function writeWorkspace(root: string): Promise<void> {
  await symlink(join(import.meta.dir, '../../../../../../node_modules'), join(root, 'node_modules'), 'dir');
  await writeFile(
    join(root, 'package.json'),
    `${JSON.stringify({ name: 'fixture', private: true, workspaces: ['packages/*'] }, null, 2)}\n`,
  );
  await writeFile(
    join(root, 'nx.json'),
    `${JSON.stringify(
      { cacheDirectory: '.nx/cache', targetDefaults: { build: { cache: true, outputs: ['{projectRoot}/dist/'] } } },
      null,
      2,
    )}\n`,
  );
}

export async function writePackage(root: string, name: string, packagePath: string, version: string): Promise<void> {
  await mkdir(join(root, packagePath), { recursive: true });
  await writeFile(join(root, packagePath, 'package.json'), `${JSON.stringify({ name, version }, null, 2)}\n`);
}

export async function writeBuildablePackage(
  root: string,
  name: string,
  packagePath: string,
  version = '1.0.0',
): Promise<void> {
  await mkdir(join(root, packagePath), { recursive: true });
  await writeFile(
    join(root, packagePath, 'package.json'),
    `${JSON.stringify(
      {
        name,
        version,
        nx: {
          name: name.startsWith('@') ? name.slice(name.indexOf('/') + 1) : name,
          targets: {
            build: {
              executor: 'nx:run-commands',
              options: { command: "mkdir -p dist && echo '{}' > dist/index.js", cwd: packagePath },
            },
          },
        },
      },
      null,
      2,
    )}\n`,
  );
}

export async function git(
  root: string,
  args: string[],
  env?: Record<string, string>,
  timeoutMs = GIT_TIMEOUT_MS,
): Promise<void> {
  const result = await gitResult(root, args, env, timeoutMs);
  if (result.timedOut || result.exitCode !== 0) {
    throw new Error(gitErrorMessage(root, args, result));
  }
}

export async function gitOutput(root: string, args: string[]): Promise<string> {
  const result = await gitResult(root, args);
  if (result.exitCode !== 0) {
    throw new Error(gitErrorMessage(root, args, result));
  }
  return new TextDecoder().decode(result.stdout).trim();
}

export async function gitSucceeds(root: string, args: string[]): Promise<boolean> {
  return (await gitResult(root, args)).exitCode === 0;
}

async function gitResult(
  root: string,
  args: string[],
  env?: Record<string, string>,
  timeoutMs = GIT_TIMEOUT_MS,
): Promise<GitResult> {
  const proc = Bun.spawn(['git', ...args], {
    cwd: root,
    detached: true,
    env: { ...process.env, ...GIT_FIXTURE_ENV, ...env },
    stdout: 'pipe',
    stderr: 'pipe',
  });
  let timedOut = false;
  // Rejects only when the group could not be ended; its pipes would then never close.
  const unended = Promise.withResolvers<never>();
  const timeout = setTimeout(() => {
    timedOut = true;
    // `git` may exit after spawning a transport or hook which still owns its
    // pipes. Ending the detached process group closes every inherited pipe.
    endProcessGroup(proc.pid).catch(unended.reject);
  }, timeoutMs);
  try {
    const [exitCode, stdout, stderr] = await Promise.race([
      Promise.all([proc.exited, streamBytes(proc.stdout), streamBytes(proc.stderr)]),
      unended.promise,
    ]);
    return { exitCode, stdout, stderr, timedOut, timeoutMs };
  } finally {
    clearTimeout(timeout);
  }
}

async function streamBytes(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

/** A `ps` state in which a process starts nothing: stopped (`T`, `t` when traced) or exited (`Z`). */
const STARTS_NOTHING = /^[TtZ]/;

/**
 * End process group `pgid`, and every member it forks while it is being ended. One SIGKILL to a
 * group is not atomic with a fork in progress: macOS adds the child a member is forking to the group
 * unsignalled, and that child lives on holding the group's pipes (measured: a kill landing while git
 * forks an alias shell leaves the shell running, and so the call it serves never returns). So the
 * group is held with SIGSTOP and listed, round after round, until a listing shows every member
 * stopped or exited: a stopped process starts nothing, so the SIGKILL that follows ends them all.
 */
async function endProcessGroup(pgid: number): Promise<void> {
  while (signalGroup(pgid, 'SIGSTOP')) {
    if ((await groupStates(pgid)).every((stat) => STARTS_NOTHING.test(stat))) {
      signalGroup(pgid, 'SIGKILL');
      return;
    }
  }
}

/** Signal process group `pgid`; false once no member of it is left to signal. */
function signalGroup(pgid: number, signal: NodeJS.Signals): boolean {
  try {
    process.kill(-pgid, signal);
    return true;
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ESRCH') {
      return false;
    }
    throw error;
  }
}

/** The `ps` state of each member of process group `pgid`, as `ps` lists it on macOS and Linux alike. */
async function groupStates(pgid: number): Promise<string[]> {
  const listing = Bun.spawn(['ps', '-A', '-o', 'pgid=', '-o', 'stat='], { stdout: 'pipe', stderr: 'pipe' });
  const [exitCode, stdout, stderr] = await Promise.all([
    listing.exited,
    streamBytes(listing.stdout),
    streamBytes(listing.stderr),
  ]);
  if (exitCode !== 0) {
    throw new Error(`ps -A failed with exit code ${exitCode} while ending process group ${pgid}: ${decode(stderr)}`);
  }
  return decode(stdout)
    .split('\n')
    .flatMap((line) => {
      const [group, stat] = line.trim().split(/\s+/);
      return group !== undefined && stat !== undefined && Number(group) === pgid ? [stat] : [];
    });
}

/**
 * Whether and how a fixture `nx` runs a daemon:
 *
 * - `off`: no daemon. A daemonless Nx leaves nothing running once it exits.
 * - `job-rule`: a daemon under the rule every cowshed job runs under, `NX_DAEMON_START=never`
 *   (specs/cowshed/04_sandbox.md), set for the fixture whatever started this process: the
 *   client uses a daemon its record names while that runs and never starts one.
 * - `fixture-owned`: {@link FIXTURE_STARTS_ITS_OWN_DAEMON}, the one escape from that rule.
 */
export interface FixtureNxOptions {
  readonly daemon: 'off' | 'job-rule' | 'fixture-owned';
}

/**
 * The escape for a test whose subject is the daemon's retirement: that test must be able to create
 * what it retires. Only the one `nx` spawned with these options runs without `NX_DAEMON_START`, so
 * its client starts a daemon; the surrounding job, this process and every other fixture `nx` keep
 * the job's rule. The daemon it starts inherits {@link FIXTURE_DAEMON_MARKER} naming the fixture
 * root, so the test can prove the daemon it sees is that fixture's own ({@link fixtureDaemonOwnedBy})
 * and never passes on a daemon some other run left. Spelled out at the call site so the escape is
 * visible: an invisible one would become the leak the rule exists to stop.
 */
export const FIXTURE_STARTS_ITS_OWN_DAEMON: FixtureNxOptions = { daemon: 'fixture-owned' };

/** The variable a fixture-owned daemon is started with, naming its fixture root. */
export const FIXTURE_DAEMON_MARKER = 'SMOO_FIXTURE_NX_DAEMON';

/**
 * `nx` against a fixture root, with {@link fixtureNxEnv}. The root must lie in
 * a live fixture, which then stops whatever this client starts and retires the
 * socket alias published for the root here.
 */
export async function runFixtureNx(
  root: string,
  args: string[],
  options: FixtureNxOptions = { daemon: 'off' },
): Promise<void> {
  const fixture = [...liveFixtures].find((live) => root === live.root || root.startsWith(`${live.root}/`));
  if (fixture === undefined) {
    throw new Error(`runFixtureNx: no live withFixtureRepo fixture contains ${root}; its test has already finished`);
  }
  fixture.nxRoots.add(root);
  const client = spawnFixtureNx(root, args, options, await publishFixtureSocketDirectory(root));
  fixture.clients.add(client.pid);
  try {
    await expectNxSuccess(args, client);
  } finally {
    fixture.clients.delete(client.pid);
  }
}

/**
 * The environment of a fixture `nx`: it owns its own workspace root, graph,
 * cache and daemon. The daemon runs only when `options` asks for it, whatever
 * the inherited `NX_DAEMON` or CI flag says, and the socket directory of
 * whichever workspace launched this process never reaches the fixture: its own
 * is `socketDirectory`, the root's published `fixtureSocketDirectory`, the one
 * a stop of its daemon and the fixture's retirement name as well. Building it
 * publishes nothing. Host CI runners export the state locations to a per-lane
 * tree shared by every task in the run; a fixture inheriting them overwrote the
 * real workspace's project graph with its own one-project graph, and the
 * concurrent `nx run-many -t test` died with "Could not find project lmao-ttsc".
 */
export function fixtureNxEnv(root: string, options: FixtureNxOptions, socketDirectory: string): Record<string, string> {
  const env = definedProcessEnv();
  delete env.NX_SOCKET_DIR;
  delete env.NX_DAEMON_SOCKET_DIR;
  delete env[FIXTURE_DAEMON_MARKER];
  const daemon: Record<string, string> =
    options.daemon === 'off'
      ? { NX_DAEMON: 'false' }
      : options.daemon === 'job-rule'
        ? { NX_DAEMON: 'true', NX_DAEMON_START: 'never' }
        : { NX_DAEMON: 'true', [FIXTURE_DAEMON_MARKER]: root };
  if (options.daemon === 'fixture-owned') {
    delete env.NX_DAEMON_START;
  }
  return {
    ...env,
    ...daemon,
    NX_WORKSPACE_ROOT_PATH: root,
    NX_SOCKET_DIR: socketDirectory,
    NX_CACHE_DIRECTORY: join(root, '.nx', 'cache'),
    NX_WORKSPACE_DATA_DIRECTORY: join(root, '.nx', 'workspace-data'),
    ...FIXTURE_NX_DIAGNOSTICS,
  };
}

/**
 * Whether process `pid` was started with {@link FIXTURE_DAEMON_MARKER} naming `root`: the daemon a
 * {@link FIXTURE_STARTS_ITS_OWN_DAEMON} `nx` of that fixture started. Reads the environment the
 * process was started with: `/proc/<pid>/environ` on Linux, `ps -E` (this user's processes only)
 * on macOS. A process that has exited is no fixture's daemon.
 */
export async function fixtureDaemonOwnedBy(pid: number, root: string): Promise<boolean> {
  const variable = `${FIXTURE_DAEMON_MARKER}=${root}`;
  if (process.platform === 'linux') {
    const environ = await readFile(`/proc/${pid}/environ`, 'utf8').catch((error: unknown) => {
      if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return null;
      throw error;
    });
    return environ?.split('\0').includes(variable) ?? false;
  }
  // `ps -E` follows the command line with the start environment as space-separated words; the
  // variable is a word of its own, so what follows it is a space or the end of the line.
  const ps = Bun.spawnSync(['/bin/ps', '-E', '-ww', '-o', 'command=', '-p', String(pid)]);
  const line = ` ${decode(ps.stdout).trimEnd()} `;
  return line.includes(` ${variable} `);
}

function spawnFixtureNx(
  root: string,
  args: string[],
  options: FixtureNxOptions,
  socketDirectory: string,
): Bun.Subprocess<'ignore', 'pipe', 'pipe'> {
  return Bun.spawn(['nx', ...args], {
    cwd: root,
    env: fixtureNxEnv(root, options, socketDirectory),
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
  });
}

async function expectNxSuccess(args: string[], client: Bun.Subprocess<'ignore', 'pipe', 'pipe'>): Promise<void> {
  const [exitCode, stdoutBytes, stderrBytes] = await Promise.all([
    client.exited,
    streamBytes(client.stdout),
    streamBytes(client.stderr),
  ]);
  if (exitCode !== 0) {
    const stdout = decode(stdoutBytes);
    const stderr = decode(stderrBytes);
    printCommandOutput(stdout, stderr);
    throw new Error(`nx ${args.join(' ')} failed with exit code ${exitCode}\n${stdout}\n${stderr}`);
  }
}

/**
 * Release the Nx daemon a root started, through `nx daemon --stop`, and wait
 * until the daemon and every process it ran have exited. The stop names the
 * root's socket directory without publishing it: a root with a daemon was
 * published by the `runFixtureNx` that started it.
 */
function stopFixtureNxDaemon(root: string): Promise<void> {
  return stopNxDaemon(root, () =>
    expectNxSuccess(
      ['daemon', '--stop'],
      spawnFixtureNx(root, ['daemon', '--stop'], { daemon: 'job-rule' }, fixtureSocketDirectory(root)),
    ),
  );
}

function definedProcessEnv(): Record<string, string> {
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (value !== undefined) {
      env[key] = value;
    }
  }
  return env;
}

interface GitResult {
  exitCode: number;
  stdout: Uint8Array;
  stderr: Uint8Array;
  timedOut: boolean;
  timeoutMs: number;
}

function gitErrorMessage(root: string, args: string[], result: GitResult): string {
  const stderr = new TextDecoder().decode(result.stderr).trim();
  const timeoutText = result.timedOut
    ? ` timed out after ${result.timeoutMs}ms`
    : ` failed with exit code ${result.exitCode}`;
  return [`git ${args.join(' ')}${timeoutText}`, `cwd: ${root}`, stderr].filter(Boolean).join('\n');
}

export async function tag(root: string, tagName: string, date: string): Promise<void> {
  await git(root, ['tag', '-a', tagName, '-m', tagName], { GIT_COMMITTER_DATE: date });
}

export async function gitReleaseTagsByCreatorDate(root: string): Promise<GitReleaseTagInfo[]> {
  const result = await gitResult(root, [
    'for-each-ref',
    '--sort=-creatordate',
    '--format=%(refname:short)%09%(creatordate:unix)%09%(*objectname)%09%(objectname)',
    'refs/tags',
  ]);
  if (result.exitCode !== 0) {
    throw new Error(gitErrorMessage(root, ['for-each-ref', 'refs/tags'], result));
  }
  return new TextDecoder()
    .decode(result.stdout)
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const [name, timestampText, peeledSha, objectSha] = line.split('\t');
      const timestamp = Number(timestampText);
      const sha = peeledSha || objectSha;
      if (!name || !sha || !Number.isSafeInteger(timestamp)) {
        throw new Error(`Unable to parse release tag ref: ${line}`);
      }
      return { name, sha, timestamp };
    });
}

export async function gitIsAncestor(root: string, ancestor: string, descendant: string): Promise<boolean> {
  return gitSucceeds(root, ['merge-base', '--is-ancestor', ancestor, descendant]);
}

export async function packageVersionAtRef(root: string, packagePath: string, ref: string): Promise<string | null> {
  const result = await gitResult(root, ['show', `${ref}:${packagePath}/package.json`]);
  if (result.exitCode !== 0) {
    return null;
  }
  const parsed = JSON.parse(new TextDecoder().decode(result.stdout));
  return typeof parsed.version === 'string' ? parsed.version : null;
}
