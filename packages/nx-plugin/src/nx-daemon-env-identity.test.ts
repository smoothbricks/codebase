import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { existsSync, statSync } from 'node:fs';
import { chmod, mkdir, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { delimiter, dirname, join, relative } from 'node:path';
import type * as NxDaemonEnvironment from 'nx/src/daemon/client/daemon-environment';
import type * as NxWorkspaceRoot from 'nx/src/utils/workspace-root';
import { fixtureNxEnv, nxFixtureRoot, withNxFixture } from './__tests__/fixture-nx-env.js';

/**
 * Guards the run-scaffolding hunk of `patches/nx@23.2.1.patch` in `daemon-environment.js`. A client that a package
 * manager runs (`bun nx`, `bun run`, `npm run` …) sends its daemon a PATH with `node_modules/.bin` directories in front
 * and, from Bun, NODE; a plain `nx` client of the same shell sends neither. Stock 23.2.1 counted both as project-graph
 * identity, so every switch between the two kinds of client discarded the daemon's graph and computed it again: on
 * a consumer's dev boot about 2.6 s and a fresh task hasher per switch, with all 47 task hashes the same under both.
 * The daemon still receives and applies the runtime env; only the graph identity leaves the scaffolding out.
 */

const repositoryRoot = join(import.meta.dir, '../../..');
const nxEntry = join(repositoryRoot, 'node_modules', '.bin', 'nx');
const workspaceRequire = createRequire(join(repositoryRoot, 'package.json'));
const environment: typeof NxDaemonEnvironment = workspaceRequire('nx/src/daemon/client/daemon-environment');
const { workspaceRoot }: typeof NxWorkspaceRoot = workspaceRequire('nx/src/utils/workspace-root');

const RECOMPUTED = 'Graph recompute necessary due to env variable refresh. Changed keys: ';
const KEPT = /Env variable refresh changed only runtime values \(([^)]*)\); graph identity is unchanged/;

async function fixtureWorkspace(workspace: string): Promise<void> {
  await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  await writeFile(join(workspace, 'nx.json'), '{}\n');
  await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'daemon-env-identity', private: true }));
  await mkdir(join(workspace, 'p'));
  await writeFile(join(workspace, 'p', 'project.json'), JSON.stringify({ name: 'p' }));
}

/**
 * Run `nx show projects --json` through `client` against the fixture's daemon and return what the daemon logged while
 * serving it. The daemon applies a client's env, and logs what it decided, before it answers that client, and it
 * writes its log synchronously to the file, so the log is complete once the client has exited.
 */
async function served(workspace: string, env: Record<string, string>, client: readonly string[]): Promise<string> {
  const log = join(workspace, '.nx/workspace-data/d/daemon.log');
  const before = (await readFile(log)).length;
  const [command, ...args] = client;
  if (command === undefined) throw new Error('a client names its command');
  const ran = spawnSync(command, [...args, 'show', 'projects', '--json'], { cwd: workspace, env, encoding: 'utf8' });
  expect(ran.status, `${client.join(' ')} show projects\n${ran.stdout}${ran.stderr}`).toBe(0);
  expect(JSON.parse(ran.stdout.trim().split('\n').at(-1) ?? '')).toEqual(['p']);
  return (await readFile(log)).subarray(before).toString('utf8');
}

it('keeps the daemon graph while package-manager runs and plain clients alternate', async () => {
  await withNxFixture('nx-env-identity-', async ({ workspace, socketDirectory }) => {
    await fixtureWorkspace(workspace);
    const env: Record<string, string> = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true' };
    const started = spawnSync('bun', [nxEntry, 'daemon', '--start'], { cwd: workspace, env, encoding: 'utf8' });
    expect(started.status, started.stdout + started.stderr).toBe(0);
    // `bun <file>` runs the entry as it is; `bun nx` runs the package bin as `bun run` does, with its scaffolding.
    const plain = ['bun', nxEntry];
    const packageManagerRun = ['bun', 'nx'];
    await served(workspace, env, plain);
    for (const [name, client] of [
      ['bun nx', packageManagerRun],
      ['nx', plain],
      ['bun nx', packageManagerRun],
    ] as const) {
      const log = await served(workspace, env, client);
      expect(log, `${name} after the other kind of client`).not.toContain(RECOMPUTED);
      // The env did move: without this the absence above could be a client that sent the same env.
      expect(KEPT.exec(log)?.[1]?.split(', '), `${name}: the runtime keys the daemon applied\n${log}`).toContain(
        'PATH',
      );
    }
    // A PATH directory that no package manager put there is graph identity: a tool on it can change what a plugin
    // infers.
    const tools = join(workspace, 'tools');
    await mkdir(tools);
    const extended = await served(workspace, { ...env, PATH: `${tools}${delimiter}${env.PATH}` }, plain);
    expect(extended).toContain(`${RECOMPUTED}PATH`);
  });
});

it('leaves exactly the run scaffolding out of an environment graph identity', async () => {
  const root = await nxFixtureRoot('nx-env-shapes-');
  try {
    const directory = async (name: string, node: boolean) => {
      const path = join(root, name);
      await mkdir(path, { recursive: true });
      if (node) await writeFile(join(path, 'node'), '#!/bin/sh\n', { mode: 0o755 });
      return path;
    };
    const usr = await directory('usr-bin', true);
    const alt = await directory('alt-bin', true);
    const tools = await directory('tools', false);
    const graph = (env: NodeJS.ProcessEnv, platform: NodeJS.Platform = 'darwin') =>
      environment.normalizeDaemonEnvironmentForGraph(env, platform);
    const shell = [usr, alt, tools].join(':');
    const plain = graph({ PATH: shell, HOME: '/home/u' });
    // `bun run` puts node_modules/.bin in front of PATH from the run's directory (twice) up to the filesystem root.
    // Those in the workspace go, and those above it that do not exist; one above it that exists stays.
    const ancestors: string[] = [];
    for (let directory = dirname(workspaceRoot); ; directory = dirname(directory)) {
      ancestors.push(join(directory, 'node_modules/.bin'));
      if (directory === dirname(directory)) break;
    }
    const chain = [
      join(workspaceRoot, 'packages/app/node_modules/.bin'),
      join(workspaceRoot, 'packages/app/node_modules/.bin'),
      join(workspaceRoot, 'packages/node_modules/.bin'),
      join(workspaceRoot, 'node_modules/.bin'),
      ...ancestors,
    ];
    // Only a lookup that proves absence drops one; a directory this process may not search keeps it.
    const provedAbsent = (path: string) => {
      try {
        statSync(path);
        return false;
      } catch (error) {
        if (error instanceof Error && 'code' in error) return error.code === 'ENOENT' || error.code === 'ENOTDIR';
        throw error;
      }
    };
    const above = ancestors.filter((bin) => !provedAbsent(bin));
    const bunRun = [...chain, shell].join(':');
    // With a node on PATH, NODE names the one PATH resolves.
    const plainAbove = graph({ PATH: [...above, shell].join(':'), HOME: '/home/u' });
    expect(graph({ PATH: bunRun, NODE: join(usr, 'node'), HOME: '/home/u' })).toEqual(plainAbove);
    // A shell that keeps the checkout's node_modules/.bin on PATH compares equal to the runs it starts.
    expect(graph({ PATH: `${join(workspaceRoot, 'node_modules/.bin')}:${shell}`, HOME: '/home/u' })).toEqual(plain);
    // With none, NODE names a temporary directory whose `node` is Bun, first on PATH; a run inside a run puts its
    // chain before the outer shim.
    const shim = join(root, 'bun-node-c6da4a4d3');
    await mkdir(shim);
    await writeFile(join(shim, 'node'), '#!/bin/sh\n', { mode: 0o755 });
    const noNode = graph({ PATH: [...above, ...above, tools].join(':'), HOME: '/home/u' });
    expect(graph({ PATH: [shim, ...chain, tools].join(':'), NODE: shim, HOME: '/home/u' })).toEqual(
      graph({ PATH: [...above, tools].join(':'), HOME: '/home/u' }),
    );
    expect(graph({ PATH: [...chain, shim, ...chain, tools].join(':'), NODE: shim, HOME: '/home/u' })).toEqual(noNode);
    // Yarn Berry's per-invocation bin folder stays left out, beside the rest.
    expect(graph({ PATH: `/tmp/xfs-1a2b/:${bunRun}`, BERRY_BIN_FOLDER: '/tmp/xfs-1a2b/', HOME: '/home/u' })).toEqual({
      ...plainAbove,
      BERRY_BIN_FOLDER: '<YARN_BERRY_BIN_FOLDER>',
    });
    // Any other directory, another checkout's package bin directory, one merely named like a package bin directory,
    // a relative one, and a reordered PATH are identity.
    const foreign = join(root, 'other-checkout/node_modules/.bin');
    await mkdir(foreign, { recursive: true });
    // A directory this process may not search: whether a package bin under it exists is unknown, not absent.
    const locked = join(root, 'locked');
    await mkdir(join(locked, 'node_modules/.bin'), { recursive: true });
    await chmod(locked, 0o000);
    for (const entry of [
      '/opt/tools',
      foreign,
      // Spelled through the workspace, resolving outside it.
      `${workspaceRoot}/${relative(workspaceRoot, foreign)}`,
      join(root, 'absent-checkout/node_modules/.bin'),
      join(locked, 'node_modules/.bin'),
      join(workspaceRoot, 'node_modules/.bin-tools'),
      join(workspaceRoot, 'node_modules/bin'),
      'node_modules/.bin',
    ]) {
      expect(graph({ PATH: `${entry}:${shell}`, HOME: '/home/u' }), entry).not.toEqual(plain);
    }
    expect(graph({ PATH: [alt, usr, tools].join(':'), HOME: '/home/u' })).not.toEqual(plain);
    // A NODE that is not the node PATH resolves was chosen, not found: another node on PATH, one off it, a directory
    // on PATH that is no Bun shim, and a shim behind another node all stay, and so does every PATH entry.
    for (const node of [join(alt, 'node'), '/opt/node-24/bin/node', tools, shim]) {
      const env = { PATH: [usr, tools, shim].join(':'), NODE: node, HOME: '/home/u' };
      expect(graph(env), node).toEqual(env);
    }
  } finally {
    if (existsSync(join(root, 'locked'))) await chmod(join(root, 'locked'), 0o700);
    await rm(root, { recursive: true, force: true });
  }
});

/**
 * The graph identity `normalizeDaemonEnvironmentForGraph` gives `env` in a process whose workspace is `workspace`:
 * where an entry lies is judged against the workspace, which Nx reads once per process (`NX_WORKSPACE_ROOT_PATH`).
 */
function graphIn(workspace: string, env: Record<string, string>): Record<string, string> {
  const installed = workspaceRequire.resolve('nx/src/daemon/client/daemon-environment');
  const probe = `process.stdout.write(JSON.stringify(require(${JSON.stringify(installed)}).normalizeDaemonEnvironmentForGraph(${JSON.stringify(env)})))`;
  const ran = spawnSync('bun', ['-e', probe], {
    cwd: workspace,
    env: { HOME: process.env.HOME ?? '/', PATH: process.env.PATH ?? '', NX_WORKSPACE_ROOT_PATH: workspace },
    encoding: 'utf8',
  });
  expect(ran.status, ran.stderr).toBe(0);
  const graph: unknown = JSON.parse(ran.stdout);
  if (typeof graph !== 'object' || graph === null) throw new Error(`no env from ${ran.stdout}`);
  return Object.fromEntries(Object.entries(graph).map(([key, value]) => [key, String(value)]));
}

it('judges a package bin directory on where the kernel resolves it, and keeps one it cannot see', async () => {
  const root = await nxFixtureRoot('nx-env-where-');
  const above = join(root, 'above');
  const workspace = join(above, 'workspace');
  const outside = join(root, 'outside');
  try {
    await mkdir(workspace, { recursive: true });
    await mkdir(join(outside, 'child'), { recursive: true });
    await mkdir(join(outside, 'tools/node_modules/.bin'), { recursive: true });
    await symlink(join(outside, 'child'), join(workspace, 'link'), 'dir');
    // The kernel resolves `..` after the link: this is outside's tools, another checkout's bin directory.
    const throughLink = `${workspace}/link/../tools/node_modules/.bin`;
    expect(graphIn(workspace, { PATH: `${throughLink}:/usr/bin` }).PATH).toBe(`${throughLink}:/usr/bin`);
    // A bin directory above the workspace that the lookup proves absent resolves nothing, and goes.
    const aboveBin = join(above, 'node_modules/.bin');
    expect(graphIn(workspace, { PATH: `${aboveBin}:/usr/bin` }).PATH).toBe('/usr/bin');
    // One the lookup cannot see into (EACCES) may exist, and stays.
    await mkdir(aboveBin, { recursive: true });
    await chmod(join(above, 'node_modules'), 0o000);
    expect(graphIn(workspace, { PATH: `${aboveBin}:/usr/bin` }).PATH).toBe(`${aboveBin}:/usr/bin`);
  } finally {
    if (existsSync(join(above, 'node_modules'))) await chmod(join(above, 'node_modules'), 0o700);
    await rm(root, { recursive: true, force: true });
  }
});

it('gives plugins the same environment digest under a package-manager run', () => {
  // `hashDaemonClientEnv` keys a plugin's disk cache of what it inferred, so it is graph identity too: a digest that
  // moved with the run's scaffolding would miss that cache on every switch the daemon itself now ignores.
  const installed = workspaceRequire.resolve('nx/src/daemon/client/daemon-environment');
  const digest = (env: Record<string, string>) => {
    const probe = `process.stdout.write(require(${JSON.stringify(installed)}).hashDaemonClientEnv())`;
    const ran = spawnSync('bun', ['-e', probe], { env, encoding: 'utf8' });
    expect(ran.status, ran.stderr).toBe(0);
    return ran.stdout;
  };
  const shell = process.env.PATH ?? '';
  const base = { HOME: process.env.HOME ?? '/', PATH: shell };
  const plain = digest(base);
  const scaffolded = [join(repositoryRoot, 'node_modules/.bin'), '/node_modules/.bin', shell].join(delimiter);
  // NODE as `bun run` sets it: the node a lookup of PATH finds.
  const node = Bun.which('node', { PATH: shell });
  if (node === null) throw new Error(`no node on PATH ${shell}`);
  expect(digest({ ...base, PATH: scaffolded, NODE: node })).toBe(plain);
  expect(digest({ ...base, PATH: `/opt/tools${delimiter}${shell}` })).not.toBe(plain);
});
