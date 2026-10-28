import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { chmod, mkdir, readFile, symlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { compileFunction } from 'node:vm';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { whenChanged } from './__tests__/path-change.js';
import { nxDaemonProcesses } from './testing.js';

// A graph plugin that asks its daemon for files, as globIndexedFiles does: from an isolated worker Nx routes the glob
// through the worker's own daemon client.
const DAEMON_GLOB_PLUGIN = `const { globWithWorkspaceContext } = require('nx/src/utils/workspace-context');
const { workspaceRoot } = require('nx/src/utils/workspace-root');
exports.createNodesV2 = ['**/project.json', async () => {
  await globWithWorkspaceContext(workspaceRoot, ['**/project.json']);
  return [];
}];
`;
// The daemon logs its open connection count on every connect and close.
const OPEN_CONNECTIONS = /Number of open connections: (\d+)$/;
// A sync generator the daemon runs that asks the daemon for files over its socket. Code the daemon runs does that when it
// reaches Nx through another copy of the nx package (a plugin linked from outside the workspace): that copy's project
// graph module holds no graph, so it asks the daemon as any client would, from inside the daemon.
const SELF_CLIENT_GENERATOR = `const { daemonClient } = require('nx/src/daemon/client/client');
exports.default = async () => {
  await daemonClient.glob(['**/project.json']);
};
`;
const SELF_CLIENT = './generators.json:self-client';

/** The open connection count the daemon log `log` reports last; `undefined` before it reports one. */
function latestOpenConnections(log: string): string | undefined {
  return log
    .split('\n')
    .map((line) => OPEN_CONNECTIONS.exec(line)?.[1])
    .filter((count) => count !== undefined)
    .at(-1);
}

it('serves real daemon requests with the installed five-minute inactivity policy', async () => {
  const require = createRequire(import.meta.url);
  const installedNx = require('nx/src/daemon/server/shutdown-utils.js');
  expect(installedNx.SERVER_INACTIVITY_TIMEOUT_MS).toBe(5 * 60_000);
  const repositoryRoot = join(import.meta.dir, '../../..');
  const nxEntry = join(repositoryRoot, 'node_modules/.bin/nx');
  await withNxFixture('nx-idle-policy-', async ({ workspace, socketDirectory }) => {
    await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await writeFile(join(workspace, 'nx.json'), '{}\n');
    await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'idle-policy', private: true }));
    await mkdir(join(workspace, 'p'));
    await writeFile(join(workspace, 'p/project.json'), JSON.stringify({ name: 'p' }));
    const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true' };
    const started = spawnSync('node', [nxEntry, 'daemon', '--start'], { cwd: workspace, env, encoding: 'utf8' });
    expect(started.status, started.stdout + started.stderr).toBe(0);
    const requested = spawnSync('node', [nxEntry, 'show', 'projects', '--json'], {
      cwd: workspace,
      env,
      encoding: 'utf8',
    });
    expect(requested.status, requested.stdout + requested.stderr).toBe(0);
    expect(JSON.parse(requested.stdout)).toEqual(['p']);
    expect(await readFile(join(workspace, '.nx/workspace-data/d/daemon.log'), 'utf8')).toContain(
      join(dirname(require.resolve('nx/package.json')), 'dist/src/daemon/server'),
    );
  });
});

/**
 * Nx's Nx Console and AI-agent configuration checks run each editor's CLI (`code`, `cursor`) from the daemon.
 * Upstream starts both as the daemon starts, every client asks for the first and every task run for the second, so a
 * daemon that only ever served non-interactive runs (a cowshed job, a gate, a test) still forked editors that outlived
 * it in its process group (a gate's test shard held `code --version` past the daemon it came from). The patch starts
 * neither at daemon start, and only a client on a terminal, which can show what they find, asks for them. Editor
 * stand-ins on `PATH` record any run.
 */
it('runs no editor probe from a daemon that serves only non-interactive clients', async () => {
  const repositoryRoot = join(import.meta.dir, '../../..');
  const nxEntry = join(repositoryRoot, 'node_modules/.bin/nx');
  await withNxFixture('nx-console-probe-', async ({ root, workspace, socketDirectory }) => {
    await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await writeFile(join(workspace, 'nx.json'), '{}\n');
    await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'console-probe', private: true }));
    await mkdir(join(workspace, 'p'));
    await writeFile(
      join(workspace, 'p/project.json'),
      JSON.stringify({ name: 'p', targets: { noop: { command: 'true' } } }),
    );
    const editors = join(root, 'editors');
    const ran = join(root, 'editors-ran');
    await mkdir(editors);
    for (const editor of ['code', 'cursor']) {
      await writeFile(join(editors, editor), `#!/bin/sh\necho $0 "$@" >> ${JSON.stringify(ran)}\n`);
      await chmod(join(editors, editor), 0o755);
    }
    const base = fixtureNxEnv(workspace, socketDirectory);
    const env = { ...base, NX_DAEMON: 'true', PATH: `${editors}:${base.PATH ?? ''}` };
    const started = spawnSync('node', [nxEntry, 'daemon', '--start'], { cwd: workspace, env, encoding: 'utf8' });
    expect(started.status, started.stdout + started.stderr).toBe(0);
    const requested = spawnSync('node', [nxEntry, 'show', 'projects', '--json'], {
      cwd: workspace,
      env,
      encoding: 'utf8',
    });
    expect(requested.status, requested.stdout + requested.stderr).toBe(0);
    const task = spawnSync('node', [nxEntry, 'run', 'p:noop', '--outputStyle=static'], {
      cwd: workspace,
      env,
      encoding: 'utf8',
    });
    expect(task.status, task.stdout + task.stderr).toBe(0);
    const stopped = spawnSync('node', [nxEntry, 'daemon', '--stop'], { cwd: workspace, env, encoding: 'utf8' });
    expect(stopped.status, stopped.stdout + stopped.stderr).toBe(0);
    const log = await readFile(join(workspace, '.nx/workspace-data/d/daemon.log'), 'utf8');
    expect(log, 'the daemon ran its Nx Console check').not.toContain('[NX-CONSOLE]');
    expect(await readFile(ran, 'utf8').catch(() => ''), 'an editor CLI ran').toBe('');
  });
});

it('lets a resident plugin worker ask its daemon for files without holding the daemon open', async () => {
  const repositoryRoot = join(import.meta.dir, '../../..');
  const nxEntry = join(repositoryRoot, 'node_modules/.bin/nx');
  await withNxFixture('nx-worker-connection-', async ({ workspace, socketDirectory }) => {
    await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await writeFile(join(workspace, 'nx.json'), JSON.stringify({ plugins: ['./daemon-glob-plugin.js'] }));
    await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'worker-connection', private: true }));
    await writeFile(join(workspace, 'daemon-glob-plugin.js'), DAEMON_GLOB_PLUGIN);
    await mkdir(join(workspace, 'p'));
    await writeFile(join(workspace, 'p/project.json'), JSON.stringify({ name: 'p' }));
    const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true', NX_ISOLATE_PLUGINS: 'true' };
    const requested = spawnSync('node', [nxEntry, 'show', 'projects', '--json'], {
      cwd: workspace,
      env,
      encoding: 'utf8',
    });
    expect(requested.status, requested.stdout + requested.stderr).toBe(0);
    expect(JSON.parse(requested.stdout)).toEqual(['p']);
    const logPath = join(workspace, '.nx/workspace-data/d/daemon.log');
    // The daemon logs its open connection count on every connect and close, so the wait ends on that log. A connection
    // the worker kept would keep the daemon from its five-minute idle stop for as long as the worker lives, and the daemon
    // keeps a worker with graph hooks for as long as it lives.
    // Each change the kernel reports on the log re-reads it whole, and its latest count decides, so a count line that
    // comes while a read is in flight still brings another read.
    await whenChanged(logPath, () => {
      const log = readFileSync(logPath, 'utf8');
      expect(log).toContain('Handled json message GLOB');
      return latestOpenConnections(log) === '0';
    });
    const workers = (await nxDaemonProcesses(workspace)).filter(({ command }) => command.includes('plugin-worker'));
    expect(workers.length, 'the resident plugin worker that asked the daemon for files').toBeGreaterThan(0);
  });
});

it('lets a daemon that is its own client go of its connection after the reply', async () => {
  const repositoryRoot = join(import.meta.dir, '../../..');
  const nxEntry = join(repositoryRoot, 'node_modules/.bin/nx');
  await withNxFixture('nx-self-client-', async ({ workspace, socketDirectory }) => {
    await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    // Measured 2026-10-22: a shed daemon that ran a linked plugin's sync generator this way held its own connection,
    // logged one open connection with no client left, and reset its five-minute idle stop for half an hour until it was signalled by hand.
    await writeFile(join(workspace, 'nx.json'), JSON.stringify({ sync: { globalGenerators: [SELF_CLIENT] } }));
    await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'self-client', private: true }));
    await writeFile(
      join(workspace, 'generators.json'),
      JSON.stringify({ generators: { 'self-client': { factory: './self-client.js', schema: './schema.json' } } }),
    );
    await writeFile(join(workspace, 'schema.json'), JSON.stringify({ type: 'object', properties: {} }));
    await writeFile(join(workspace, 'self-client.js'), SELF_CLIENT_GENERATOR);
    await mkdir(join(workspace, 'p'));
    await writeFile(join(workspace, 'p/project.json'), JSON.stringify({ name: 'p' }));
    const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true' };
    // The daemon computes the graph once it is up and then runs every global sync generator itself.
    const started = spawnSync('node', [nxEntry, 'daemon', '--start'], { cwd: workspace, env, encoding: 'utf8' });
    expect(started.status, started.stdout + started.stderr).toBe(0);
    const logPath = join(workspace, '.nx/workspace-data/d/daemon.log');
    // Each change the kernel reports on the log re-reads it whole: the generator's end, then the latest count, decides.
    await whenChanged(logPath, () => {
      const log = readFileSync(logPath, 'utf8');
      if (log.includes(`${SELF_CLIENT} error:`))
        throw new Error(`the sync generator failed in the daemon:
${log}`);
      if (!log.includes(`${SELF_CLIENT} changes:`)) return false;
      expect(log).toContain('Handled json message GLOB');
      return latestOpenConnections(log) === '0';
    });
  });
});

// Node wraps every CommonJS file in this function, so its body is where a `return` or a `require` is legal.
const COMMONJS_PARAMETERS = ['exports', 'require', 'module', '__filename', '__dirname'];

/** The syntax error `source` has as a CommonJS module body, or `undefined` when it compiles. Runs nothing. */
function commonJsSyntaxError(source: string, filename: string): string | undefined {
  // Node accepts a leading hashbang line as a comment; Bun's compileFunction does not, so the same line is read as one.
  const body = source.replace(/^#!/, '//');
  try {
    compileFunction(body, COMMONJS_PARAMETERS, { filename });
    return undefined;
  } catch (error) {
    if (error instanceof Error) return error.message;
    throw error;
  }
}

it('syntax-checks every JavaScript file patched in the actual installed Nx product', () => {
  const installedNxRoot = dirname(createRequire(import.meta.url).resolve('nx/package.json'));
  const patch = readFileSync(new URL('../../../patches/nx@23.2.1.patch', import.meta.url), 'utf8');
  const patchedJavaScript = patch
    .split('\n')
    .filter((line) => line.startsWith('+++ b/') && line.endsWith('.js'))
    .map((line) => line.slice('+++ b/'.length));
  expect(patchedJavaScript.length).toBeGreaterThan(0);
  // Every patched file is CommonJS (Nx is `type: commonjs`, none is ESM), so none needs a module check.
  for (const relative of patchedJavaScript) {
    const file = join(installedNxRoot, relative);
    const failure = commonJsSyntaxError(readFileSync(file, 'utf8'), file);
    expect(failure, `${relative} does not compile as a CommonJS module body`).toBeUndefined();
  }
});
