import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { mkdir, readFile, symlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
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
// How long the daemon may take to see a closed connection: the aftermath of a reply, not an idle interval.
const CLOSE_SEEN_MS = 10_000;

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
    const deadline = Date.now() + CLOSE_SEEN_MS;
    let open: string | undefined;
    for (;;) {
      const log = await readFile(logPath, 'utf8');
      expect(log).toContain('Handled json message GLOB');
      open = log
        .split('\n')
        .map((line) => OPEN_CONNECTIONS.exec(line)?.[1])
        .filter((count) => count !== undefined)
        .at(-1);
      if (open === '0' || Date.now() > deadline) break;
      await sleep(50);
    }
    // A connection the worker kept would keep the daemon from its five-minute idle stop for as long as the worker
    // lives, and the daemon keeps a worker with graph hooks for as long as it lives.
    expect(open, `open daemon connections ${CLOSE_SEEN_MS}ms after the only client exited`).toBe('0');
    const workers = (await nxDaemonProcesses(workspace)).filter(({ command }) => command.includes('plugin-worker'));
    expect(workers.length, 'the resident plugin worker that asked the daemon for files').toBeGreaterThan(0);
  });
});

it('syntax-checks every JavaScript file patched in the actual installed Nx product', () => {
  const installedNxRoot = dirname(createRequire(import.meta.url).resolve('nx/package.json'));
  const patch = readFileSync(new URL('../../../patches/nx@23.2.1.patch', import.meta.url), 'utf8');
  const patchedJavaScript = patch
    .split('\n')
    .filter((line) => line.startsWith('+++ b/') && line.endsWith('.js'))
    .map((line) => line.slice('+++ b/'.length));
  expect(patchedJavaScript.length).toBeGreaterThan(0);
  for (const relative of patchedJavaScript) {
    const checked = spawnSync('node', ['--check', join(installedNxRoot, relative)], {
      encoding: 'utf8',
    });
    expect(checked.status, `${relative}\n${checked.stdout}${checked.stderr}`).toBe(0);
  }
});
