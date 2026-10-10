import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { mkdir, readFile, symlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

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
