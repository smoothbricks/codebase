import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdir, readFile, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

/**
 * Guards the plugin-worker hunks of `patches/nx@24.0.3.patch`. This package's `preTasksExecution` publishes the run's
 * endpoints (`MBX_RESERVATIONS_SOCKET`, the CPU pool's socket) by setting `process.env`, and Nx carries those writes to
 * the run's task processes: it wraps a hook's `process.env` in a proxy that records them and returns them to the
 * client, which applies them. Stock Nx installs that proxy when plugin isolation is on or a daemon is enabled. The
 * patched Nx loads a plugin whose entry is source inside the checkout in a plugin worker under every setting
 * (`NX_ISOLATE_PLUGINS=false`, which the managed shell exports, included), so a run whose client has no daemon (one
 * that never started it, or whose daemon stopped under it) ran the hook in a worker that installed no proxy. The hook
 * set the worker's own environment, nothing came back, and every reserved leaf of the run refused to start for want of
 * the endpoint the hook had served.
 */

const repositoryRoot = join(import.meta.dir, '../../..');
const nxEntry = join(repositoryRoot, 'node_modules', '.bin', 'nx');

const PUBLISHED = 'published by the hook';

/** A plugin that is source in the checkout: the hook publishes through `process.env`, as this package's does. */
const HOOK_PLUGIN = `exports.preTasksExecution = async () => {
  process.env.HOOK_PUBLISHED = ${JSON.stringify(PUBLISHED)};
};
`;

it('a local plugin hook sets an environment its run publishes to the tasks of a client that runs without a daemon', async () => {
  await withNxFixture('nx-local-plugin-hook-env', async ({ workspace, socketDirectory }) => {
    await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await writeFile(
      join(workspace, 'package.json'),
      JSON.stringify({ name: 'nx-local-plugin-hook-env', private: true }),
    );
    await writeFile(join(workspace, 'nx.json'), JSON.stringify({ plugins: ['./hook-plugin.js'] }));
    await writeFile(join(workspace, 'hook-plugin.js'), HOOK_PLUGIN);
    await mkdir(join(workspace, 'p'));
    await writeFile(
      join(workspace, 'p', 'project.json'),
      JSON.stringify({
        name: 'p',
        targets: {
          show: {
            command: 'mkdir -p p/dist && printf "%s" "$HOOK_PUBLISHED" > p/dist/seen.txt',
            outputs: ['{projectRoot}/dist/'],
            cache: true,
          },
        },
      }),
    );
    // The job rule, no daemon running: the client computes the command in its own process, as one whose daemon
    // stopped under it does.
    const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true', NX_DAEMON_START: 'never' };
    const ran = spawnSync('bun', [nxEntry, 'run', 'p:show', '--outputStyle=static'], {
      cwd: workspace,
      env,
      encoding: 'utf8',
    });
    const output = `${ran.stdout}${ran.stderr}`;
    expect(ran.status, `the run: ${output}`).toBe(0);
    expect(await readFile(join(workspace, 'p/dist/seen.txt'), 'utf8'), `what the task saw: ${output}`).toBe(PUBLISHED);
  });
});
