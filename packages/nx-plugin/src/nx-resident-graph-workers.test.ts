import { expect, it } from 'bun:test';
import { execFileSync, spawn, spawnSync } from 'node:child_process';
import { mkdir, readFile, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { guardEvent } from './__tests__/counted-cargo.js';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

/**
 * Guards the resident-worker hunk of `patches/nx@23.2.1.patch`, the local build
 * of nrwl/nx#37271. Nx 23.2.1's `PluginLifecycleManager` stops each isolated
 * plugin worker after the last phase it has hooks for, which for the built-in
 * graph plugins is the graph itself. In a one-shot client that frees the
 * worker for the tasks; in the daemon, which recomputes the graph for every
 * tracked file change, it means every change spawns and loads each graph
 * plugin's worker again: 0.4-0.5 s per change on an idle host and 0.9-1.5 s
 * under a gate's load, measured in a dev watcher's fixture daemon. The patched
 * daemon keeps a worker with graph hooks for its own lifetime.
 *
 * The fixture's daemon computes the graph, sees one tracked edit, and computes
 * it again. Unpatched, no graph plugin worker outlives the first graph and the
 * second spawns new ones; patched, the same worker processes answer both.
 * Drop the hunk, and keep this test, once the Nx version in use contains that
 * fix.
 */

const repositoryRoot = join(import.meta.dir, '../../..');

async function showApp(root: string, env: Record<string, string>): Promise<unknown> {
  const child = spawn('node', [join(repositoryRoot, 'node_modules/.bin/nx'), 'show', 'project', 'app', '--json'], {
    cwd: root,
    env,
    detached: process.platform !== 'win32',
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '';
  let stderr = '';
  child.stdout?.setEncoding('utf8').on('data', (text: string) => {
    stdout += text;
  });
  child.stderr?.setEncoding('utf8').on('data', (text: string) => {
    stderr += text;
  });
  const { promise: ended, resolve, reject } = Promise.withResolvers<number | null>();
  child.once('error', reject);
  child.once('close', resolve);
  try {
    const code = await guardEvent(
      ended,
      'nx show project app to close',
      () => `pid ${child.pid}, exit ${child.exitCode}, signal ${child.signalCode}\n${stdout}${stderr}`,
    );
    expect(code, `${stdout}${stderr}`).toBe(0);
    return JSON.parse(stdout) as unknown;
  } catch (error) {
    if (child.pid !== undefined && child.exitCode === null && child.signalCode === null) {
      try {
        process.kill(process.platform === 'win32' ? child.pid : -child.pid, 'SIGKILL');
      } catch (killError) {
        if (!(killError instanceof Error && 'code' in killError && killError.code === 'ESRCH')) {
          throw new AggregateError([error, killError], 'could not retire the failed fixture Nx command');
        }
      }
    }
    await ended.catch(() => undefined);
    throw error;
  }
}

/** The pids of the isolated plugin workers `daemon` has running now. */
function pluginWorkers(daemon: number): number[] {
  const found = spawnSync('pgrep', ['-P', String(daemon), '-f', 'plugin-worker.js'], { encoding: 'utf8' });
  // pgrep exits 1 when nothing matches, and 2 or more on a real failure.
  if (found.status === 1) return [];
  if (found.status !== 0) throw new Error(`pgrep failed (${found.status}): ${found.stderr}`, { cause: found.error });
  return found.stdout
    .split('\n')
    .filter((line) => line !== '')
    .map(Number)
    .sort((a, b) => a - b);
}

it('keeps the daemon graph plugin workers running from one tracked change to the next', async () => {
  await withNxFixture('nx-resident-workers-', async ({ workspace: root }) => {
    // fixtureNxEnv runs plugins in process and without a daemon; this
    // regression is about isolated workers, and the daemon is their host.
    const env = { ...fixtureNxEnv(root), NX_ISOLATE_PLUGINS: 'true', NX_DAEMON: 'true' };
    await mkdir(join(root, 'app'));
    await symlink(join(repositoryRoot, 'node_modules'), join(root, 'node_modules'), 'dir');
    await writeFile(join(root, '.gitignore'), 'node_modules\n.nx\n');
    await writeFile(join(root, 'package.json'), JSON.stringify({ name: 'resident-workers-fixture', private: true }));
    await writeFile(join(root, 'nx.json'), '{}\n');
    await writeFile(join(root, 'app/project.json'), JSON.stringify({ name: 'app', tags: ['first'] }));
    execFileSync('git', ['init', '--quiet', root], { stdio: 'pipe' });

    expect(await showApp(root, env)).toMatchObject({ tags: ['first'] });
    const record = await readFile(join(root, '.nx/workspace-data/d/server-process.json'), 'utf8');
    const server: unknown = JSON.parse(record);
    if (!(typeof server === 'object' && server !== null && 'processId' in server)) {
      throw new Error(`the fixture daemon record names no process: ${record}`);
    }
    const daemon = Number(server.processId);
    const afterFirstGraph = pluginWorkers(daemon);
    expect(afterFirstGraph).not.toEqual([]);

    // A tracked edit the daemon recomputes the graph for, through the same
    // project.json plugin worker that built the first graph.
    await writeFile(join(root, 'app/project.json'), JSON.stringify({ name: 'app', tags: ['second'] }));
    expect(await showApp(root, env)).toMatchObject({ tags: ['second'] });
    expect(await readFile(join(root, '.nx/workspace-data/d/server-process.json'), 'utf8')).toBe(record);
    expect(pluginWorkers(daemon)).toEqual(afterFirstGraph);
  });
});
