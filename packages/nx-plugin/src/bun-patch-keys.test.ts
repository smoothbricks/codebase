import { expect, test } from 'bun:test';
import { realpathSync } from 'node:fs';
import { mkdir, readFile, stat, symlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import typia from 'typia';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { whenChanged } from './__tests__/path-change.js';

const nodeModules = join(import.meta.dir, '../../../node_modules');
const patched = '@fixture/patched';
const built = '@fixture/built';
const patchFile = 'patches/fixture.patch';
const movedFile = 'patches/renamed.patch';
const targets = {
  direct: patched,
  indirect: 'carrier@1.0.0',
  alias: 'installed-as',
  unrelated: 'unrelated@1.0.0',
  otherVersion: `${patched}@2.0.0`,
};

const PROBE = `
const { getDaemonProcessIdSync } = require('nx/src/daemon/cache');
const { daemonClient } = require('nx/src/daemon/client/client');
const { readNxJson } = require('nx/src/config/nx-json');
const { createTaskHasher } = require('nx/src/hasher/create-task-hasher');
const { createProjectGraphAsync } = require('nx/src/project-graph/project-graph');
const { createTaskGraph } = require('nx/src/tasks-runner/create-task-graph');
(async () => {
  const graph = await createProjectGraphAsync({ exitOnError: false });
  const taskGraph = createTaskGraph(graph, {}, ['app'], process.argv.slice(2), undefined, {});
  const tasks = Object.values(taskGraph.tasks);
  const hasher = createTaskHasher(graph, readNxJson(), {}, undefined, undefined);
  const hashes = await hasher.hashTasks(tasks, taskGraph, Object.fromEntries(tasks.map(({ id }) => [id, process.env])));
  process.stdout.write(JSON.stringify({
    pid: getDaemonProcessIdSync(),
    daemon: daemonClient.enabled(),
    nodes: graph.externalNodes,
    edges: graph.dependencies['npm:carrier@1.0.0'],
    tasks: Object.fromEntries(tasks.map(({ target }, i) => [target.target, hashes[i].value])),
  }));
})().catch((error) => { console.error(error); process.exitCode = 1; });
`;

const PLUGIN = `const { builtOutputLockfileNodes, builtOutputLockfileDependencies } = require('./built-output.cjs');
exports.createNodesV2 = ['package.json', (files, options, context) =>
  files.map(file => [file, file === 'package.json' ? { externalNodes: builtOutputLockfileNodes(options, context) } : {}])];
exports.createDependencies = builtOutputLockfileDependencies;
`;

interface Measured {
  readonly pid: number | null;
  readonly daemon: boolean;
  readonly nodes: Record<string, { data: { hash: string } }>;
  readonly edges: unknown;
  readonly tasks: { readonly [K in keyof typeof targets]: string };
}
const parseMeasured = typia.json.createAssertParse<Measured>();

//#region smoo/cowshed!n/patch-bytes-in-keys
for (const replacement of [false, true]) {
  test(`keys only consumers of installed patch bytes with ${replacement ? 'built-output' : 'default'} Nx nodes`, async () => {
    await withNxFixture('bun-patch-keys-', async ({ workspace, socketDirectory }) => {
      const file = (path: string, bytes: string) => writeFile(join(workspace, path), bytes);
      const json = (path: string, data: unknown) => file(path, `${JSON.stringify(data, null, 2)}\n`);
      const env = fixtureNxEnv(workspace, socketDirectory);
      for (const dir of ['patches', 'app']) await mkdir(join(workspace, dir));
      await symlink(nodeModules, join(workspace, 'node_modules'), 'dir');
      await file('probe.cjs', PROBE);
      if (replacement) {
        const bundle = await Bun.build({
          entrypoints: [join(import.meta.dir, 'built-output-packages.ts')],
          plugins: [
            {
              name: 'fixture-pinned-nx',
              setup(build) {
                const require = createRequire(import.meta.url);
                build.onResolve({ filter: /^nx\// }, ({ path }) => ({
                  path: realpathSync(require.resolve(path)),
                  external: true,
                }));
              },
            },
          ],
          format: 'cjs',
          target: 'node',
        });
        const output = bundle.outputs[0];
        if (!bundle.success || output === undefined) throw new AggregateError(bundle.logs, 'built-output fixture');
        await file('built-output.cjs', await output.text());
        await file('plugin.cjs', PLUGIN);
      }
      await json(
        'nx.json',
        replacement
          ? {
              pluginsConfig: { '@nx/js': { analyzeLockfile: false } },
              plugins: [{ plugin: './plugin.cjs', options: { builtOutputPackages: [built] } }],
            }
          : {},
      );
      await json('app/project.json', {
        name: 'app',
        targets: Object.fromEntries(
          Object.entries(targets).map(([name, dependency]) => [
            name,
            {
              executor: 'nx:noop',
              inputs: [{ externalDependencies: [dependency] }],
            },
          ]),
        ),
      });
      const dependencies = {
        [patched]: '1.0.0',
        [built]: '1.0.0',
        carrier: '1.0.0',
        'installed-as': `npm:${patched}@1.0.0`,
        unrelated: '1.0.0',
      };
      const manifest = { name: 'root', private: true, dependencies };
      const lock = {
        lockfileVersion: 3,
        workspaces: { '': { name: 'root', dependencies } },
        packages: {
          [patched]: [`${patched}@1.0.0`, '', {}, 'sha512-patched'],
          'installed-as': [`${patched}@1.0.0`, '', {}, 'sha512-patched'],
          [`unrelated/${patched}`]: [`${patched}@2.0.0`, '', {}, 'sha512-other-version'],
          [built]: [`${built}@1.0.0`, '', {}, 'sha512-built'],
          carrier: ['carrier@1.0.0', '', { dependencies: { [patched]: '^1.0.0' } }, 'sha512-carrier'],
          unrelated: ['unrelated@1.0.0', '', { dependencies: { [patched]: '^2.0.0' } }, 'sha512-unrelated'],
        },
      };
      const registration = async (path?: string) => {
        const patchMap =
          path === undefined
            ? {}
            : {
                patchedDependencies: {
                  [`${patched}@1.0.0`]: path,
                  [`${built}@1.0.0`]: path,
                  [`${patched}@3.0.0`]: 'patches/not-installed.patch',
                },
              };
        await json('package.json', { ...manifest, ...patchMap });
        await json('bun.lock', { ...lock, ...patchMap });
      };
      const measure = async (useDaemon = false): Promise<Measured> => {
        const child = Bun.spawn(['node', 'probe.cjs', ...Object.keys(targets)], {
          cwd: workspace,
          env: { ...env, NX_DAEMON: String(useDaemon), NX_DAEMON_START: 'never' },
          stdin: 'ignore',
          stdout: 'pipe',
          stderr: 'pipe',
        });
        const [exit, stdout, stderr] = await Promise.all([
          child.exited,
          new Response(child.stdout).text(),
          new Response(child.stderr).text(),
        ]);
        expect(exit, `${stdout}\n${stderr}`).toBe(0);
        return parseMeasured(stdout);
      };

      await registration();
      const none = await measure();
      await file(patchFile, 'patch version A\n');
      await registration(patchFile);
      await mkdir(socketDirectory, { recursive: true, mode: 0o700 });
      const daemonStart = realpathSync(createRequire(import.meta.url).resolve('nx/src/daemon/server/start.js'));
      const daemon = Bun.spawn(['node', daemonStart], { cwd: workspace, env, stdin: 'ignore', stderr: 'pipe' });
      const stderr = new Response(daemon.stderr).text();
      const stop = new AbortController();
      const exited = daemon.exited.then(async (code) => {
        stop.abort(new Error(`Nx daemon exited (${code}): ${await stderr}`));
        return code;
      });
      let added: Measured;
      let edited: Measured;
      try {
        const socket = join(socketDirectory, 'd.sock');
        await whenChanged(
          socketDirectory,
          async () => {
            try {
              return (await stat(socket)).isSocket();
            } catch (error) {
              if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return false;
              throw error;
            }
          },
          stop.signal,
        );
        added = await measure(true);
        const lockBytes = await readFile(join(workspace, 'bun.lock'), 'utf8');
        await file(patchFile, 'patch version B\n');
        edited = await measure(true);
        expect(await readFile(join(workspace, 'bun.lock'), 'utf8')).toBe(lockBytes);
        for (const row of [added, edited]) {
          expect(row.daemon).toBe(true);
          expect(row.pid).toBe(daemon.pid);
        }
      } finally {
        if (daemon.exitCode === null) daemon.kill();
        await exited;
      }
      await file(movedFile, 'patch version B\n');
      await registration(movedFile);
      const moved = await measure();
      await registration();
      const removed = await measure();

      for (const row of [none, added, edited, moved, removed]) {
        expect(row.edges).toEqual([{ source: 'npm:carrier@1.0.0', target: `npm:${patched}@1.0.0`, type: 'static' }]);
        expect(Object.keys(row.nodes).sort()).toEqual(Object.keys(none.nodes).sort());
        for (const name of ['npm:carrier', 'npm:unrelated', `npm:${patched}@2.0.0`]) {
          expect(row.nodes[name]).toEqual(none.nodes[name]);
        }
        expect(row.tasks.unrelated).toBe(none.tasks.unrelated);
        expect(row.tasks.otherVersion).toBe(none.tasks.otherVersion);
        if (replacement) expect(row.nodes[`npm:${built}`]?.data.hash).toBe(`built-output:${built}`);
      }
      for (const name of [`npm:${patched}`, `npm:${patched}@1.0.0`, 'npm:installed-as']) {
        expect(added.nodes[name]?.data.hash, `${name}: add`).not.toBe(none.nodes[name]?.data.hash);
        expect(edited.nodes[name]?.data.hash, `${name}: content edit`).not.toBe(added.nodes[name]?.data.hash);
        expect(moved.nodes[name]).toEqual(edited.nodes[name]);
        expect(removed.nodes[name]).toEqual(none.nodes[name]);
      }
      for (const target of ['direct', 'indirect', 'alias'] as const) {
        expect(added.tasks[target], `${target}: add`).not.toBe(none.tasks[target]);
        expect(edited.tasks[target], `${target}: content edit`).not.toBe(added.tasks[target]);
        expect(moved.tasks[target]).toBe(edited.tasks[target]);
        expect(removed.tasks[target]).toBe(none.tasks[target]);
      }
    });
  });
}
//#endregion smoo/cowshed!n/patch-bytes-in-keys
