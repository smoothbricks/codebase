import { expect, it } from 'bun:test';
import { mkdir, realpath, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import typia from 'typia';

import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { builtOutputHash, builtOutputLockfileNodes } from './built-output-packages.js';

const repositoryRoot = join(import.meta.dir, '../../..');
const pluginModule = join(import.meta.dir, 'built-output-packages.ts');

/**
 * The plugin's two hooks around this module, as `index.ts` wires them, for a fixture that runs no other inference. The
 * probe hands the module over: Nx loads a plugin through its own type stripping, which resolves no `.js` specifier of
 * the module's sources to its `.ts` file.
 */
const PLUGIN = `const { builtOutputLockfileNodes, builtOutputLockfileDependencies } = globalThis.builtOutputPackages;
exports.createNodesV2 = ['package.json', (files, options, context) => {
  const externalNodes = builtOutputLockfileNodes(options, context);
  return files.map((file) => [file, { externalNodes }]);
}];
exports.createDependencies = (options, context) => builtOutputLockfileDependencies(options, context);
`;

/**
 * Nx's own graph and hash of `app:check`, computed in the fixture by the installed Nx, under Bun. Nx runs a local plugin
 * in a Node worker of its own, so the probe loads it in-process instead (as serial-group-wiring's graph child does).
 */
const PROBE = `globalThis.builtOutputPackages = require(${JSON.stringify(pluginModule)});
const { loadNxPlugin } = require('nx/src/project-graph/plugins/in-process-loader.js');
require('nx/src/project-graph/plugins/get-plugins.js').loadingMethod = (plugin, root, index) => loadNxPlugin(plugin, root, index);
const { createProjectGraphAsync } = require('nx/src/project-graph/project-graph.js');
const { readNxJson } = require('nx/src/config/nx-json.js');
const { createTaskGraph } = require('nx/src/tasks-runner/create-task-graph.js');
const { createTaskHasher } = require('nx/src/hasher/create-task-hasher.js');
const { hashTask } = require('nx/src/hasher/hash-task.js');
(async () => {
  const graph = await createProjectGraphAsync({ exitOnError: true });
  const hasher = createTaskHasher(graph, readNxJson(), {}, undefined, undefined);
  const taskGraph = createTaskGraph(graph, {}, ['app'], ['check'], undefined, {});
  const task = taskGraph.tasks['app:check'];
  await hashTask(hasher, graph, taskGraph, task, process.env, undefined);
  const packageEdges = Object.fromEntries(Object.entries(graph.dependencies).filter(([source]) => source.startsWith('npm:')));
  process.stdout.write(JSON.stringify({ hash: task.hash, externalNodes: graph.externalNodes, packageEdges, lib: graph.dependencies.lib }));
})().catch((error) => { console.error(error); process.exit(1); });
`;

interface Versions {
  readonly tool: string;
  readonly other: string;
}

/**
 * A workspace whose `lib` imports `tool`, which depends on `other`, and whose `app` depends on `lib`: `app:check` keys
 * both packages only through `^default`, as a test keys the packages its dependencies import. A bump writes the root
 * manifest and the lockfile, as an install does; no project file names a version.
 */
async function writeWorkspace(workspace: string, nxJson: unknown, { tool, other }: Versions): Promise<void> {
  const write = async (path: string, text: string) => {
    await mkdir(join(workspace, path, '..'), { recursive: true });
    await writeFile(join(workspace, path), text);
  };
  const json = (value: unknown) => `${JSON.stringify(value, null, 2)}\n`;
  await write('nx.json', json(nxJson));
  await write('built-output-plugin.cjs', PLUGIN);
  await write('probe.cjs', PROBE);
  await write(
    'package.json',
    json({ name: 'root', private: true, workspaces: ['packages/*'], dependencies: { other, tool } }),
  );
  await write(
    'bun.lock',
    json({
      lockfileVersion: 1,
      workspaces: {
        '': { name: 'root', dependencies: { other, tool } },
        'packages/app': { name: 'app', dependencies: { lib: 'workspace:*' } },
        'packages/lib': { name: 'lib', dependencies: { tool: '*' } },
      },
      packages: {
        app: ['app@workspace:packages/app'],
        lib: ['lib@workspace:packages/lib'],
        other: [`other@${other}`, '', {}, `sha512-other-${other}`],
        tool: [`tool@${tool}`, '', { dependencies: { other } }, `sha512-tool-${tool}`],
      },
    }),
  );
  await write('node_modules/tool/package.json', json({ name: 'tool', version: tool, dependencies: { other } }));
  await write('node_modules/other/package.json', json({ name: 'other', version: other }));
  await symlink(await realpath(join(repositoryRoot, 'node_modules/nx')), join(workspace, 'node_modules/nx'), 'dir');
  await write(
    'packages/app/package.json',
    json({
      name: 'app',
      dependencies: { lib: 'workspace:*' },
      nx: {
        targets: {
          check: { executor: 'nx:noop', cache: true, inputs: ['default', '^default', { externalDependencies: [] }] },
        },
      },
    }),
  );
  await write('packages/lib/package.json', json({ name: 'lib', dependencies: { tool: '*' } }));
}

const parseMeasured = typia.json.createAssertParse<Measured>();
interface Measured {
  readonly hash: string;
  readonly externalNodes: Record<string, { data: { packageName: string; hash?: string } }>;
  readonly packageEdges: Record<string, unknown>;
  readonly lib: unknown;
}

async function measure(nxJson: unknown, versions: Versions): Promise<Measured> {
  return withNxFixture('built-output-packages-', async ({ workspace, socketDirectory }) => {
    await writeWorkspace(workspace, nxJson, versions);
    const child = Bun.spawn([process.execPath, 'probe.cjs'], {
      cwd: workspace,
      env: fixtureNxEnv(workspace, socketDirectory),
      stdin: 'ignore',
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const [exit, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    expect(exit, stderr).toBe(0);
    return parseMeasured(stdout);
  });
}

const OWN_NODES = {};
const BUILT_OUTPUT_TOOL = {
  pluginsConfig: { '@nx/js': { analyzeLockfile: false } },
  plugins: [{ plugin: './built-output-plugin.cjs', options: { builtOutputPackages: ['tool'] } }],
};
const BASE: Versions = { tool: '1.0.0', other: '2.0.0' };

it("keys a named package's dependents on its graph presence, not its version, and every other package as Nx does", async () => {
  const [own, ownBumped, named, namedBumped, namedOtherBumped] = await Promise.all([
    measure(OWN_NODES, BASE),
    measure(OWN_NODES, { ...BASE, tool: '1.0.1' }),
    measure(BUILT_OUTPUT_TOOL, BASE),
    measure(BUILT_OUTPUT_TOOL, { ...BASE, tool: '1.0.1' }),
    measure(BUILT_OUTPUT_TOOL, { ...BASE, other: '2.0.1' }),
  ]);
  // Nx's own nodes key `app:check` on tool's integrity, which a version bump moves.
  expect(ownBumped.hash, JSON.stringify({ lib: own.lib, edges: own.packageEdges })).not.toBe(own.hash);
  // Named, tool's bump keeps the key; a bump of its dependency still moves it.
  expect(namedBumped.hash).toBe(named.hash);
  expect(namedOtherBumped.hash).not.toBe(named.hash);

  // The graph is Nx's own: the same nodes, versions and edges, and tool's hash names tool alone.
  const toolNodes = Object.keys(own.externalNodes).filter(
    (name) => own.externalNodes[name]?.data.packageName === 'tool',
  );
  expect(toolNodes.length).toBeGreaterThan(0);
  const expected = structuredClone(own.externalNodes);
  for (const name of toolNodes) {
    const node = expected[name];
    if (node !== undefined) node.data.hash = builtOutputHash('tool');
  }
  expect(named.externalNodes).toEqual(expected);
  expect(named.packageEdges).toEqual(own.packageEdges);
  expect(named.lib).toEqual(own.lib);
});

it("refuses the option while Nx's own lockfile nodes would replace the plugin's", () => {
  expect(() =>
    builtOutputLockfileNodes({ builtOutputPackages: ['tool'] }, { nxJsonConfiguration: {}, workspaceRoot: '/none' }),
  ).toThrow('analyzeLockfile');
});

it('refuses a named package the lockfile installs nowhere', async () => {
  await withNxFixture('built-output-packages-absent-', async ({ workspace }) => {
    await writeWorkspace(workspace, BUILT_OUTPUT_TOOL, BASE);
    expect(() =>
      builtOutputLockfileNodes(
        { builtOutputPackages: ['tool', 'absent'] },
        { nxJsonConfiguration: BUILT_OUTPUT_TOOL, workspaceRoot: workspace },
      ),
    ).toThrow('absent');
  });
});

it('publishes nothing and reads nothing for a workspace that names no package', () => {
  expect(builtOutputLockfileNodes({}, { nxJsonConfiguration: {}, workspaceRoot: '/none' })).toEqual({});
  expect(builtOutputLockfileNodes(undefined, { nxJsonConfiguration: {}, workspaceRoot: '/none' })).toEqual({});
});
