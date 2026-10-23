import { expect, it } from 'bun:test';
import { mkdir, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

const repositoryRoot = join(import.meta.dir, '../../..');

// The child owns Nx's workspace singleton. Every observation delegates to the real installed planner and inspector.
const probe = String.raw`
const assert = require('node:assert/strict');
const { join } = require('node:path');
const nx = process.argv[2];
const measure = process.argv[3] === 'measure';
const { createProjectGraphAsync } = require(join(nx, 'dist/src/project-graph/project-graph.js'));
const { readNxJson } = require(join(nx, 'dist/src/config/nx-json.js'));
const { getTaskRawInputs } = require(join(nx, 'dist/src/hasher/check-task-files.js'));
const { HashPlanInspector } = require(join(nx, 'dist/src/hasher/hash-plan-inspector.js'));
const { HashPlanner } = require(join(nx, 'dist/src/native/index.js'));

const normalize = (inputs) => Object.fromEntries(
  Object.entries(inputs).map(([kind, values]) => [kind, [...values].sort()])
);

(async () => {
  const root = process.cwd();
  const projectGraph = await createProjectGraphAsync();
  const nxJson = readNxJson(root);
  const seed = { projectGraph, nxJson };
  const planned = [];
  const original = HashPlanner.prototype.getPlansReference;
  HashPlanner.prototype.getPlansReference = function(ids, graph) {
    planned.push({ ids: [...ids].sort(), tasks: Object.keys(graph.tasks).sort(), dependencies: graph.dependencies });
    return original.call(this, ids, graph);
  };
  const originalInspect = HashPlanInspector.prototype.inspectTaskInputs;
  let inspectionMs;
  HashPlanInspector.prototype.inspectTaskInputs = function(...args) {
    const started = performance.now();
    try { return originalInspect.apply(this, args); }
    finally { inspectionMs = performance.now() - started; }
  };
  try {
    for (const configuration of measure ? ['production'] : ['production', 'development']) {
      const canonical = 'app:check:' + configuration;
      const query = configuration === 'production' ? 'app:check' : canonical;
      const actual = await getTaskRawInputs(query, seed);
      assert.notEqual(actual, null, 'the requested canonical task must have a raw-input plan');
      const observed = planned.splice(0);
      if (measure) {
        process.stdout.write(JSON.stringify({ inspectionMs, canonical, planned: observed, inputs: normalize(actual) }) + '\n');
        return;
      }
      assert.equal(observed.length, 1, 'one raw query must make one native plan request');
      assert.deepEqual(observed[0].ids, [canonical], 'raw inputs must inspect only the requested canonical task');
      const allTasks = ['app:check:', 'common:build:', 'left:build:', 'right:build:'].map((id) => id + configuration).sort();
      assert.deepEqual(observed[0].tasks, allTasks, 'planning must retain the complete producer graph');
      assert.deepEqual(observed[0].dependencies[canonical].slice().sort(), ['left:build:', 'right:build:'].map((id) => id + configuration));

      const inspector = new HashPlanInspector(projectGraph, root, nxJson);
      await inspector.init();
      const closure = inspector.inspectTaskInputs({ project: 'app', target: 'check', configuration });
      const closureObserved = planned.splice(0);
      assert.equal(closureObserved.length, 1);
      assert.deepEqual(closureObserved[0].ids, allTasks, 'the plural inspector must still resolve the full closure');
      assert.deepEqual(Object.keys(closure).sort(), allTasks);
      assert.deepEqual(normalize(actual), normalize(closure[canonical]), 'root-only inspection must preserve every input bucket');
      assert.deepEqual(actual.depOutputs.slice().sort(), ['common', 'left', 'right'].map((name) => 'packages/' + name + '/dist-' + configuration + '/generated.d.ts').sort());
      assert(actual.files.includes('packages/app/src/input.ts'));
      assert(!actual.depOutputs.some((file) => file.includes('unrelated') || file.endsWith('.js')));
    }
    await assert.rejects(getTaskRawInputs('app:missing', seed), {
      message: 'Invalid taskId "app:missing" — project "app" has no target "missing".',
    });
    await assert.rejects(getTaskRawInputs('app:check:missing', seed), {
      message: 'Invalid taskId "app:check:missing" — target "check" of project "app" has no configuration "missing". Available configurations: production, development.',
    });
    process.stdout.write('root inputs, producer graph, configurations, closure API and refusals preserved\n');
  } finally {
    HashPlanner.prototype.getPlansReference = original;
    HashPlanInspector.prototype.inspectTaskInputs = originalInspect;
  }
})().catch((error) => { console.error(error); process.exitCode = 1; });
`;

it('inspects only the requested raw-input plan without dropping producer inputs or the closure API', async () => {
  await withNxFixture('nx-root-raw-inputs-', async ({ workspace, socketDirectory }) => {
    await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'root-input-proof', private: true }));
    await writeFile(join(workspace, 'nx.json'), '{}\n');
    await writeFile(join(workspace, 'probe.cjs'), probe);
    for (const name of ['app', 'common', 'left', 'right', 'unrelated']) {
      const directory = join(workspace, 'packages', name);
      await mkdir(join(directory, 'src'), { recursive: true });
      await writeFile(join(directory, 'src/input.ts'), `export const value = '${name}';\n`);
      for (const configuration of ['production', 'development']) {
        await mkdir(join(directory, `dist-${configuration}`));
        await writeFile(
          join(directory, `dist-${configuration}/generated.d.ts`),
          `export declare const value: '${name}';\n`,
        );
        await writeFile(join(directory, `dist-${configuration}/incidental.js`), 'not a declaration\n');
      }
      const target = {
        executor: 'nx:noop',
        inputs: ['{projectRoot}/src/**/*', { dependentTasksOutputFiles: '**/*.d.ts', transitive: true }],
        outputs: ['{options.outputPath}/'],
        defaultConfiguration: 'production',
        options: { outputPath: `packages/${name}/dist-production` },
        configurations: {
          production: { outputPath: `packages/${name}/dist-production` },
          development: { outputPath: `packages/${name}/dist-development` },
        },
        dependsOn:
          name === 'app' ? ['left:build', 'right:build'] : ['left', 'right'].includes(name) ? ['common:build'] : [],
      };
      await writeFile(
        join(directory, 'project.json'),
        JSON.stringify({ name, targets: { [name === 'app' ? 'check' : 'build']: target } }),
      );
    }
    const child = Bun.spawn(['node', join(workspace, 'probe.cjs'), join(repositoryRoot, 'node_modules/nx')], {
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
    expect(exit, stdout + stderr).toBe(0);
    expect(stdout).toBe('root inputs, producer graph, configurations, closure API and refusals preserved\n');
    // Restore the old selection at the real JS entry: the native planner still runs unchanged.
    const oldSelection = join(workspace, 'old-selection.cjs');
    await writeFile(
      oldSelection,
      String.raw`
const { join } = require('node:path');
const { HashPlanInspector } = require(join(process.argv[2], 'dist/src/hasher/hash-plan-inspector.js'));
const original = HashPlanInspector.prototype.inspectTaskInputs;
HashPlanInspector.prototype.inspectTaskInputs = function(target, parsedArgs, dependencies, excludeDependencies) {
  return original.call(this, target, parsedArgs, dependencies, excludeDependencies);
};
`,
    );
    const mutant = Bun.spawn(
      ['node', '--require', oldSelection, join(workspace, 'probe.cjs'), join(repositoryRoot, 'node_modules/nx')],
      {
        cwd: workspace,
        env: fixtureNxEnv(workspace, socketDirectory),
        stdin: 'ignore',
        stdout: 'pipe',
        stderr: 'pipe',
      },
    );
    const [mutantExit, mutantOutput, mutantError] = await Promise.all([
      mutant.exited,
      new Response(mutant.stdout).text(),
      new Response(mutant.stderr).text(),
    ]);
    expect(mutantExit, mutantOutput + mutantError).not.toBe(0);
    expect(mutantError).toContain('raw inputs must inspect only the requested canonical task');
  });
});
