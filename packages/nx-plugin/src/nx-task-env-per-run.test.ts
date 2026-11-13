import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdir, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

/**
 * Guards the task-environment hunk of `patches/nx@24.0.3.patch` (patches/README.md, "A task's environment lives for
 * one run"). A long-lived client runs one Nx invocation after another in one process: a consumer's Nx worker answers
 * each gate request with `runPreTasksExecution`, `invokeTasksRunner` and `runPostTasksExecution`. This package's pre
 * hook publishes the run's own endpoints (`MBX_RESERVATIONS_SOCKET`, the CPU pool's socket) in `process.env`, and the
 * post hook closes them. Stock Nx 24.0.3 memoized each task's environment in a module-level map keyed by task id, so a
 * later run's task was hashed and forked with the first run's environment: its reserved leaf asked an endpoint the
 * first run had already closed, `connect ENOENT …/mbx-r-*\/r.sock`, and was ended with no test verdict.
 */

const repositoryRoot = join(import.meta.dir, '../../..');

/** A plugin that is source in the checkout: the hook publishes its run's id, as this package's publishes its endpoints. */
const HOOK_PLUGIN = `exports.preTasksExecution = async (_options, context) => {
  process.env.HOOK_RUN = context.id;
};
`;

/** Two Nx invocations in one process, each through the hooks and the tasks runner, as a long-lived client makes them. */
const TWO_RUNS = `const { readFileSync, writeFileSync } = require('node:fs');
const { createProjectGraphAsync } = require('nx/src/project-graph/project-graph');
const { readNxJson } = require('nx/src/config/nx-json');
const { splitArgsIntoNxArgsAndOverrides } = require('nx/src/utils/command-line-utils');
const { createTaskGraph } = require('nx/src/tasks-runner/create-task-graph');
const { invokeTasksRunner } = require('nx/src/tasks-runner/run-command');
const { runPreTasksExecution, runPostTasksExecution } = require('nx/src/project-graph/plugins/tasks-execution-hooks');

async function run(id) {
  // A run named \`later…\` first writes a local plugin the earlier runs never named, and names it.
  if (id.startsWith('later')) {
    writeFileSync('later-plugin.js', "exports.preTasksExecution = async (_options, context) => { process.env.HOOK_RUN = 'later-plugin ' + context.id; };\\n");
    writeFileSync('nx.json', JSON.stringify({ plugins: ['./later-plugin.js'] }));
  }
  const projectGraph = await createProjectGraphAsync({ exitOnError: false });
  const nxJson = readNxJson();
  const { nxArgs } = splitArgsIntoNxArgsAndOverrides({ targets: ['show'], outputStyle: 'static' }, 'run-many', { printWarnings: false }, nxJson);
  const taskGraph = createTaskGraph(projectGraph, {}, ['p'], ['show'], undefined, {}, false);
  const tasks = Object.values(taskGraph.tasks);
  const context = { id, workspaceRoot: process.cwd(), nxJsonConfiguration: nxJson, argv: process.argv };
  await runPreTasksExecution({ ...context, projectGraph, taskGraph });
  const { taskResults } = await invokeTasksRunner({ tasks, projectGraph, taskGraph, lifeCycle: {}, nxJson, nxArgs, loadDotEnvFiles: true, initiatingProject: null, initiatingTasks: tasks });
  await runPostTasksExecution({ ...context, taskResults, startTime: 0, endTime: 0 });
  const statuses = Object.values(taskResults).map((result) => result.status).join(',');
  return id + ' ' + statuses + ' saw ' + readFileSync('p/dist/seen.txt', 'utf8');
}

(async () => {
  for (const id of process.argv.slice(2)) console.log(await run(id));
  process.exit(0);
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
`;

/** The runs `ids` of one Bun process over a fixture whose local plugin publishes the run's id to a task keyed on it. */
async function runsInOneProcess(
  label: string,
  ids: readonly string[],
): Promise<{ status: number | null; lines: string[]; output: string }> {
  return await withNxFixture(label, async ({ workspace, socketDirectory }) => {
    await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: label, private: true }));
    await writeFile(join(workspace, 'nx.json'), JSON.stringify({ plugins: ['./hook-plugin.js'] }));
    await writeFile(join(workspace, 'hook-plugin.js'), HOOK_PLUGIN);
    await writeFile(join(workspace, 'two-runs.cjs'), TWO_RUNS);
    await mkdir(join(workspace, 'p'));
    await writeFile(
      join(workspace, 'p', 'project.json'),
      JSON.stringify({
        name: 'p',
        targets: {
          show: {
            command: 'mkdir -p p/dist && printf "%s" "$HOOK_RUN" > p/dist/seen.txt',
            inputs: [{ env: 'HOOK_RUN' }],
            outputs: ['{projectRoot}/dist/'],
            cache: true,
          },
        },
      }),
    );
    // The job rule, no daemon running: the client runs its hooks and tasks in its own process.
    const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_DAEMON: 'true', NX_DAEMON_START: 'never' };
    const ran = spawnSync('bun', ['two-runs.cjs', ...ids], { cwd: workspace, env, encoding: 'utf8' });
    return { status: ran.status, lines: ran.stdout.trim().split('\n'), output: `${ran.stdout}${ran.stderr}` };
  });
}

it.each([
  {
    name: 'each run of a long-lived Nx client hashes and forks its tasks with the environment its own pre hook published',
    label: 'nx-task-env-per-run',
    runs: ['run-one', 'run-two'],
    saw: ['run-one success saw run-one', 'run-two success saw run-two'],
  },
  {
    // Bun's resolver keeps a directory's entries once it has looked anything up there, so the plugin file the second
    // run writes in the workspace root was invisible to a relative specifier (patches/README.md, "Local plugin paths").
    name: 'a long-lived Bun client loads a local plugin nx.json names by a relative path, written after its first run',
    label: 'nx-local-plugin-later',
    runs: ['run-one', 'later'],
    saw: ['run-one success saw run-one', 'later success saw later-plugin later'],
  },
] as const)('$name', async ({ label, runs, saw }) => {
  const { status, lines, output } = await runsInOneProcess(label, runs);
  expect(status, `the runs: ${output}`).toBe(0);
  expect(lines, `the runs: ${output}`).toEqual([...saw]);
});
