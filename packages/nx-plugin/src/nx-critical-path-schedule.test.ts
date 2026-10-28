import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdir, readFile, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

/**
 * Guards the critical-path hunks of `patches/nx@23.2.1.patch` (tasks-schedule.js and
 * task-orchestrator.js). Stock Nx orders ready tasks by how many tasks depend on them directly and
 * starts no task while every slot is held, so a gate's critical chain started behind short leaves
 * and then waited for slots held by tests running at a tenth of a core. The patched scheduler
 * orders ready tasks by critical path (estimated duration plus the longest path through their
 * dependents), and the orchestrator starts the ready head of the run's longest remaining chain as
 * one extra worker while every slot is held: the jobserver, not the slot count, admits CPU.
 *
 * Drop the hunks, and keep these tests, once the Nx in use schedules by critical path.
 */

const repoRoot = join(import.meta.dir, '../../..');
const nxRoot = join(repoRoot, 'node_modules/nx/dist/src');
const nxEntry = join(repoRoot, 'node_modules/.bin/nx');

const append = (name: string) => ({ executor: 'nx:run-commands', options: { command: `echo ${name} >> order.log` } });

it('starts the head of the longest chain before a task that merely has more direct dependents', async () => {
  await withNxFixture('nx-critical-path-', async ({ workspace, socketDirectory }) => {
    const git = spawnSync('git', ['init', '--quiet', workspace]);
    expect(git.status, git.stderr.toString()).toBe(0);
    await symlink(join(repoRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await writeFile(join(workspace, 'nx.json'), '{}\n');
    await mkdir(join(workspace, 'p'));
    // c1 has two direct dependents and heads a chain of two; a0 has one and heads a chain of four.
    await writeFile(
      join(workspace, 'p', 'project.json'),
      JSON.stringify({
        name: 'p',
        targets: {
          a0: append('a0'),
          a1: { ...append('a1'), dependsOn: ['a0'] },
          a2: { ...append('a2'), dependsOn: ['a1'] },
          a3: { ...append('a3'), dependsOn: ['a2'] },
          c1: append('c1'),
          c2: { ...append('c2'), dependsOn: ['c1'] },
          c3: { ...append('c3'), dependsOn: ['c1'] },
        },
      }),
    );
    const child = spawnSync(
      'bun',
      [nxEntry, 'run-many', '-t', 'a3,c2,c3', '-p', 'p', '--parallel=1', '--outputStyle=static'],
      { cwd: workspace, env: fixtureNxEnv(workspace, socketDirectory), encoding: 'utf8' },
    );
    expect(child.status, child.stdout + child.stderr).toBe(0);
    const order = (await readFile(join(workspace, 'order.log'), 'utf8')).trim().split('\n');
    expect(order.slice(0, 2), order.join(' ')).toEqual(['a0', 'a1']);
    expect([...order].sort()).toEqual(['a0', 'a1', 'a2', 'a3', 'c1', 'c2', 'c3']);
  });
});

it('offers a task past the slot count only while it heads the run’s longest remaining chain', async () => {
  await withNxFixture('nx-critical-extra-', async ({ workspace, socketDirectory }) => {
    await mkdir(join(workspace, '.nx/workspace-data'), { recursive: true });
    await writeFile(join(workspace, 'nx.json'), '{}\n');
    // `t1`, `t2` independent; `x` heads `x -> a0 -> a1`. Two slots: `x` and `t1` take them.
    const program = `
const assert = require('node:assert/strict');
const path = require('node:path');
const { TasksSchedule } = require(path.join(${JSON.stringify(nxRoot)}, 'tasks-runner/tasks-schedule.js'));
const task = (id) => ({ id: 'p:' + id, target: { project: 'p', target: id }, overrides: {}, outputs: [], cache: false });
const ids = ['x', 'a0', 'a1', 't1', 't2'];
const dependencies = { 'p:x': [], 'p:a0': ['p:x'], 'p:a1': ['p:a0'], 'p:t1': [], 'p:t2': [] };
const taskGraph = {
  roots: ['p:x', 'p:t1', 'p:t2'],
  tasks: Object.fromEntries(ids.map((id) => ['p:' + id, task(id)])),
  dependencies,
  continuousDependencies: Object.fromEntries(ids.map((id) => ['p:' + id, []])),
};
const projectGraph = { nodes: { p: { name: 'p', type: 'lib', data: { root: 'p' } } }, dependencies: { p: [] } };
(async () => {
  const schedule = new TasksSchedule(projectGraph, { p: { root: 'p' } }, taskGraph, { lifeCycle: {}, batch: false });
  await schedule.init();
  await schedule.scheduleNextTasks();
  const any = () => true;
  assert.equal(schedule.nextTask(any).id, 'p:x', 'the chain head takes a slot first');
  assert.equal(schedule.nextTask(any).id, 'p:t1');
  assert.equal(schedule.nextCriticalTask(any), null, 'the running x heads a longer chain than t2');
  // x ends where it frees no slot (its result came from elsewhere): a0 is ready and heads the longest chain.
  schedule.complete(['p:x']);
  await schedule.scheduleNextTasks();
  assert.equal(schedule.nextCriticalTask(any).id, 'p:a0', 'a0 heads the longest remaining chain');
  assert.equal(schedule.nextCriticalTask(any), null, 'the running a0 heads a longer chain than t2');
})().catch((error) => { console.error(error); process.exit(1); });
`;
    const child = spawnSync('node', ['-e', program], {
      cwd: workspace,
      env: fixtureNxEnv(workspace, socketDirectory),
      encoding: 'utf8',
    });
    expect(child.status, child.stdout + child.stderr).toBe(0);
  });
});
