import { Database } from 'bun:sqlite';
import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdir, readdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

/**
 * Guards the complete pre-start failure boundary in the installed Nx patch, not only StoreRunInformationLifeCycle.
 * A hash failure has neither an execution start nor a hash: retain its failure in run.json, but do not invent a
 * native/legacy execution-history row, elapsed duration or executed profile interval. Valid database failures still
 * propagate. Drop these hunks only when upstream supports the same lifecycle ordering, retaining these regressions.
 */
const nxRoot = join(import.meta.dir, '../../../node_modules/nx/dist/src');

const setup = `
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const nx = (file) => require(path.join(${JSON.stringify(nxRoot)}, file));
const { CompositeLifeCycle } = nx('tasks-runner/life-cycle.js');
const { StoreRunInformationLifeCycle } = nx('tasks-runner/life-cycles/store-run-information-life-cycle.js');
const makeTask = (id, timing = {}) => ({
  id: 'p:' + id, target: { project: 'p', target: id }, overrides: {}, outputs: [], cache: false, ...timing,
});
const unstarted = makeTask('unstarted', { endTime: 125 });
const hashedUnstarted = makeTask('hashed-unstarted', { hash: 'hashed-unstarted', endTime: 125 });
const unhashedStarted = makeTask('unhashed-started', { startTime: 100, endTime: 125 });
const supplied = makeTask('supplied', { hash: 'supplied', startTime: 0, endTime: 125 });
const notified = makeTask('notified', { hash: 'notified' });
const stored = [];
const store = new StoreRunInformationLifeCycle('hash failure regression', (run) => stored.push(run));
async function complete(reporter) {
  const lifecycle = new CompositeLifeCycle([store, reporter]);
  await lifecycle.startCommand();
  await lifecycle.startTasks([notified, unhashedStarted], { groupId: 0 });
  notified.endTime = Date.now();
  await lifecycle.endTasks([
    { task: unstarted, status: 'failure', code: 1, terminalOutput: 'HASH_TASKS write EPIPE' },
    { task: hashedUnstarted, status: 'failure', code: 1 },
    { task: unhashedStarted, status: 'failure', code: 1 },
    { task: supplied, status: 'success', code: 0 },
    { task: notified, status: 'success', code: 0 },
  ], { groupId: 0 });
  await lifecycle.endCommand();
  assert.equal(unstarted.startTime, undefined);
  assert.equal(unstarted.hash, undefined);
  const failedRun = stored[0].tasks.find((task) => task.taskId === unstarted.id);
  assert.equal(failedRun.status, 1);
  assert.equal(failedRun.startTime, undefined);
  assert.equal(failedRun.hash, undefined);
  assert.equal(stored[0].tasks.find((task) => task.taskId === supplied.id).startTime, '1970-01-01T00:00:00.000Z');
  fs.writeFileSync('stored-run.json', JSON.stringify(stored));
}
`;

async function fixture(body: (root: string, env: Record<string, string>) => Promise<void>) {
  await withNxFixture('nx-failure-lifecycle-', async ({ workspace, socketDirectory }) => {
    await mkdir(join(workspace, '.nx/workspace-data'), { recursive: true });
    await writeFile(join(workspace, 'package.json'), '{"name":"failure-lifecycle-fixture","private":true}\n');
    await writeFile(join(workspace, 'nx.json'), '{}\n');
    await body(workspace, fixtureNxEnv(workspace, socketDirectory));
  });
}

function run(root: string, env: Record<string, string>, program: string) {
  const child = spawnSync('node', ['-e', `${setup}\n${program}`], { cwd: root, env, encoding: 'utf8' });
  if (child.error) throw child.error;
  expect(child.signal, child.stdout + child.stderr).toBeNull();
  return child;
}

async function expectStoredFailure(root: string) {
  const stored: unknown = JSON.parse(await readFile(join(root, 'stored-run.json'), 'utf8'));
  expect(stored).toMatchObject([
    {
      tasks: [
        { taskId: 'p:unstarted', status: 1, endTime: '1970-01-01T00:00:00.125Z' },
        { taskId: 'p:hashed-unstarted', status: 1 },
        { taskId: 'p:unhashed-started', status: 1 },
        { taskId: 'p:supplied', status: 0 },
        { taskId: 'p:notified', status: 0 },
      ],
    },
  ]);
}

it('completes Store plus native task history without fabricating hash/start, retaining valid history', async () => {
  await fixture(async (root, env) => {
    const child = run(
      root,
      env,
      `
const { TaskDetails } = nx('native/index.js');
const { getDbConnection } = nx('utils/db-connection.js');
new TaskDetails(getDbConnection()).recordTaskDetails([
  { hash: 'supplied', project: 'p', target: 'supplied' },
  { hash: 'notified', project: 'p', target: 'notified' },
]);
const { getTasksHistoryLifeCycle } = nx('tasks-runner/life-cycles/task-history-life-cycle.js');
const history = getTasksHistoryLifeCycle();
complete(history).then(() => assert.deepEqual(history.startTimings, {}));
`,
    );
    expect(child.status, child.stdout + child.stderr).toBe(0);
    await expectStoredFailure(root);
    const files = (await readdir(join(root, '.nx/workspace-data'))).filter((file) => file.endsWith('.db'));
    expect(files).toHaveLength(1);
    const file = files[0];
    if (file === undefined) throw new Error('no fixture Nx history database');
    const database = new Database(join(root, '.nx/workspace-data', file), { readwrite: true });
    try {
      expect(
        database
          .query<{ hash: string; status: string; code: number }, []>(
            'SELECT hash, status, code FROM task_history ORDER BY hash',
          )
          .all(),
      ).toEqual([
        { hash: 'notified', status: 'success', code: 0 },
        { hash: 'supplied', status: 'success', code: 0 },
      ]);
    } finally {
      database.close();
    }
  });
});

it('completes Store plus legacy task history without fabricating hash/start, retaining valid history', async () => {
  await fixture(async (root, env) => {
    const child = run(
      root,
      env,
      `
const { LegacyTaskHistoryLifeCycle } = nx('tasks-runner/life-cycles/task-history-life-cycle-old.js');
const history = new LegacyTaskHistoryLifeCycle();
complete(history).then(() => assert.deepEqual(history.startTimings, {}));
`,
    );
    expect(child.status, child.stdout + child.stderr).toBe(0);
    await expectStoredFailure(root);
    const lines = (await readFile(join(root, '.nx/workspace-data/task-history.csv'), 'utf8')).trim().split('\n');
    expect(lines).toHaveLength(3);
    expect(lines[0]).toBe('project,target,configuration,hash,code,status,start,end');
    expect(lines[1]).toBe('p,supplied,,supplied,0,success,0,125');
    expect(lines[2]).toMatch(/^p,notified,,notified,0,success,\d+,\d+$/);
  });
});

it('completes Store plus timing diagnostics with null duration for an unstarted task', async () => {
  await fixture(async (root, env) => {
    const child = run(
      root,
      env,
      `
const { TaskTimingsLifeCycle } = nx('tasks-runner/life-cycles/task-timings-life-cycle.js');
complete(new TaskTimingsLifeCycle());
`,
    );
    expect(child.status, child.stdout + child.stderr).toBe(0);
    await expectStoredFailure(root);
    expect(child.stdout).toContain('"p:unstarted": null');
    expect(child.stdout).toContain('"p:hashed-unstarted": null');
    expect(child.stdout).toContain('"p:unhashed-started": 25');
    expect(child.stdout).toContain('"p:supplied": 125');
  });
});

it('completes Store plus profiling without inventing an executed interval', async () => {
  await fixture(async (root, env) => {
    const child = run(
      root,
      env,
      `
const { TaskProfilingLifeCycle } = nx('tasks-runner/life-cycles/task-profiling-life-cycle.js');
complete(new TaskProfilingLifeCycle('profile.json'));
`,
    );
    expect(child.status, child.stdout + child.stderr).toBe(0);
    await expectStoredFailure(root);
    const profile: unknown = JSON.parse(await readFile(join(root, 'profile.json'), 'utf8'));
    expect(profile).toEqual([
      expect.objectContaining({ ph: 'M', name: 'thread_name' }),
      expect.objectContaining({ ph: 'X', name: 'p:unhashed-started', ts: 100000, dur: 25000 }),
      expect.objectContaining({ ph: 'X', name: 'p:supplied', ts: 0, dur: 125000 }),
      expect.objectContaining({ ph: 'X', name: 'p:notified', dur: expect.any(Number) }),
    ]);
  });
});

it('does not suppress a genuine history database failure for a hashed started task', async () => {
  await fixture(async (root, env) => {
    const child = run(
      root,
      env,
      `
const { getTasksHistoryLifeCycle } = nx('tasks-runner/life-cycles/task-history-life-cycle.js');
const history = getTasksHistoryLifeCycle();
history.endTasks([{ task: supplied, status: 'success', code: 0 }])
  .then(() => history.endCommand());
`,
    );
    expect(child.status).toBe(1);
    expect(child.stderr).toContain('FOREIGN KEY');
    expect(child.stderr).not.toContain('Missing field');
  });
});
