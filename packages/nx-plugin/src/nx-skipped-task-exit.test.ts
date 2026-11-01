import { expect, it, onTestFinished } from 'bun:test';
import { spawn, spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdir, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { guardEvent } from './__tests__/counted-cargo.js';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';
import { whenChanged } from './__tests__/path-change.js';
import { publishFixtureSocketDirectory, retireFixtureSocketDirectory } from './testing.js';

/**
 * Guards a failed run's exit status, which Nx 24.0.3 settles in `tasks-runner/run-outcome.js` (this repository carried
 * it as a patch hunk on Nx 23.2.1). When a task fails, Nx skips the tasks that depend on it and tells no life cycle
 * about a skipped task, so the results `nx run` reads have none for it. `didCommandComplete` also reads the runner's
 * task statuses, where a skipped task is settled, so an ordinary failure exits 1, never 130, the status of an
 * interrupted run; a gate must tell a failed test from a run someone stopped.
 *
 * The run lists the skipped task as not run. The task results everything else sees stay Nx's own: the plugins'
 * `postTasksExecution` hooks get no result for a skipped task, so a caller that replays Nx's skip rule over those results
 * keeps counting it once. With `--nx-bail=true` Nx starts nothing after a failure, settles every task it did not start
 * as skipped, and lets a running task finish; a real SIGINT stops the running task and exits 130.
 */

const repositoryRoot = join(import.meta.dir, '../../..');
const nxEntry = join(repositoryRoot, 'node_modules', '.bin', 'nx');
// The fixture's failing and held tasks wait through the same waker, run by Bun from this checkout.
const pathChange = join(import.meta.dir, '__tests__', 'path-change.ts');

/** A local plugin whose `postTasksExecution` hook writes the task results it was handed. */
const resultsHook = `
const { writeFileSync } = require('node:fs');
const { join } = require('node:path');
exports.name = 'results-hook';
exports.postTasksExecution = async (options, context) => {
  const results = Object.fromEntries(Object.entries(context.taskResults).map(([id, r]) => [id, r.status]));
  writeFileSync(join(context.workspaceRoot, 'task-results.json'), JSON.stringify(results));
};
`;

const exitLabel = 'exits 1, not 130, when a failed task leaves a task that depends on it unrun';
const interruptLabel = 'distinguishes parallel failure bailout from a real interrupt with pending tasks';

/** The held task: it listens, writes `ready` to its workspace, and ends once `p:fail` has failed or is interrupted. */
const holdScript = `import { createServer } from 'node:net';
import { existsSync, writeFileSync } from 'node:fs';
import { whenChanged } from ${JSON.stringify(pathChange)};
const server = createServer().listen(0, '127.0.0.1', () => {
  writeFileSync('ready', '');
  console.log('nx-interrupt-ready');
});
await whenChanged('.', () => existsSync('failed'));
server.close();
`;

/** Fails once the `release` file exists, marking `failed` first; a `release` that never appears leaves it pending. */
function failScript(release: string): string {
  return `import { existsSync, writeFileSync } from 'node:fs';
import { whenChanged } from ${JSON.stringify(pathChange)};
await whenChanged('.', () => existsSync(${JSON.stringify(release)}));
writeFileSync('failed', '');
process.exit(1);
`;
}

/** Clears what an earlier step left in its workspace, so its assertions read only its own run. */
async function clearStep(workspace: string): Promise<void> {
  await rm(join(workspace, 'task-results.json'), { force: true });
  await rm(join(workspace, 'ready'), { force: true });
  await rm(join(workspace, 'failed'), { force: true });
}

/** A fixture workspace's `node_modules` link, Nx config, results hook and package. */
async function writeWorkspace(workspace: string, name: string): Promise<void> {
  await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
  await writeFile(join(workspace, 'nx.json'), JSON.stringify({ plugins: ['./results-hook.js'] }));
  await writeFile(join(workspace, 'results-hook.js'), resultsHook);
  await writeFile(join(workspace, 'package.json'), JSON.stringify({ name, private: true }));
}

/**
 * One step of the parallel walk: `run-many` of `held` and `after` with `--nx-bail=true`. Without an interrupt the step
 * bails out on the failing task, lets `held` finish and exits 1; with one it sends SIGINT once `held` is listening,
 * and must exit 130.
 */
async function runParallelStep(workspace: string, socketDirectory: string, interrupt: boolean): Promise<void> {
  const step = `${interruptLabel}, interrupt=${interrupt}`;
  await clearStep(workspace);
  const stopReady = new AbortController();
  const ready = interrupt ? whenChanged(workspace, () => existsSync(join(workspace, 'ready')), stopReady.signal) : null;
  const child = spawn(
    'bun',
    [nxEntry, 'run-many', '-t', 'held,after', '-p', 'p', '--parallel=2', '--nx-bail=true', '--outputStyle=static'],
    {
      cwd: workspace,
      env: fixtureNxEnv(workspace, socketDirectory),
      detached: process.platform !== 'win32',
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  );
  let output = '';
  const ended = Promise.withResolvers<number | null>();
  child.once('error', (error) => {
    ended.reject(error);
  });
  child.once('close', ended.resolve);
  child.stdout.setEncoding('utf8').on('data', (text: string) => {
    output += text;
  });
  child.stderr.setEncoding('utf8').on('data', (text: string) => {
    output += text;
  });
  const describe = () => `${step}\npid ${child.pid}, exit ${child.exitCode}, signal ${child.signalCode}\n${output}`;
  const retire = async () => {
    stopReady.abort();
    try {
      if (child.pid !== undefined && child.exitCode === null && child.signalCode === null) {
        try {
          process.kill(process.platform === 'win32' ? child.pid : -child.pid, 'SIGKILL');
        } catch (error) {
          if (!(error instanceof Error && 'code' in error && error.code === 'ESRCH')) throw error;
        }
      }
      await ended.promise;
    } finally {
      // Nx and its tasks are reaped first; a failure of the readiness wait is still reported after.
      await ready?.catch((error: unknown) => {
        if (error !== stopReady.signal.reason) throw error;
      });
    }
  };
  onTestFinished(retire);
  try {
    if (ready !== null) {
      await guardEvent(
        Promise.race([
          ready,
          ended.promise.then((code) => {
            throw new Error(`Nx exited ${code} before its task was ready\n${output}`);
          }),
        ]),
        `${step}: Nx held task readiness`,
        describe,
      );
      expect(child.kill('SIGINT')).toBe(true);
    }
    const code = await guardEvent(ended.promise, `${step}: Nx bailout or interrupt to exit`, describe);
    expect(code, `${step}\n${output}`).toBe(interrupt ? 130 : 1);
    const results = JSON.parse(await readFile(join(workspace, 'task-results.json'), 'utf8'));
    if (interrupt) {
      expect(results, step).toMatchObject({ 'p:held': 'stopped' });
    } else {
      expect(results, step).toEqual({ 'p:held': 'success', 'p:fail': 'failure' });
      expect(output, step).toContain(
        'Tasks not run because their dependencies failed, --nx-bail=true, or their batch ended before reaching them:\n\n- p:after',
      );
    }
  } finally {
    await retire();
  }
}

it('exits 1 for a failed task and a parallel bailout, 130 only for a real interrupt', async () => {
  await withNxFixture(
    'nx-skipped-exit-',
    async ({ root, workspace, socketDirectory }) => {
      // Walks 1 and 2: `p:after` is skipped behind the failed `p:fail`, with and without --nx-bail.
      await writeWorkspace(workspace, 'skipped-exit');
      await mkdir(join(workspace, 'p'));
      await writeFile(
        join(workspace, 'p', 'project.json'),
        JSON.stringify({
          name: 'p',
          targets: {
            fail: { executor: 'nx:run-commands', options: { command: 'exit 1' } },
            after: { executor: 'nx:run-commands', dependsOn: ['fail'], options: { command: 'echo after' } },
          },
        }),
      );
      for (const bail of [false, true]) {
        const step = `${exitLabel}, --nx-bail=${bail}`;
        await clearStep(workspace);
        const ran = spawnSync('bun', [nxEntry, 'run', 'p:after', `--nx-bail=${bail}`, '--outputStyle=static'], {
          cwd: workspace,
          env: fixtureNxEnv(workspace, socketDirectory),
          encoding: 'utf8',
        });
        const output = `${ran.stdout}${ran.stderr}`;
        expect(ran.status, `${step}\n${output}`).toBe(1);
        expect(output, step).toContain(
          'Tasks not run because their dependencies failed, --nx-bail=true, or their batch ended before reaching them:\n\n- p:after',
        );
        expect(output, step).toContain('Failed tasks:\n\n- p:fail');
        // Nx's own results: the failed task, and nothing for the task it skipped or bailed before.
        expect(JSON.parse(await readFile(join(workspace, 'task-results.json'), 'utf8')), step).toEqual({
          'p:fail': 'failure',
        });
      }

      // Walks 3 and 4 run in a sibling workspace: this walk's `p:fail` waits for a release, while the first walk's
      // `p:fail` exits at once, so the two `p` projects cannot share a workspace. The SIGINT walk runs last, since it
      // destroys the pending state of its workspace.
      const parallel = join(root, 'parallel');
      await mkdir(join(parallel, 'p'), { recursive: true });
      await writeWorkspace(parallel, 'bail-interrupt');
      await writeFile(join(parallel, 'hold.ts'), holdScript);
      await writeFile(
        join(parallel, 'p', 'project.json'),
        JSON.stringify({
          name: 'p',
          targets: {
            held: { executor: 'nx:run-commands', options: { command: 'bun hold.ts' } },
            fail: { executor: 'nx:run-commands', options: { command: 'bun fail.ts' } },
            after: { executor: 'nx:run-commands', dependsOn: ['fail'], options: { command: 'echo after' } },
          },
        }),
      );
      // withNxFixture publishes and retires only its own workspace's socket alias. This workspace runs no daemon
      // (NX_DAEMON=false), so retiring it is its alias alone; removing the root takes the rest.
      const parallelSocket = await publishFixtureSocketDirectory(parallel);
      try {
        for (const interrupt of [false, true]) {
          await writeFile(join(parallel, 'fail.ts'), failScript(interrupt ? 'never-released' : 'ready'));
          await runParallelStep(parallel, parallelSocket, interrupt);
        }
      } finally {
        await retireFixtureSocketDirectory(parallel);
      }
    },
    'exit',
  );
});
