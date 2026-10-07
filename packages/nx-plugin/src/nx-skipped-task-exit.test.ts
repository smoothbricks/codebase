import { expect, it, onTestFinished } from 'bun:test';
import { spawn, spawnSync } from 'node:child_process';
import { existsSync, watch } from 'node:fs';
import { mkdir, readFile, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { guardEvent } from './__tests__/counted-cargo.js';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

/**
 * Guards the exit-status hunk of `patches/nx@23.2.1.patch`. When a task fails, Nx skips the tasks that depend on it,
 * and tells no life cycle about a skipped task, so the results `nx run` reads its exit status from had none for it.
 * `didCommandComplete` takes a discrete task without a result for one the run never finished, so an ordinary failure
 * exited 130, the status of an interrupted run, and a gate could not tell a failed test from a run someone stopped.
 *
 * The run now exits 1, and still lists the skipped task as not run. The task results everything else sees stay Nx's
 * own: `invokeTasksRunner` and the plugins' `postTasksExecution` hooks get no result for a skipped task, as they never
 * did, so a caller that replays Nx's skip rule over those results keeps counting it once.
 * Bail leaves pending tasks absent and can stop active tasks during cleanup; the CLI uses the orchestrator's own
 * bailout and interruption state to keep ordinary failure at 1 and a real SIGINT at 130.
 * Drop the hunk, and keep this test, once the Nx version in use exits 1 for failure and 130 only for interruption.
 */

const repositoryRoot = join(import.meta.dir, '../../..');
const nxEntry = join(repositoryRoot, 'node_modules', '.bin', 'nx');

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

it('exits 1, not 130, when a failed task leaves a task that depends on it unrun', async () => {
  await withNxFixture('nx-skipped-exit-', async ({ workspace }) => {
    await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await writeFile(join(workspace, 'nx.json'), JSON.stringify({ plugins: ['./results-hook.js'] }));
    await writeFile(join(workspace, 'results-hook.js'), resultsHook);
    await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'skipped-exit', private: true }));
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
      const ran = spawnSync('bun', [nxEntry, 'run', 'p:after', `--nx-bail=${bail}`, '--outputStyle=static'], {
        cwd: workspace,
        env: fixtureNxEnv(workspace),
        encoding: 'utf8',
      });
      const output = `${ran.stdout}${ran.stderr}`;
      expect(ran.status, output).toBe(1);
      expect(output).toContain('Tasks not run because their dependencies failed or --nx-bail=true:\n\n- p:after');
      expect(output).toContain('Failed tasks:\n\n- p:fail');
      // Nx's own results: the failed task, and nothing for the task it skipped or bailed before.
      expect(JSON.parse(await readFile(join(workspace, 'task-results.json'), 'utf8'))).toEqual({ 'p:fail': 'failure' });
    }
  });
}, 60_000);

it('distinguishes parallel failure bailout from a real interrupt with pending tasks', async () => {
  for (const interrupt of [false, true]) {
    await withNxFixture('nx-bail-interrupt-', async ({ workspace }) => {
      await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
      await writeFile(join(workspace, 'nx.json'), JSON.stringify({ plugins: ['./results-hook.js'] }));
      await writeFile(join(workspace, 'results-hook.js'), resultsHook);
      await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'bail-interrupt', private: true }));
      await mkdir(join(workspace, 'p'));
      await writeFile(
        join(workspace, 'hold.cjs'),
        `const { createServer } = require('node:net');
const { writeFileSync } = require('node:fs');
createServer().listen(0, '127.0.0.1', () => {
  writeFileSync('ready', '');
  console.log('nx-interrupt-ready');
});
`,
      );
      await writeFile(
        join(workspace, 'fail.cjs'),
        `const { existsSync, watch } = require('node:fs');
const wanted = ${JSON.stringify(interrupt ? 'never-released' : 'ready')};
const watcher = watch('.', () => {
  if (existsSync(wanted)) { watcher.close(); process.exit(1); }
});
if (existsSync(wanted)) { watcher.close(); process.exit(1); }
`,
      );
      await writeFile(
        join(workspace, 'p/project.json'),
        JSON.stringify({
          name: 'p',
          targets: {
            held: { executor: 'nx:run-commands', options: { command: 'node hold.cjs' } },
            fail: { executor: 'nx:run-commands', options: { command: 'node fail.cjs' } },
            after: { executor: 'nx:run-commands', dependsOn: ['fail'], options: { command: 'echo after' } },
          },
        }),
      );
      const ready = Promise.withResolvers<void>();
      const readyWatcher = interrupt
        ? watch(workspace, () => {
            if (existsSync(join(workspace, 'ready'))) ready.resolve();
          })
        : undefined;
      const child = spawn(
        'bun',
        [nxEntry, 'run-many', '-t', 'held,after', '-p', 'p', '--parallel=2', '--nx-bail=true', '--outputStyle=static'],
        {
          cwd: workspace,
          env: fixtureNxEnv(workspace),
          detached: process.platform !== 'win32',
          stdio: ['ignore', 'pipe', 'pipe'],
        },
      );
      let output = '';
      const ended = Promise.withResolvers<number | null>();
      child.once('error', (error) => {
        if (interrupt) ready.reject(error);
        ended.reject(error);
      });
      child.once('close', ended.resolve);
      child.stdout.setEncoding('utf8').on('data', (text: string) => {
        output += text;
      });
      child.stderr.setEncoding('utf8').on('data', (text: string) => {
        output += text;
      });
      const describe = () => `pid ${child.pid}, exit ${child.exitCode}, signal ${child.signalCode}\n${output}`;
      const retire = async () => {
        readyWatcher?.close();
        if (child.pid !== undefined && child.exitCode === null && child.signalCode === null) {
          try {
            process.kill(process.platform === 'win32' ? child.pid : -child.pid, 'SIGKILL');
          } catch (error) {
            if (!(error instanceof Error && 'code' in error && error.code === 'ESRCH')) throw error;
          }
        }
        await ended.promise;
      };
      onTestFinished(retire);
      try {
        if (interrupt) {
          await guardEvent(
            Promise.race([
              ready.promise,
              ended.promise.then((code) => {
                throw new Error(`Nx exited ${code} before its task was ready\n${output}`);
              }),
            ]),
            'Nx held task readiness',
            describe,
          );
          expect(child.kill('SIGINT')).toBe(true);
        }
        const code = await guardEvent(ended.promise, 'Nx bailout or interrupt to exit', describe);
        expect(code, output).toBe(interrupt ? 130 : 1);
        const results = JSON.parse(await readFile(join(workspace, 'task-results.json'), 'utf8'));
        if (interrupt) {
          expect(results).toMatchObject({ 'p:held': 'stopped' });
        } else {
          expect(results).toEqual({ 'p:held': 'stopped', 'p:fail': 'failure' });
          expect(output).toContain('Tasks not run because their dependencies failed or --nx-bail=true:\n\n- p:after');
        }
      } finally {
        await retire();
      }
    });
  }
}, 60_000);
