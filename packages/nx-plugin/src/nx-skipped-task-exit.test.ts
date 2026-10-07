import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdir, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

/**
 * Guards the exit-status hunk of `patches/nx@23.2.1.patch`. When a task fails, Nx skips the tasks that depend on it,
 * and tells no life cycle about a skipped task, so the results `nx run` reads its exit status from had none for it.
 * `didCommandComplete` takes a discrete task without a result for one the run never finished, so an ordinary failure
 * exited 130, the status of an interrupted run, and a gate could not tell a failed test from a run someone stopped.
 *
 * The run now exits 1, and still lists the skipped task as not run. Drop the hunk, and keep this test, once the Nx
 * version in use exits 1 here.
 */

const repositoryRoot = join(import.meta.dir, '../../..');
const nxEntry = join(repositoryRoot, 'node_modules', '.bin', 'nx');

it('exits 1, not 130, when a failed task leaves a task that depends on it unrun', async () => {
  await withNxFixture('nx-skipped-exit-', async ({ workspace }) => {
    await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await writeFile(join(workspace, 'nx.json'), JSON.stringify({}));
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

    const ran = spawnSync('bun', [nxEntry, 'run', 'p:after', '--outputStyle=static'], {
      cwd: workspace,
      env: fixtureNxEnv(workspace),
      encoding: 'utf8',
    });
    const output = `${ran.stdout}${ran.stderr}`;
    expect(ran.status, output).toBe(1);
    expect(output).toContain('Tasks not run because their dependencies failed or --nx-bail=true:\n\n- p:after');
    expect(output).toContain('Failed tasks:\n\n- p:fail');
  });
}, 60_000);
