import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdir, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

/**
 * Guards the incomplete-run summary hunk of `patches/nx@23.2.1.patch`. The default tasks runner ends its life cycle in
 * a `finally`, so the static summary also prints when the orchestrator throws part way through a run. That summary
 * printed "Successfully ran" whenever no task had failed or stopped, and counted every task the run never reported
 * as "skipped" under it: a run whose daemon socket died in the middle claimed success over the tasks it never ran.
 *
 * The orchestrator reports every task it finishes except one it skips, and it skips a task only behind a failed or
 * stopped one, so with neither, an unreported task is one the run did not finish. The summary now says the run did
 * not complete and names those tasks. Here the orchestrator throws while it hashes the second task, after the first
 * finished: the second's inputs name an external dependency the workspace does not have, and a task whose inputs
 * read its dependencies' outputs is hashed only once they finish. Drop the hunk, and keep this test, once the Nx
 * version in use prints no success for a run that threw.
 */

const repositoryRoot = join(import.meta.dir, '../../..');
const nxEntry = join(repositoryRoot, 'node_modules', '.bin', 'nx');

it('does not report success for a run the orchestrator abandoned part way through', async () => {
  await withNxFixture('nx-incomplete-run-', async ({ workspace }) => {
    await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await writeFile(join(workspace, 'nx.json'), JSON.stringify({}));
    await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'incomplete-run', private: true }));
    await mkdir(join(workspace, 'p'));
    await writeFile(
      join(workspace, 'p', 'project.json'),
      JSON.stringify({
        name: 'p',
        targets: {
          first: {
            executor: 'nx:run-commands',
            outputs: ['{workspaceRoot}/dist/first'],
            options: { command: 'mkdir -p dist && echo first > dist/first' },
          },
          second: {
            executor: 'nx:run-commands',
            dependsOn: ['first'],
            inputs: [{ dependentTasksOutputFiles: '**/*' }, { externalDependencies: ['absent-package'] }],
            options: { command: 'echo second' },
          },
        },
      }),
    );

    // `nx run` ends in the static run-one summary, `nx run-many` in the run-many one: the field incident's.
    for (const command of [
      ['run', 'p:second'],
      ['run-many', '-t', 'second', '-p', 'p'],
    ]) {
      const ran = spawnSync('bun', [nxEntry, ...command, '--outputStyle=static'], {
        cwd: workspace,
        env: fixtureNxEnv(workspace),
        encoding: 'utf8',
      });
      const output = `nx ${command.join(' ')}\n${ran.stdout}${ran.stderr}`;
      expect(ran.status, output).toBe(1);
      expect(output).toContain("The externalDependency 'absent-package' for 'p:second' could not be found");
      // p:first finished, from the cache on the second command, before the throw.
      expect(output).toContain('nx run p:first');
      expect(output).not.toContain('Successfully ran');
      expect(output).toContain('Running target second for project p and 1 task it depends on did not complete');
      expect(output).toContain('Tasks the run ended without finishing:\n\n- p:second');
    }
  });
}, 60_000);
