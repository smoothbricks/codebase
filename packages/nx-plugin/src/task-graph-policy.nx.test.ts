import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdir, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { TargetConfiguration } from 'nx/src/devkit-exports.js';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

const repositoryRoot = join(import.meta.dir, '../../..');
it('nx show project refuses both graph defects through the installed plugin', async () => {
  await withNxFixture('nx-task-rules-', async ({ workspace, socketDirectory }) => {
    await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await mkdir(join(workspace, 'app'));
    await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'graph-rules', private: true }));
    await writeFile(join(workspace, 'nx.json'), JSON.stringify({ plugins: ['@smoothbricks/nx-plugin'] }));
    const show = () => {
      const child = spawnSync(
        'bun',
        [join(repositoryRoot, 'node_modules/.bin/nx'), 'show', 'project', 'app', '--json'],
        {
          cwd: workspace,
          env: fixtureNxEnv(workspace, socketDirectory),
          encoding: 'utf8',
        },
      );
      if (child.error) throw child.error;
      return { status: child.status, output: child.stdout + child.stderr };
    };
    const writeTargets = (targets: Record<string, TargetConfiguration>) =>
      writeFile(join(workspace, 'app/project.json'), JSON.stringify({ name: 'app', root: 'app', targets }));
    await writeTargets({
      build: { executor: 'nx:noop', outputs: [], dependsOn: ['prepare'] },
      prepare: { executor: 'nx:noop', outputs: [], dependsOn: ['contract-test'] },
      'contract-test': { executor: 'nx:noop', outputs: [] },
    });
    const testEdge = show();
    expect(testEdge.status, testEdge.output).toBe(1);
    expect(testEdge.output).toContain('app:build: app:prepare -> app:contract-test');
    await writeTargets({
      bundle: {
        executor: 'nx:noop',
        outputs: ['{options.out}/'],
        options: { out: 'app/dist' },
        configurations: { production: { out: 'app/dist' } },
      },
    });
    const overlap = show();
    expect(overlap.status, overlap.output).toBe(1);
    expect(overlap.output).toContain('app:bundle: "app/dist" overlaps app:bundle:production: "app/dist"');
    await writeTargets({
      bundle: {
        executor: 'nx:noop',
        outputs: ['{options.out}/'],
        options: { out: 'app/dev' },
        configurations: { production: { out: 'app/dist' } },
      },
    });
    const valid = show();
    expect(valid.status, valid.output).toBe(0);
    expect(valid.output).toContain('app/dev');
  });
});
