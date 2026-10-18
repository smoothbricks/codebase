import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdir, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

/**
 * Guards the project-graph error hunks of patches/nx@23.2.1.patch (nrwl/nx#37103 carried, with cause chains) against
 * the actual Bun-installed Nx: a createNodes failure names itself without --verbose. Before, `nx run-many` printed
 * only "Failed to process project graph." and a consumer's CI could not say which plugin refused or why.
 */
const repositoryRoot = join(import.meta.dir, '../../..');
const nxEntry = join(repositoryRoot, 'node_modules/.bin/nx');

const FAILURE = 'fixture.rs names @smoothbricks/nx-plugin/dist/bin/smoo-nx-absent.js, which the plugin does not ship';
const CAUSE = 'fixture cause of the refusal';
const STACK_FRAME = /failure\.cjs:\d+:\d+/;

for (const command of [
  ['run-many', '-t', 'build'],
  ['show', 'projects', '--json'],
]) {
  for (const isolate of ['false', 'true']) {
    it(`names a createNodes failure and its cause for nx ${command[0]} (isolated plugins: ${isolate})`, async () => {
      await withNxFixture('nx-graph-errors-', async ({ workspace, socketDirectory }) => {
        await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
        await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'graph-errors', private: true }));
        await mkdir(join(workspace, 'packages/app'), { recursive: true });
        await writeFile(
          join(workspace, 'packages/app/package.json'),
          JSON.stringify({ name: 'app', scripts: { build: 'true' } }),
        );
        const plugin = join(workspace, 'failure.cjs');
        await writeFile(
          plugin,
          `exports.createNodes = ['**/package.json', () => {
  throw new Error(${JSON.stringify(FAILURE)}, { cause: new Error(${JSON.stringify(CAUSE)}) });
}];
`,
        );
        await writeFile(join(workspace, 'nx.json'), JSON.stringify({ plugins: [plugin] }));
        const run = (verbose: boolean) => {
          const env = { ...fixtureNxEnv(workspace, socketDirectory), NX_ISOLATE_PLUGINS: isolate };
          const ran = spawnSync('bun', [nxEntry, ...command], {
            cwd: workspace,
            env: verbose ? { ...env, NX_VERBOSE_LOGGING: 'true' } : env,
            encoding: 'utf8',
          });
          return { status: ran.status, output: `${ran.stdout}${ran.stderr}` };
        };

        const quiet = run(false);
        expect(quiet.status, quiet.output).toBe(1);
        expect(quiet.output).toContain('Failed to process project graph.');
        expect(quiet.output).toContain(FAILURE);
        expect(quiet.output).toContain(`Caused by: ${CAUSE}`);
        expect(quiet.output).toContain('Pass --verbose to see the stacktraces.');
        expect(quiet.output).not.toMatch(STACK_FRAME);

        // Verbose keeps the stack frames and the cause chain. Stack text is the runtime's own: under Bun an
        // in-process plugin error's stack (its cause's too) can read as a bare `Error`, so naming the messages is the
        // non-verbose body's job, asserted above for every command and isolation mode.
        const verbose = run(true);
        expect(verbose.status, verbose.output).toBe(1);
        expect(verbose.output).toContain('Caused by:');
        expect(verbose.output).toMatch(STACK_FRAME);
      });
    });
  }
}
