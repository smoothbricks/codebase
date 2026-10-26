import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdir, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { localPluginPath, pluginsAffectedByChanges } from 'nx/src/project-graph/plugins/local-plugin-source.js';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

/** Guards the plugin-load aggregation hunk of patches/nx@23.2.1.patch against the actual Bun-installed Nx. */
const repositoryRoot = join(import.meta.dir, '../../..');
const nxEntry = join(repositoryRoot, 'node_modules/.bin/nx');

for (const fixture of [
  { changed: ['a/entry.cjs'], expected: ['a'] },
  { changed: ['shared/helper.mjs'], expected: ['a', 'b'] },
  { changed: ['b/lazy.ts'], expected: ['b'] },
  { changed: ['deleted.json'], expected: ['a'] },
  { changed: ['unimported.ts', 'node_modules/library/index.js'], expected: [] },
  { changed: [], expected: [] },
]) {
  it(`invalidates exactly the local plugin closures intersecting ${JSON.stringify(fixture.changed)}`, () => {
    const closures = new Map([
      ['a', new Set(['a/entry.cjs', 'shared/helper.mjs', 'deleted.json'])],
      ['b', new Set(['b/entry.mjs', 'shared/helper.mjs', 'b/lazy.ts'])],
      ['unrelated', new Set(['other/plugin.ts'])],
    ]);
    expect(pluginsAffectedByChanges(closures, fixture.changed)).toEqual(fixture.expected);
  });
}

for (const fixture of [
  { file: '/checkout/tooling/plugin.ts', expected: 'tooling/plugin.ts' },
  { file: '/checkout/packages/member/index.js', expected: 'packages/member/index.js' },
  { file: '/checkout/node_modules/installed/index.js', expected: null },
  { file: '/checkout/packages/a/node_modules/installed/index.js', expected: null },
  { file: '/checkout-other/plugin.js', expected: null },
  { file: '/linked/plugin.js', expected: null },
]) {
  it(`classifies the resolved plugin module ${fixture.file}`, () => {
    expect(localPluginPath('/checkout', fixture.file)).toBe(fixture.expected);
  });
}

it('names the duplicate project and both roots when local plugin resolution fails', async () => {
  await withNxFixture('nx-plugin-duplicate-', async ({ workspace, socketDirectory }) => {
    await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'plugin-duplicate', private: true }));
    await writeFile(join(workspace, 'nx.json'), JSON.stringify({ plugins: ['fixture-local-plugin'] }));
    await writeFile(
      join(workspace, 'tsconfig.base.json'),
      JSON.stringify({
        compilerOptions: { baseUrl: '.', paths: { 'fixture-local-plugin': ['packages/first/index.cjs'] } },
      }),
    );
    for (const root of ['packages/first', 'packages/second']) {
      await mkdir(join(workspace, root), { recursive: true });
      await writeFile(join(workspace, root, 'project.json'), JSON.stringify({ name: 'duplicate-project' }));
    }
    await writeFile(join(workspace, 'packages/first/index.cjs'), "throw new Error('fixture-plugin-executed');\n");

    const ran = spawnSync('bun', [nxEntry, 'show', 'projects', '--json'], {
      cwd: workspace,
      env: fixtureNxEnv(workspace, socketDirectory),
      encoding: 'utf8',
    });
    const output = `${ran.stdout}${ran.stderr}`;
    expect(ran.status, output).toBe(1);
    expect(output).toContain('Failed to load 1 Nx plugin(s)');
    expect(output).toContain('Could not load plugin fixture-local-plugin');
    expect(output).toContain('Failed to create project configurations');
    expect(output).toContain('The following projects are defined in multiple locations');
    expect(output).toContain('- duplicate-project:');
    expect(output).toContain('- packages/first');
    expect(output).toContain('- packages/second');
    expect(output).not.toContain('fixture-plugin-executed');
  });
});

for (const fixture of [
  {
    name: 'native errors',
    source: `
const shared = new Error('shared child');
throw new AggregateError([
  shared,
  new Error('second child', { cause: new Error('deep child') }),
  shared,
], 'outer aggregate', {
  cause: new Error('first cause', { cause: new Error('deep cause') }),
});
`,
    messages: ['outer aggregate', 'first cause', 'deep cause', 'shared child', 'second child', 'deep child'],
    repeated: 'shared child',
  },
  {
    name: 'serialized errors',
    source: `
throw {
  message: 'serialized failure',
  cause: { message: 'first cause', cause: { message: 'deep cause' } },
  errors: [
    { message: 'first child', cause: { message: 'leaf cause' } },
    { message: 'second child', errors: [{ message: 'deep child' }, 'string child'] },
    ['fixture.json', { message: 'tuple failure', cause: { message: 'tuple cause' } }],
  ],
};
`,
    messages: [
      'serialized failure',
      'first cause',
      'deep cause',
      'first child',
      'leaf cause',
      'second child',
      'deep child',
      'string child',
      'fixture.json',
      'tuple failure',
      'tuple cause',
    ],
    repeated: undefined,
  },
  {
    name: 'circular errors',
    source: `
const failure = new AggregateError([new Error('sibling child')], 'circular failure');
failure.cause = failure;
throw failure;
`,
    messages: ['circular failure', '[Circular error]', 'sibling child'],
    repeated: undefined,
  },
]) {
  it(`shows the complete nested plugin-load diagnostic for ${fixture.name}`, async () => {
    await withNxFixture('nx-plugin-causes-', async ({ workspace, socketDirectory }) => {
      await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
      await writeFile(join(workspace, 'package.json'), JSON.stringify({ name: 'plugin-causes', private: true }));
      const plugin = join(workspace, 'failure.cjs');
      await writeFile(plugin, fixture.source);
      await writeFile(join(workspace, 'nx.json'), JSON.stringify({ plugins: [plugin] }));
      const ran = spawnSync('bun', [nxEntry, 'show', 'projects', '--json'], {
        cwd: workspace,
        env: fixtureNxEnv(workspace, socketDirectory),
        encoding: 'utf8',
      });
      const output = `${ran.stdout}${ran.stderr}`;
      expect(ran.status, output).toBe(1);
      expect(output).toContain('Failed to load 1 Nx plugin(s)');
      for (const message of fixture.messages) expect(output).toContain(message);
      if (fixture.repeated !== undefined) expect(output.split(fixture.repeated).length - 1).toBe(2);
    });
  });
}
