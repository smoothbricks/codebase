import { expect, it } from 'bun:test';
import { execFileSync, spawn } from 'node:child_process';
import { mkdir, readFile, realpath, symlink, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { resolveFromWorkspace } from 'nx/src/utils/installation-directory.js';
import { readModulePackageJson } from 'nx/src/utils/package-json.js';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

/**
 * Guards the workspace-resolution hunk of `patches/nx@23.2.1.patch`, the local build of nrwl/nx#37272. The
 * fixture links the installed Nx and this repository's TypeScript into a workspace, so the graph is computed by the
 * Nx this repository runs. Nx 23.2.1 loads `typescript` with a plain `require` from its own location; where that
 * location cannot see TypeScript, its dependency analysis skips every TypeScript import and the graph loses each
 * edge one project's source import makes to another, without a warning (12 of this repository's 44 edges). Bun's
 * project-local layout hoists a TypeScript into the ancestry of Nx, so the graph alone no longer proves the fix: the
 * test also asserts that `resolveFromWorkspace` returns the workspace's `typescript`. Release version actions take
 * the same resolution; `packages/cli`'s release-version tests run them through it.
 *
 * Drop the hunk, and keep this test, once the Nx version in use contains that fix.
 */

const repositoryRoot = join(import.meta.dir, '../../..');

it('keeps the source-import edges of the installed Nx, resolving typescript from the workspace', async () => {
  const nx = await realpath(dirname(readModulePackageJson('nx', [repositoryRoot]).path));
  const typescript = dirname(readModulePackageJson('typescript', [repositoryRoot]).path);

  await withNxFixture('nx-store-resolution-', async ({ workspace: root, socketDirectory }) => {
    await mkdir(join(root, 'node_modules'));
    await symlink(nx, join(root, 'node_modules/nx'), 'dir');
    await symlink(typescript, join(root, 'node_modules/typescript'), 'dir');
    expect(await realpath(resolveFromWorkspace('typescript', root))).toBe(
      await realpath(createRequire(join(repositoryRoot, 'package.json')).resolve('typescript')),
    );
    await writeFile(join(root, '.gitignore'), 'node_modules\n.nx\n');
    await writeFile(join(root, 'package.json'), JSON.stringify({ name: 'store-resolution-fixture', private: true }));
    // The fixture's root package.json names no @nx/* package, so Nx analyzes source files only when told to.
    await writeFile(
      join(root, 'nx.json'),
      JSON.stringify({ pluginsConfig: { '@nx/js': { analyzeSourceFiles: true } } }),
    );
    await writeFile(
      join(root, 'tsconfig.base.json'),
      JSON.stringify({ compilerOptions: { baseUrl: '.', paths: { '@fixture/a': ['a/src/index.ts'] } } }),
    );
    for (const name of ['a', 'b']) {
      await mkdir(join(root, name, 'src'), { recursive: true });
      await writeFile(join(root, name, 'project.json'), JSON.stringify({ name, sourceRoot: `${name}/src` }));
    }
    await writeFile(join(root, 'a/src/index.ts'), 'export const a = 1;\n');
    // b depends on a only through this import: no package.json, no implicitDependencies.
    await writeFile(join(root, 'b/src/index.ts'), "import { a } from '@fixture/a';\nexport const b = a + 1;\n");
    execFileSync('git', ['init', '--quiet', root], { stdio: 'pipe' });

    const graphFile = join(root, 'graph.json');
    const child = spawn('node', [join(nx, 'dist/bin/nx.js'), 'graph', `--file=${graphFile}`], {
      cwd: root,
      env: fixtureNxEnv(root, socketDirectory),
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let output = '';
    child.stdout?.setEncoding('utf8').on('data', (text: string) => {
      output += text;
    });
    child.stderr?.setEncoding('utf8').on('data', (text: string) => {
      output += text;
    });
    // The test waits for what it needs, the command's close, however long a loaded host makes the real Nx take: a
    // deadline here measured the host, not the resolution (a 15 s guard failed a land gate at load 20 with the command
    // still running). A command that never closes is the runner's idle-hang backstop's to report.
    const code = await new Promise<number | null>((resolve, reject) => {
      child.once('error', reject);
      child.once('close', resolve);
    });
    expect(code, output).toBe(0);
    expect(JSON.parse(await readFile(graphFile, 'utf8'))).toMatchObject({
      graph: { dependencies: { b: [{ source: 'b', target: 'a', type: 'static' }] } },
    });
  });
});
