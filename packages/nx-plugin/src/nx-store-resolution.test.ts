import { expect, it } from 'bun:test';
import { execFileSync, spawn } from 'node:child_process';
import { mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { guardEvent } from './__tests__/counted-cargo.js';
import { fixtureNxEnv } from './__tests__/fixture-nx-env.js';

/**
 * Guards the workspace-resolution hunk of `patches/nx@23.2.1.patch`, the local build of nrwl/nx#37272. The
 * repository installs Nx from an immutable tarball, so Bun links it from its global store
 * (`~/.bun/install/cache/links`), next to nothing but Nx's own dependencies. Nx 23.2.1 loads `typescript` with a
 * plain `require`, which from there finds nothing: its dependency analysis then skips every TypeScript import and
 * the graph loses each edge one project's source import makes to another, without a warning (12 of this
 * repository's 44 edges). The patched Nx resolves `typescript` from the workspace first. Release version actions
 * take the same resolution; `packages/cli`'s release-version tests run them through it.
 *
 * Drop the hunk, and keep this test, once the Nx version in use contains that fix.
 */

const repositoryRoot = join(import.meta.dir, '../../..');

it('keeps the source-import edges when Nx is linked from the global store', async () => {
  const nx = await realpath(join(repositoryRoot, 'node_modules/nx'));
  // The regression only exists for an Nx linked from Bun's store, outside this checkout (wherever the install
  // cache lives on this host); a project-local Nx resolves typescript by walking up to the repository's
  // node_modules, and this test would prove nothing.
  const checkout = await realpath(repositoryRoot);
  expect(nx.startsWith(`${checkout}/`), `${nx} is inside the checkout`).toBe(false);

  const root = await realpath(await mkdtemp(join(tmpdir(), 'nx-store-resolution-')));
  try {
    await symlink(join(repositoryRoot, 'node_modules'), join(root, 'node_modules'), 'dir');
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
    const child = spawn('node', [join(repositoryRoot, 'node_modules/.bin/nx'), 'graph', `--file=${graphFile}`], {
      cwd: root,
      env: { ...fixtureNxEnv(root), NX_DAEMON: 'false' },
      detached: process.platform !== 'win32',
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let output = '';
    child.stdout?.setEncoding('utf8').on('data', (text: string) => {
      output += text;
    });
    child.stderr?.setEncoding('utf8').on('data', (text: string) => {
      output += text;
    });
    const { promise: ended, resolve, reject } = Promise.withResolvers<number | null>();
    child.once('error', reject);
    child.once('close', resolve);
    let code: number | null;
    try {
      code = await guardEvent(
        ended,
        'nx graph to close',
        () => `pid ${child.pid}, exit ${child.exitCode}, signal ${child.signalCode}\n${output}`,
      );
    } catch (error) {
      if (child.pid !== undefined && child.exitCode === null && child.signalCode === null) {
        try {
          process.kill(process.platform === 'win32' ? child.pid : -child.pid, 'SIGKILL');
        } catch (killError) {
          if (!(killError instanceof Error && 'code' in killError && killError.code === 'ESRCH')) {
            throw new AggregateError([error, killError], 'could not retire the failed fixture Nx command');
          }
        }
      }
      await ended.catch(() => undefined);
      throw error;
    }
    expect(code, output).toBe(0);
    expect(JSON.parse(await readFile(graphFile, 'utf8'))).toMatchObject({
      graph: { dependencies: { b: [{ source: 'b', target: 'a', type: 'static' }] } },
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
