import { expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { mkdir, symlink, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fixtureNxEnv, withNxFixture } from './__tests__/fixture-nx-env.js';

const repositoryRoot = join(import.meta.dir, '../../..');

it('fails typecheck through tsc-js on lib errors, typecheck-tests on test errors, and real typecheck on emit-less projects', async () => {
  await withNxFixture('nx-typecheck-once-', async ({ workspace, socketDirectory }) => {
    await symlink(join(repositoryRoot, 'node_modules'), join(workspace, 'node_modules'), 'dir');
    await writeFile(
      join(workspace, 'package.json'),
      JSON.stringify({ name: 'typecheck-once-fixture', private: true, workspaces: ['packages/*'] }),
    );
    await writeFile(
      join(workspace, 'nx.json'),
      JSON.stringify({
        plugins: ['@smoothbricks/nx-plugin'],
        namedInputs: {
          default: ['{projectRoot}/**/*'],
          production: ['default', '!{projectRoot}/**/*.test.ts'],
        },
      }),
    );
    await writeFile(
      join(workspace, 'tsconfig.base.json'),
      JSON.stringify({
        compilerOptions: {
          target: 'ES2022',
          module: 'NodeNext',
          moduleResolution: 'NodeNext',
          strict: true,
          skipLibCheck: true,
          composite: true,
        },
      }),
    );

    const runNx = (...args: string[]) => {
      const child = spawnSync('bun', [join(repositoryRoot, 'node_modules/.bin/nx'), ...args], {
        cwd: workspace,
        env: fixtureNxEnv(workspace, socketDirectory),
        encoding: 'utf8',
      });
      if (child.error) throw child.error;
      return { status: child.status, output: child.stdout + child.stderr };
    };

    // Standard lib+test projects: `typecheck` delegates to `tsc-js`; test files are checked by `typecheck-tests`.
    // lib-pkg carries a lib type error, test-pkg a clean lib and a test-file type error.
    const writeLibPackage = async (name: string, librarySource: string, testSource: string): Promise<void> => {
      await mkdir(join(workspace, `packages/${name}/src`), { recursive: true });
      await writeFile(join(workspace, `packages/${name}/package.json`), JSON.stringify({ name, type: 'module' }));
      await writeFile(
        join(workspace, `packages/${name}/tsconfig.lib.json`),
        JSON.stringify({
          extends: '../../tsconfig.base.json',
          compilerOptions: { rootDir: 'src', outDir: 'dist', types: [] },
          include: ['src/**/*.ts'],
          exclude: ['src/**/*.test.ts', 'src/test-suite-tracer.ts'],
        }),
      );
      await writeFile(
        join(workspace, `packages/${name}/tsconfig.test.json`),
        JSON.stringify({
          extends: '../../tsconfig.base.json',
          compilerOptions: { composite: false, declaration: false, declarationMap: false, noEmit: true, types: [] },
          include: ['src/**/*.test.ts', 'src/test-suite-tracer.ts'],
          references: [{ path: './tsconfig.lib.json' }],
        }),
      );
      await writeFile(join(workspace, `packages/${name}/src/index.ts`), librarySource);
      await writeFile(join(workspace, `packages/${name}/src/index.test.ts`), testSource);
      await writeFile(join(workspace, `packages/${name}/src/test-suite-tracer.ts`), 'export const tracer = 1;\n');
    };
    await writeLibPackage(
      'lib-pkg',
      "export const broken: number = 'bad';\n",
      "import { broken } from './index.js';\nexport const tested: number = broken;\n",
    );
    await writeLibPackage(
      'test-pkg',
      'export const fine: number = 1;\n',
      "import { fine } from './index.js';\nexport const tested: string = fine;\n",
    );

    // 2. Emit-less project (no tsconfig.lib.json, declares its own typecheck) and custom-emit project (declares tsc-js).
    await mkdir(join(workspace, 'packages/emitless/src'), { recursive: true });
    await writeFile(
      join(workspace, 'packages/emitless/package.json'),
      JSON.stringify({
        name: 'emitless',
        type: 'module',
        nx: {
          targets: {
            typecheck: {
              executor: 'nx:run-commands',
              options: {
                command: '"$SMOO_TYPECHECK_TSC" -p tsconfig.json --noEmit',
                cwd: '{projectRoot}',
              },
            },
          },
        },
      }),
    );
    await writeFile(
      join(workspace, 'packages/emitless/tsconfig.json'),
      JSON.stringify({
        extends: '../../tsconfig.base.json',
        compilerOptions: { composite: false, noEmit: true, types: [] },
        include: ['src/**/*.ts'],
        exclude: ['src/**/*.test.ts'],
      }),
    );
    await writeFile(
      join(workspace, 'packages/emitless/tsconfig.test.json'),
      JSON.stringify({
        extends: '../../tsconfig.base.json',
        compilerOptions: { composite: false, declaration: false, declarationMap: false, noEmit: true, types: [] },
        include: ['src/**/*.test.ts'],
      }),
    );
    await writeFile(join(workspace, 'packages/emitless/src/index.ts'), "export const bad: number = 'nope';\n");
    await writeFile(join(workspace, 'packages/emitless/src/index.test.ts'), 'export const ok = 1;\n');

    await mkdir(join(workspace, 'packages/custom-emit/src'), { recursive: true });
    await writeFile(
      join(workspace, 'packages/custom-emit/package.json'),
      JSON.stringify({
        name: 'custom-emit',
        type: 'module',
        nx: {
          targets: {
            'tsc-js': { executor: 'nx:noop' },
          },
        },
      }),
    );
    await writeFile(
      join(workspace, 'packages/custom-emit/tsconfig.lib.json'),
      JSON.stringify({
        extends: '../../tsconfig.base.json',
        compilerOptions: { rootDir: 'src', outDir: 'dist', types: [] },
        include: ['src/**/*.ts'],
      }),
    );
    await writeFile(join(workspace, 'packages/custom-emit/src/index.ts'), "export const bad: number = 'nope';\n");

    // Each Nx process pays a whole graph build and plugin load, so the cases share two processes: the caller that runs
    // `typecheck` alone, and one run-many over every other case (no task of it depends on another's failure).
    // A lib type error fails `nx run lib-pkg:typecheck` through `lib-pkg:tsc-js`.
    const libError = runNx('run', 'lib-pkg:typecheck');
    expect(libError.status, libError.output).not.toBe(0);
    expect(libError.output).toContain('lib-pkg:tsc-js');
    expect(libError.output).toContain('TS2322');

    // test-pkg's `typecheck` passes over its clean lib while its `typecheck-tests` fails on the test file; the emit-less
    // project's own typecheck fails; and a project that declares its own tsc-js keeps the real `typecheck`, which fails.
    const cases = runNx(
      'run-many',
      '-t',
      'typecheck',
      'typecheck-tests',
      '-p',
      'test-pkg',
      'emitless',
      'custom-emit',
      '--nxBail=false',
      '--outputStyle=static',
    );
    expect(cases.status, cases.output).not.toBe(0);
    const failed = new Set(
      (cases.output.split('Failed tasks:')[1] ?? '')
        .split('\n')
        .map((line) => /^\s*-\s+(\S+)/.exec(line)?.[1])
        .filter((task): task is string => task !== undefined),
    );
    expect([...failed].sort(), cases.output).toEqual(
      ['custom-emit:typecheck', 'emitless:typecheck', 'test-pkg:typecheck-tests'].sort(),
    );
    expect(cases.output.match(/TS2322/g)?.length ?? 0, cases.output).toBeGreaterThanOrEqual(3);
  });
});
